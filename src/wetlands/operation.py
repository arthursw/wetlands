"""Application-neutral asynchronous operation primitives.

Operations are implemented with threads so synchronous applications do not need an
event loop.  The awaitable and async-event interfaces are adapters to an event loop
owned by the caller.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")
logger = logging.getLogger(__name__)


class OperationState(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        return self in {
            OperationState.COMPLETED,
            OperationState.FAILED,
            OperationState.CANCELED,
        }


class OperationEventKind(enum.Enum):
    STATE = "state"
    STEP = "step"
    OUTPUT = "output"
    PROGRESS = "progress"
    CANCELLATION_REQUESTED = "cancellation_requested"
    CLEANUP = "cleanup"


@dataclass(frozen=True)
class OperationFailure:
    operation_id: str
    stage: str
    message: str
    step_id: str | None = None
    command: str | None = None
    returncode: int | None = None
    stdout_tail: tuple[str, ...] = ()
    stderr_tail: tuple[str, ...] = ()
    environment: str | None = None
    cleanup_error: str | None = None


@dataclass(frozen=True)
class OperationEvent:
    sequence: int
    timestamp: float
    operation_id: str
    environment: str | None
    kind: OperationEventKind
    state: OperationState
    stage: str | None
    message: str
    step_id: str | None = None
    stream: str | None = None
    line: str | None = None
    current: int | None = None
    maximum: int | None = None


class OperationError(RuntimeError):
    def __init__(self, failure: Any):
        super().__init__(failure.message)
        self.failure = failure


class PreparationError(OperationError):
    pass


class ProvisioningError(OperationError):
    pass


class ExecutionError(OperationError):
    def __init__(self, failure: Any):
        message = failure.summary() if hasattr(failure, "summary") else failure.message
        RuntimeError.__init__(self, message)
        self.failure = failure


class OperationCanceled(RuntimeError):
    def __init__(self, operation_id: str, message: str = "Operation was canceled"):
        super().__init__(message)
        self.operation_id = operation_id


class Operation(Generic[T]):
    """A cleanup-aware unit of asynchronous work.

    Subclasses arrange execution by calling ``_start_runner`` exactly once.
    Cancellation is only a request until the runner has completed termination and
    cleanup and calls ``_set_canceled``.
    """

    _EVENT_HISTORY_LIMIT = 2048

    def __init__(self, operation_id: str | None = None, *, environment: str | None = None) -> None:
        self._id = operation_id or str(uuid.uuid4())
        self._environment = environment
        self._state = OperationState.PENDING
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._cancellation_requested = False
        self._cancellation_sealed = False
        self._cancel_callback: Callable[[], None] | None = None
        self._listeners: list[tuple[Callable[[OperationEvent], None], Callable[[OperationEvent], None]]] = []
        self._events: deque[OperationEvent] = deque(maxlen=self._EVENT_HISTORY_LIMIT)
        self._sequence = 0
        self._lock = threading.RLock()
        self._done = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def state(self) -> OperationState:
        with self._lock:
            return self._state

    @property
    def cancellation_requested(self) -> bool:
        with self._lock:
            return self._cancellation_requested

    def cancel(self) -> bool:
        with self._lock:
            if self._state.terminal or self._cancellation_sealed:
                return False
            first_request = not self._cancellation_requested
            self._cancellation_requested = True
            callback = self._cancel_callback
        if first_request:
            self.emit(
                OperationEventKind.CANCELLATION_REQUESTED,
                "Cancellation requested",
            )
        if callback is not None:
            try:
                callback()
            except Exception:
                logger.exception("Operation cancellation callback failed")
        return True

    def wait_for(self, timeout: float | None = None) -> T:
        if not self._done.wait(timeout):
            raise TimeoutError(f"Operation {self.id} did not finish within {timeout} seconds")
        with self._lock:
            state = self._state
            result = self._result
            exception = self._exception
        if state is OperationState.COMPLETED:
            return cast(T, result)
        if state is OperationState.CANCELED:
            if isinstance(exception, OperationCanceled):
                raise exception
            raise OperationCanceled(self.id)
        if exception is not None:
            raise exception
        raise RuntimeError(f"Operation {self.id} reached invalid terminal state {state.value}")

    def __await__(self):
        return self._async_result().__await__()

    async def _async_result(self) -> T:
        loop = asyncio.get_running_loop()
        waiter = loop.run_in_executor(None, self.wait_for)
        try:
            return await asyncio.shield(waiter)
        except asyncio.CancelledError:
            self.cancel()
            while not self._done.is_set():
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            raise

    def listen(self, callback: Callable[[OperationEvent], None], *, replay: bool = True) -> Operation[T]:
        delivery_lock = threading.Lock()
        buffered: list[OperationEvent] = []
        replaying = True

        def receive(event: OperationEvent) -> None:
            nonlocal replaying
            with delivery_lock:
                if replaying:
                    buffered.append(event)
                    return
            self._notify_listener(callback, event)

        with self._lock:
            history = tuple(self._events) if replay else ()
            if not self._state.terminal:
                self._listeners.append((callback, receive))
        for event in history:
            self._notify_listener(callback, event)
        while True:
            with delivery_lock:
                if not buffered:
                    replaying = False
                    break
                pending = tuple(buffered)
                buffered.clear()
            for event in pending:
                self._notify_listener(callback, event)
        return self

    def remove_listener(self, callback: Callable[[OperationEvent], None]) -> None:
        with self._lock:
            for entry in self._listeners:
                if entry[0] is callback:
                    self._listeners.remove(entry)
                    break

    async def events(self, *, replay: bool = True) -> AsyncIterator[OperationEvent]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[OperationEvent] = asyncio.Queue(maxsize=self._EVENT_HISTORY_LIMIT)

        def enqueue(event: OperationEvent) -> None:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)

        def receive(event: OperationEvent) -> None:
            loop.call_soon_threadsafe(enqueue, event)

        # Enqueue replay while holding the same lock used by emit(), then install
        # the live listener before releasing it.  This prevents a concurrent event
        # from overtaking replayed events.
        with self._lock:
            if replay:
                for event in self._events:
                    queue.put_nowait(event)
            terminal_without_replay = self._state.terminal and not replay
            if not self._state.terminal:
                self._listeners.append((receive, receive))
        if terminal_without_replay:
            return
        try:
            while True:
                event = await queue.get()
                yield event
                if event.state.terminal:
                    return
        finally:
            self.remove_listener(receive)

    def emit(
        self,
        kind: OperationEventKind,
        message: str,
        *,
        stage: str | None = None,
        step_id: str | None = None,
        stream: str | None = None,
        line: str | None = None,
        current: int | None = None,
        maximum: int | None = None,
        environment: str | None = None,
    ) -> OperationEvent:
        with self._lock:
            self._sequence += 1
            event = OperationEvent(
                sequence=self._sequence,
                timestamp=time.time(),
                operation_id=self._id,
                environment=environment if environment is not None else self._environment,
                kind=kind,
                state=self._state,
                stage=stage,
                message=message,
                step_id=step_id,
                stream=stream,
                line=line,
                current=current,
                maximum=maximum,
            )
            self._events.append(event)
            listeners = tuple(receiver for _, receiver in self._listeners)
        for listener in listeners:
            self._notify_listener(listener, event)
        return event

    def _notify_listener(self, listener: Callable[[OperationEvent], None], event: OperationEvent) -> None:
        try:
            listener(event)
        except Exception:
            logger.exception("Operation listener failed")

    def _set_cancel_callback(self, callback: Callable[[], None]) -> None:
        with self._lock:
            self._cancel_callback = callback
            requested = self._cancellation_requested
        if requested:
            callback()

    def _seal_cancellation(self) -> bool:
        """Linearize final publication against cancellation.

        Returns ``False`` when cancellation already won.  Once this returns
        ``True``, subsequent cancellation requests are rejected and the runner
        must proceed directly to terminal success or failure.
        """

        with self._lock:
            if self._cancellation_requested:
                return False
            self._cancellation_sealed = True
            return True

    def _start_runner(self, runner: Callable[[], T], *, thread_name: str) -> None:
        with self._lock:
            if self._thread is not None:
                raise RuntimeError("Operation runner already started")
            self._thread = threading.Thread(target=self._run, args=(runner,), name=thread_name, daemon=True)
            self._thread.start()

    def _runs_on_current_thread(self) -> bool:
        with self._lock:
            return self._thread is threading.current_thread()

    def _run(self, runner: Callable[[], T]) -> None:
        if self.cancellation_requested:
            self._set_canceled()
            return
        self._set_running()
        try:
            result = runner()
        except OperationCanceled as error:
            self._set_canceled(error)
        except BaseException as error:
            self._set_failed(error)
        else:
            if self.cancellation_requested and not self._cancellation_sealed:
                self._set_canceled()
            else:
                self._set_completed(result)

    def _set_running(self) -> None:
        with self._lock:
            if self._state is not OperationState.PENDING:
                return
            self._state = OperationState.RUNNING
        self.emit(OperationEventKind.STATE, "Operation started")

    def _set_completed(self, result: T) -> None:
        with self._lock:
            if self._state.terminal:
                return
            self._result = result
            self._state = OperationState.COMPLETED
        self.emit(OperationEventKind.STATE, "Operation completed")
        self._done.set()

    def _set_failed(self, error: BaseException) -> None:
        with self._lock:
            if self._state.terminal:
                return
            self._exception = error
            self._state = OperationState.FAILED
        self.emit(OperationEventKind.STATE, str(error) or type(error).__name__)
        self._done.set()

    def _set_canceled(self, error: OperationCanceled | None = None) -> None:
        with self._lock:
            if self._state.terminal:
                return
            self._exception = error or OperationCanceled(self.id)
            self._state = OperationState.CANCELED
        self.emit(OperationEventKind.STATE, "Operation canceled")
        self._done.set()


class PreparationOperation(Operation[T]):
    pass


class ProvisioningOperation(Operation[T]):
    pass
