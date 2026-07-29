"""Cleanup-aware execution tasks for isolated workers."""

from __future__ import annotations

import asyncio
import enum
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Generic, TYPE_CHECKING, TypeVar, cast

import sys

if TYPE_CHECKING:
    from typing_extensions import Self
elif sys.version_info >= (3, 11):
    from typing import Self
else:
    try:
        from typing_extensions import Self
    except ImportError:
        Self = Any  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from wetlands._internal.diagnostics import TaskFailure as ExecutionFailure
else:
    try:
        from wetlands._internal.diagnostics import TaskFailure as ExecutionFailure
    except ImportError:
        ExecutionFailure = None  # type: ignore[assignment,misc]

try:
    from wetlands.operation import ExecutionError, OperationCanceled
except ImportError:
    # The isolated worker loads this module without installing Wetlands.
    ExecutionError = RuntimeError  # type: ignore[assignment,misc]
    OperationCanceled = RuntimeError  # type: ignore[assignment,misc]

T = TypeVar("T")


class ExecutionState(enum.Enum):
    """Status of a task through its lifecycle."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        """Whether the execution reached a terminal state."""
        return self in (ExecutionState.COMPLETED, ExecutionState.FAILED, ExecutionState.CANCELED)


class ExecutionEventKind(enum.Enum):
    """Types of events emitted by an execution task."""

    STARTED = "started"
    UPDATE = "update"
    COMPLETION = "completion"
    FAILURE = "failure"
    CANCELLATION_REQUESTED = "cancellation_requested"
    CANCELLATION = "cancellation"


@dataclass(frozen=True)
class ExecutionEvent:
    """Immutable observation snapshot emitted by an execution task."""

    sequence: int
    timestamp: float
    task_id: str
    kind: ExecutionEventKind
    state: ExecutionState
    message: str
    current: int | None = None
    maximum: int | None = None
    progress: float | None = None
    failure: ExecutionFailure | None = None


class ExecutionTask(Generic[T]):
    """Represents an asynchronous unit of work in a remote environment.

    Type parameter T is the return type of the remote function.
    """

    _EVENT_HISTORY_LIMIT = 2048

    def __init__(self, task_id: str | None = None) -> None:
        self._id = task_id or str(uuid.uuid4())
        self._status = ExecutionState.PENDING
        self._result: T | None = None
        self._error: ExecutionFailure | None = None
        self._traceback: str | None = None
        self._exception: ExecutionError | None = None
        self._message: str | None = None
        self._current: int | None = None
        self._maximum: int | None = None
        self._outputs: dict[str, Any] = {}
        self._listeners: list[Callable[[ExecutionEvent], None]] = []
        self._events: deque[ExecutionEvent] = deque(maxlen=self._EVENT_HISTORY_LIMIT)
        self._sequence = 0
        self._last_event_timestamp = 0.0
        self._future: Future[T] = Future()
        self._cancellation_requested = False
        self._lock = threading.RLock()
        self._done_event = threading.Event()
        self._payload: dict[str, Any] = {}

        # Set by the environment before dispatch
        self._start_fn: Callable[[], None] | None = None
        self._cancel_fn: Callable[[], None] | None = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def state(self) -> ExecutionState:
        """The execution lifecycle state."""
        with self._lock:
            return self._status

    @property
    def result(self) -> T:
        """The return value. Raises InvalidStateError if not COMPLETED."""
        if self._status != ExecutionState.COMPLETED:
            raise InvalidStateError(f"Task is {self._status.value}, not completed")
        return self._result  # type: ignore[return-value]

    @property
    def error(self) -> ExecutionFailure | None:
        return self._error

    @property
    def traceback(self) -> str | None:
        return self._traceback

    @property
    def exception(self) -> ExecutionError | None:
        return self._exception

    @property
    def message(self) -> str | None:
        return self._message

    @property
    def current(self) -> int | None:
        return self._current

    @property
    def maximum(self) -> int | None:
        return self._maximum

    @property
    def progress(self) -> float | None:
        """current / maximum as a float in [0, 1]. None if unavailable."""
        if self._current is not None and self._maximum is not None and self._maximum > 0:
            return self._current / self._maximum
        return None

    @property
    def outputs(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._outputs)

    # --- Control ---

    def start(self) -> Self:
        """Dispatch the task to the remote environment.
        No-op if already started. Returns self for chaining.
        """
        with self._lock:
            if self._status != ExecutionState.PENDING:
                return self
            if self._start_fn is None:
                raise InvalidStateError("Task has no start function. Was it created via submit()?")
            start_fn = self._start_fn
        start_fn()
        return self

    @property
    def cancellation_requested(self) -> bool:
        with self._lock:
            return self._cancellation_requested

    def cancel(self) -> bool:
        """Request cooperative cancellation.
        Sets a flag that the remote code can check via task.cancel_requested.
        Does nothing if the task is already finished.
        """
        with self._lock:
            if self._status.terminal:
                return False
            first_request = not self._cancellation_requested
            self._cancellation_requested = True
            cancel_fn = self._cancel_fn
            event_data = (
                self._record_event_locked(
                    ExecutionEventKind.CANCELLATION_REQUESTED,
                    "Cancellation requested",
                )
                if first_request
                else None
            )
        if event_data is not None:
            self._notify(*event_data)
        if first_request and cancel_fn is not None:
            cancel_fn()
        return True

    def wait_for(self, timeout: float | None = None) -> T:
        """Block until the task reaches a terminal state.
        Raises TimeoutError if timeout (in seconds) is exceeded.
        Does NOT cancel the task on timeout (matches concurrent.futures behavior).
        Returns the task result.
        """
        if not self._done_event.wait(timeout=timeout):
            raise TimeoutError(f"Task {self._id} did not finish within {timeout}s")
        if self._status == ExecutionState.COMPLETED:
            return cast(T, self._result)
        if self._status == ExecutionState.CANCELED:
            raise OperationCanceled(self._id)
        if self._exception is not None:
            raise self._exception
        raise InvalidStateError(f"Task reached unexpected terminal state {self._status.value}")

    # --- Observation ---

    def listen(
        self,
        callback: Callable[[ExecutionEvent], None],
        *,
        replay: bool = True,
    ) -> Self:
        """Register a listener, replaying bounded history by default."""
        with self._lock:
            history = tuple(self._events) if replay else ()
            for event in history:
                self._notify_listener(callback, event)
            if not self._status.terminal:
                self._listeners.append(callback)
        return self

    def remove_listener(self, callback: Callable[[ExecutionEvent], None]) -> None:
        """Remove a previously registered listener."""
        with self._lock:
            if callback in self._listeners:
                self._listeners.remove(callback)

    # --- Awaitable ---

    def __await__(self):
        """Return an awaiter which does not bypass cancellation cleanup."""
        return self._async_result().__await__()

    async def _async_result(self) -> T:
        loop = asyncio.get_running_loop()
        waiter = asyncio.wrap_future(self._future, loop=loop)
        try:
            return await asyncio.shield(waiter)
        except asyncio.CancelledError:
            self.cancel()
            cleanup_waiter = loop.run_in_executor(None, self._done_event.wait)
            while not cleanup_waiter.done():
                try:
                    await asyncio.shield(cleanup_waiter)
                except asyncio.CancelledError:
                    continue
            try:
                await asyncio.shield(waiter)
            except BaseException:
                pass
            raise

    # --- Async event stream ---

    async def events(self, *, replay: bool = True) -> AsyncIterator[ExecutionEvent]:
        """Iterate over bounded history and live events until terminal state."""
        queue: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=self._EVENT_HISTORY_LIMIT)
        loop = asyncio.get_running_loop()

        def enqueue(event: ExecutionEvent) -> None:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)

        def receive(event: ExecutionEvent) -> None:
            loop.call_soon_threadsafe(enqueue, event)

        with self._lock:
            terminal_without_replay = self._status.terminal and not replay
            if replay:
                for event in self._events:
                    queue.put_nowait(event)
            if not self._status.terminal:
                self._listeners.append(receive)
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

    # --- Internal methods (called by the environment/IPC reader) ---

    def _set_start_fn(self, fn: Callable[[], None]) -> None:
        self._start_fn = fn

    def _set_cancel_fn(self, fn: Callable[[], None]) -> None:
        self._cancel_fn = fn

    def _set_running(self) -> None:
        with self._lock:
            if self._status is not ExecutionState.PENDING:
                return
            self._status = ExecutionState.RUNNING
            event_data = self._record_event_locked(
                ExecutionEventKind.STARTED,
                "Execution started",
            )
        self._notify(*event_data)

    def _set_completed(self, result: T) -> None:
        with self._lock:
            if self._status.terminal:
                return
            if self._cancellation_requested:
                canceled = True
            else:
                canceled = False
                self._status = ExecutionState.COMPLETED
                self._result = result
                event_data = self._record_event_locked(
                    ExecutionEventKind.COMPLETION,
                    "Execution completed",
                )
        if canceled:
            self._set_canceled()
            return
        self._future.set_result(result)
        self._done_event.set()
        self._notify(*event_data)

    def _set_failed(self, error: Any, traceback: list[str] | str | None = None) -> None:
        call_target = self._payload.get("_call_target") if isinstance(self._payload, dict) else None
        if ExecutionFailure is not None:
            failure = ExecutionFailure.normalize(
                error,
                traceback=traceback,
                task_id=self._id,
                call_target=call_target,
            )
            exception = ExecutionError(failure)
        else:
            failure = error
            exception = ExecutionError(str(error))
        with self._lock:
            if self._status.terminal:
                return
            self._status = ExecutionState.FAILED
            self._error = failure
            self._traceback = failure.traceback if ExecutionFailure is not None else traceback  # type: ignore[union-attr,assignment]
            self._exception = exception
            event_data = self._record_event_locked(
                ExecutionEventKind.FAILURE,
                str(exception) or "Execution failed",
                failure=failure,
            )
        self._future.set_exception(self._exception)
        self._done_event.set()
        self._notify(*event_data)

    def _set_canceled(self) -> None:
        with self._lock:
            if self._status.terminal:
                return
            self._status = ExecutionState.CANCELED
            event_data = self._record_event_locked(
                ExecutionEventKind.CANCELLATION,
                "Execution canceled",
            )
        self._future.set_exception(OperationCanceled(self._id))
        self._done_event.set()
        self._notify(*event_data)

    def _set_update(
        self,
        message: str | None = None,
        current: int | None = None,
        maximum: int | None = None,
        outputs: dict[str, Any] | None = None,
    ) -> None:
        if message is not None and not isinstance(message, str):
            raise TypeError("Progress message must be a string")
        for field, value in {"current": current, "maximum": maximum}.items():
            if value is not None and (type(value) is not int or value < 0):
                raise TypeError(f"Progress {field} must be a nonnegative integer")
        if outputs:
            _validate_intermediate_value(outputs, path="outputs")
        with self._lock:
            if message is not None:
                self._message = message
            if current is not None:
                self._current = current
            if maximum is not None:
                self._maximum = maximum
            if outputs:
                self._outputs.update(outputs)
            event_data = self._record_event_locked(
                ExecutionEventKind.UPDATE,
                message or self._message or "Execution progress updated",
            )
        self._notify(*event_data)

    def _record_event_locked(
        self,
        kind: ExecutionEventKind,
        message: str,
        *,
        failure: Any | None = None,
    ) -> tuple[ExecutionEvent, tuple[Callable[[ExecutionEvent], None], ...]]:
        self._sequence += 1
        timestamp = max(time.time(), self._last_event_timestamp)
        self._last_event_timestamp = timestamp
        progress = (
            self._current / self._maximum
            if self._current is not None and self._maximum is not None and self._maximum > 0
            else None
        )
        event = ExecutionEvent(
            sequence=self._sequence,
            timestamp=timestamp,
            task_id=self._id,
            kind=kind,
            state=self._status,
            message=message,
            current=self._current,
            maximum=self._maximum,
            progress=progress,
            failure=failure,
        )
        self._events.append(event)
        return event, tuple(self._listeners)

    def _notify(
        self,
        event: ExecutionEvent,
        listeners: tuple[Callable[[ExecutionEvent], None], ...],
    ) -> None:
        for listener in listeners:
            self._notify_listener(listener, event)

    def _notify_listener(
        self,
        listener: Callable[[ExecutionEvent], None],
        event: ExecutionEvent,
    ) -> None:
        try:
            listener(event)
        except Exception:
            logging.getLogger(__name__).exception("Execution listener failed")

    def _on_message(self, message: dict[str, Any]) -> None:
        """Handle an IPC message from the remote worker."""
        action = message.get("action")
        if action == "execution finished":
            self._set_completed(cast(T, message.get("result")))
        elif action == "error":
            self._set_failed(message)
        elif action == "update":
            self._set_update(
                message=message.get("message"),
                current=message.get("current"),
                maximum=message.get("maximum"),
                outputs=message.get("outputs"),
            )
        elif action == "canceled":
            self._set_canceled()


class RemoteTaskHandle:
    """Available to remote code for progress reporting and cancellation.

    Injected only when the submitter explicitly selects a context keyword.
    """

    def __init__(self, task_id: str, connection_lock: threading.Lock, connection: Any) -> None:
        self._task_id = task_id
        self._cancel_requested = False
        self._lock = connection_lock
        self._connection = connection

    @property
    def cancel_requested(self) -> bool:
        """True if the caller has requested cancellation."""
        return self._cancel_requested

    def update(
        self,
        message: str | None = None,
        *,
        current: int | None = None,
        maximum: int | None = None,
    ) -> None:
        """Report progress. Sends an UPDATE event to the caller."""
        if message is not None and not isinstance(message, str):
            raise TypeError("Progress message must be a string")
        for field, value in {"current": current, "maximum": maximum}.items():
            if value is not None and (type(value) is not int or value < 0):
                raise TypeError(f"Progress {field} must be a nonnegative integer")
        payload: dict[str, Any] = self._message("update")
        if message is not None:
            payload["message"] = message
        if current is not None:
            payload["current"] = current
        if maximum is not None:
            payload["maximum"] = maximum
        self._send(payload, "update")

    def set_output(self, key: str, value: Any) -> None:
        """Publish a simple named intermediate value.

        Arrays and extension-codec values are intentionally unsupported for
        intermediate output in execution protocol version 1.
        """
        if not isinstance(key, str) or not key:
            raise TypeError("Intermediate output keys must be nonempty strings")
        _validate_intermediate_value(value, path=f"outputs[{key!r}]")
        payload = self._message("update", outputs={key: value})
        self._send(payload, "output")

    def cancel(self) -> None:
        """Request cancellation of the current worker function."""
        self._set_cancel_requested()

    def log(self, message: str, level: int = logging.INFO) -> None:
        """Send a log message to the caller's logging system."""
        if not isinstance(message, str):
            raise TypeError("Log message must be a string")
        if type(level) is not int:
            raise TypeError("Log level must be an integer")
        payload = self._message("log", message=message, level=level)
        self._send(payload, "log")

    def _set_cancel_requested(self) -> None:
        """Called by the module_executor when a cancel message is received."""
        self._cancel_requested = True

    def _send(self, payload: dict[str, Any], context: str) -> None:
        try:
            with self._lock:
                self._connection.send(payload)
        except Exception as e:
            raise RemoteTaskSerializationError(context, e) from e

    def _message(self, action: str, **payload: Any) -> dict[str, Any]:
        return {
            "action": action,
            "protocol_version": 1,
            "task_id": self._task_id,
            **payload,
        }


class RemoteTaskSerializationError(RuntimeError):
    """Raised in remote code when progress/output/log IPC payloads cannot be serialized."""

    def __init__(self, context: str, original: BaseException) -> None:
        self.category = "serialization"
        self.serialization_context = context
        super().__init__(f"Failed to serialize task {context}: {original}")


class InvalidStateError(Exception):
    """Raised when accessing task result in an invalid state."""


def _validate_intermediate_value(value: Any, *, path: str, seen: set[int] | None = None) -> None:
    if value is None or type(value) in {bool, int, float, str, bytes}:
        return
    if not isinstance(value, (list, tuple, dict)):
        raise TypeError(f"{path}: unsupported intermediate value type {type(value).__qualname__}")
    active = seen if seen is not None else set()
    identity = id(value)
    if identity in active:
        raise TypeError(f"{path}: cyclic intermediate values are unsupported")
    active.add(identity)
    try:
        if isinstance(value, dict):
            for key, item in value.items():
                if key is not None and type(key) not in {bool, int, float, str, bytes}:
                    raise TypeError(f"{path}: unsupported dictionary key {key!r}")
                _validate_intermediate_value(item, path=f"{path}[{key!r}]", seen=active)
        else:
            for index, item in enumerate(value):
                _validate_intermediate_value(item, path=f"{path}[{index}]", seen=active)
    finally:
        active.remove(identity)
