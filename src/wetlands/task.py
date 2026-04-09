"""Task-based API for asynchronous execution in remote environments.

Provides Task[T], TaskStatus, TaskEvent, TaskEventType, and RemoteTaskHandle.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import threading
import uuid
from collections.abc import AsyncIterator, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    try:
        from typing_extensions import Self
    except ImportError:
        Self = Any  # type: ignore[assignment,misc]

try:
    from wetlands._internal.exceptions import ExecutionException
except ImportError:
    # When loaded in isolated environments via import_from_path,
    # wetlands package is not available. RemoteTaskHandle doesn't need it.
    ExecutionException = Exception  # type: ignore[assignment,misc]

T = TypeVar("T")


class TaskStatus(enum.Enum):
    """Status of a task through its lifecycle."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    def is_finished(self) -> bool:
        """Return True if the task has reached a terminal state."""
        return self in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELED)


class TaskEventType(enum.Enum):
    """Types of events emitted by a Task."""

    STARTED = "started"
    UPDATE = "update"
    COMPLETION = "completion"
    FAILURE = "failure"
    CANCELATION = "cancelation"


@dataclass(frozen=True, slots=True)
class TaskEvent:
    """Emitted by a Task to notify listeners of state changes."""

    task: Task[Any]
    type: TaskEventType


class Task(Generic[T]):
    """Represents an asynchronous unit of work in a remote environment.

    Type parameter T is the return type of the remote function.
    """

    def __init__(self, task_id: str | None = None) -> None:
        self._id = task_id or str(uuid.uuid4())
        self._status = TaskStatus.PENDING
        self._result: T | None = None
        self._error: str | None = None
        self._traceback: list[str] | None = None
        self._exception: ExecutionException | None = None
        self._message: str | None = None
        self._current: int | None = None
        self._maximum: int | None = None
        self._outputs: dict[str, Any] = {}
        self._listeners: list[Callable[[TaskEvent], None]] = []
        self._terminal_event: TaskEvent | None = None
        self._future: Future[T] = Future()
        self._lock = threading.RLock()
        self._done_event = threading.Event()

        # Set by the environment before dispatch
        self._start_fn: Callable[[], None] | None = None
        self._cancel_fn: Callable[[], None] | None = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def status(self) -> TaskStatus:
        return self._status

    @property
    def result(self) -> T:
        """The return value. Raises InvalidStateError if not COMPLETED."""
        if self._status != TaskStatus.COMPLETED:
            raise InvalidStateError(f"Task is {self._status.value}, not completed")
        return self._result  # type: ignore[return-value]

    @property
    def error(self) -> str | None:
        return self._error

    @property
    def traceback(self) -> list[str] | None:
        return self._traceback

    @property
    def exception(self) -> ExecutionException | None:
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
        return self._outputs

    # --- Control ---

    def start(self) -> Self:
        """Dispatch the task to the remote environment.
        No-op if already started. Returns self for chaining.
        """
        with self._lock:
            if self._status != TaskStatus.PENDING:
                return self
            if self._start_fn is None:
                raise InvalidStateError("Task has no start function. Was it created via submit()?")
            start_fn = self._start_fn
        start_fn()
        return self

    def cancel(self) -> None:
        """Request cooperative cancellation.
        Sets a flag that the remote code can check via task.cancel_requested.
        Does nothing if the task is already finished.
        """
        with self._lock:
            if self._status.is_finished():
                return
            if self._cancel_fn is not None:
                self._cancel_fn()

    def wait_for(self, timeout: float | None = None) -> Self:
        """Block until the task reaches a terminal state.
        Raises TimeoutError if timeout (in seconds) is exceeded.
        Does NOT cancel the task on timeout (matches concurrent.futures behavior).
        Returns self for chaining.
        """
        if not self._done_event.wait(timeout=timeout):
            raise TimeoutError(f"Task {self._id} did not finish within {timeout}s")
        return self

    # --- Observation ---

    def listen(self, callback: Callable[[TaskEvent], None]) -> Self:
        """Register a listener for task events. Returns self for chaining.
        If the task has already reached a terminal state, the terminal event is
        replayed immediately to the callback.
        """
        with self._lock:
            self._listeners.append(callback)
            terminal = self._terminal_event
        if terminal is not None:
            callback(terminal)
        return self

    def remove_listener(self, callback: Callable[[TaskEvent], None]) -> None:
        """Remove a previously registered listener."""
        with self._lock:
            self._listeners.remove(callback)

    # --- concurrent.futures interop ---

    @property
    def future(self) -> Future[T]:
        """A standard Future that resolves with the task result."""
        return self._future

    # --- Awaitable ---

    def __await__(self):
        """Makes Task awaitable: result = await task"""
        return self._async_result().__await__()

    async def _async_result(self) -> T:
        loop = asyncio.get_event_loop()
        result = await asyncio.wrap_future(self._future, loop=loop)
        return result

    # --- Async event stream ---

    async def events(self) -> AsyncIterator[TaskEvent]:
        """Async iterator over task events. Terminates after the terminal event."""
        queue: asyncio.Queue[TaskEvent | None] = asyncio.Queue()
        loop = asyncio.get_event_loop()

        def _on_event(event: TaskEvent) -> None:
            loop.call_soon_threadsafe(queue.put_nowait, event)
            if event.type in (TaskEventType.COMPLETION, TaskEventType.FAILURE, TaskEventType.CANCELATION):
                loop.call_soon_threadsafe(queue.put_nowait, None)

        self.listen(_on_event)
        while True:
            event = await queue.get()
            if event is None:
                break
            yield event

    # --- Context manager (auto-cancel on exit) ---

    def __enter__(self) -> Self:
        if self._status == TaskStatus.PENDING:
            self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if not self._status.is_finished():
            self.cancel()
            self.wait_for()

    async def __aenter__(self) -> Self:
        if self._status == TaskStatus.PENDING:
            self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if not self._status.is_finished():
            self.cancel()
            await self

    # --- Internal methods (called by the environment/IPC reader) ---

    def _set_start_fn(self, fn: Callable[[], None]) -> None:
        self._start_fn = fn

    def _set_cancel_fn(self, fn: Callable[[], None]) -> None:
        self._cancel_fn = fn

    def _set_running(self) -> None:
        with self._lock:
            self._status = TaskStatus.RUNNING
        self._emit(TaskEventType.STARTED)

    def _set_completed(self, result: T) -> None:
        with self._lock:
            if self._status.is_finished():
                return
            self._status = TaskStatus.COMPLETED
            self._result = result
        self._future.set_result(result)
        self._emit(TaskEventType.COMPLETION)
        self._done_event.set()

    def _set_failed(self, error: str, traceback: list[str] | None = None) -> None:
        with self._lock:
            if self._status.is_finished():
                return
            self._status = TaskStatus.FAILED
            self._error = error
            self._traceback = traceback
            self._exception = ExecutionException({"exception": error, "traceback": traceback or []})
        self._future.set_exception(self._exception)
        self._emit(TaskEventType.FAILURE)
        self._done_event.set()

    def _set_canceled(self) -> None:
        with self._lock:
            if self._status.is_finished():
                return
            self._status = TaskStatus.CANCELED
        self._future.cancel()
        self._emit(TaskEventType.CANCELATION)
        self._done_event.set()

    def _set_update(
        self,
        message: str | None = None,
        current: int | None = None,
        maximum: int | None = None,
        outputs: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if message is not None:
                self._message = message
            if current is not None:
                self._current = current
            if maximum is not None:
                self._maximum = maximum
            if outputs:
                self._outputs.update(outputs)
        self._emit(TaskEventType.UPDATE)

    def _emit(self, event_type: TaskEventType) -> None:
        event = TaskEvent(task=self, type=event_type)
        is_terminal = event_type in (TaskEventType.COMPLETION, TaskEventType.FAILURE, TaskEventType.CANCELATION)
        with self._lock:
            if is_terminal:
                self._terminal_event = event
            listeners = list(self._listeners)
        for listener in listeners:
            listener(event)

    def _on_message(self, message: dict[str, Any]) -> None:
        """Handle an IPC message from the remote worker."""
        action = message.get("action")
        if action == "execution finished":
            self._set_completed(message.get("result"))
        elif action == "error":
            self._set_failed(message.get("exception", "Unknown error"), message.get("traceback"))
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

    Injected by the module_executor when the function signature has a 'task' parameter.
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
        payload: dict[str, Any] = {"action": "update", "task_id": self._task_id}
        if message is not None:
            payload["message"] = message
        if current is not None:
            payload["current"] = current
        if maximum is not None:
            payload["maximum"] = maximum
        with self._lock:
            self._connection.send(payload)

    def set_output(self, key: str, value: Any) -> None:
        """Publish a named intermediate output (must be picklable)."""
        payload = {
            "action": "update",
            "task_id": self._task_id,
            "outputs": {key: value},
        }
        with self._lock:
            self._connection.send(payload)

    def cancel(self) -> None:
        """Acknowledge cancellation. Transitions the task to CANCELED."""
        payload = {"action": "canceled", "task_id": self._task_id}
        with self._lock:
            self._connection.send(payload)

    def log(self, message: str, level: int = logging.INFO) -> None:
        """Send a log message to the caller's logging system."""
        payload = {
            "action": "log",
            "task_id": self._task_id,
            "message": message,
            "level": level,
        }
        with self._lock:
            self._connection.send(payload)

    def _set_cancel_requested(self) -> None:
        """Called by the module_executor when a cancel message is received."""
        self._cancel_requested = True


class InvalidStateError(Exception):
    """Raised when accessing task result in an invalid state."""

    pass
