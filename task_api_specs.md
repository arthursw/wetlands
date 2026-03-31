# Wetlands Task API Specification

## Motivation

Wetlands currently exposes a **blocking, call-and-return** API: `env.execute()` sends a message, waits for the result, and returns it. This is simple but has limitations:

- **No progress reporting.** Long-running computations give no feedback until they finish.
- **No cancellation.** Once a call is dispatched, there is no way to ask the remote process to stop.
- **No structured events.** Logs, progress, intermediate outputs, and errors all flow through different channels (Python logging, IPC messages, process stdout).
- **Blocking only.** Non-blocking usage requires the caller to manage threads manually.

Appose solves these problems with a **Task** abstraction: every execution returns a Task object that emits typed events (UPDATE, COMPLETION, FAILURE, CANCELATION), supports cancellation, and can be waited on or observed asynchronously.

This document specifies a task-based API for Wetlands that goes further than Appose by leveraging Python's modern concurrency primitives (`concurrent.futures`, `async/await`, context managers, generics) and providing a **worker pool** model for true process-level parallelism.

---

## Design Principles

1. **Backward compatible.** The existing blocking `execute()`, `run_script()`, and `import_module()` APIs remain unchanged. The task API is an additional layer.
2. **Pythonic.** Use `concurrent.futures.Future`, `async/await`, context managers, generics, and standard logging rather than inventing new primitives.
3. **Minimal surface.** A single generic `Task[T]` class covers all execution modes.
4. **Cooperative cancellation.** Remote code must opt in to cancellation by checking a flag.
5. **Progress is optional.** Remote code can report progress, but the API works fine without it.
6. **Process-level parallelism.** Concurrent execution uses multiple worker processes (not threads), each running in its own `module_executor` instance within the same conda environment. No GIL issues, no thread-safety burden on user code.

---

## API Overview

```
EnvironmentManager
  └── Environment / ExternalEnvironment
        ├── execute()              # existing, blocking
        ├── run_script()           # existing, blocking
        ├── import_module()        # existing, blocking proxy
        ├── submit()               # NEW: returns Task[T] (non-blocking)
        ├── submit_script()        # NEW: returns Task[None] (non-blocking)
        ├── map()                  # NEW: batch parallel execution
        ├── map_tasks()            # NEW: batch parallel with Task objects
        └── execute_commands()     # existing, unchanged
```

---

## Task Lifecycle

```
PENDING ──(auto/start())──> RUNNING ──(success)──> COMPLETED
                               │
                               ├──(error)──────> FAILED
                               │
                               └──cancel()──(ack)──> CANCELED
```

A `Task` is created by `submit()` or `submit_script()`. By default it starts immediately (`start=True`). With `start=False`, the task stays in `PENDING` until `task.start()` is called — useful for attaching listeners before execution begins. Terminal events (COMPLETION, FAILURE, CANCELATION) are always replayed to late listeners, so no final state is ever lost.

---

## Core Types

### `TaskStatus` (Enum)

```python
import enum

class TaskStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    def is_finished(self) -> bool:
        """Return True if the task has reached a terminal state."""
        return self in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELED)
```

### `TaskEventType` (Enum)

```python
class TaskEventType(enum.Enum):
    STARTED = "started"           # task has been dispatched
    UPDATE = "update"             # progress or intermediate output
    COMPLETION = "completion"     # task finished successfully
    FAILURE = "failure"           # task raised an exception
    CANCELATION = "cancelation"   # task was canceled cooperatively
```

### `TaskEvent`

```python
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class TaskEvent:
    """Emitted by a Task to notify listeners of state changes."""
    task: "Task[Any]"
    type: TaskEventType
```

### `Task[T]` -- Generic, Awaitable, Context-Managed

```python
from typing import TypeVar, Generic, Self, Any
from concurrent.futures import Future
from collections.abc import Callable, AsyncIterator

T = TypeVar("T")

class Task(Generic[T]):
    """Represents an asynchronous unit of work in a remote environment.

    Type parameter T is the return type of the remote function.
    """

    # --- Read-only state ---

    @property
    def status(self) -> TaskStatus: ...

    @property
    def result(self) -> T:
        """The return value. Raises InvalidStateError if not COMPLETED."""
        ...

    @property
    def error(self) -> str | None:
        """Error message. Available after FAILURE."""
        ...

    @property
    def traceback(self) -> list[str] | None:
        """Remote traceback. Available after FAILURE."""
        ...

    @property
    def exception(self) -> ExecutionException | None:
        """The exception object. Available after FAILURE."""
        ...

    # --- Progress (set by remote code via task.update()) ---

    @property
    def message(self) -> str | None:
        """Free-form progress message from the remote side."""
        ...

    @property
    def current(self) -> int | None:
        """Current progress value."""
        ...

    @property
    def maximum(self) -> int | None:
        """Maximum progress value."""
        ...

    @property
    def progress(self) -> float | None:
        """Convenience: current / maximum as a float in [0, 1]. None if unavailable."""
        ...

    @property
    def outputs(self) -> dict[str, Any]:
        """Intermediate named outputs published by the remote code."""
        ...

    # --- Control ---

    def start(self) -> Self:
        """Dispatch the task to the remote environment.
        No-op if already started. Returns self for chaining.
        """
        ...

    def cancel(self) -> None:
        """Request cooperative cancellation.
        Sets a flag that the remote code can check via task.cancel_requested.
        Does nothing if the task is already finished.
        """
        ...

    def wait_for(self, timeout: float | None = None) -> Self:
        """Block until the task reaches a terminal state.
        Raises TimeoutError if timeout (in seconds) is exceeded.
        Does NOT cancel the task on timeout (matches concurrent.futures behavior).
        Returns self for chaining.
        """
        ...

    # --- Observation ---

    def listen(self, callback: Callable[[TaskEvent], None]) -> Self:
        """Register a listener for task events. Returns self for chaining.
        If the task has already reached a terminal state, the terminal event is
        replayed immediately to the callback (so late listeners never miss the outcome).
        UPDATE events before registration are NOT replayed (they are transient).
        Multiple listeners can be registered.
        """
        ...

    def remove_listener(self, callback: Callable[[TaskEvent], None]) -> None:
        """Remove a previously registered listener."""
        ...

    # --- concurrent.futures interop ---

    @property
    def future(self) -> Future[T]:
        """A standard Future that resolves with the task result.
            result = task.future.result(timeout=10)
            concurrent.futures.wait([t1.future, t2.future])
        """
        ...

    # --- Awaitable ---

    def __await__(self):
        """Makes Task awaitable in async code:
            result = await env.submit("mod.py", "func", args=(x,))
        Resolves to the task result (of type T).
        Raises ExecutionException on FAILURE, asyncio.CancelledError on CANCELATION.
        """
        ...

    # --- Async event stream ---

    async def events(self) -> AsyncIterator[TaskEvent]:
        """Async iterator over task events.
            async for event in task.events():
                match event.type:
                    case TaskEventType.UPDATE: ...
        Terminates after the terminal event.
        """
        ...

    # --- Context manager (auto-cancel on exit) ---

    def __enter__(self) -> Self:
        """Start the task if not already started."""
        ...

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """If the task is still running, cancel and wait."""
        ...

    async def __aenter__(self) -> Self:
        """Async context manager entry. Start the task."""
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit. Cancel and await if still running."""
        ...
```

---

## Environment Methods

### `submit()` -- non-blocking function execution

```python
def submit(
    self,
    module_path: str | Path,
    function: str,
    args: tuple = (),
    kwargs: dict[str, Any] | None = None,
    *,
    start: bool = True,
) -> Task[Any]:
    """Submit a function for execution in the remote environment.

    Unlike execute(), this returns immediately with a Task object.

    Args:
        module_path: Path to the module to import.
        function: Name of the function to execute.
        args: Positional arguments (must be picklable).
        kwargs: Keyword arguments (must be picklable).
        start: If True (default), dispatch the task immediately.
               If False, the task stays PENDING until task.start() is called,
               which is useful for attaching listeners before execution begins.

    Returns:
        A Task. If start=True, it is already RUNNING.
        If start=False, it is PENDING.
    """
```

### `submit_script()` -- non-blocking script execution

```python
def submit_script(
    self,
    script_path: str | Path,
    args: tuple = (),
    run_name: str = "__main__",
    *,
    start: bool = True,
) -> Task[None]:
    """Submit a script for execution in the remote environment.

    Args:
        script_path: Path to the Python script.
        args: Command-line arguments (becomes sys.argv[1:]).
        run_name: Value for runpy.run_path(run_name=...).
        start: If True (default), dispatch the task immediately.

    Returns:
        A Task[None] (scripts do not return values).
    """
```

### `map()` -- batch parallel execution

```python
def map(
    self,
    module_path: str | Path,
    function: str,
    iterable: Iterable[Any],
    *,
    timeout: float | None = None,
    ordered: bool = True,
) -> Iterator[Any]:
    """Execute function once for each item, distributing across workers.

    Each item in iterable becomes the sole positional argument to function.
    For multiple arguments, pass tuples and have the remote function unpack,
    or use submit() directly.

    Inspired by concurrent.futures.Executor.map() and multiprocessing.Pool.map().

    Args:
        module_path: Module containing the function.
        function: Function name.
        iterable: Items to process (one task per item).
        timeout: Max seconds to wait for each result. None means no limit.
        ordered: If True (default), yield results in submission order.
                 If False, yield results as they complete (faster overall
                 when items have varying processing times).

    Returns:
        Iterator of results.

    Raises:
        TimeoutError: If a result isn't ready within timeout.
        ExecutionException: If any task fails (raised when iterating to
            the failing task's position).
    """
```

### `map_tasks()` -- batch parallel with full Task control

```python
def map_tasks(
    self,
    module_path: str | Path,
    function: str,
    iterable: Iterable[Any],
) -> list[Task[Any]]:
    """Submit one task per item, distributing across workers.

    All tasks are started immediately. Returns Task objects for full
    control over progress, cancellation, and event listening.

    Args:
        module_path: Module containing the function.
        function: Function name.
        iterable: Items to process (one task per item).

    Returns:
        List of Task objects (one per item, in submission order).
    """
```

---

## Worker Pool: Process-Level Parallelism

### Architecture

When `max_workers > 1`, `launch()` starts multiple `module_executor` processes, all activating the **same conda environment**. This provides true process-level parallelism without duplicating the environment on disk.

```
                                ┌─ module_executor (pid 1001) ─ port 5001
env = em.create("cellpose", …) │
env.launch(max_workers=4) ──────├─ module_executor (pid 1002) ─ port 5002
  (one conda env on disk)       ├─ module_executor (pid 1003) ─ port 5003
                                └─ module_executor (pid 1004) ─ port 5004
```

### Why multi-process instead of multi-thread?

An earlier version of this spec considered running concurrent tasks as threads within a single `module_executor` process. Multi-process is strictly better for Wetlands' use cases:

| Concern | Multi-thread (rejected) | Multi-process (chosen) |
|---------|------------------------|------------------------|
| GIL | Blocks pure Python parallelism | No GIL: separate interpreters |
| Thread safety | User code must be thread-safe | Each worker runs in isolation |
| `sys.path` / `sys.argv` | Global state, must be fixed | Separate per process |
| Failure isolation | One crash kills all tasks | Workers are independent |
| GPU | Shared CUDA context (contention) | Separate contexts, can use different GPUs |
| Memory | Shared (low) | N × process memory (higher, but controlled) |
| Install time | N/A | N/A: all workers share the same conda prefix |
| Disk space | N/A | N/A: no duplication, same installed packages |
| IPC complexity | Multiplex over one connection | One connection per worker (simpler) |

The only cost is memory: each Python process has its own interpreter and imported modules. For a typical scientific stack, ~200-400 MB per worker. With 4 workers, ~1-1.5 GB total. The user controls this via `max_workers`.

### `launch()` with workers

```python
def launch(
    self,
    additional_activate_commands: Commands = {},
    *,
    max_workers: int = 1,
    worker_env: Callable[[int], dict[str, str]] | None = None,
) -> None:
    """Launch the environment's module executor process(es).

    Args:
        additional_activate_commands: Platform-specific activation commands.
        max_workers: Number of worker processes to start.
            All workers share the same conda environment (no duplication).
            Default is 1 (single worker, backward compatible).
        worker_env: Optional callable that receives the worker index (0-based)
            and returns a dict of extra environment variables for that worker.
            Useful for GPU assignment:
                worker_env=lambda i: {"CUDA_VISIBLE_DEVICES": str(i)}
    """
```

### Task dispatch

When a task is submitted (via `submit()`, `map()`, or `execute()`):

1. **If a worker is idle** → dispatch to it immediately.
2. **If all workers are busy** → queue the task internally; dispatch when the next worker becomes available.

The dispatch is fully transparent. The user never sees or manages individual workers. The environment object manages the pool internally.

```python
# Internal architecture (not user-facing)
class ExternalEnvironment:
    _workers: list[_Worker]       # One per module_executor process
    _idle_workers: Queue[_Worker] # Workers available for dispatch
    _task_queue: Queue[Task]      # Tasks waiting for a worker
```

Each `_Worker` holds its own `subprocess.Popen`, port, `Connection`, and a dedicated IPC reader thread. When a worker finishes a task, it is returned to the idle pool and the next queued task (if any) is dispatched to it.

### `max_workers=1` (default)

With a single worker, behavior is identical to the current Wetlands:

- `submit()` returns immediately, but the task is dispatched to the single worker. If the worker is already busy, the task queues.
- `execute()` blocks until the single worker finishes. Multiple threads calling `execute()` concurrently are serialized.
- No architectural overhead: the single worker has one connection, one reader thread. The pool machinery is a trivial pass-through.

---

## Remote-side API: Progress and Cancellation

Remote code running inside the environment can report progress and check for cancellation through a `task` parameter. Wetlands detects the presence of a `task` parameter via `inspect.signature()` and injects a `RemoteTaskHandle` automatically. The parameter is optional: functions that don't declare it work exactly as before.

```python
# remote_module.py -- runs in the isolated environment
def long_computation(data, *, task=None):
    """The 'task' keyword argument is injected by Wetlands automatically."""
    results = []
    for i, item in enumerate(data):
        if task and task.cancel_requested:
            task.cancel()
            return None

        if task:
            task.update(f"Processing item {i}", current=i, maximum=len(data))

        result = expensive_operation(item)
        results.append(result)

        if task:
            task.set_output("partial_results", results.copy())

    return results
```

### `RemoteTaskHandle`

```python
class RemoteTaskHandle:
    """Available to remote code for progress reporting and cancellation."""

    @property
    def cancel_requested(self) -> bool:
        """True if the caller has requested cancellation."""
        ...

    def update(
        self,
        message: str | None = None,
        *,
        current: int | None = None,
        maximum: int | None = None,
    ) -> None:
        """Report progress. Sends an UPDATE event to the caller."""
        ...

    def set_output(self, key: str, value: Any) -> None:
        """Publish a named intermediate output (must be picklable).
        Available on the caller side via task.outputs[key].
        """
        ...

    def cancel(self) -> None:
        """Acknowledge cancellation. Transitions the task to CANCELED.
        The remote function should return (or raise) after calling this.
        """
        ...

    def log(self, message: str, level: int = logging.INFO) -> None:
        """Send a log message to the caller's logging system
        with proper Wetlands context metadata.
        """
        ...
```

---

## Wire Protocol Changes

The existing IPC protocol (over `multiprocessing.connection`) is extended with new message types. Each worker has its own connection, so no multiplexing is needed — each connection handles one task at a time. The `task_id` field is still included for routing on the client side.

### Client-to-server messages

```python
# Execute (extended with task_id)
{"action": "execute", "task_id": str, "module_path": str, "function": str, "args": tuple, "kwargs": dict}

# Run script (extended with task_id)
{"action": "run", "task_id": str, "script_path": str, "args": tuple, "run_name": str}

# Cancel a running task
{"action": "cancel", "task_id": str}

# Exit (unchanged)
{"action": "exit"}
```

### Server-to-client messages

```python
# Completion (extended with task_id)
{"action": "execution finished", "task_id": str, "result": Any}

# Error (extended with task_id)
{"action": "error", "task_id": str, "exception": str, "traceback": list[str]}

# Progress update (NEW)
{"action": "update", "task_id": str, "message": str | None, "current": int | None, "maximum": int | None, "outputs": dict[str, Any]}

# Cancellation acknowledged (NEW)
{"action": "canceled", "task_id": str}
```

When `task_id` is absent in a response, the message is routed to the legacy blocking path (`_send_and_wait`), preserving backward compatibility.

---

## Usage Examples

### Basic: submit and wait

```python
from wetlands.environment_manager import EnvironmentManager

em = EnvironmentManager()
env = em.create("numpy", {"pip": ["numpy==2.2.4"]})
env.launch()

# submit() starts immediately by default, returns a Task
task = env.submit("compute.py", "heavy_computation", args=(data,))

# Do other work while the task runs...
print("Computing in background")

# Block for the result
task.wait_for()
print(f"Result: {task.result}")

env.exit()
```

### Progress reporting

```python
from wetlands.task import TaskEventType

def on_event(event):
    match event.type:
        case TaskEventType.UPDATE:
            t = event.task
            print(f"[{t.progress:.0%}] {t.message}")
        case TaskEventType.COMPLETION:
            print(f"Done: {event.task.result}")
        case TaskEventType.FAILURE:
            print(f"Failed: {event.task.error}")
        case TaskEventType.CANCELATION:
            print("Canceled")

# start=False to register listener before dispatch
task = env.submit("analysis.py", "process", args=(dataset,), start=False)
task.listen(on_event).start()
task.wait_for()
```

### Cancellation

```python
from time import sleep

task = env.submit("simulation.py", "run_simulation", args=(params,))

sleep(5)
if not task.status.is_finished():
    task.cancel()

task.wait_for()
print(f"Final status: {task.status.name}")  # COMPLETED or CANCELED
```

### Context manager (auto-cancel)

```python
with env.submit("training.py", "train_model", args=(config,), start=False) as task:
    task.listen(on_event).start()
    # If we exit the block early (exception, break, etc.),
    # the task is automatically canceled and awaited.
    task.wait_for(timeout=300)
# Task is guaranteed finished here
```

### Batch parallel execution with `map()`

```python
em = EnvironmentManager()
env = em.create("cellpose", {"pip": ["cellpose"]})
env.launch(max_workers=4)  # 4 worker processes, same conda env

images = load_images("data/")

# Process all images across 4 workers, results in order
results = list(env.map("segment.py", "segment", images))
print(f"Segmented {len(results)} images")

env.exit()
```

### Batch parallel with unordered results (faster)

```python
env.launch(max_workers=8)

# Results arrive as workers finish, not in submission order
for result in env.map("analysis.py", "analyze", datasets, ordered=False):
    save_result(result)
```

### Batch parallel with progress and cancellation via `map_tasks()`

```python
env.launch(max_workers=4)

tasks = env.map_tasks("segment.py", "segment", images)

# Attach progress listeners
for task in tasks:
    task.listen(lambda e: print(f"[{e.task.progress:.0%}] {e.task.message}"))

# Cancel all remaining tasks if one fails
try:
    for task in tasks:
        task.wait_for()
except ExecutionException:
    for t in tasks:
        t.cancel()
```

### GPU-per-worker assignment

```python
env.launch(
    max_workers=4,
    worker_env=lambda i: {"CUDA_VISIBLE_DEVICES": str(i)}
)

# Each worker uses a different GPU
results = list(env.map("inference.py", "predict", batches))
```

### Concurrent tasks across different environments

```python
import concurrent.futures

# Different environments — true parallelism (separate processes, separate deps)
t1 = stardist_env.submit("segment.py", "segment", args=(image,))
t2 = cellpose_env.submit("segment.py", "segment", args=(image,))

concurrent.futures.wait([t1.future, t2.future])
print(f"Stardist masks: {t1.result.shape}")
print(f"Cellpose masks: {t2.result.shape}")
```

### Async/await

```python
import asyncio

async def main():
    env = em.create("numpy", {"pip": ["numpy==2.2.4"]})
    env.launch()

    # await directly resolves to the result
    result = await env.submit("compute.py", "fibonacci", args=(50,))
    print(f"Result: {result}")

    env.exit()

asyncio.run(main())
```

### Async event stream

```python
import asyncio

async def monitor(task):
    async for event in task.events():
        match event.type:
            case TaskEventType.UPDATE:
                print(f"Progress: {event.task.progress:.0%}")
            case TaskEventType.COMPLETION:
                print(f"Result: {event.task.result}")

async def main():
    task = env.submit("compute.py", "long_work", args=(data,))
    await monitor(task)

asyncio.run(main())
```

### One-liner via Future

```python
future = env.submit("compute.py", "fib", args=(50,)).future
# ... do other work ...
result = future.result(timeout=60)
```

### GUI integration (PyQt6)

```python
from PyQt6.QtCore import pyqtSignal, QObject

class TaskBridge(QObject):
    progress = pyqtSignal(float, str)   # progress ratio, message
    completed = pyqtSignal(object)
    failed = pyqtSignal(str)

bridge = TaskBridge()
bridge.progress.connect(progress_bar.setValue)
bridge.completed.connect(on_result)

def on_event(event):
    match event.type:
        case TaskEventType.UPDATE:
            bridge.progress.emit(event.task.progress or 0.0, event.task.message or "")
        case TaskEventType.COMPLETION:
            bridge.completed.emit(event.task.result)
        case TaskEventType.FAILURE:
            bridge.failed.emit(event.task.error or "Unknown error")

task = env.submit("segment.py", "segment_image", args=(image,), start=False)
task.listen(on_event).start()
```

### Comparison with blocking API

```python
# Blocking (existing API, unchanged)
result = env.execute("compute.py", "fibonacci", args=(50,))

# Non-blocking (new task API)
task = env.submit("compute.py", "fibonacci", args=(50,))
task.wait_for()
result = task.result

# Or even shorter with Future
result = env.submit("compute.py", "fibonacci", args=(50,)).future.result()
```

---

## Comparison with Appose

| Concept | Appose | Wetlands Task API |
|---------|--------|-------------------|
| Create task | `groovy.task(script)` | `env.submit(module, func, args)` |
| Listen | `task.listen(callback)` | `task.listen(callback)` |
| Start | `task.start()` (required) | `start=True` (default) or `task.start()` |
| Cancel | `task.cancel()` | `task.cancel()` |
| Wait | `task.wait_for()` | `task.wait_for(timeout=None)` |
| Remote progress | `task.update(msg, cur, max)` | `task.update(msg, current=, maximum=)` |
| Check cancel | `task.cancelRequested` | `task.cancel_requested` |
| Outputs | `task.outputs["key"]` | `task.outputs["key"]` |
| Status | `task.status.is_finished()` | `task.status.is_finished()` |
| Event types | UPDATE, COMPLETION, CANCELATION, FAILURE | STARTED, UPDATE, COMPLETION, CANCELATION, FAILURE |
| Typed result | No | `Task[T]` generic |
| Future interop | No | `task.future` → `concurrent.futures.Future[T]` |
| Async/await | No | `result = await task` |
| Async events | No | `async for event in task.events()` |
| Context manager | No | `with task:` / `async with task:` (auto-cancel) |
| Progress ratio | Manual | `task.progress` (computed `current/maximum`) |
| Batch parallel | No | `env.map()` / `env.map_tasks()` |
| Worker pool | No | `env.launch(max_workers=N)` |
| GPU assignment | No | `worker_env=lambda i: {"CUDA_VISIBLE_DEVICES": str(i)}` |
| Execution model | Inline scripts (Groovy/Python) | Python modules + functions |
| Shared memory | No | `NDArray` objects in args/results |
| Late listeners | Lose events | Terminal events replayed |

---

## Module Structure

New files:

```
src/wetlands/
    task.py              # Task, TaskStatus, TaskEvent, TaskEventType, RemoteTaskHandle
```

Modified files:

```
src/wetlands/
    external_environment.py   # Add submit(), submit_script(), map(), map_tasks();
                              #   worker pool management; IPC reader threads
    environment.py            # Add submit(), submit_script(), map(), map_tasks()
                              #   (abstract + base defaults)
    internal_environment.py   # Add submit(), submit_script(), map(), map_tasks()
                              #   (local ThreadPoolExecutor-based execution)
    module_executor.py        # Add task_id to all messages; handle "cancel" action;
                              #   inject RemoteTaskHandle into functions
```

---

## Implementation Notes

1. **Task IDs.** Each task gets a `uuid.uuid4()`. Included in all IPC messages for routing on the client side.

2. **One connection per worker.** Each worker is a separate `module_executor` process with its own port and `multiprocessing.connection`. No multiplexing needed — each connection handles one task at a time. This is simpler than multiplexing multiple tasks over a single connection.

3. **IPC reader thread per worker.** Each worker connection has a dedicated daemon reader thread. The reader receives messages and dispatches them to the associated `Task._on_message()` method. When the task completes, the worker is returned to the idle pool.

4. **Worker dispatch.** A simple idle-pool model:
   - `submit()` checks `_idle_workers` queue. If a worker is available, dispatch immediately. Otherwise, enqueue the task in `_task_queue`.
   - When a worker finishes a task, it dequeues the next pending task (if any) and dispatches it.
   - This is lightweight: no background dispatcher thread needed, just queue operations in `submit()` and in the reader thread's completion handler.

5. **`concurrent.futures.Future[T]` bridge.** Each `Task` wraps a `Future` internally. The reader thread resolves the future when a terminal message arrives. `wait_for()` delegates to `future.result(timeout)`. `__await__` wraps the future for asyncio compatibility.

6. **Terminal event replay.** When a listener is attached after the task has finished, the terminal event (COMPLETION/FAILURE/CANCELATION) is immediately delivered to the new listener. This guarantees that late listeners never miss the final outcome.

7. **`async events()` implementation.** Uses an `asyncio.Queue` internally. The IPC reader thread puts events into the queue via `loop.call_soon_threadsafe()`. The async iterator yields from the queue and stops after a terminal event.

8. **Cancellation delivery.** `task.cancel()` sends `{"action": "cancel", "task_id": ...}` to the worker that owns the task. The server sets `RemoteTaskHandle.cancel_requested = True`. The remote function must check this flag cooperatively and call `task.cancel()` to acknowledge. If the function returns normally after a cancel request without acknowledging, the result is delivered as a normal COMPLETION.

9. **Backward compatibility.** `execute()` and `run_script()` remain unchanged. Internally they can be reimplemented as `submit(...).wait_for().result` once the worker pool architecture is in place, but this is an implementation detail — the public API does not change.

10. **`import_module()` proxy.** Continues to use blocking `execute()` calls. A future enhancement could add `submit_module()` returning a proxy whose methods return `Task` objects.

11. **`map()` implementation.** Submits all tasks up front, then yields results. With `ordered=True`, yields in submission order (may block waiting for earlier tasks even if later ones finish first). With `ordered=False`, uses an internal completion queue to yield results as they arrive — similar to `concurrent.futures.as_completed()`.

12. **Shared memory.** `NDArray` objects work through Tasks exactly as they do through `execute()` today — they are pickled as lightweight descriptors and reconstructed via shared memory attachment. With multiple workers, each worker can attach to the same shared memory block (read-only) or write to its own block.

13. **Logging.** Each worker's stdout flows through its own `ProcessLogger` instance. `RemoteTaskHandle.log()` sends messages routed through the Wetlands logging system with context metadata (`log_source="execution"`, `env_name`, `call_target`). Worker index is included in log context for disambiguation.

14. **Cleanup.** `env.exit()` sends `"exit"` to all workers and waits for them to terminate. If a worker doesn't respond within a timeout, it is killed via `psutil`. All reader threads are daemon threads and terminate automatically.

---

## Design Decisions

### Why `start=True` by default?

Most users want fire-and-forget: `task = env.submit(...)` then later `task.wait_for()`. Requiring an explicit `start()` call (as Appose does) adds ceremony for the common case.

For the less common case where listeners must be attached before execution begins, `start=False` is available. Terminal events are replayed to late listeners anyway, so even with `start=True`, the final outcome is never lost — only transient UPDATE events can be missed.

### Why `Task[T]` is generic?

Static type checkers can propagate the return type:

```python
task: Task[np.ndarray] = env.submit("segment.py", "segment", args=(image,))
masks: np.ndarray = task.wait_for().result  # type checker knows this is np.ndarray
```

In practice, `T` is `Any` because Wetlands cannot introspect the remote function's return type at submit time. But it documents intent and allows users to annotate.

### Why not `asyncio.Task` directly?

`asyncio.Task` is tightly coupled to an event loop and cannot be used from synchronous code. Wetlands' `Task` works in both sync and async contexts. The `__await__` protocol and `events()` async iterator provide first-class async support without requiring an event loop for basic usage.

### Why `timeout` does not auto-cancel?

Matching `concurrent.futures.Future.result()` semantics: `TimeoutError` means "I gave up waiting" not "I want to stop the work." The caller can explicitly cancel after a timeout if desired:

```python
try:
    task.wait_for(timeout=30)
except TimeoutError:
    task.cancel()
    task.wait_for()  # wait for cancellation to complete
```

### Why multi-process instead of multi-thread for concurrency?

See the [Worker Pool](#worker-pool-process-level-parallelism) section. In short: Wetlands' primary use case is scientific computing (numpy, torch, cellpose, stardist). These libraries release the GIL, so separate processes provide true parallelism. Multi-process also eliminates thread-safety concerns for user code, `sys.path`/`sys.argv` issues, and provides failure isolation — all at the cost of ~200-400 MB memory per worker.

### Why `map()` and `map_tasks()` as separate methods?

`map()` follows the `concurrent.futures.Executor.map()` and `multiprocessing.Pool.map()` convention: it yields results directly, hiding the Task machinery. This is the simplest API for batch processing.

`map_tasks()` returns `Task` objects for users who need progress reporting, cancellation, or event listening on individual batch items. It's the power-user variant.
