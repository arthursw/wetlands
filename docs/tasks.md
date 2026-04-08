

### Tasks and Parallel Execution

Wetlands provides a task-based API for non-blocking execution, progress reporting, cancellation, and parallel processing across multiple worker processes.

Every call to [`env.submit()`][wetlands.environment.Environment.submit] or [`env.submit_script()`][wetlands.environment.Environment.submit_script] returns a [`Task[T]`][wetlands.task.Task] object that you can monitor, cancel, or wait on.

#### Task lifecycle

```
PENDING ──(start)──> RUNNING ──(success)──> COMPLETED
                        │
                        ├──(error)──────> FAILED
                        │
                        └──(cancel)──> CANCELED
```

By default, `submit()` starts the task immediately (`start=True`). With `start=False`, the task stays `PENDING` until `task.start()` is called — useful for attaching listeners before execution begins.

You can check whether a task has reached a terminal state with `task.status.is_finished()`.

#### Basic usage

```python
# Submit a function for non-blocking execution
task = env.submit("compute.py", "heavy_computation", args=(data,))

# Do other work while the task runs...
print(f"Status: {task.status}")

# Block for the result when ready
task.wait_for()
print(f"Result: {task.result}")
```

You can also submit scripts:

```python
task = env.submit_script("train.py", args=("--epochs", "10"))
task.wait_for()
```

---

### Task properties

Once a task is created, you can inspect its state through these read-only properties:

| Property | Type | Description |
|----------|------|-------------|
| `id` | `str` | Unique identifier (UUID4) |
| `status` | [`TaskStatus`][wetlands.task.TaskStatus] | Current lifecycle state (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELED`) |
| `result` | `T` | Return value of the remote function. Raises `InvalidStateError` if the task is not `COMPLETED`. |
| `error` | `str \| None` | Error message string when `FAILED`, otherwise `None` |
| `exception` | [`ExecutionException`][wetlands._internal.exceptions.ExecutionException] `\| None` | Exception wrapping the error message and traceback. `None` unless the task has failed. |
| `traceback` | `list[str] \| None` | Traceback lines when `FAILED`, otherwise `None` |
| `message` | `str \| None` | Latest progress message from `update()` |
| `current` | `int \| None` | Current progress counter from `update()` |
| `maximum` | `int \| None` | Maximum progress counter from `update()` |
| `progress` | `float \| None` | Computed as `current / maximum` (a float in [0, 1]). `None` if either value is missing or `maximum` is 0. |
| `outputs` | `dict[str, Any]` | Accumulated named intermediate outputs from `set_output()` |
| `future` | `Future[T]` | Standard `concurrent.futures.Future` — see [interop section](#concurrentfutures-interop) |

!!! note "`result` does not block"

    Unlike `future.result()`, accessing `task.result` never blocks. It returns the value immediately if the task is completed, or raises `InvalidStateError` otherwise. Use `task.wait_for()` or `await task` to wait first.

---

### Progress Reporting

Remote code can report progress by declaring a `task` parameter in the function signature. Wetlands detects it via `inspect.signature()` and injects a [`RemoteTaskHandle`][wetlands.task.RemoteTaskHandle] automatically.

```python
# remote_module.py — runs inside the isolated environment
def long_computation(data, *, task=None):
    results = []
    for i, item in enumerate(data):
        if task and task.cancel_requested:
            task.cancel()  # acknowledge cancellation
            return None
        if task:
            task.update(f"Processing item {i}", current=i, maximum=len(data))
        results.append(expensive_operation(item))
    return results
```

The [`RemoteTaskHandle`][wetlands.task.RemoteTaskHandle] provides:

- `task.update(message, current=, maximum=)` — report progress
- `task.set_output(key, value)` — publish named intermediate outputs (available via `task.outputs[key]` on the caller side)
- `task.cancel_requested` — check if cancellation was requested
- `task.cancel()` — acknowledge cancellation (transitions the task to `CANCELED`)
- `task.log(message, level=)` — send log messages to the caller's logging system

!!! note "Functions without a `task` parameter work exactly as before"

    The `task` parameter is optional. Functions that don't declare it receive no injection and behave identically to a plain `execute()` call.

---

### Event Listeners

On the caller side, you can observe task events by registering a listener:

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

# start=False to register listener before dispatch
task = env.submit("remote_module.py", "long_computation",
                  args=(dataset,), start=False)
task.listen(on_event).start()
task.wait_for()
```

Terminal events (`COMPLETION`, `FAILURE`, `CANCELATION`) are replayed to late listeners, so attaching a listener after the task finishes still delivers the final outcome. Progress updates are transient and not replayed.

A listener can be removed with `task.remove_listener(callback)`.

Each [`TaskEvent`][wetlands.task.TaskEvent] has two fields:

| Field | Type | Description |
|-------|------|-------------|
| `task` | `Task` | The task that emitted the event |
| `type` | `TaskEventType` | The kind of event |

Event types:

| Event | Meaning |
|-------|---------|
| `STARTED` | Task has been dispatched to a worker |
| `UPDATE` | Progress or intermediate output from the remote side |
| `COMPLETION` | Task finished successfully |
| `FAILURE` | Task raised an exception |
| `CANCELATION` | Task was canceled cooperatively |

---

### Cancellation

Cancellation is cooperative: requesting cancellation sets a flag that the remote code checks via `task.cancel_requested`. The remote function must acknowledge cancellation by calling `task.cancel()`.

```python
task = env.submit("simulation.py", "run_simulation", args=(params,))

# ... later ...
task.cancel()
task.wait_for()
print(f"Final status: {task.status.name}")  # COMPLETED or CANCELED
```

If the remote function returns normally after a cancel request without acknowledging, the result is delivered as a normal `COMPLETION`.

---

### Waiting and Timeouts

`task.wait_for()` blocks until the task reaches a terminal state. An optional `timeout` (in seconds) raises `TimeoutError` if exceeded — but does **not** cancel the task:

```python
try:
    task.wait_for(timeout=30)
except TimeoutError:
    print("Still running — deciding whether to cancel...")
    task.cancel()
    task.wait_for()
```

---

### Error Handling

When a task fails, you can inspect the error in several ways:

```python
task.wait_for()

if task.status == TaskStatus.FAILED:
    print(task.error)               # error message string
    print(task.traceback)           # list of traceback lines
    print(task.exception)           # ExecutionException wrapping both

    # Or via the underlying Future:
    print(task.future.exception())  # same ExecutionException
```

The `exception` property returns an [`ExecutionException`][wetlands._internal.exceptions.ExecutionException] that carries both the error message (`.exception`) and the traceback lines (`.traceback`).

---

### Context Managers

Tasks can be used as context managers for automatic cancellation on early exit:

```python
with env.submit("training.py", "train_model", args=(config,)) as task:
    task.wait_for(timeout=300)
# If we exit the block early (exception, timeout, etc.),
# the task is automatically canceled and awaited.
```

Entering the context auto-starts a `PENDING` task. Exiting cancels and waits if the task is still running.

Async context managers are also supported:

```python
async with env.submit("training.py", "train_model", args=(config,)) as task:
    result = await task
```

---

### `concurrent.futures` Interop

Each task wraps a standard `concurrent.futures.Future[T]`, making it easy to integrate with existing concurrent code:

```python
import concurrent.futures

t1 = env_a.submit("segment.py", "segment", args=(image,))
t2 = env_b.submit("segment.py", "segment", args=(image,))

concurrent.futures.wait([t1.future, t2.future])
print(t1.result, t2.result)
```

One-liner via Future:

```python
result = env.submit("compute.py", "fib", args=(50,)).future.result(timeout=60)
```

---

### Async/Await

Tasks are natively awaitable:

```python
import asyncio

async def main():
    result = await env.submit("compute.py", "fibonacci", args=(50,))
    print(f"Result: {result}")

asyncio.run(main())
```

You can also iterate over events asynchronously:

```python
async def monitor(task):
    async for event in task.events():
        match event.type:
            case TaskEventType.UPDATE:
                print(f"Progress: {event.task.progress:.0%}")
            case TaskEventType.COMPLETION:
                print(f"Result: {event.task.result}")

task = env.submit("compute.py", "long_work", args=(data,))
await monitor(task)
```

---

### Parallel Execution

When `max_workers > 1` is passed to `launch()`, Wetlands starts multiple worker processes all sharing the **same Conda environment on disk**. This provides true process-level parallelism with no environment duplication.

```
                                ┌─ worker 0 (pid 1001) ─ port 5001
env.launch(max_workers=4) ──────├─ worker 1 (pid 1002) ─ port 5002
  (one conda env on disk)       ├─ worker 2 (pid 1003) ─ port 5003
                                └─ worker 3 (pid 1004) ─ port 5004
```

Tasks are dispatched to idle workers automatically. When all workers are busy, tasks queue internally and are dispatched as workers become available. The user never sees or manages individual workers.

You can assign specific environment variables per worker, for example to assign GPUs:

```python
env.launch(
    max_workers=4,
    worker_env=lambda i: {"CUDA_VISIBLE_DEVICES": str(i)}
)
```

#### `map()` — batch execution

[`env.map()`][wetlands.environment.Environment.map] distributes work across workers and yields results, similar to `concurrent.futures.Executor.map()`:

```python
env.launch(max_workers=4)

images = load_images("data/")
results = list(env.map("segment.py", "segment", images))
print(f"Segmented {len(results)} images")
```

Use `ordered=False` to yield results as they complete (faster when items have varying processing times):

```python
for result in env.map("analysis.py", "analyze", datasets, ordered=False):
    save_result(result)
```

#### `map_tasks()` — batch execution with full Task control

[`env.map_tasks()`][wetlands.environment.Environment.map_tasks] returns a list of `Task` objects for when you need progress reporting or cancellation on individual items:

```python
tasks = env.map_tasks("segment.py", "segment", images)

for task in tasks:
    task.listen(lambda e: print(f"[{e.task.progress:.0%}] {e.task.message}"))

for task in tasks:
    task.wait_for()
```

---

### GUI Integration

Tasks integrate naturally with GUI frameworks. Since task events are delivered from background threads, use thread-safe mechanisms to update the UI:

```python
from PyQt6.QtCore import pyqtSignal, QObject

class TaskBridge(QObject):
    progress = pyqtSignal(float, str)
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
