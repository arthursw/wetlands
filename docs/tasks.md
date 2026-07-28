# Operations and execution tasks

Wetlands uses observable asynchronous objects for both host-side lifecycle work and worker execution.

`PreparationOperation[T]` and `ProvisioningOperation[T]` represent host-side work.
`ExecutionTask[T]` represents one worker call.

All three support callbacks, blocking waits, cancellation, awaiting, and async event streams.
No running event loop is required for synchronous use.

## Preparation and provisioning operations

```python
operation = manager.provision("analysis", spec)

operation.listen(lambda event: print(event.message))
environment = operation.wait_for(timeout=600)
```

Operation states are:

- `PENDING`;
- `RUNNING`;
- `COMPLETED`;
- `FAILED`;
- `CANCELED`.

Events include a monotonically increasing sequence, timestamp, operation ID, kind, current state, human-readable message, and optional stage, step, stream, output line, or progress fields.

```python
from wetlands import OperationEventKind


def on_event(event) -> None:
    if event.kind is OperationEventKind.OUTPUT:
        print(f"[{event.stage}:{event.stream}] {event.line}")


operation.listen(on_event)
```

Listeners may run on Wetlands background threads.
GUI applications must marshal UI mutations onto their own UI thread.

## Structured failure

Failed operations raise a specialized `OperationError`.
Its `failure` value identifies the operation, stage, safe command display, return code, bounded output tails, environment name, and any cleanup failure.

```python
from wetlands import ProvisioningError

try:
    environment = operation.wait_for()
except ProvisioningError as error:
    print(error.failure.stage)
    print(error.failure.command)
    print(error.failure.stderr_tail)
```

Wetlands sanitizes command displays and captured output before publishing them.

## Provisioning cancellation

```python
operation = manager.provision("analysis", spec)
operation.cancel()

try:
    operation.wait_for()
except OperationCanceled:
    pass
```

`cancel()` requests cancellation and returns immediately.
It is idempotent and returns `False` after a terminal state.

The operation reaches `CANCELED` only after the active subprocess tree has terminated and cleanup has completed.
Wetlands first requests graceful termination, waits for the configured grace period, then forcibly terminates surviving processes.

## Execution tasks

```python
task = pool.submit_import(
    "analysis_package.pipeline:run",
    kwargs={"input_data": data},
)

task.listen(
    lambda event: print(
        event.sequence,
        event.kind.value,
        event.state.value,
        event.message,
        event.progress,
    )
)
result = task.wait_for()
```

Execution task states are `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, and `CANCELED`.

Execution events are immutable snapshots containing a monotonically increasing sequence, timestamp, task ID, kind, state, message, and progress fields.
Listeners replay the bounded event history by default; pass `replay=False` to observe only future events.

`wait_for(timeout)` does not cancel the task when its timeout expires.
Call `cancel()` explicitly when the work should stop.

Remote failures retain the remote exception identity, traceback, task ID, call target, and worker diagnostics.

## Progress and cooperative cancellation

A worker callable may optionally accept a runtime-context keyword chosen by the submitter.
Wetlands never infers injection from a callable signature.

```python
def process(items, task=None):
    results = []

    for index, item in enumerate(items):
        if task is not None and task.cancel_requested:
            task.cancel()
            return None

        results.append(process_one(item))

        if task is not None:
            task.update(
                f"Processed {index + 1} items",
                current=index + 1,
                maximum=len(items),
            )

    return results
```

Request the injection explicitly when submitting:

```python
task = pool.submit_import(
    "analysis_package.pipeline:process",
    args=(items,),
    context_keyword="task",
)
```

Progress messages and intermediate outputs are initially limited to simple supported values.
NumPy arrays should be returned as the terminal result rather than published as intermediate output.

Cancellation is cooperative for running Python code.
The worker observes `task.cancel_requested` and may acknowledge it with `task.cancel()`.
If a worker becomes unhealthy or disconnects, Wetlands fails its assigned task and replaces the worker.

## Async use

Operations and tasks can be awaited directly:

```python
environment = await manager.provision("analysis", spec)

with environment.start() as pool:
    result = await pool.submit_import(
        "analysis_package.pipeline:run",
        kwargs={"input_data": data},
    )
```

Events are available through an async iterator:

```python
operation = manager.provision("analysis", spec)

async for event in operation.events():
    render_activity(event)

environment = await operation
```

The terminal event closes the stream.
The event adapter safely moves notifications from Wetlands threads to the caller's event loop.

Canceling an awaiting coroutine requests cancellation of the underlying operation or task and waits for mandatory cleanup before propagating `asyncio.CancelledError`.

## Worker pools

```python
with environment.start(workers=4, worker_timeout=300) as pool:
    tasks = [
        pool.submit_import("analysis_package.pipeline:run", args=(item,))
        for item in items
    ]
    results = [task.wait_for() for task in tasks]
```

Workers are warm and process tasks from the pool queue.
Health monitoring detects disconnects and inactivity.
A replacement worker is started when a pool worker fails.

`persistent=True` keeps trusted local workers alive when a controller detaches.
Persistent pools use authenticated loopback connections and exclusive controller ownership.
They do not change Wetlands' trusted-local execution model.
