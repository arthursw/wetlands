# Operations, tasks, and lifecycle states

Wetlands represents work that finishes later with two kinds of objects: **operations** and **execution tasks**.

An operation performs work on the host computer, such as preparing Pixi, provisioning an environment, or removing an environment.
An execution task represents one function call handled by a worker.

Both can be observed, canceled, waited for, or awaited with `asyncio`.

## Lifecycle states

Operations and tasks use the same five state names:

| State | Meaning |
| --- | --- |
| `PENDING` | The work has been created but has not started. |
| `RUNNING` | The work is active. |
| `COMPLETED` | The work finished and produced its result. |
| `FAILED` | The work stopped with an error. |
| `CANCELED` | Cancellation and required cleanup finished. |

The last three states are **terminal states**.
A terminal object will not change state again.

## Operations

Preparing Pixi and provisioning or removing an environment can start subprocesses and change files below the manager root.
Wetlands publishes events while this work runs.

```python
operation = manager.provision("analysis", spec)
operation.listen(lambda event: print(event.message))
environment = operation.wait_for(timeout=600)
```

`wait_for()` blocks the current thread until the operation finishes.
It returns the result for a completed operation and raises a public exception for failure, cancellation, or a waiting timeout.

## Execution tasks

Submitting a call returns immediately with an `ExecutionTask`:

```python
task = workers.submit_import(
    "analysis_package.pipeline:run",
    args=(data,),
)
result = task.wait_for()
```

The task moves from the worker-pool queue to a worker and then to a terminal state.
Listeners receive immutable event snapshots with an increasing sequence number.

## Timeout is not cancellation

`wait_for(timeout=5)` limits how long the caller waits.
It does not tell the worker to stop.

Call `task.cancel()` explicitly if the remote work should end.
Read [Handle timeouts](how-to/timeouts.md) for a runnable example.

## Cancellation and cleanup

Cancellation is a request, not an immediate state change.
A task becomes `CANCELED` only after the worker has cooperated or Wetlands has stopped and replaced an unresponsive worker.

An operation becomes `CANCELED` only after active subprocesses stop and incomplete state is cleaned up.
This rule prevents callers from seeing a terminal state while required cleanup is still running.

Read [Cancel a task](how-to/cancel_tasks.md) for cooperative and forced cancellation examples.

## Worker replacement

The pool replaces a worker after a crash, a broken connection, or forced cancellation.
Queued tasks can then continue on the healthy pool.

`worker_timeout` detects a worker that has stopped sending messages.
It is an inactivity timeout, not a maximum duration for a task.

## Related reference

- [Events and logging](logging.md)
- [Injected task context](reference/task_context.md)
- [Errors and failure categories](reference/errors.md)
