# Cancel a task

Call `ExecutionTask.cancel()` when the remote work should stop.

Wetlands first gives running Python code a chance to stop cooperatively.
If the code does not finish during the manager's `termination_grace`, Wetlands stops that worker process and creates a replacement.

## Complete example

<!-- fmt: off -->
```python
--8<-- "examples/task_cancellation.py"
```
<!-- fmt: on -->

Representative output is:

```text
Cooperative task canceled after worker cleanup
Non-cooperative task canceled after worker replacement
Replacement worker result: 42
```

## Cooperative cancellation

Cooperative worker code periodically checks the injected context's `cancel_requested` property.
When it sees a request, it finishes its own cleanup and calls the worker-side `task.cancel()` to acknowledge cancellation.

This is the preferred path for loops that can safely stop between items.
Check often enough that cancellation remains responsive, but do not skip application cleanup.

## Forced cancellation

Some library calls block for a long time and cannot check `cancel_requested`.
After `termination_grace` seconds, Wetlands terminates the worker's process tree, waits for transfer cleanup, marks the task canceled, and starts a replacement worker.

Forced cancellation loses process-local state held by that worker.
The follow-up call in the example proves that the pool is usable again.

## Catch the terminal result

`cancel()` returns immediately after recording the request.
`wait_for()` raises `OperationCanceled` only after cancellation and required cleanup are complete.

Calling `cancel()` again is harmless.
It returns `False` after a task has reached a terminal state.

See [Operations, tasks, and lifecycle states](../tasks.md) for the complete state model.
