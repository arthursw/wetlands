# Handle timeouts

A timeout passed to `wait_for()` limits how long the caller blocks.
It does not stop the task running in the worker.

## Complete example

<!-- fmt: off -->
```python
--8<-- "examples/task_timeout.py"
```
<!-- fmt: on -->

Representative output is:

```text
Wait timed out; task state is still running
Task canceled after cleanup
```

After `TimeoutError`, choose one of three actions:

- Call `wait_for()` again to keep waiting.
- Leave the task running and observe it through events.
- Call `cancel()` when the work is no longer useful.

The `worker_timeout` argument to `environment.start()` is different.
It detects a worker that stops sending any protocol messages and is intended for health monitoring, not as a task deadline.

See [Cancel a task](cancel_tasks.md) for cooperative and forced cancellation behavior.
