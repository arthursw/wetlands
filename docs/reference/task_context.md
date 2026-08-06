# Injected task context

A worker callable can receive a task context for progress reporting, intermediate output, logging, and cooperative cancellation.

The host requests injection explicitly:

```python
task = workers.submit_import(
    "worker_package.pipeline:run",
    args=(items,),
    context_keyword="task",
)
```

The target must accept that keyword or `**kwargs`.
The keyword must be a valid Python identifier and cannot also appear in submitted `kwargs`.

The injected object is intentionally not a separately exported public class.
Worker code should use the interface documented below.

## `cancel_requested`

```python
task.cancel_requested: bool
```

Becomes `True` after the host calls `ExecutionTask.cancel()`.
Long-running worker code should check it at safe interruption points.

## `update()`

```python
task.update(
    message=None,
    *,
    current=None,
    maximum=None,
)
```

Publishes an `ExecutionEventKind.UPDATE` event.
`message` must be a string when supplied.
`current` and `maximum` must be non-negative integers when supplied.
When both are present and `maximum` is greater than zero, the host event's `progress` is `current / maximum`.

## `set_output()`

```python
task.set_output(key, value)
```

Publishes a small named intermediate value and adds it to the host task's `outputs` mapping.
The key must be a nonempty string.

Values may contain `None`, booleans, integers, floats, strings, bytes, nested lists and tuples, and dictionaries whose keys are also simple scalar values.
Cyclic values, arbitrary objects, NumPy arrays, and extension-codec values are rejected.

Later calls with the same key replace the value visible on the host.

## Worker-side `cancel()`

```python
task.cancel()
```

Marks the worker function as canceled.
The function should finish its own cleanup and return promptly afterward; after it returns, the host task reaches `ExecutionState.CANCELED` instead of publishing the function's return value.
Worker code normally calls this after observing `cancel_requested`, but it can also use it to cancel itself.

This is different from host-side `ExecutionTask.cancel()`, which requests that the remote task stop.

## `log()`

```python
task.log(message, level=logging.INFO)
```

Sends a string to the host's `wetlands` logger at the integer logging level provided.
The host application controls handlers, formatting, and destinations.

## Example

See [Report progress, intermediate output, and worker logs](../how-to/progress.md) for a complete runnable example.
