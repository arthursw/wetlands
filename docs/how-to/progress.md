# Report progress, intermediate output, and worker logs

Use an injected task context when worker code needs to report activity before returning its final result.

The submitter chooses the keyword name explicitly with `context_keyword`.
Wetlands never changes a function call by guessing from its signature.

## Complete example

<!-- fmt: off -->
```python
--8<-- "examples/task_progress.py"
```
<!-- fmt: on -->

The worker function used by the example calls `task.update()`, `task.set_output()`, and `task.log()`.
Its source is in `examples/example_module.py`.

Representative output is:

```text
Progress: 1/4 (25%)
Progress: 2/4 (50%)
Progress: 3/4 (75%)
Progress: 4/4 (100%)
INFO: Worker finished processing items
Intermediate output: 4 items
Result: [2, 4, 6, 8]
```

## Listen only for progress updates

Every task also emits started, completion, failure, and cancellation events.
Filter for `ExecutionEventKind.UPDATE` when a callback only renders progress.
An intermediate-output change is also an update and repeats the latest numeric progress, so the example renders only changed `current` and `maximum` pairs.

Listeners may run on Wetlands background threads.
A desktop application must forward UI changes through its toolkit's thread-safe mechanism.

## Choose the right output channel

- Use `update()` for a human-readable status and numeric progress.
- Use `set_output()` for a small named value the caller may inspect through `task.outputs`.
- Use `log()` for diagnostic messages handled by the application's `wetlands` logger configuration.
- Return large values and NumPy arrays as the final task result.

Intermediate values are intentionally limited to small supported Python values.
NumPy arrays are not supported as intermediate outputs.

See [Injected task context](../reference/task_context.md) for exact method signatures and validation rules.
