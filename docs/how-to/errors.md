# Handle execution and provisioning errors

Wetlands reports environment setup failures separately from worker-call failures.
Catch the specific public exception so your application can show a useful message and choose a safe recovery.

## Complete example

<!-- fmt: off -->
```python
--8<-- "examples/task_errors.py"
```
<!-- fmt: on -->

The example first catches an exception raised by worker code.
It then catches a deliberately failing post-install command and provisions a corrected environment under the same name.

Representative output is:

```text
Category: remote_exception
Target: example_module:raise_example_error
Remote error: ValueError: The worker could not process this input
Traceback (most recent call last):
...
Provisioning stage: post_install
Safe command: python -c ...
Return code: 7
deliberate setup failure
Corrected retry result: 42
```

Traceback paths and the exact safe command display depend on the local installation.

## Worker-call failures

`ExecutionError.failure` is an `ExecutionFailure` record.
Use its stable `category` to decide how to respond and its other fields for diagnostics.

For a remote Python exception, `remote_exception` contains the exception module, type, message, traceback, cause, and context.
The original exception object cannot cross the worker boundary and is not re-raised in the host.

## Provisioning failures

`ProvisioningError.failure` identifies the failed stage, safe command display, return code, bounded output tails, environment name, and any cleanup failure.

Wetlands removes an incomplete environment before the operation becomes terminal.
A corrected call can therefore use the same environment name, as the example demonstrates.

Command displays and captured output are sanitized, but applications should still avoid printing secrets from their own commands.

## Decide what to do

| Situation | Inspect | Typical response |
| --- | --- | --- |
| Preparation or provisioning failed | `PreparationError` or `ProvisioningError` and `error.failure.stage` | Correct connectivity, dependency, or command configuration, then retry. |
| Worker function raised | `ExecutionError` with category `REMOTE_EXCEPTION` | Report the remote type/message; retry only if the function and input make retry safe. |
| Unsupported argument or result | `ValueEncodingError`, `ValueDecodingError`, or category `SERIALIZATION` | Convert the value to a supported type before retrying. |
| Worker crashed or disconnected | Category `WORKER_DIED` or `WORKER_CONNECTION` | Record diagnostics; the pool replaces the worker, so a safe idempotent task may be retried. |
| Waiting timed out | Built-in `TimeoutError` | Continue waiting or explicitly cancel; the task was not canceled automatically. |
| Cancellation completed | `OperationCanceled` | Treat it as an expected user action when cancellation was requested. |

See [Errors and failure categories](../reference/errors.md) for the complete field reference.
