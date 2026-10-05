# Errors and failure categories

Wetlands uses specialized exceptions for lifecycle operations and `ExecutionError` for failed worker calls.

## Operation exceptions

| Exception | Meaning |
| --- | --- |
| `PreparationError` | Pixi discovery, download, or verification failed. |
| `ProvisioningError` | Environment creation, installation, or validation failed. |
| `RemovalError` | A managed environment could not be removed cleanly. |
| `OperationCanceled` | An operation or task completed cancellation and cleanup. |
| `TimeoutError` | The caller's `wait_for()` deadline expired; underlying work continues. |

An `OperationError` has a `failure` record containing:

- `operation_id` and optional `environment`;
- `stage` and optional `step_id`;
- a human-readable `message`;
- a sanitized `command` display and `returncode` when a subprocess failed;
- bounded `stdout_tail` and `stderr_tail` values;
- an optional `cleanup_error`.

## Runtime content admission

`RuntimeContentUnavailableError` reports missing, inconsistent, or stale ready-content authority without changing the environment.
`EditableRuntimeSourceChangedError` specializes it for a changed editable import footprint and exposes `distribution_name`, `source_root`, `expected_digest`, and `actual_digest`.
Close workers and explicitly recreate that environment before executing changed source; receipt inspection does not hot reload or remove it.
See [installed runtime content](../concepts/environment_identity.md#installed-runtime-content) for the owner-managed scope and manual-mutation limits.

## Execution failures

`ExecutionError.failure` is an `ExecutionFailure` with a stable category:

| Category | Meaning |
| --- | --- |
| `REMOTE_EXCEPTION` | The target callable raised in the worker. |
| `INTERNAL_EXCEPTION` | Host-side execution handling raised unexpectedly. |
| `SERIALIZATION` | An argument, result, or intermediate value could not be encoded or decoded. |
| `WORKER_CONNECTION` | Communication with the assigned worker failed. |
| `WORKER_DIED` | The assigned worker process exited. |
| `TIMEOUT` | Worker health monitoring detected prolonged inactivity. |
| `ENVIRONMENT` | The managed environment could not execute the task. |
| `UNKNOWN` | The failure did not match a more specific category. |

The record may include task and target identities, remote traceback information, worker identity, process exit information, timeout details, or serialization context.

For `REMOTE_EXCEPTION`, inspect `failure.remote_exception` for the remote module, type name, message, traceback, cause, and context.

## Managed-process errors

Managed commands keep their process outcome separate from worker-call failures:

| Exception | Meaning |
| --- | --- |
| `ProcessExitError` | A command exited non-zero while checked behavior was requested; inspect `result`. |
| `ProcessTimeoutError` | A command wait deadline won and its owned tree was cleaned; inspect `timeout` and the partial `result`. |
| `ProcessOutputLimitError` | Combined stdout and stderr crossed the configured raw-byte limit; inspect `limit`, `truncated_streams`, and the partial `result`. |
| `ProcessLineTimeoutError` | `wait_for_line()` found no match before its deadline; the command continues. |
| `ProcessEventLagError` | An output observer fell behind the bounded event history; the command and other observers continue. |
| `ProcessCleanupError` | Wetlands could not verify process-tree termination, pipe-reader completion, or reaping; cleanup remains retryable. |

Every `ProcessError` includes the requested argv, environment name, and generation ID.
Input-validation and operating-system launch errors keep their ordinary Python exception types.
See [Run commands and services](../how-to/managed_processes.md) for the process and output lifecycle.

## Lifecycle and environment errors

The public API also exposes errors for recipe conflicts, missing or unmanaged environments, live workers that prevent removal, generation changes, worker startup, invalid local packages, invalid task state, unsupported values, and manager shutdown cleanup.

Use the generated **Errors and diagnostics** page under **Python API** in the navigation for exact constructors, attributes, and inheritance.
See [Handle execution and provisioning errors](../how-to/errors.md) for recovery examples.
