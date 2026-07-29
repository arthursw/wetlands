# Debugging running workers

You do not need to enable a special debug mode before starting your application.
Wetlands workers include the debug adapter as part of their managed runtime, but they do not start a debug listener until you request one.

This supports the usual debugging workflow: run the application normally, notice a problem, attach a debugger to its existing worker, and reproduce the task.

## Find the worker

List the live workers for an environment:

```console
wetlands workers --root ./wetlands --environment analysis
```

The command reports each worker's stable runtime ID, pool index, process ID, pool, persistence, and debugger status.

If only one worker is live, start its adapter with:

```console
wetlands debug --root ./wetlands --environment analysis
```

If several workers are live, select the worker that runs the task:

```console
wetlands debug \
  --root ./wetlands \
  --environment analysis \
  --worker WORKER_ID
```

The command prints the loopback host and port accepted by the worker's `debugpy` adapter.
Starting the adapter is idempotent: running the command again reports the same endpoint while that worker remains alive.

## Open VS Code

Wetlands can generate a VS Code workspace containing an attach configuration:

```console
wetlands debug \
  --root ./wetlands \
  --environment analysis \
  --worker WORKER_ID \
  --editor vscode \
  --source .
```

The generated `.code-workspace` file is stored below the Wetlands root at `state/debug/workspaces/`.
Wetlands does not create or overwrite `.vscode/launch.json` in your project.

Use `--no-launch` to generate the workspace without invoking the `code` command:

```console
wetlands debug \
  --root ./wetlands \
  --environment analysis \
  --worker WORKER_ID \
  --editor vscode \
  --source . \
  --no-launch
```

After VS Code opens, select the generated **Attach to Wetlands worker** configuration and start debugging.
Set breakpoints, then make the application submit the problematic task again.

## Use another debugger

Without `--editor`, the command prints a generic Debug Adapter Protocol endpoint:

```text
Adapter: debugpy
Host: 127.0.0.1
Port: 43123
```

Configure a `debugpy`-compatible editor to attach to that host and port.
The debug adapter listens only on the local loopback interface.
It is not itself authenticated, so another process running on the same machine may be able to attach and control the worker.

## Reconnect after detaching

The adapter belongs to the worker, not to the CLI process or editor session.
It remains active until the worker exits.

If the editor disconnects, run `wetlands debug` again and attach to the reported endpoint.
Debugger access is independent of execution-controller ownership, so this works while the original application remains connected to the pool and continues dispatching tasks.

A debugger cannot restore a Python stack that has already failed and unwound.
After attaching, rerun or otherwise reproduce the failed operation.
A currently blocked or long-running Python task can be inspected after attachment when the debugger can pause that thread.

## Source files and warm workers

An editable local package is convenient during development because the source open in the editor has the same path as the module loaded in the Pixi environment:

```python
from pathlib import Path

from wetlands import EnvironmentSpec, LocalPackage

spec = EnvironmentSpec(
    python="3.12.*",
    local=(LocalPackage(Path("."), editable=True),),
)
```

Editable installation is not required to start the debugger.
For a non-editable package, open the source installed in the managed environment or configure the editor's local-to-remote source mapping.

Normal Python import caching still applies in warm workers.
Restart the worker pool after changing an installed module, or use `submit_path(..., cache=False)` for source files that should be reloaded on every call.

With several workers, a reproduced task may run on a worker other than the one carrying the breakpoint.
Use one worker while investigating routing-sensitive problems, or attach to each relevant worker separately.

## Diagnose failures without an interactive debugger

Provisioning operations expose their failed stage, sanitized command, return code, and bounded output tails:

```python
from wetlands import ProvisioningError

operation = manager.provision("analysis", spec, replace_existing=True)

try:
    environment = operation.wait_for()
except ProvisioningError as error:
    print(error.failure.stage)
    print(error.failure.command)
    print(*error.failure.stderr_tail, sep="\n")
```

Attach a listener before waiting to see live Pixi output:

```python
operation.listen(lambda event: print(event.stage, event.message))
```

Execution failures retain the remote traceback and task context:

```python
try:
    result = task.wait_for()
except Exception:
    print(task.error)
    print(task.traceback)
    raise
```

A worker crash or protocol mismatch is reported separately from an exception raised by the target callable.
Unhealthy workers are removed and replaced by the pool.

## Trust model

Debugger attachment allows inspection and modification of code running with the current user's privileges.
The worker management request is authenticated, but the resulting debug adapter is a loopback service for trusted local use.
Another local process may be able to attach to that adapter and control the worker.
Do not expose its port outside the local machine or use it on a machine where other users or processes are not trusted.
