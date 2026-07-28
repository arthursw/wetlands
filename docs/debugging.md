# Debugging workers

Wetlands workers execute ordinary Python in a Pixi environment, so normal remote-debugging tools can attach to a running worker process.

## Prepare a debug environment

Include the debugger in the environment recipe and keep the source package editable:

```python
from pathlib import Path

from wetlands import EnvironmentSpec, LocalPackage

spec = EnvironmentSpec(
    python="3.12.*",
    pypi=("debugpy",),
    local=(LocalPackage(Path("."), editable=True),),
)

environment = manager.provision(
    "worker-debug",
    spec,
    replace_existing=True,
).wait_for()
```

Start one worker while debugging so task routing is deterministic:

```python
pool = environment.start(workers=1)
```

## Target local source explicitly

```python
task = pool.submit_path(
    "worker_package/pipeline.py",
    "run",
    args=(data,),
    cache=False,
)
```

Disabling the path cache makes edits visible on subsequent submissions.
For installed-package execution, restart the pool after changing imported modules because normal Python import caching applies within a warm worker.

## Diagnose provisioning

Provisioning operations expose the failed stage, sanitized command, return code, and bounded output tails:

```python
from wetlands import ProvisioningError

operation = manager.provision("worker-debug", spec, replace_existing=True)

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

## Diagnose worker failures

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

## Cleanup while debugging

Always close the pool when the debugger detaches:

```python
pool.close()
manager.close()
```

If a debugging session terminates the host abruptly, the worker-health and persistent-worker state under the manager root can help identify a surviving local process.
Only attach to workers created by trusted local applications.
