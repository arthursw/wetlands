# Migrate from Wetlands 1

Wetlands 2 is a major release and intentionally does not preserve the Wetlands 1 public API.
The migration separates construction, provisioning, and execution and removes application-facing transport management.

## Summary

| Wetlands 1 | Wetlands 2 |
| --- | --- |
| Constructor may prepare tooling | Constructor stores configuration only |
| Manager chosen through constructor flags | Pixi is the sole environment manager |
| Dependency dictionaries | Immutable `EnvironmentSpec` |
| `create()` returns an environment synchronously | `provision()` returns an operation that can be observed, waited for, or awaited |
| Environment launches itself and executes calls | `ManagedEnvironment.start()` returns a `WorkerPool` |
| Manager executes shell commands and exposes `Popen` | `ManagedEnvironment.run()` and `spawn()` own argv-based commands |
| Filename-stem module imports | Qualified package targets or explicit path targets |
| Manual `NDArray` transport | Automatic ordinary-value and NumPy transport |
| Ad hoc provisioning commands | Structured stages, events, cancellation, and failures |
| Debug mode selected before worker startup | Post-hoc debugger attachment to a running worker |
| Reconnection through legacy environment state | Explicit persistent-pool `detach()` and `attach_pool()` |

## Manager construction

Before:

```python
manager = EnvironmentManager(
    wetlands_instance_path="wetlands",
    manager="pixi",
)
```

After:

```python
from wetlands import EnvironmentManager

manager = EnvironmentManager(root="wetlands")
```

There is no manager-selection parameter.
Use `pixi_executable=` only to provide an existing Pixi executable.

## Explicit preparation

Before, construction or the first synchronous operation could install the environment tool.

After:

```python
preparation = manager.prepare()
preparation.listen(on_event)
pixi = preparation.wait_for()
```

Applications may skip the explicit call because `provision()` performs preparation as a coordinated first stage.
The explicit form is useful for startup activity reporting and early network or installation failures.

## Environment recipes

Before:

```python
dependencies = {
    "python": "3.10",
    "conda": ["cellpose==3.1.0"],
    "pip": ["worker-package"],
}
environment = manager.create("cellpose", dependencies)
```

After:

```python
from wetlands import EnvironmentSpec

spec = EnvironmentSpec(
    python="3.10.*",
    conda=("cellpose==3.1.0",),
    pypi=("worker-package",),
)
environment = manager.provision("cellpose-v1", spec).wait_for()
```

Use tuples and typed `LocalPackage` or `PostInstallCommand` values instead of manager-specific dictionary keys.
Use `pixi_lock=` when an application supplies a pre-resolved lockfile.
Regenerate older lockfiles with the Wetlands 2 manifest because supplied locks must include Wetlands' managed worker-runtime dependency.

## Execution

Before:

```python
environment.launch()
result = environment.execute(
    "worker.py",
    "segment",
    kwargs={"image": image},
)
environment.exit()
```

After, for an installed package:

```python
with environment.start() as pool:
    result = pool.execute_import(
        "worker_package.segmentation:segment",
        kwargs={"image": image},
    )
```

After, for local source development:

```python
with environment.start() as pool:
    result = pool.execute_path(
        "worker.py",
        "segment",
        kwargs={"image": image},
        cache=False,
    )
```

Production integrations should install a worker package into the environment and use `submit_import()` or `execute_import()`.
Path execution is explicit and never relies on filename stems or `sys.path` mutation.

## External commands and services

Wetlands 2 does not restore `EnvironmentManager.execute_commands()` or expose `subprocess.Popen`.
The replacement belongs to the provisioned environment so Wetlands can preserve generation identity and own process-tree cleanup.

Use `run()` for a short command installed in the environment:

```python
result = environment.run(["example-cli", "--version"], timeout=30)
print(result.stdout)
```

Use `spawn()` for a service, GUI, or another independently supervised command:

```python
with environment.spawn(["example-server", "--port", "0"]) as process:
    ready = process.wait_for_line(
        lambda event: event.text.startswith("Listening on "),
        timeout=30,
    )
    print(ready.text, end="")
```

Both methods accept only argument vectors and execute through the exact immutable Pixi generation.
Managed processes block removal or same-name replacement while active and are terminated when their manager closes.
See [Run commands and services](how-to/managed_processes.md) for output, timeout, environment-overlay, asyncio, and cross-platform cleanup semantics.

## NumPy transport

Before:

```python
from wetlands.ndarray import NDArray

with NDArray(image) as remote_image:
    output = environment.execute("worker.py", "segment", args=(remote_image,))
```

After:

```python
with environment.start() as pool:
    output = pool.execute_import(
        "worker_package.segmentation:segment",
        args=(image,),
    )
```

Do not allocate output shared memory or call transport cleanup methods.
Wetlands copies inputs, owns shared-memory leases, copies results into caller-owned arrays, and cleans up on every terminal path.

Object-dtype arrays and unsupported Python objects now fail explicitly at submission.

## Debug a running worker

Do not pass a debug flag to `EnvironmentManager`, declare `debugpy` in the recipe, or call `debugpy.listen()` from worker code.
Run the application normally and attach after a problem appears:

```console
wetlands workers --root ./wetlands --environment cellpose-v1
wetlands debug --root ./wetlands --environment cellpose-v1 --worker WORKER_ID --editor vscode --source .
```

Wetlands owns the worker-compatible `debugpy` version and starts its adapter lazily through an authenticated management connection.
See [Debug a running worker](debugging.md) for editor configuration and reconnection behavior.

## Persistent worker reconnection

Start a pool with `persistent=True`, call `pool.detach()` to leave its workers alive, and use `environment.attach_pool()` from a later controller.
Only one process may own task dispatch at a time; debugger attachment is independent of that execution-controller claim.
See [Keep and reconnect to persistent workers](persistent_workers.md) for the complete lifecycle.

## Non-blocking and asyncio use

Callbacks and blocking waits:

```python
operation = manager.provision("cellpose-v1", spec)
operation.listen(on_event)
environment = operation.wait_for()

with environment.start() as pool:
    task = pool.submit_import(
        "worker_package.segmentation:segment",
        args=(image,),
    )
    task.listen(on_task_event)
    output = task.wait_for()
```

Async use:

```python
import asyncio


environment = await manager.provision("cellpose-v1", spec)

pool = await asyncio.to_thread(environment.start)
try:
    output = await pool.submit_import(
        "worker_package.segmentation:segment",
        args=(image,),
    )
finally:
    await asyncio.to_thread(pool.close)
```

Use `operation.events()` or `task.events()` for async event consumption.
The embedding application owns its event loop and UI-thread integration.
Run blocking pool startup and shutdown, and manager shutdown, with `asyncio.to_thread()`.

## Replacement and upgrades

Wetlands 2 never reuses an incomplete environment.
Failed, canceled, and crash-interrupted provisioning is removed and rebuilt.

`replace_existing=True` can remove an old same-name environment before its replacement succeeds.
Applications that require rollback should include a recipe or release identity in the managed environment name, provision the new name, then update their own logical mapping.

## Removed assumptions

- Construction is no longer a provisioning action.
- Arbitrary dependency dictionaries are no longer accepted.
- Loading an unmanaged same-name environment is not part of the managed API.
- Worker packages do not import Wetlands transport types.
- Installed-package execution does not infer modules from filenames.
- Debugging does not require an application-wide mode chosen before worker startup.
- Legacy manager command APIs and raw subprocess handles are not restored.
