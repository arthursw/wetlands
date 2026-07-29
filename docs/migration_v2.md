# Migrating to Wetlands 2

Wetlands 2 is a major release and intentionally does not preserve the Wetlands 1 public API.
The migration separates construction, provisioning, and execution and removes application-facing transport management.

## Summary

| Wetlands 1 | Wetlands 2 |
| --- | --- |
| Constructor may prepare tooling | Constructor stores configuration only |
| Manager chosen through constructor flags | Pixi is the sole environment manager |
| Dependency dictionaries | Immutable `EnvironmentSpec` |
| `create()` returns an environment synchronously | `provision()` returns an observable operation |
| Environment launches itself and executes calls | `ManagedEnvironment.start()` returns a `WorkerPool` |
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

## Debugging running workers

Do not pass a debug flag to `EnvironmentManager`, declare `debugpy` in the recipe, or call `debugpy.listen()` from worker code.
Run the application normally and attach after a problem appears:

```console
wetlands workers --root ./wetlands --environment cellpose-v1
wetlands debug --root ./wetlands --environment cellpose-v1 --worker WORKER_ID --editor vscode --source .
```

Wetlands owns the worker-compatible `debugpy` version and starts its adapter lazily through an authenticated management connection.
See [Debugging running workers](debugging.md) for editor configuration and reconnection behavior.

## Persistent worker reconnection

Start a pool with `persistent=True`, call `pool.detach()` to leave its workers alive, and use `environment.attach_pool()` from a later controller.
Only one process may own task dispatch at a time; debugger attachment is independent of that execution-controller claim.
See [Persistent workers and reconnection](persistent_workers.md) for the complete lifecycle.

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
environment = await manager.provision("cellpose-v1", spec)

with environment.start() as pool:
    output = await pool.submit_import(
        "worker_package.segmentation:segment",
        args=(image,),
    )
```

Use `operation.events()` or `task.events()` for async event consumption.
The embedding application owns its event loop and UI-thread integration.

## Replacement and upgrades

Wetlands 2 never reuses an incomplete environment.
Failed, canceled, and crash-interrupted provisioning is removed and rebuilt.

`replace_existing=True` can remove an old same-name environment before its replacement succeeds.
Applications that require rollback should include a recipe or release identity in the physical environment name, provision the new name, then update their own logical mapping.

## Removed assumptions

- Construction is no longer a provisioning action.
- Arbitrary dependency dictionaries are no longer accepted.
- Loading an unmanaged same-name environment is not part of the managed API.
- Worker packages do not import Wetlands transport types.
- Installed-package execution does not infer modules from filenames.
- Debugging does not require an application-wide mode chosen before worker startup.
