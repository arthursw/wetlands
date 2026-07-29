# Getting started

This guide provisions a Pixi environment, starts a worker pool, and calls an installed Python function by qualified name.
The complete example uses NumPy, so it does not require you to create or publish a separate worker package.

## Install Wetlands

```sh
pip install "wetlands[shared-memory]"
```

The optional dependency installs NumPy in the host environment for automatic array transport.
The worker environment must declare its own NumPy dependency.

## Construct the manager

```python
from wetlands import EnvironmentManager

manager = EnvironmentManager(root="wetlands")
```

Construction validates and stores configuration only.
It does not download Pixi, start a subprocess, create an environment, access the network, or create runtime state files.

If an application supplies its own Pixi executable:

```python
manager = EnvironmentManager(
    root="wetlands",
    pixi_executable="/opt/pixi/bin/pixi",
)
```

## Prepare Pixi

```python
operation = manager.prepare()
operation.listen(lambda event: print(event.stage, event.message))
pixi = operation.wait_for()
```

Preparation discovers and validates the configured executable or downloads and verifies Wetlands' registered Pixi release.
The returned `PixiInfo` reports the executable path, version, and whether Wetlands manages it.

The first managed preparation requires network access and may take a few minutes.
Provisioning also downloads the packages declared by the environment recipe.
Wetlands stores its Pixi installation, managed environments, locks, logs, and runtime state below the manager root.

Calling `provision()` without a preceding `prepare()` is also valid.
Provisioning performs preparation as its first coordinated stage.

## Provision an environment

```python
from wetlands import EnvironmentSpec

spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy>=2",),
)

operation = manager.provision("numpy-example", spec)
operation.listen(lambda event: print(event.kind.value, event.message))
environment = operation.wait_for()
```

Provisioning creates a Pixi project under the manager root, resolves or validates `pixi.lock`, installs the declared dependencies, validates the result, and publishes managed metadata last.

The operation emits ordered stages and subprocess output suitable for an activity UI or application log.

Reuse is strict.
A ready environment is reused only when its managed recipe and lockfile match.
Pass `replace_existing=True` to remove and rebuild a different recipe under the same physical name.

## Start workers and execute

```python
import numpy as np

with environment.start(workers=2) as pool:
    image = np.arange(16, dtype=np.float32).reshape(4, 4)
    task = pool.submit_import(
        "numpy:negative",
        args=(image,),
    )
    result = task.wait_for()

np.testing.assert_array_equal(result, -image)
```

`submit_import()` accepts `package.module:qualified.callable`.
Nested class attributes such as `package.module:Processor.run` are valid.

The host input array is copied into transport storage, so worker mutation cannot change the caller's array.
The returned array is a normal independently owned NumPy array.

## Call your own worker package

Worker packages expose ordinary Python functions and declare their dependencies normally:

```python
def threshold(image: "numpy.ndarray", value: float) -> "numpy.ndarray":
    return image > value
```

Install the package in the managed environment with a pinned PyPI requirement or a `LocalPackage`, then call it by qualified name:

```python
with environment.start() as pool:
    task = pool.submit_import(
        "my_worker_package.filters:threshold",
        kwargs={"image": image, "value": 7.5},
    )
    mask = task.wait_for()
```

Wetlands imports the package inside the worker and never imports it into the host process.

## Execute local source during development

For a runnable path-target example, save this as `worker_code.py`:

```python
def threshold(image, value):
    return image > value
```

```python
with environment.start() as pool:
    task = pool.submit_path(
        "worker_code.py",
        "threshold",
        kwargs={"image": image, "value": 7.5},
        cache=False,
    )
    mask = task.wait_for()
```

Use `submit_path()` for editable or local development only.
Installed packages should use `submit_import()` so imports follow normal Python package rules.

## Use asyncio

```python
import asyncio

import numpy as np

from wetlands import EnvironmentManager, EnvironmentSpec


async def run() -> None:
    manager = EnvironmentManager(root="wetlands")

    preparation = manager.prepare()

    async def report() -> None:
        async for event in preparation.events():
            print(event.message)

    reporter = asyncio.create_task(report())
    await preparation
    await reporter

    environment = await manager.provision(
        "numpy-example",
        EnvironmentSpec(
            python="3.12.*",
            conda=("numpy>=2",),
        ),
    )

    with environment.start() as pool:
        image = np.arange(16, dtype=np.float32).reshape(4, 4)
        result = await pool.submit_import(
            "numpy:negative",
            args=(image,),
        )
        print(result)


asyncio.run(run())
```

Wetlands adapts its thread- and process-based internals to the caller's running event loop.
It does not create, run, or stop the application's loop.

Cancel through the returned object:

```python
operation.cancel()
task.cancel()
```

Awaiting coroutine cancellation requests cancellation of the underlying operation or task and waits for its required cleanup before propagating `CancelledError`.

Provisioning cancellation terminates the active subprocess tree and removes the incomplete environment before it becomes terminal.
Task cancellation is cooperative during the configured grace period.
If the worker does not finish in time, Wetlands terminates its process tree, marks the task canceled after cleanup, and starts a replacement worker.

## Discover and remove environments

Applications can inspect the environments owned by a manager root without scanning Wetlands directories:

```python
from wetlands import ManagedEnvironmentState

for info in manager.managed_environments():
    if info.state is ManagedEnvironmentState.READY:
        print(f"{info.name} is ready at {info.path}")
    else:
        print(f"{info.name} is incomplete and can be removed or rebuilt")
```

`managed_environments()` returns immutable `ManagedEnvironmentInfo` snapshots in `READY` or `INCOMPLETE` state.
Discovery reports incomplete owned targets left by an interrupted host process as well as ready environments.
It ignores directories that Wetlands cannot prove it owns.

Removal returns an awaitable and listenable `RemovalOperation`:

```python
removal = manager.remove("numpy-example")
removal.listen(lambda event: print(event.kind.value, event.message))
removed = removal.wait_for()
print(removed.name)
```

Wetlands raises `EnvironmentNotFoundError` for a missing name and refuses to remove an unmanaged target or an environment with live workers.
Close its worker pools first.
Calling `cancel()` can stop removal while Wetlands is waiting or inspecting the target.
Once Wetlands seals cancellation and begins destructive deletion, cancellation is refused and the operation completes the deletion instead of reporting a misleading canceled state.

## Close resources

`WorkerPool` is a context manager.
Exiting it stops its workers.

`EnvironmentManager` is also a context manager and closes any pools it owns:

```python
with EnvironmentManager(root="wetlands") as manager:
    environment = manager.provision("numpy-example", spec).wait_for()
    with environment.start() as pool:
        image = np.arange(16, dtype=np.float32).reshape(4, 4)
        result = pool.execute_import(
            "numpy:negative",
            args=(image,),
        )
```

`execute_import()` and `execute_path()` are blocking conveniences around submission and `wait_for()`.

Closing a manager first cancels and joins active preparation and provisioning operations, then attempts to close every known worker pool.
If any cleanup fails, `ManagerCloseError.errors` contains every collected failure.
A later `close()` call retries pools that remain open, but a manager cannot be used again after shutdown begins.
