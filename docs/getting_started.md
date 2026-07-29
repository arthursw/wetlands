# Getting started

This guide provisions a Pixi environment, starts a worker pool, and calls an installed Python function by qualified name.

## Install Wetlands

```sh
pip install "wetlands[shared-memory]"
```

The optional dependency installs NumPy in the host environment for automatic array transport.
The worker environment must declare its own NumPy dependency.

## Define the worker package

Installable worker code should expose ordinary Python functions:

```python
def threshold(image: "numpy.ndarray", value: float) -> "numpy.ndarray":
    return image > value
```

The package containing this function must be installed into the managed environment.
Wetlands imports it inside the worker and never imports it into the host process.

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
    pypi=("example-worker-package==1.0.0",),
)

operation = manager.provision("threshold-v1", spec)
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
        "example_worker.filters:threshold",
        kwargs={"image": image, "value": 7.5},
    )
    mask = task.wait_for()
```

`submit_import()` accepts `package.module:qualified.callable`.
Nested class attributes such as `package.module:Processor.run` are valid.

The host input array is copied into transport storage, so worker mutation cannot change the caller's array.
The returned array is a normal independently owned NumPy array.

## Execute local source during development

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
        "threshold-v1",
        EnvironmentSpec(
            python="3.12.*",
            conda=("numpy>=2",),
            pypi=("example-worker-package==1.0.0",),
        ),
    )

    with environment.start() as pool:
        result = await pool.submit_import(
            "example_worker.filters:threshold",
            kwargs={"image": image, "value": 7.5},
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

## Close resources

`WorkerPool` is a context manager.
Exiting it stops its workers.

`EnvironmentManager` is also a context manager and closes any pools it owns:

```python
with EnvironmentManager(root="wetlands") as manager:
    environment = manager.provision("threshold-v1", spec).wait_for()
    with environment.start() as pool:
        result = pool.execute_import(
            "example_worker.filters:threshold",
            kwargs={"image": image, "value": 7.5},
        )
```

`execute_import()` and `execute_path()` are blocking conveniences around submission and `wait_for()`.

Closing a manager first cancels and joins active preparation and provisioning operations, then attempts to close every known worker pool.
If any cleanup fails, `ManagerCloseError.errors` contains every collected failure.
A later `close()` call retries pools that remain open, but a manager cannot be used again after shutdown begins.
