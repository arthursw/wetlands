![](Wetland.png)

# Wetlands

[![Wetlands tests](https://github.com/arthursw/wetlands/actions/workflows/ci.yml/badge.svg?event=push&branch=main)](https://github.com/arthursw/wetlands/actions/)
[![Wetlands PyPI](https://img.shields.io/pypi/v/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)
[![Wetlands Python versions](https://img.shields.io/pypi/pyversions/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)

Wetlands is a Python library for creating isolated environments with [Pixi](https://pixi.sh/) and running Python functions inside them.

This lets an application use libraries with incompatible dependencies at the same time.
For example, [Cellpose](https://www.cellpose.org/) and [StarDist](https://github.com/stardist/stardist) can each run in their own environment while exchanging ordinary Python values and NumPy arrays with the main application.

Wetlands creates these environments when needed, installs their dependencies, keeps worker processes ready for repeated calls, and cleans up their resources automatically.
It can be used in desktop applications, servers, and plugin systems.

> Wetlands is intended for code you trust.
> Isolated environments prevent dependency conflicts, but they do not restrict what code can access on your computer.

Wetlands 2 provides:

- side-effect-light manager construction;
- observable and cancellable preparation and provisioning operations;
- reproducible Pixi projects and `pixi.lock` files;
- warm worker pools;
- qualified installed-package targets and path targets for local development;
- automatic transport of ordinary Python values and NumPy arrays;
- blocking, callback-based, and `asyncio`-friendly execution.

The first preparation may download a verified Pixi executable, and the first provisioning of an environment downloads its declared packages.
These operations require network access and can take several minutes.
Wetlands stores Pixi, managed environments, locks, logs, and runtime state below the manager root you choose.

## Installation

```sh
pip install wetlands
```

Install the optional host-side NumPy dependency when arrays cross the execution boundary:

```sh
pip install "wetlands[shared-memory]"
```

## Quick start

The manager constructor only validates and stores configuration.
Downloading or inspecting Pixi begins when `prepare()` or `provision()` is called.

```python
import numpy as np

from wetlands import EnvironmentManager, EnvironmentSpec

manager = EnvironmentManager(root="wetlands")

preparation = manager.prepare()
preparation.listen(lambda event: print(event.stage, event.message))
pixi = preparation.wait_for()

spec = EnvironmentSpec(
    python="3.10.*",
    conda=("cellpose==3.1.0",),
    pypi=("napari-wsegmenter",),
)
environment = manager.provision("cellpose", spec).wait_for()

with environment.start(workers=1) as workers:
    image = np.zeros((256, 256), dtype=np.float32)
    task = workers.submit_import(
        "napari_wsegmenter._cellpose:segment",
        kwargs={
            "image": image,
            "model_type": "cyto",
            "use_gpu": False,
            "diameter": 30.0,
        },
    )
    masks = task.wait_for()
```

The worker callable receives and returns normal NumPy arrays:

```python
def segment(
    image: "numpy.ndarray",
    model_type: str,
    use_gpu: bool,
    diameter: float,
) -> "numpy.ndarray":
    import cellpose.models

    model = cellpose.models.Cellpose(gpu=use_gpu, model_type=model_type)
    masks, *_ = model.eval(image, diameter=diameter, channels=[0, 0])
    return masks
```

Wetlands owns the shared-memory details.
Inputs use copy-in semantics and returned arrays are independently owned by the caller.

## Async applications

Preparation, provisioning, and execution objects are awaitable.
Their `events()` methods expose async event streams while the caller retains ownership of its event loop.

```python
import asyncio

from wetlands import EnvironmentManager, EnvironmentSpec


async def main() -> None:
    manager = EnvironmentManager(root="wetlands")

    preparation = manager.prepare()
    async for event in preparation.events():
        print(event.kind.value, event.message)
    await preparation

    environment = await manager.provision(
        "analysis",
        EnvironmentSpec(python="3.12.*", conda=("numpy",)),
    )

    with environment.start(workers=2) as workers:
        task = workers.submit_import(
            "analysis_package.statistics:mean",
            args=([1.0, 2.0, 3.0],),
        )
        result = await task
        print(result)


asyncio.run(main())
```

Cancel an operation or execution task with `cancel()`.
A canceled provisioning operation becomes terminal only after its active process tree has stopped and its incomplete environment has been cleaned up.
For a running worker task, Wetlands first requests cooperative cancellation.
If the worker does not finish during the configured grace period, Wetlands terminates its process tree and starts a replacement worker.

## Pixi projects and lockfiles

`EnvironmentSpec` is the complete managed recipe:

```python
from pathlib import Path

from wetlands import EnvironmentSpec, LocalPackage, PostInstallCommand

spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy>=2", "scikit-image", "pip"),
    pypi=("example-pypi-package==1.2.0",),
    channels=("conda-forge",),
    local=(LocalPackage(Path("../worker-package"), editable=True),),
    post_install=(PostInstallCommand(("python", "-m", "worker_package.prepare_assets")),),
    pixi_lock=Path("pixi.lock"),
)
```

When `pixi_lock` is supplied, Wetlands provisions from those exact locked dependencies.
If the recipe contains local packages, the supplied lockfile must already resolve those same local sources and editable settings.
It must also include Wetlands' exact managed worker-runtime dependencies, including its `debugpy` pin.
Without one, Pixi resolves the generated project and Wetlands preserves the resulting lockfile in the managed environment.

Wetlands owns the managed `debugpy` version, so applications must not declare it in `EnvironmentSpec`.
The managed runtime pin participates in recipe identity and causes environments to rebuild when it changes.

An environment is ready only after every installation and validation step succeeds and Wetlands atomically publishes its ready metadata.
Failed, canceled, or crash-interrupted provisioning is rebuilt on the next attempt rather than resumed.
The returned `ManagedEnvironment` exposes its canonical project and lockfile paths, Pixi executable and version, recipe hash, lockfile hash, and generation ID.

## Worker targets

Installed packages use a qualified target:

```python
task = workers.submit_import(
    "package.module:ClassName.method",
    args=(value,),
)
```

The module is imported inside the isolated worker.

Local development can use an explicit source path:

```python
task = workers.submit_path(
    "worker_code.py",
    "segment",
    kwargs={"image": image},
    cache=False,
)
```

Path targets are keyed by canonical path and content, so equal filename stems do not collide.

## Debug running workers

Wetlands can start a debugger after an application and its workers are already running.
No debug flag or debugger call is required in application or worker code.

```sh
wetlands workers --root ./wetlands --environment cellpose
wetlands debug --root ./wetlands --environment cellpose --worker WORKER_ID --editor vscode --source .
```

The debug adapter remains available for reconnection until its worker exits.
See [Debugging running workers](docs/debugging.md) and [Persistent workers and reconnection](docs/persistent_workers.md).

## Supported values

Execution arguments and results may contain:

- `None`, booleans, integers, floats, strings, and bytes;
- nested lists, tuples, and dictionaries with simple keys;
- NumPy arrays without object dtype.

Unsupported objects fail explicitly at the boundary.
Non-contiguous arrays are transported as contiguous arrays.
Intermediate task outputs are limited to simple values in Wetlands 2.

## Migration from Wetlands 1

Wetlands 2 is a major release with a deliberately smaller public API.
Applications should migrate explicitly instead of relying on compatibility shims.

See the [Wetlands 2 migration guide](docs/migration_v2.md).

## Development

Install the development environment with:

```sh
uv sync --frozen --group dev --extra shared-memory
```

Run the fast test suite with:

```sh
uv run --extra shared-memory pytest -m "not integration and not compat and not manual"
```

Run the representative real-Pixi integration suite with:

```sh
UV_PROJECT_ENVIRONMENT=.venv-py314 uv run --python 3.14 --extra shared-memory pytest tests/test_v2_pixi_integration.py
```

Run linting with:

```sh
uv run ruff check
uv run ruff format --check
```

Build the package with:

```sh
uv build
```

## Documentation

The complete documentation is available at [arthursw.github.io/wetlands](https://arthursw.github.io/wetlands/latest/).
Contributor-facing architecture and codec boundaries are described in the [developer guide](docs/developer/architecture.md).

## License

Wetlands is licensed under the [MIT License](LICENSE).

See the [security policy](SECURITY.md) before executing third-party worker code or post-install commands.
