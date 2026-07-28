![](Wetland.png)

# Wetlands

[![Wetlands tests](https://github.com/arthursw/wetlands/actions/workflows/ci.yml/badge.svg?event=push&branch=main)](https://github.com/arthursw/wetlands/actions/)
[![Wetlands PyPI](https://img.shields.io/pypi/v/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)
[![Wetlands Python versions](https://img.shields.io/pypi/pyversions/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)

Wetlands provisions isolated [Pixi](https://pixi.sh/) environments and runs Python callables in managed worker processes.

It is intended for trusted local dependency isolation.
It is not a security sandbox.

Wetlands 2 gives applications a small, application-neutral API for:

- side-effect-light manager construction;
- observable and cancellable preparation and provisioning operations;
- reproducible Pixi projects and `pixi.lock` files;
- warm worker pools;
- qualified installed-package targets and path targets for local development;
- automatic transport of ordinary Python values and NumPy arrays;
- blocking, callback-based, and `asyncio`-friendly execution.

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
    post_install=(
        PostInstallCommand(("python", "-m", "worker_package.prepare_assets")),
    ),
    pixi_lock=Path("pixi.lock"),
)
```

When `pixi_lock` is supplied, Wetlands provisions from those exact locked dependencies.
If the recipe contains local packages, the supplied lockfile must already resolve those same local sources and editable settings.
Without one, Pixi resolves the generated project and Wetlands preserves the resulting lockfile in the managed environment.

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
uv sync --frozen --group dev
```

Run the fast test suite with:

```sh
uv run pytest -m "not integration and not compat and not manual"
```

Run the representative real-Pixi integration suite with:

```sh
UV_PROJECT_ENVIRONMENT=.venv-py313 uv run --python 3.13 pytest -m "not manual and not compat and agent_integration"
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

## License

Wetlands is licensed under the MIT License.
