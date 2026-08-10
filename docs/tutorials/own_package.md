# Run code from your own package

The first tutorial called Python's built-in `sum` function.
This tutorial installs a small package into the managed environment and calls one of its functions.

You will also pass a NumPy array to the worker and receive a new array as the result.

## Prerequisites

Complete [Run your first task](../getting_started.md) first.
Install Wetlands with host-side NumPy support:

```sh
pip install "wetlands[shared-memory]"
```

This tutorial runs the example package included in the Wetlands repository.
Download it and enter its root directory:

```sh
git clone https://github.com/arthursw/wetlands.git
cd wetlands
```

## Example package

The repository's `examples` directory is an installable package with this relevant structure:

```text
examples/
├── pyproject.toml
├── example_module.py
└── getting_started.py
```

`example_module.py` contains an ordinary Python function:

```python
def threshold(image, value):
    return image > value
```

The worker package does not need to import Wetlands for ordinary calls.

Its `pyproject.toml` gives the package a name and tells Python how to install the `example_module` module:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "wetlands-example-workers"
version = "0.0.0"

[tool.setuptools]
py-modules = ["example_module"]
```

Your own worker code can use a regular package layout instead.

## Complete application

<!-- fmt: off -->
```python
--8<-- "examples/getting_started.py"
```
<!-- fmt: on -->

Run the example from the repository root:

```console
$ python examples/getting_started.py
[[False False False False]
 [False False False False]
 [ True  True  True  True]
 [ True  True  True  True]]
```

## Understand the boundary

The application process imports `wetlands` and the host copy of NumPy.
It does not import `example_module`.

`LocalPackage(example_directory)` tells Wetlands to install the example package inside the managed environment.
The environment also has its own NumPy installation because the specification includes `numpy>=2`.

`example_module:threshold` means “import `example_module` inside the worker, then call its `threshold` attribute.”

When the application calls `threshold`, Wetlands transfers the NumPy data through operating-system shared memory automatically:

1. the application copies `image` into a temporary host-owned shared-memory segment;
2. the worker copies that data into a private, writable array;
3. the worker places the returned mask in a temporary worker-owned shared-memory segment;
4. the application copies the result into a normal array that it owns.

Wetlands cleans up both temporary segments.
This is shared-memory transport, not a shared mutable array or zero-copy API, so changes made by the worker cannot modify the application's original array.

The worker and manager stop when their context managers exit.
The ready `numpy-example` environment remains below the Wetlands root so a later run can reuse it.

## Use a published package

Production applications normally install a versioned package from PyPI instead of a local directory:

```python
spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy>=2",),
    pypi=("my-worker-package==1.4.0",),
)
```

Call it with the same `package.module:function` syntax.

## Next steps

- [Define environment dependencies](../dependencies.md)
- [Report task progress](../how-to/progress.md)
- [Understand NumPy shared-memory transport](../shared_memory.md)
