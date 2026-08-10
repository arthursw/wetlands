![](Wetland.png)

# Wetlands

[![Wetlands tests](https://github.com/arthursw/wetlands/actions/workflows/ci.yml/badge.svg?event=push&branch=main)](https://github.com/arthursw/wetlands/actions/)
[![Wetlands PyPI](https://img.shields.io/pypi/v/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)
[![Wetlands Python versions](https://img.shields.io/pypi/pyversions/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)

Wetlands lets a Python application run functions and external commands in separate [Pixi](https://pixi.sh/) environments.

This is useful when one application needs libraries whose dependencies cannot be installed together.
Wetlands creates the environments, keeps worker processes ready for repeated calls, transfers ordinary Python values between the application and workers, and supervises commands and services installed in an environment.

> Wetlands is intended for code you trust.
> Environments prevent dependency conflicts, but they do not restrict what code can access on your computer.

## Install

```sh
pip install wetlands
```

Install the optional NumPy support when arrays cross the worker boundary:

```sh
pip install "wetlands[shared-memory]"
```

## Run a task

```python
from wetlands import EnvironmentManager, EnvironmentSpec

with EnvironmentManager(root="wetlands") as manager:
    environment = manager.provision(
        "first-example",
        EnvironmentSpec(python="3.12.*"),
    ).wait_for()

    with environment.start() as workers:
        result = workers.execute_import(
            "builtins:sum",
            args=([20, 22],),
        )

print(result)  # 42
```

The first run may download Pixi and Python packages and can take several minutes.

Continue with the [first-task tutorial](https://arthursw.github.io/wetlands/latest/getting_started/) or browse the [complete documentation](https://arthursw.github.io/wetlands/latest/).
Existing Wetlands 1 users should read the [migration guide](docs/migration_v2.md).

## Development

Install the development environment:

```sh
uv sync --frozen --group dev --extra shared-memory
```

Run the fast tests and static checks:

```sh
uv run --extra shared-memory pytest -m "not integration and not compat and not manual"
uv run ruff check
uv run ruff format --check
uv run mypy src/wetlands
```

Start with the [contributor setup and validation guide](docs/developer/contributing.md).
The [architecture guide](docs/developer/architecture.md) explains the runtime design.
Maintainers should follow [RELEASING.md](RELEASING.md) when publishing a release.

Wetlands is licensed under the [MIT License](LICENSE).
Read the [security policy](SECURITY.md) before executing third-party worker code, managed commands, or post-install commands.
