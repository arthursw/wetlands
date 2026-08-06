# Contributor setup and validation

This page is for contributors changing Wetlands itself.
User applications do not need the development dependencies or commands below.

## Install the development environment

```sh
uv sync --frozen --group dev --extra shared-memory
```

## Run the fast test suite

```sh
uv run --extra shared-memory pytest -m "not integration and not compat and not manual"
```

## Run the representative real-Pixi suite

```sh
UV_PROJECT_ENVIRONMENT=.venv-py314 uv run --python 3.14 --extra shared-memory pytest tests/test_v2_pixi_integration.py
```

This suite downloads and provisions real environments and takes longer than the fast tests.

## Run static checks

```sh
uv run ruff check
uv run ruff format --check
uv run mypy src/wetlands
```

## Build documentation strictly

```sh
uv run --frozen --python 3.14 --extra docs --no-dev mkdocs build --strict
```

The strict build fails on invalid navigation, missing snippet files, and documentation warnings.

## Build distribution artifacts

```sh
uv build
```

## Understand the codebase

- [How Wetlands works](../how_it_works.md) follows the main runtime flow.
- [Architecture and execution protocol](architecture.md) defines boundaries contributors must preserve.
- [Transport codecs](codecs.md) explains value encoding and shared-memory lease ownership.

Maintainers publishing a release must follow the canonical [release checklist](https://github.com/arthursw/wetlands/blob/main/RELEASING.md).
