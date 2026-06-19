![](Wetland.png)

# Wetlands

[![Wetlands tests](https://github.com/arthursw/wetlands/actions/workflows/ci.yml/badge.svg?event=push&branch=main)](https://github.com/arthursw/wetlands/actions/)
[![Wetlands pypi](https://img.shields.io/pypi/v/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)
[![Wetlands python versions](https://img.shields.io/pypi/pyversions/wetlands.svg?color=%2334D058)](https://pypi.org/project/wetlands/)

**Wetlands** is a lightweight Python library for managing **Conda** environments.

**Wetlands** can create Conda environments on demand, install dependencies, and execute arbitrary code within them. This makes it easy to build *plugin systems* or integrate external modules into an application without dependency conflicts, as each environment remains isolated.

For example, if your application needs to use both [Stardist](https://github.com/stardist/stardist) and [Cellpose](https://www.cellpose.org/), installing them in the same environment may not work due to conflicting dependencies. With Wetlands, you can create a dedicated environment for each library and run them both as needed from your main script.

The name ***Wetlands*** comes from the tropical *environments* where anacondas thrive.

[Appose Python](https://github.com/apposed/appose-python) is a great alternative to Wetlands. It even provides the ability to run Java environments (see [Appose Java](https://github.com/apposed/appose-java)) and share memory between the Python world and the Java world.
There are other minor differences between the two libraries. For example, Wetlands provides integrated debugging tools to attach VS Code or PyCharm to isolated environments for step-through debugging with breakpoints. See the [Debugging guide](https://arthursw.github.io/wetlands/latest/debugging/) for more information.

---

**Documentation:** https://arthursw.github.io/wetlands/latest/

**Source Code:** https://github.com/arthursw/wetlands/

---

## ✨ Features

- **Automatic Environment Management**: Create and configure environments on demand.
- **Dependency Isolation**: Install dependencies without conflicts.
- **Embedded Execution**: Run Python functions or scripts inside isolated environments, with both blocking and non-blocking (task-based) APIs.
- **Task API**: Execute code asynchronously with progress reporting, cancellation, and event-driven callbacks.
- **Parallel Execution**: Launch multiple worker processes sharing a single Conda environment and distribute work.
- **Persistent Workers**: Keep trusted local workers alive and reconnect to them from a later `EnvironmentManager`.
- **Integrated Debugging**: Debug code running in isolated environments using VS Code or PyCharm with breakpoints and step-through execution.
- **Scoped Logs**: Keep manager and worker log files under the Wetlands instance directory by default.
- **Pixi & Micromamba**: Wetlands uses either a self-contained `pixi` or `micromamba` for fast and lightweight Conda environment handling.

## 📦 Installation

To install **Wetlands**, simply use `pip`:

```sh
pip install wetlands
```

## 🚀 Usage Example

If the user doesn't have pixi or micromamba installed, Wetlands will download and set it up automatically.

```python
from wetlands.environment_manager import EnvironmentManager

# Initialize the environment manager
environment_manager = EnvironmentManager()
# Logs are stored under wetlands/ by default:
# wetlands.log for manager operations and environments.log for worker processes.

# Create and launch an isolated Conda environment named "numpy"
env = environment_manager.create("numpy", {"pip": ["numpy==2.2.4"]})
env.launch()

# Import a module proxy and call functions in the environment
minimal_module = env.import_module("minimal_module.py")
result = minimal_module.sum([1, 2, 3])
print(f"Result: {result}")

# Or use execute() for a direct blocking call
result = env.execute("minimal_module.py", "sum", args=([1, 2, 3],))

# Clean up
env.exit()
```

Wetlands records a hash of each environment's creation recipe.
Calling `create()` again with the same name reuses the existing environment only when the stored recipe hash matches the requested dependencies, backend, platform, and creation commands.
Use `replace_existing=True` to recreate a same-name environment with a different recipe, or `load(name)` to intentionally load the existing default-path environment without recipe validation.

with `minimal_module.py`:

```python
def sum(x):
    import numpy as np
    return int(np.sum(x))
```

### Non-blocking execution with tasks

`submit()` returns a `Task` object immediately, letting you monitor progress, cancel, or wait for the result:

```python
# Submit a function for non-blocking execution
task = env.submit("compute.py", "heavy_computation", args=(data,))

# Do other work while the task runs...
print(f"Status: {task.status}")

# Block for the result when ready
task.wait_for()
print(f"Result: {task.result}")
```

### Parallel execution with multiple workers

Launch multiple worker processes sharing the same Conda environment:

```python
env.launch(max_workers=4)

# Distribute work across workers
results = list(env.map("segment.py", "segment", images))

# Or get individual Task objects for full control
tasks = env.map_tasks("segment.py", "segment", images)
```

Workers that crash or hang are detected and replaced automatically. Set `worker_timeout` to fail tasks when a worker stops responding:

```python
env.launch(max_workers=4, worker_timeout=300)  # 5-minute inactivity timeout
```

### Persistent workers

By default, `env.exit()` stops workers when you are done.
For trusted local workflows that need to reconnect from a later manager process, launch persistent workers directly with `persistent=True` or use `launch_or_attach()` to attach to existing persistent workers and launch them when needed:

```python
env = manager.create("cellpose", deps)
env = manager.launch_or_attach(env, max_workers=2)
env.detach()  # close local connections, keep workers alive

new_manager = EnvironmentManager()
env = new_manager.launch_or_attach("cellpose")
result = env.execute("minimal_module.py", "sum", args=([1, 2, 3],))
env.exit()  # stop persistent workers and remove their registry entries
```

`launch_or_attach()` first tries to attach to live persistent workers, then launches new persistent workers only when the manager already knows the environment and no live workers remain.
Passing only a name is reconnect-only unless the manager has already created or loaded that environment.
Use plain `env.launch()` for non-persistent workers.
Persistent workers use authenticated local TCP connections with a root-local auth key stored under `wetlands/state/auth.key`.
Attach makes one bounded connection attempt to each live worker.
If a live worker is busy or cannot complete authentication, Wetlands raises an error with the worker PID, port, and commands to stop it through Wetlands or the operating system.
The API still executes arbitrary Python in the target environment, so it is intended for trusted local use.

See the `examples/` folder and the [documentation](https://arthursw.github.io/wetlands/latest/) for more detailed examples.

## 🐛 Debugging

Wetlands includes tools to debug code running in isolated environments using VS Code or PyCharm. You can set breakpoints, step through code, and inspect variables in real-time.

### Quick Debugging Example

```bash
# List all running environments and their debug ports
wetlands list

# Attach VS Code to an environment for debugging
wetlands debug -s /path/to/my/project -n my_env

# Or use PyCharm instead
wetlands debug -s /path/to/my/project -n my_env -ide pycharm

# Kill an environment when done
wetlands kill -n my_env
```

For detailed debugging instructions and workflows, see the [Debugging guide](https://arthursw.github.io/wetlands/latest/debugging/).

## 🔗 Related Projects

- [Conda](https://anaconda.org/)
- [Pixi](https://pixi.sh/)
- [Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html)

## 🤖 Development

Use [uv](https://docs.astral.sh/uv/) to easily manage the project.

### Check & Format

Check for code errors with `uv run ruff check` and format the code with `uv run ruff format`.

### Tests

Wetlands uses pytest markers to keep routine checks fast while preserving real environment coverage.

Fast unit tests skip real external environments, cross-Python subprocess checks, and manual-only tests:

`uv run pytest -m "not integration and not compat and not manual"`

Compatibility tests exercise cross-Python behavior, especially Python 3.9:

`UV_PROJECT_ENVIRONMENT=.venv-py39 uv run --python 3.9 pytest -m compat`

Agent integration runs a small representative set of real pixi environment and worker tests:

`UV_PROJECT_ENVIRONMENT=.venv-py313 uv run --python 3.13 pytest -m "not manual and not compat and (not integration or agent_integration)" --backend=pixi`

Manual full suite:

`uv run pytest`

Manual full suite for one backend:

`uv run pytest --backend=pixi`

`uv run pytest --backend=micromamba`

Marker categories:

- `integration`: tests that use real external environments, real pixi/micromamba commands, worker processes in external environments, or real package installs.
- `agent_integration`: a small representative integration subset agents may run after broad environment or executor changes.
- `compat`: cross-Python compatibility tests, especially tests invoking Python 3.9.
- `manual`: complete, expensive, or flaky-by-nature tests intended for local manual or scheduled CI runs.
- `slow`: non-manual tests expected to take noticeably longer than normal unit tests.

Agents should normally run the fast unit tests, add `compat` only when Python-version behavior changes, and run agent integration after broad environment, dependency, worker, or executor changes.

For debugging with `ipdb`: `uv run pytest tests/ --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb`

Use `--last-failed` to only re-run the failures: `uv run pytest tests/ --last-failed`

### Build and Publish

Build with `uv build`
Publish with `uv publish dist/wetlands-VERSION_NAME*`

### Generate documentation

The Wetlands documentation is generated with [`mkdocs-material`](https://squidfunk.github.io/mkdocs-material/), [`mkdocstrings`](https://mkdocstrings.github.io/), [`mike`](https://github.com/jimporter/mike) and others.

Install the doc tools with `uv pip install  ".[docs]"`.

MkDocs includes a live preview server, so you can preview your changes as you write your documentation. The server will automatically rebuild the site upon saving. Start it with: `uv run mkdocs serve`.

[`mike`](https://github.com/jimporter/mike) is used to generate multiple versions of the docs. To create a new version, `mike deploy [version]` is used by Github Actions, just update `.github/workflows/ci.yml`.

The doc is automatically generated by [Github Actions](https://squidfunk.github.io/mkdocs-material/publishing-your-site/#with-github-actions-material-for-mkdocs) (see `.github/workflows/ci.yml`).

The script `scripts/gen_ref_pages.py` is used by mkdocs to generate the API reference automatically (see [mkdocstrings recipes](https://mkdocstrings.github.io/recipes/)).

## 📋 Todo

- Use Pixi features and environment instead of creating one workspace per environment.

## 📜 License

This project was made by the [SAIRPICO team](https://www.inria.fr/en/sairpico) at Inria in Rennes (Centre Inria de l'Université de Rennes) and is licensed under the MIT License.

The logo Wetland was made by [Dan Hetteix](https://thenounproject.com/creator/DHETTEIX/) from Noun Project (CC BY 3.0).
