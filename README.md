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
- **Embedded Execution**: Run Python functions or scripts inside isolated environments.
- **Integrated Debugging**: Debug code running in isolated environments using VS Code or PyCharm with breakpoints and step-through execution.
- **Micromamba**: Wetlands uses a self-contained `micromamba` for fast and lightweight Conda environment handling.

## 📦 Installation

To install **Wetlands**, simply use `pip`:

```sh
pip install wetlands
```

## 🚀 Usage Example

If the user doesn't have pixi or micromamba installed, Wetlands will download and set it up automatically.

```python

from wetlands.environment_manager import EnvironmentManager

# Initialize the environment manager with a wetlands instance path
# The wetlandsInstancePath will contain logs, debug information, and by default the conda installation
# Wetlands will use the existing Pixi or Micromamba installation if available;
# otherwise it will automatically download and install Pixi or Micromamba in a self-contained manner.
environmentManager = EnvironmentManager()

# Create and launch an isolated Conda environment named "numpy"
env = environmentManager.create("numpy", {"pip":["numpy==2.2.4"]})
env.launch()

# Import example_module in the environment, see example_module.py below
minimal_module = env.importModule("minimal_module.py")
# example_module is a proxy to example_module.py in the environment
array = [1,2,3]
result = minimal_module.sum(array)

print(f"Sum of {array} is {result}.")

# You can also run Python scripts directly using runScript()
# env.runScript("script.py", args=("arg1", "arg2"))

# Clean up and exit the environment
env.exit()

```

with `example_module.py` as follow:

```python
def sum(x):
    import numpy as np
    return int(np.sum(x))
```

See the `examples/` folder for more detailed examples.

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

Test wetlands with `uv` and `ipdb`: `uv run pytest --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb tests`
Use `--last-failed` to only re-run the failures: `uv run pytest --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb --last-failed tests`

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