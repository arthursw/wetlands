# Cema - Conda Environment Manager

**Cema** (Conda Environment MAnager) is a lightweight Python library for managing **Conda** environments.

**Cema** can create Conda environments on demand, install dependencies, and execute arbitrary code within them. This makes it easy to build *plugin systems* or integrate external modules into an application without dependency conflicts, as each environment remains isolated.


## ✨ Features

- **Automatic Environment Management**: Create and configure environments on demand.
- **Dependency Isolation**: Install dependencies without conflicts.
- **Embedded Execution**: Run Python functions inside isolated environments.
- **Micromamba**: Cema uses a self-contained `micromamba` for fast and lightweight Conda environment handling.

---

## 📦 Installation

To install **Cema**, simply use `pip`:

```sh
pip install cema
```

## 🚀 Usage Example

If the user doesn't have micromamba installed, Cema will download and set it up automatically.

```python
from cema.environment_manager import EnvironmentManager

# Initialize the environment manager
# Cema will use the existing Micromamba installation at the specified path (e.g., "micromamba/") if available;
# otherwise it will automatically download and install Micromamba in a self-contained manner.
env_manager = EnvironmentManager("micromamba/")

# Create and launch an isolated Conda environment named "cellpose"
env = env_manager.createAndLaunch("cellpose", dependencies={"conda": ["cellpose==3.1.0"]})

# Execute the "segment" function from "example_module.py" inside the isolated environment
diameters = env.execute("example_module.py", "segment", ["image.png", "image_segmentation.png"])

# Clean up and exit the environment
env_manager.exit(env)

```

See the `examples/` folder for a more detailed example.

## 🔗 Related Projects

- [Conda](https://anaconda.org/)
- [Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html)

## Development

### Tests

To run the tests with `uv` and `ipdb`: `uv run pytest --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb tests`

## 📜 License

This project is licensed under the MIT License.