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
import requests

# Initialize the environment manager
# Cema will use the existing Micromamba installation at the specified path (e.g., "micromamba/") if available;
# otherwise it will automatically download and install Micromamba in a self-contained manner.
environmentManager = EnvironmentManager("micromamba/")

# Create and launch an isolated Conda environment named "cellpose"
env = environmentManager.create("cellpose", {"conda":["cellpose==3.1.0"]})
env.launch()

# Download example image from cellpose
imagePath = "cellpose_img02.png"
imageData = requests.get("https://www.cellpose.org/static/images/img02.png").content
with open(imagePath, "wb") as handler:
    handler.write(imageData)

segmentationPath = imagePath.replace(".png", "_segmentation.png")

# Import example_module in the environment
example_module = env.importModule("example_module.py")
# example_module is a proxy to example_module.py in the environment,
# calling example_module.function_name(args) will run env.execute(module_name, function_name, args)
diameters = example_module.segment(imagePath, segmentationPath)

# Or use env.execute() directly
# diameters = env.execute("example_module.py", "segment", (imagePath, segmentationPath))

print(f"Found diameters of {diameters} pixels.")

# Clean up and exit the environment
env.exit()

```

See the `examples/` folder for more examples.

## 🔗 Related Projects

- [Conda](https://anaconda.org/)
- [Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html)

## 🤖 Development

Use [uv](https://docs.astral.sh/uv/) to easily manage the project.

### Check & Format

Check for code errors with `uv run ruff check` and format the code with `uv run ruff format`.

### Tests

Test cema with `uv` and `ipdb`: `uv run pytest --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb tests`

## 📜 License

This project is licensed under the MIT License.