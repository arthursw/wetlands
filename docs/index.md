![Wetland](Wetland.svg)

# Wetlands

**Wetlands** is a lightweight Python library for managing **Conda** environments.

**Wetlands** can create Conda environments on demand, install dependencies, and execute arbitrary code within them. This makes it easy to build *plugin systems* or integrate external modules into an application without dependency conflicts, as each environment remains isolated.

The name ***Wetlands*** comes from the tropical *environments* where anacondas thrive.

---

**Documentation:** [https://arthursw.github.io/wetlands/latest/](https://arthursw.github.io/wetlands/latest/)

**Source Code:** [https://github.com/arthursw/wetlands/](https://github.com/arthursw/wetlands/)

---

## ✨ Features

- **Automatic Environment Management**: Create and configure environments on demand.
- **Dependency Isolation**: Install dependencies without conflicts.
- **Embedded Execution**: Run Python functions inside isolated environments.
- **Pixi & Micromamba**: Wetlands uses either a self-contained `pixi` or `micromamba` for fast and lightweight Conda environment handling.

---

## 📦 Installation

To install **Wetlands**, simply run:

```sh
pip install wetlands
```

---

## 🚀 Usage

### Minimal example

Here is a minimal example usage:

```python
from wetlands.environment_manager import EnvironmentManager

# Initialize the environment manager
environmentManager = EnvironmentManager("pixi/")

# Create and launch a Conda environment named "numpy_env"
env = environmentManager.create("numpy_env", {"pip": ["numpy==2.2.4"]})
env.launch()

# Import minimal_module in the environment (see minimal_module.py below)
minimal_module = env.importModule("minimal_module.py")
# minimal_module is a proxy to minimal_module.py in the environment
array = [1, 2, 3]
# Execute the sum() function in the numpy_env environment and get the result
result = minimal_module.sum(array)

print(f"Sum of {array} is {result}.")

# Clean up and exit the environment
env.exit()
```

With `minimal_module.py`:

```python
def sum(x):
    import numpy as np  # type: ignore
    return int(np.sum(x))
```

### General usage

Wetlands allows you to interact with isolated Conda environments in two main ways:

1.  **Simplified Execution ([`env.importModule`][wetlands.environment.Environment.importModule] / [`env.execute`][wetlands.environment.Environment.execute]):** Wetlands manages the communication details, providing a proxy object to call functions within the environment seamlessly. See [Getting started](getting_started.md).
2.  **Manual Control ([`env.executeCommands`][wetlands.environment.Environment.executeCommands]):** You run specific commands (like starting a Python script that listens for connections) and manage the inter-process communication yourself. See [Advanced example](advanced_example.md).

You can run those examples form the [`examples/` folder](https://github.com/arthursw/wetlands/tree/main/examples) in the repository.

Explore the inner workings on the [How it Works](how_it_works.md) page.

## 📜 License

This project was made at Inria in Rennes (Centre Inria de l'Université de Rennes) and is licensed under the MIT License.

The logo Wetland was made by [Dan Hetteix](https://thenounproject.com/creator/DHETTEIX/) from Noun Project (CC BY 3.0).