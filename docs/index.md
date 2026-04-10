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
- **Embedded Execution**: Run Python functions inside isolated environments, with both blocking and non-blocking (task-based) APIs.
- **Task API**: Execute code asynchronously with progress reporting, cancellation, and event-driven callbacks. See [Tasks and parallel execution](tasks.md).
- **Parallel Execution**: Launch multiple worker processes sharing a single Conda environment and distribute work across them. See [Tasks and parallel execution](tasks.md).
- **Worker Health Monitoring**: Automatic detection of dead or hung worker processes, with configurable inactivity timeouts and transparent replacement of failed workers.
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
environment_manager = EnvironmentManager("pixi/")

# Create and launch a Conda environment named "numpy_env"
env = environment_manager.create("numpy_env", {"pip": ["numpy==2.2.4"]})
env.launch()

# Import minimal_module in the environment (see minimal_module.py below)
minimal_module = env.import_module("minimal_module.py")
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

Wetlands provides several ways to execute code in isolated Conda environments:

1.  **Task-based execution ([`env.submit`][wetlands.environment.Environment.submit]):** Submit a function for non-blocking execution and get a task object back, with progress reporting, cooperative cancellation, event listeners, and `async/await`. For batch processing, use [`env.map()`][wetlands.environment.Environment.map] or [`env.map_tasks()`][wetlands.environment.Environment.map_tasks] to distribute work across multiple worker processes. See [Tasks and parallel execution](tasks.md).
2.  **Blocking execution ([`env.import_module`][wetlands.environment.Environment.import_module] / [`env.execute`][wetlands.environment.Environment.execute]):** Convenience shortcuts that block until the result is ready. Wetlands manages the communication details, providing a proxy object to call functions within the environment seamlessly. See [Getting started](getting_started.md).
3.  **Manual Control ([`env.execute_commands`][wetlands.environment.Environment.execute_commands]):** You run specific commands (like starting a Python script that listens for connections) and manage the inter-process communication yourself. See [Manual communication](manual_communication.md).

You can run those examples from the [`examples/` folder](https://github.com/arthursw/wetlands/tree/main/examples) in the repository.

Explore the inner workings on the [How it Works](how_it_works.md) page.

## 📜 License

This project was made at Inria in Rennes (Centre Inria de l'Université de Rennes) and is licensed under the MIT License.

The logo Wetland was made by [Dan Hetteix](https://thenounproject.com/creator/DHETTEIX/) from Noun Project (CC BY 3.0).