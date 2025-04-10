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

To install **Cema**, simply run:

```sh
pip install cema
```

---

## 🚀 Usage

### Minimal example

Here is a minimal example usage:

```python
from cema.environment_manager import EnvironmentManager

# Initialize the environment manager
environmentManager = EnvironmentManager("micromamba/")

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

Cema allows you to interact with isolated Conda environments in two main ways:

1.  **Simplified Execution (`env.importModule` / `env.execute`):** Cema manages the communication details, providing a proxy object to call functions within the environment seamlessly. See the [Getting Started](getting_started.md).
2.  **Manual Control (`env.executeCommands`):** You run specific commands (like starting a Python script that listens for connections) and manage the inter-process communication yourself. See the [advanced example](advanced_example.md).

You can run those examples form the [`examples/` folder](https://github.com/arthursw/cema/tree/main/examples) in the repository.

---

## 🎓 How It Works

Cema leverages **Micromamba**, a fast, native reimplementation of the Conda package manager.

1.  **Micromamba Setup:** When `EnvironmentManager` is initialized, it checks for a `micromamba` executable at the specified path (e.g., `"micromamba/"`). If not found, it downloads a self-contained Micromamba binary suitable for the current operating system and architecture into that directory. This means Cema doesn't require a pre-existing Conda/Mamba installation.
2.  **Environment Creation:** `create(name, dependencies)` uses Micromamba commands (`micromamba create -n name -c channel package ...`) to build a new, isolated Conda environment within the Micromamba prefix (e.g., `micromamba/envs/name`). Note that the main environemnt is returned if it already satisfies the required dependencies.
3.  **Dependency Installation:** Dependencies (Conda packages, Pip packages) are installed into the target environment using `micromamba install ...` and `pip install ...` (executed within the activated environment).
4.  **Execution (`launch`/`execute`/`importModule`):**
    *   `launch()` starts a helper Python script (`cema._internal.executor_server`) *within* the activated target environment using `subprocess.Popen`.
    *   This server listens on a local socket using `multiprocessing.connection.Listener`.
    *   The main process connects to this server using `multiprocessing.connection.Client`.
    *   `execute(module, func, args)` sends a message containing the module path, function name, and arguments to the server.
    *   The server imports the module (if not already imported), executes the function with the provided arguments, and sends the result (or exception) back to the main process.
    *   `importModule(module)` creates a proxy object in the main process. When methods are called on this proxy, it triggers the `execute` mechanism described above.
5.  **Direct Execution (`executeCommands`):** This method directly activates the target environment and runs the provided shell commands using `subprocess.Popen` (no communication server involved here). The user is responsible for managing the launched process and any necessary communication.
6.  **Isolation:** Each environment created by Cema is fully isolated, preventing dependency conflicts between different environments or with the main application's environment.


## ⚙️ Under the Hood

Cema uses the `EnvironmentManager.executeCommands()` for different operations (to create environments, install dependencies, etc). 
Behind the scenes, this method creates and executes a temporary script (a bash script on Linux and Mac, and a PowerShell script on Windows) which looks like the following:

```bash
# Install Micromamba (only if necessary)
cd "/path/to/examples/micromamba"
echo "Installing micromamba..."
curl  -Ls https://micro.mamba.pm/api/micromamba/osx-arm64/latest | tar -xvj bin/micromamba

# Initialize Micromamba
cd "/path/to/examples/micromamba"
export MAMBA_ROOT_PREFIX="/path/to/examples/micromamba"
eval "$(bin/micromamba shell hook -s posix)"

# Create the cellpose environment
cd "/Users/amasson/Travail/cema/examples"
micromamba --rc-file "/path/to/examples/micromamba/.mambarc" create -n cellpose python=3.12.7 -y

# Activate the environment
cd "/path/to/examples/"
micromamba activate cellpose

# Install the dependencies
echo "Installing conda dependencies..."
micromamba --rc-file "/path/to/examples/micromamba/.mambarc" install "cellpose==3.1.0" -y

# Execute optional custom commands
python -u example_module.py
```


---
