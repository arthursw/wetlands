## 🎓 Step by Step

Wetlands leverages **Pixi**, a package management tool for developers, or **Micromamba**, a fast, native reimplementation of the Conda package manager.

1.  **Pixi or Micromamba Setup:** When `EnvironmentManager` is initialized, it checks for a `pixi` or `micromamba` executable at the specified path (e.g., `"micromamba/"`). If not found, it downloads a self-contained Pixi or Micromamba binary suitable for the current operating system and architecture into that directory. This means Wetlands doesn't require a pre-existing Conda/Mamba installation.
2.  **Environment Creation:** `create(envName, dependencies)` uses Pixi or Micromamba commands (`pixi init /path/to/envName` or  `micromamba create -n envName -c channel package ...`) to build a new, isolated Conda environment within the Pixi or Micromamba prefix (e.g., `pixi/workspaces/envName/envs/default/` or `micromamba/envs/envName`). When using Pixi, Wetlands also creates a workspace for the environment (e.g. `pixi/workspace/envName/`). Note that the main environemnt is returned if it already satisfies the required dependencies.
3.  **Dependency Installation:** Dependencies (Conda packages, Pip packages) are installed into the target environment using `pixi add ...` or `micromamba install ...` and `pip install ...` (executed within the activated environment).
4.  **Execution (`launch`/`execute`/`importModule`):**
    *   `launch()` starts a helper Python script (`wetlands._internal.executor_server`) *within* the activated target environment using `subprocess.Popen`.
    *   This server listens on a local socket using `multiprocessing.connection.Listener`.
    *   The main process connects to this server using `multiprocessing.connection.Client`.
    *   `execute(module, func, args)` sends a message containing the module path, function name, and arguments to the server.
    *   The server imports the module (if not already imported), executes the function with the provided arguments, and sends the result (or exception) back to the main process.
    *   `importModule(module)` creates a proxy object in the main process. When methods are called on this proxy, it triggers the `execute` mechanism described above.
5.  **Direct Execution (`executeCommands`):** This method directly activates the target environment and runs the provided shell commands using `subprocess.Popen` (no communication server involved here). The user is responsible for managing the launched process and any necessary communication.
6.  **Isolation:** Each environment created by Wetlands is fully isolated, preventing dependency conflicts between different environments or with the main application's environment.


## ⚙️ Under the Hood


Wetlands uses the `EnvironmentManager.executeCommands()` for different operations (to create environments, install dependencies, etc). 
Behind the scenes, this method creates and executes a temporary script (a bash script on Linux and Mac, and a PowerShell script on Windows) which looks like the following:

```bash
# Initialize Micromamba
cd "/path/to/examples/micromamba"
export MAMBA_ROOT_PREFIX="/path/to/examples/micromamba"
eval "$(micromamba shell hook -s posix)"

# Create the cellpose environment
cd "/Users/amasson/Travail/wetlands/examples"
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
