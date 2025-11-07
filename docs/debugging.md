# Debugging Wetlands Environments

Wetlands provides built-in debugging tools that allow you to attach a debugger to an isolated environment and step through code execution in real-time. This is essential for development and troubleshooting when running code in isolated Conda environments.

## Overview

The debugging system includes:

- **IDE Integration**: Support for VS Code and PyCharm
- **Remote Debugging**: Attach to running environment processes
- **Environment Management**: List and kill running environments
- **Port Allocation**: Automatic debug port assignment for each environment

## Installation

The debugging tools are part of the Wetlands package and are automatically installed with:

```bash
pip install wetlands
```

## Usage Commands

### 1. `wetlands list` - View Running Environments

List all currently running Wetlands environments and their debug ports.

```bash
wetlands list [-wip PATH]
```

**Arguments:**

- `-wip, --wetlandsInstancePath PATH` (optional): Path to the Wetlands instance folder (default: `pixi/wetlands`)

**Example:**

```bash
$ wetlands list
Running wetlands environments (for all wetlands instance):

Command line | Process ID | Parent process ID
---
python /path/to/wetlands/module_executor.py env1 --wetlandsInstancePath /tmp/wetlands | 12345 | 12340
python /path/to/wetlands/module_executor.py env2 --wetlandsInstancePath /tmp/wetlands | 12346 | 12340

Environments of the wetlands instance /tmp/wetlands:

Environment | Debug Port | Path
---
env1 | 5678 | /path/to/module_executor.py
env2 | 5679 | /path/to/module_executor.py
```

This command displays:
- **Running processes**: All active Wetlands environment processes with their PIDs
- **Available debug ports**: The port number assigned to each environment for debugging
- **Module executor paths**: Where the module executor is located for each environment

### 2. `wetlands debug` - Attach Debugger to an Environment

Attach VS Code or PyCharm to a running Wetlands environment for debugging.

```bash
wetlands debug -s SOURCE_PATH -n ENV_NAME [-ide {vscode,pycharm}] [-wip PATH] [-jmc]
```

**Arguments:**

- `-s, --sources SOURCE_PATH` (required): Path to the source code directory you want to debug
- `-n, --name ENV_NAME` (required): Name of the environment to debug
- `-ide, --ide {vscode,pycharm}` (optional): IDE to use (default: `vscode`)
- `-wip, --wetlandsInstancePath PATH` (optional): Path to the Wetlands instance folder (default: `pixi/wetlands`)
- `-jmc, --justMyCode` (optional, VS Code only): Only debug your source files, not library code

**Example - VS Code:**

```bash
wetlands debug -s /path/to/my/project -n my_env
```

**Example - PyCharm:**

```bash
wetlands debug -s /path/to/my/project -n my_env -ide pycharm
```

**Example - VS Code with Just My Code:**

```bash
wetlands debug -s /path/to/my/project -n my_env -jmc
```

#### What Happens When You Run `wetlands debug`

1. **Configuration Detection**: Wetlands searches for running processes matching the environment name
2. **Port Discovery**: The debug port for the environment is read from `debug_ports.json` in the Wetlands instance directory
3. **IDE Configuration**:
   - **VS Code**: Creates/updates `.vscode/launch.json` with remote attach configuration
   - **PyCharm**: Creates `.idea/runConfigurations/Remote_Attach_Wetlands.xml` with remote debugging configuration
4. **IDE Launch**: Opens the specified IDE with the source directory
5. **Debugger Connection**: The IDE connects to the remote debugger running in the isolated environment

### 3. `wetlands kill` - Stop an Environment

Terminate a running Wetlands environment and all its child processes.

```bash
wetlands kill -n ENV_NAME [-wip PATH]
```

**Arguments:**

- `-n, --name ENV_NAME` (required): Name of the environment to kill
- `-wip, --wetlandsInstancePath PATH` (optional): Path to the Wetlands instance folder (default: `pixi/wetlands`)

**Example:**

```bash
wetlands kill -n my_env
```

## Debugging with VS Code

### Setup and Debugging

1. **Start your Wetlands environment** in your Python script:

```python
from wetlands.environment_manager import EnvironmentManager

env_manager = EnvironmentManager("pixi/")
env = env_manager.create("my_env", {"pip": ["numpy", "pandas"]})
env.launch()

# Your code here
```

2. **List available environments** to verify it's running:

```bash
wetlands list
```

3. **Attach the debugger** with VS Code:

```bash
wetlands debug -s /path/to/my/project -n my_env
```

4. **Set breakpoints** in VS Code and interact with your environment code

5. **Stop debugging** when finished:

```bash
wetlands kill -n my_env
```

### Configuration Details

The `launch.json` file created by Wetlands for VS Code contains:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python Debugger: Remote Attach Wetlands",
      "type": "debugpy",
      "request": "attach",
      "justMyCode": false,
      "connect": {
        "host": "localhost",
        "port": 5678
      },
      "pathMappings": [
        {
          "localRoot": "/path/to/module/executor",
          "remoteRoot": "/path/to/module/executor"
        }
      ]
    }
  ]
}
```

- **type**: Uses `debugpy` for Python debugging
- **connect**: Specifies `localhost` and the assigned debug port
- **justMyCode**: Set to `true` if you used the `-jmc` flag
- **pathMappings**: Maps local source paths to remote paths in the environment

## Debugging with PyCharm

### Setup and Debugging

1. **Start your Wetlands environment** in your Python script (same as VS Code)

2. **Attach the debugger** with PyCharm:

```bash
wetlands debug -s /path/to/my/project -n my_env -ide pycharm
```

3. **Select the run configuration** in PyCharm:
   - Look for "Remote_Attach_Wetlands" in the run configurations dropdown
   - Click the Debug button

4. **Set breakpoints** in PyCharm and interact with your environment code

5. **Stop debugging** when finished:

```bash
wetlands kill -n my_env
```

### Configuration Details

The XML configuration file created in `.idea/runConfigurations/Remote_Attach_Wetlands.xml` contains:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<component name="ProjectRunConfigurationManager">
  <configuration name="Remote Attach Wetlands" type="Python"
                 factoryName="Python" show_console_on_std_err="false"
                 show_console_on_std_out="false">
    <module name="$PROJECT_NAME" />
    <option name="PATH_MAPPINGS">
      <list>
        <item index="0" itemvalue="/path/to/module/executor:/path/to/module/executor" />
      </list>
    </option>
  </configuration>
</component>
```

- Creates a Python remote debugging configuration
- Path mappings ensure source code paths are correctly resolved

## Workflow Example

Here's a complete workflow for debugging a Wetlands environment:

### 1. Create Your Project Structure

```
my_project/
├── main.py              # Main script
├── module_to_debug.py   # Code to debug
└── requirements.txt     # Dependencies
```

### 2. Create the Main Script

```python
# main.py
from pathlib import Path
from wetlands.environment_manager import EnvironmentManager

env_manager = EnvironmentManager("pixi/")
env = env_manager.create(
    "my_debug_env",
    {"pip": ["requests", "numpy"]}
)
env.launch()

# Import module and call function
my_module = env.importModule("module_to_debug.py")
result = my_module.process_data([1, 2, 3, 4, 5])

print(f"Result: {result}")
env.exit()
```

### 3. Create the Module to Debug

```python
# module_to_debug.py
import numpy as np

def process_data(data):
    """Process data with numpy"""
    arr = np.array(data)
    result = np.sum(arr) * 2
    return int(result)  # Breakpoint here to inspect values
```

### 4. Start Your Application

```bash
python main.py
```

### 5. Launch the Environment

Keep the main script running, and in another terminal:

```bash
wetlands list
```

You'll see `my_debug_env` listed.

### 6. Attach the Debugger

```bash
# For VS Code:
wetlands debug -s /path/to/my_project -n my_debug_env

# Or for PyCharm:
wetlands debug -s /path/to/my_project -n my_debug_env -ide pycharm
```

### 7. Debug Your Code

- Set breakpoints in `module_to_debug.py`
- Call functions from your main script
- Step through code and inspect variables
- VS Code/PyCharm will break at your breakpoints

### 8. Clean Up

When finished:

```bash
wetlands kill -n my_debug_env
```

## Troubleshooting

### "Debug ports file does not exist"

**Cause**: The Wetlands instance hasn't created the `debug_ports.json` file yet.

**Solution**: Make sure your environment has been launched with `env.launch()` before running the debug command.

### "Debug port not found for environment"

**Cause**: The environment name doesn't exist or is spelled incorrectly.

**Solution**:
- Verify the environment is running: `wetlands list`
- Check the exact environment name
- Make sure you're using the correct `-wip` path if you have multiple Wetlands instances

### "No wetlands process with environment name found"

**Cause**: The specified environment is not currently running.

**Solution**:
- Start your Python script that creates and launches the environment
- Keep the script running while debugging
- Verify with `wetlands list`

### Debugger doesn't break at breakpoints

**Cause**: The source code path mapping might be incorrect.

**Solution**:
- Ensure the source path you provide to `wetlands debug` matches your actual source code location
- Check the path mappings in the generated configuration file
- Verify that the line numbers haven't changed since you set the breakpoint

### "Kill environment: no process found"

**Cause**: The environment is not currently running.

**Solution**: This is usually not a problem. Verify with `wetlands list` that the environment isn't running, and proceed.

## Advanced Usage

### Debugging Multiple Environments

You can debug multiple environments simultaneously by:

1. Opening multiple IDE windows
2. Using `wetlands debug` with different environment names
3. Each environment gets its own debug port

```bash
# Terminal 1
wetlands debug -s /project1 -n env1

# Terminal 2
wetlands debug -s /project2 -n env2
```

### Debugging with Custom Wetlands Instances

If you have multiple Wetlands instances, specify the instance path:

```bash
wetlands debug -s /path/to/project -n my_env -wip /custom/wetlands/path
```

### VS Code Only: Just My Code Mode

Debug only your code, skipping library internals:

```bash
wetlands debug -s /path/to/project -n my_env -jmc
```

This sets `justMyCode: true` in the VS Code launch configuration, which speeds up debugging by not stepping into library code.

## How It Works Under the Hood

1. **Process Detection**: Wetlands uses `psutil` to find running processes matching your environment
2. **Port Assignment**: Each environment gets a unique debug port from `debug_ports.json`
3. **Debugpy Integration**: The environment's module executor runs `debugpy` in socket mode
4. **IDE Configuration**: Wetlands generates IDE-specific configuration files for remote attach
5. **Network Connection**: Your IDE connects via localhost to the debug port in the isolated environment

## See Also

- [Getting Started](getting_started.md) - Basic Wetlands usage
- [Advanced Example](advanced_example.md) - More complex scenarios
- [How It Works](how_it_works.md) - Internal architecture
