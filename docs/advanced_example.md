
## Manual Communication with [`env.executeCommands`][wetlands.environment.Environment.executeCommands]

This example shows how to use Wetlands to run a specific script within the environment and manage the communication manually using Python's [`multiprocessing.connection`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection). This gives you full control over the interaction protocol but requires more setup.

Let's see the main script [`advanced_example.py`](https://github.com/arthursw/wetlands/blob/main/examples/advanced_example.py) step by step. 

### Initialize Wetlands and Logging

We import necessary modules, including [`Client`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Client) for manual connection and standard Python libraries like [`subprocess`](https://docs.python.org/3/library/subprocess.html#module-subprocess), [`threading`](https://docs.python.org/3/library/threading.html), and [`logging`](https://docs.python.org/3/library/logging.html). We also enable debug logging for Wetlands to see more internal details and initialize the [`EnvironmentManager`][wetlands.environment_manager.EnvironmentManager].

```python
# main_script_manual.py
from multiprocessing.connection import Client
import subprocess
import sys
import threading
import logging
from pathlib import Path
import time

from wetlands.environment_manager import EnvironmentManager
from wetlands import logger

logger.setLogLevel(logging.DEBUG)

environmentManager = EnvironmentManager("micromamba/", False)
```

### Create the Environment

Similar to the first example, we create the environment (`advanced_cellpose_env`) and specify its dependencies.

```python
deps = {"conda": ["cellpose==3.1.0"]}
env = environmentManager.create("advanced_cellpose_env", deps)
```

### Execute a Custom Script in the Environment

Instead of [`env.launch()`][wetlands.environment.Environment.launch], we use [`env.executeCommands()`][wetlands.environment.Environment.executeCommands]. This method allows us to run arbitrary shell commands within the activated environment. Here, we execute a specific Python script ([`advanced_example_module.py`](https://github.com/arthursw/wetlands/blob/main/examples/advanced_example_module.py)) using `python -u` (unbuffered output, important for reading stdout line-by-line immediately). We capture the `Popen` object for the launched process. We also redirect stderr to stdout for easier log capture.

```python
print("Executing advanced_example_module.py in environment...")
process = env.executeCommands(["python -u advanced_example_module.py"])
```

!!! note "Windows users"

    The `python` command will be available since it will be run in the conda environment.

### Establish Manual Connection

The script we just launched (`advanced_example_module.py`) is designed to start a server and print the port it's listening on to its standard output. Our main script now needs to read the `stdout` of the `process` launched by Wetlands to discover this port number. We loop through the output lines until we find the line indicating the port.

```python
port = None
if process.stdout is None:
    print("Process has no stdout stream.", file=sys.stderr)
    sys.exit(1)
print("Waiting for environment process to report listening port...")
for line in process.stdout:
    if line.strip().startswith("Listening port "):
        port = int(line.strip().replace("Listening port ", ""))
        break

print(f"Connecting to localhost:{port}...")
connection = Client(("localhost", port))
```

### Log Environment Output (Optional)

To see ongoing output from the script running in the environment, we can start a background thread that continuously reads and prints lines from the process's stdout.

```python
def log_output(proc: subprocess.Popen):
    if proc.stdout:
        for line_bytes in iter(proc.stdout.readline, b''):
            print(f"[Env Output]: {line_bytes.decode().strip()}")

output_thread = threading.Thread(target=log_output, args=(process,), daemon=True)
output_thread.start()
```

### Send Commands and Receive Results Manually

Now that we have a direct `connection` object (from [`multiprocessing.connection.Client`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Client)), we can implement our own communication protocol. We send dictionaries containing an `action`, `function` name, and `args`. We then wait (`connection.recv()`) for a response dictionary from the server script running in the environment.

```python
imagePath = "cellpose_img02.png"

print(f"Sending command: download image {imagePath}")
connection.send(dict(action="execute", function="downloadImage", args=[imagePath]))
result = connection.recv()
print(f"Received response: {result}")

segmentationPath = "cellpose_img02_segmentation.png"
print(f"Sending command: segment image {imagePath}")
args = [str(imagePath), str(segmentationPath)]
connection.send(dict(action="execute", function="segmentImage", args=args))
result = connection.recv()
print(f"Received response: {result}")
if 'diameters' in result:
    print(f"Object diameters: {result['diameters']}")
```

### Tell the Environment Process to Exit and clean up

We send a custom 'exit' message according to our protocol. The server script is designed to shut down upon receiving this message.

```python
print("Sending exit command...")
connection.send(dict(action="exit"))
```

We close our client-side connection and wait for the process we launched with `executeCommands` to terminate.

```python
connection.close()
process.wait(timeout=10)
if process.returncode is None:
    process.kill()
```

---

Now, let's examine the `advanced_example_module.py` script, which is executed by Wetlands in the isolated environment via `executeCommands`.

**Define Callable Functions**

This script defines the functions (`downloadImage`, `segmentImage`) that the main script will invoke remotely. These functions perform the actual work (downloading, segmenting using `example_module`) *inside the environment* and use the provided `connection` object to send back results or status messages.

```python
# advanced_example_module.py
import sys
import urllib.request
from multiprocessing.connection import Listener
from pathlib import Path
import example_module # Reuse logic from the simple example module

def downloadImage(imagePath_str, connection):
    """Downloads the image *inside* the environment."""
    imagePath = Path(imagePath_str)
    imageUrl = "https://www.cellpose.org/static/images/img02.png"
    print(f"[Inside Env] Downloading image to {imagePath}...")
    try:
        with urllib.request.urlopen(imageUrl) as response:
            imageData = response.read()
        with open(imagePath, "wb") as handler:
            handler.write(imageData)
        print("[Inside Env] Image downloaded.")
        connection.send(dict(status="success", message="Image downloaded."))
    except Exception as e:
        print(f"[Inside Env] Error downloading image: {e}")
        connection.send(dict(status="error", message=str(e)))

def segmentImage(imagePath_str, segmentationPath_str, connection):
    """Runs segmentation *inside* the environment."""
    imagePath = Path(imagePath_str)
    segmentationPath = Path(segmentationPath_str)
    print(f"[Inside Env] Segmenting {imagePath}...")
    try:
        diameters = example_module.segment(imagePath, segmentationPath)
        print("[Inside Env] Segmentation complete.")
        connection.send(dict(status="success", message="Image segmented.", diameters=diameters))
    except Exception as e:
        print(f"[Inside Env] Error during segmentation: {e}")
        connection.send(dict(status="error", message=str(e)))
```

**Set Up the Server**

The main part of the script uses [`multiprocessing.connection.Listener`](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Listener) to create a server socket listening on `localhost` and an OS-assigned port (`0`). **Crucially, it prints the chosen port number to standard output**, which is how the main script discovers where to connect. It then waits for the main script to connect (`listener.accept()`).

```python

with Listener(("localhost", 0)) as listener:
    # Print the port for the main process to read
    print(f"Listening port {listener.address[1]}", flush=True)
    with listener.accept() as connection:
```

**Process Incoming Messages**

Once connected, the script enters a loop, waiting to receive messages (`connection.recv()`). It parses the received dictionary, checks the `action`, and calls the corresponding local function (`downloadImage` or `segmentImage`) if the action is `execute`. If the action is `exit`, it sends a confirmation and terminates the script (`sys.exit(0)`).

```python
        while message := connection.recv():
            if message["action"] == "execute":
                locals()[message["function"]](*(message["args"] + [connection]))
            if message["action"] == "exit":
                connection.send(dict(action="Exited."))
                sys.exit(0)

```

**Summary of Example 2 Flow:**

The main script uses [`EnvironmentManager`][wetlands.environment_manager.EnvironmentManager] to create an environment. [`env.executeCommands()`][wetlands.environment.Environment.executeCommands] starts a *custom* server script (`advanced_example_module.py`) inside the environment. The main script reads the server's port from stdout and connects manually using `Client`. Communication happens via custom message dictionaries sent over this connection. The main script explicitly tells the server to exit before cleaning up the process started by [`executeCommands`][wetlands.environment.Environment.executeCommands]. This approach offers more control but requires implementing the server logic and communication protocol.
