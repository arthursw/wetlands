from multiprocessing.connection import Client
import subprocess
import sys
import threading
import logging

from wetlands.environment_manager import EnvironmentManager

# Set up logging for wetlands
_base = logging.getLogger("wetlands")
_base.setLevel(logging.DEBUG)

# Initialize the environment manager
# Wetlands will store logs and state in the wetlands_instance_path (defaults to "wetlands/")
# Pixi/Micromamba will be installed in wetlands_instance_path/pixi by default
environment_manager = EnvironmentManager()

env = environment_manager.create("advanced_cellpose", {"conda": ["cellpose==3.1.0"]})

# Warning: set log=False since process output is handled manually below (having two loggers reading the same stdout causes issues)
process = env.execute_commands(["python -u advanced_example_module.py"], log=False)

port = 0

if process.stdout is None:
    print("Process has no stdout stream.", file=sys.stderr)
    sys.exit(1)

for line in process.stdout:
    if line.strip().startswith("Listening port "):
        port = int(line.strip().replace("Listening port ", ""))
        break

connection = Client(("localhost", port))


def log_output(process: subprocess.Popen) -> None:
    for line in iter(process.stdout.readline, ""):  # type: ignore
        print(line.strip())


thread = threading.Thread(target=log_output, args=[process]).start()

image_path = "cellpose_img02.png"
print(f"Download image {image_path}")
connection.send(dict(action="execute", function="download_image", args=[image_path]))
result = connection.recv()
print(f"Received response: {result}")

segmentation_path = image_path.replace(".png", "_segmentation.png")
print(f"Segment image {image_path}")
connection.send(dict(action="execute", function="segment_image", args=[image_path, segmentation_path]))
result = connection.recv()
print(f"Received response: {result}")
if "diameters" in result:
    print(f"Object diameters: {result['diameters']}")

connection.send(dict(action="exit"))
connection.close()
process.wait(timeout=10)
if process.returncode is None:
    process.kill()
