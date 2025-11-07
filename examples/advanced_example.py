from multiprocessing.connection import Client
import subprocess
import sys
import threading
import logging

from wetlands.environment_manager import EnvironmentManager
from wetlands import logger


logger.setLogLevel(logging.DEBUG)

# Initialize the environment manager
# Wetlands will store logs and state in the wetlandsInstancePath (defaults to "wetlands/")
# Pixi/Micromamba will be installed in wetlandsInstancePath/pixi by default
environmentManager = EnvironmentManager()

env = environmentManager.create("advanced_cellpose", {"conda": ["cellpose==3.1.0"]})

process = env.executeCommands(["python -u advanced_example_module.py"])

port = 0

if process.stdout is None:
    print("Process has no stdout stream.", file=sys.stderr)
    sys.exit(1)

for line in process.stdout:
    if line.strip().startswith("Listening port "):
        port = int(line.strip().replace("Listening port ", ""))
        break

connection = Client(("localhost", port))


def logOutput(process: subprocess.Popen) -> None:
    for line in iter(process.stdout.readline, ""):  # type: ignore
        print(line.strip())


thread = threading.Thread(target=logOutput, args=[process]).start()

imagePath = "cellpose_img02.png"
print(f"Download image {imagePath}")
connection.send(dict(action="execute", function="downloadImage", args=[imagePath]))
result = connection.recv()
print(f"Received response: {result}")

segmentationPath = imagePath.replace(".png", "_segmentation.png")
print(f"Segment image {imagePath}")
connection.send(dict(action="execute", function="segmentImage", args=[imagePath, segmentationPath]))
result = connection.recv()
print(f"Received response: {result}")
if "diameters" in result:
    print(f"Object diameters: {result['diameters']}")

connection.send(dict(action="exit"))
connection.close()
process.wait(timeout=10)
if process.returncode is None:
    process.kill()
