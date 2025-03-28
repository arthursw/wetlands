from multiprocessing.connection import Client
import sys
import threading
from typing import cast
from cema.dependency_manager import Dependencies
from cema.environment_manager import EnvironmentManager
import logging
import cema
from cema.external_environment import ExternalEnvironment

cema.setLogLevel(logging.DEBUG)

environmentManager = EnvironmentManager("micromamba")

env = cast(ExternalEnvironment, environmentManager.create("advanced_cellpose", Dependencies(conda=["cellpose==3.1.0"])))

process = env.executeCommands(['python -u advanced_example_module.py'])

port = 0
if process.stdout is None: sys.exit()
for line in process.stdout:
    if line.strip().startswith("Listening port "):
        port = int(line.strip().replace("Listening port ", ""))
        break

connection = Client(("localhost", port))

def logOutput() -> None:
    for line in iter(process.stdout.readline, ""): # type: ignore
        print(line.strip())

threading.Thread(target=logOutput, args=[process]).start()

imagePath = "cellpose_img02.png"
connection.send(dict(action='execute', function='downloadImage', args=[imagePath]))
result = connection.recv()
print(result)

segmentationPath = imagePath.replace(".png", "_segmentation.png")
connection.send(dict(action='execute', function='segmentImage', args=[imagePath, segmentationPath]))
print(connection.recv())