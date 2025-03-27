from multiprocessing.connection import Client
import threading
from cema.environment_manager import EnvironmentManager
import logging
import cema

cema.setLogLevel(logging.DEBUG)

environmentManager = EnvironmentManager("micromamba")

env = environmentManager.create("advanced_cellpose", dict(conda=["cellpose==3.1.0"]))

process = env.executeCommands(['python -u advanced_example_module.py'])

for line in process.stdout:
    if line.strip().startswith("Listening port "):
        port = int(line.strip().replace("Listening port ", ""))
        break

connection = Client(("localhost", port))

def logOutput() -> None:
    for line in iter(process.stdout.readline, ""):
        print(line.strip())

threading.Thread(target=logOutput, args=[process]).start()

imagePath = "cellpose_img02.png"
connection.send(dict(action='execute', function='downloadImage', args=[imagePath]))
result = connection.recv()
print(result)

segmentationPath = imagePath.replace(".png", "_segmentation.png")
connection.send(dict(action='execute', function='segmentImage', args=[imagePath, segmentationPath]))
print(connection.recv())