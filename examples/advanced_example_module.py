import sys
import urllib.request
from multiprocessing.connection import Listener
import example_module

def downloadImage(imagePath, connection):
    # Download example image from cellpose
    imageUrl = "https://www.cellpose.org/static/images/img02.png"
    with urllib.request.urlopen(imageUrl) as response:
        imageData = response.read()
    with open(imagePath, "wb") as handler:
        handler.write(imageData)
    connection.send(dict(message="Image downloaded."))


def segmentImage(imagePath, segmentationPath, connection):
    diameters = example_module.segment(imagePath, segmentationPath)
    connection.send(dict(message="Image segmented.", diameters=diameters))


with Listener(("localhost", 0)) as listener:
    print(f"Listening port {listener.address[1]}")
    with listener.accept() as connection:
        while message := connection.recv():
            if message["action"] == "execute":
                locals()[message["function"]](*(message["args"] + [connection]))
            if message["action"] == "exit":
                connection.send(dict(action="Exited."))
                sys.exit()
