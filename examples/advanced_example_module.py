import sys
import requests
from multiprocessing.connection import Listener
import example_module


def downloadImage(imagePath, connection):
    # Download example image from cellpose
    imageData = requests.get("https://www.cellpose.org/static/images/img02.png").content
    with open(imagePath, "wb") as handler:
        handler.write(imageData)
    connection.send(dict(message="image downloaded"))


def segmentImage(imagePath, segmentationPath, connection):
    diameters = example_module.segment(imagePath, segmentationPath)
    connection.send(dict(message="image segmented", diameters=diameters))


with Listener(("localhost", 0)) as listener:
    # Print ready message for the environment manager (it can now open a client to send messages)
    print(f"Listening port {listener.address[1]}")
    with listener.accept() as connection:
        while message := connection.recv():
            if message["action"] == "execute":
                locals()[message["function"]](message["args"] + [connection])
            if message["action"] == "exit":
                connection.send(dict(action="exited"))
                sys.exit()
