from pathlib import Path
import sys
import urllib.request
from multiprocessing.connection import Listener, Connection
import example_module


def download_image(image_path: Path | str, connection: Connection):
    # Download example image from cellpose
    image_url = "https://www.cellpose.org/static/images/img02.png"
    with urllib.request.urlopen(image_url) as response:
        image_data = response.read()
    with open(image_path, "wb") as handler:
        handler.write(image_data)
    connection.send(dict(message="Image downloaded."))


def segment_image(image_path: Path | str, segmentation_path: Path | str, connection: Connection):
    diameters = example_module.segment(image_path, segmentation_path)
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
