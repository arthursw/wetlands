from cema.environment_manager import EnvironmentManager
import logging
import cema._logging
import requests

cema._logging.setLogLevel(logging.DEBUG)

environmentManager = EnvironmentManager(
    "micromamba"
)  # if existing conda: use it, otherwise download micromamba

env = environmentManager.createAndLaunch("cellpose", dict(conda=["cellpose==3.1.0"]))

# Download example image from cellpose
image_name = "cellpose_img02.png"
image_data = requests.get("https://www.cellpose.org/static/images/img02.png").content
with open(image_name, "wb") as handler:
    handler.write(image_data)

# Call example_module.segment()
diameters = env.execute(
    "example_module.py",
    "segment",
    [image_name, image_name.replace(".png", "_segmentation.png")],
)

print(f"Found diameters of {diameters} pixels.")
environmentManager.exit(env)
