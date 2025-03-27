from cema.environment_manager import EnvironmentManager
import logging
import cema
import requests

cema.setLogLevel(logging.DEBUG)

environmentManager = EnvironmentManager(
    "micromamba"
)  # if existing conda: use it, otherwise download micromamba

env = environmentManager.create("cellpose", dict(conda=["cellpose==3.1.0"]))
env.launch()

# Download example image from cellpose
imagePath = "cellpose_img02.png"
imageData = requests.get("https://www.cellpose.org/static/images/img02.png").content
with open(imagePath, "wb") as handler:
    handler.write(imageData)

segmentationPath = imagePath.replace(".png", "_segmentation.png")

# Import example_module and execute example_module.segment()
example_module = env.importModule('example_module.py') 
# example_module is a fake module with the functions of example_module.py,
# when called, those function will run the env.execute(module_name, function_name, args)
example_module.segment(imagePath, segmentationPath)

# Or use env.execute() to call example_module.segment()
diameters = env.execute("example_module.py", "segment", [imagePath, segmentationPath])

print(f"Found diameters of {diameters} pixels.")

env.exit()