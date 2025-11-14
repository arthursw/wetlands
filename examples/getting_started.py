from pathlib import Path
from wetlands.environment_manager import EnvironmentManager
import urllib.request


def initialize():
    # Initialize the environment manager
    # Wetlands will store logs and state in the wetlandsInstancePath (defaults to "wetlands/")
    # Pixi/Micromamba will be installed in wetlandsInstancePath/pixi by default
    environmentManager = EnvironmentManager()

    # Create and launch an isolated Conda environment named "cellpose"
    env = environmentManager.create("cellpose", {"conda": ["cellpose==3.1.0"]})
    env.launch()

    # Download example image from cellpose
    imagePath = Path("cellpose_img02.png")
    imageUrl = "https://www.cellpose.org/static/images/img02.png"

    with urllib.request.urlopen(imageUrl) as response:
        imageData = response.read()

    with open(imagePath, "wb") as handler:
        handler.write(imageData)

    segmentationPath = imagePath.parent / f"{imagePath.stem}_segmentation.png"
    return imagePath, segmentationPath, env


if __name__ == "__main__":
    # Initialize: create the environment manager, the Cellpose conda environment, and download the image to segment
    imagePath, segmentationPath, env = initialize()

    # Import example_module in the environment
    exampleModule = env.importModule("example_module.py")
    # exampleModule is a proxy to example_module.py in the environment,
    # calling exampleModule.function_name(args) will run env.execute(module_name, function_name, args)
    diameters = exampleModule.segment(str(imagePath), str(segmentationPath))

    # Or use env.execute() directly to call a function in a module
    # diameters = env.execute("example_module.py", "segment", (imagePath, segmentationPath))

    # Alternatively, use env.runScript() to run an entire Python script
    # env.runScript("script.py", args=(str(imagePath), str(segmentationPath)))

    print(f"Found diameters of {diameters} pixels.")

    # Clean up and exit the environment
    env.exit()
