from pathlib import Path
from wetlands.environment_manager import EnvironmentManager
import urllib.request


def initialize():
    # Initialize the environment manager
    # Wetlands will use the existing Pixi or Micromamba installation at the specified path (e.g., "pixi/" or "micromamba/") if available;
    # otherwise it will automatically download and install Pixi or Micromamba in a self-contained manner.
    environmentManager = EnvironmentManager("pixi/")

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

    # Or use env.execute() directly
    # diameters = env.execute("example_module.py", "segment", (imagePath, segmentationPath))

    print(f"Found diameters of {diameters} pixels.")

    # Clean up and exit the environment
    env.exit()
