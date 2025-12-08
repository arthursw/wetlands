from pathlib import Path
from wetlands.environment_manager import EnvironmentManager
import urllib.request
import logging


def initialize(pip_deps=[]):
    # Initialize the environment manager
    # Wetlands will store logs and state in the wetlands_instance_path (defaults to "wetlands/")
    # Pixi/Micromamba will be installed in wetlands_instance_path/pixi by default
    logging.getLogger("wetlands").addHandler(logging.StreamHandler())
    environment_manager = EnvironmentManager()

    # Create and launch an isolated Conda environment named "cellpose"
    env = environment_manager.create("cellpose", {"conda": ["cellpose==3.1.0"], "pip": pip_deps})
    env.launch()

    # Download example image from cellpose
    image_path = Path("cellpose_img02.png")
    image_url = "https://www.cellpose.org/static/images/img02.png"

    with urllib.request.urlopen(image_url) as response:
        image_data = response.read()

    with open(image_path, "wb") as handler:
        handler.write(image_data)

    segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.png"
    return image_path, segmentation_path, env


if __name__ == "__main__":
    # Initialize: create the environment manager, the Cellpose conda environment, and download the image to segment
    image_path, segmentation_path, env = initialize()

    # Import example_module in the environment
    example_module = env.import_module("example_module.py")
    # exampleModule is a proxy to example_module.py in the environment,
    # calling exampleModule.function_name(args) will run env.execute(module_name, function_name, args)
    diameters = example_module.segment(str(image_path), str(segmentation_path))

    # Or use env.execute() directly to call a function in a module
    # diameters = env.execute("example_module.py", "segment", (image_path, segmentation_path))

    # Alternatively, use env.run_script() to run an entire Python script
    # env.run_script("script.py", args=(str(image_path), str(segmentation_path)))

    print(f"Found diameters of {diameters} pixels.")

    # Clean up and exit the environment
    env.exit()
