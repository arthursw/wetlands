from wetlands.ndarray import NDArray

import imageio  # type: ignore
import getting_started

# Create a Conda environment from getting_started.py
image_path, env = getting_started.initialize(["wetlands>=0.4.1"])

# Import shared_memory_module in the environment
shared_memory_module = env.import_module("shared_memory_provide_ndarrays_module.py")

# Open the image (requires imageio)
image = imageio.imread(image_path)  # type: ignore

# Creates the shared memory in a context to close and unlink it automatically
with (
    NDArray(image) as ndimage,
    NDArray(shape=image.shape[:2], dtype="uint8") as ndsegmentation,
):
    # run env.execute(module_name, function_name, args)
    shared_memory_module.segment(ndimage, ndsegmentation)

    # Save the segmentation from the shared memory
    segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.bin"
    ndsegmentation.array.tofile(segmentation_path)

# Clean up and exit the environment
env.exit()
