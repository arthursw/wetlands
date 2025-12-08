from multiprocessing import resource_tracker
from wetlands.ndarray import NDArray

import getting_started

# Create a Conda environment from getting_started.py
image_path, segmentation_path, env = getting_started.initialize(["wetlands>=0.4.1"])

# Import shared_memory_module in the environment
shared_memory_module = env.import_module("shared_memory_module_minimal.py")

# run env.execute(module_name, function_name, args)
masks_ndarray: NDArray = shared_memory_module.segment(str(image_path))

# You could open the image on this side and send it as NDArray
# image = imagio.imread(image_path)
# image_ndarray = NDArray(image)
# masks_ndarray: NDArray = shared_memory_module.segment(image_ndarray)

# Save the segmentation from the shared memory
segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.bin"
masks_ndarray.array.tofile(segmentation_path)

# Clean up the shared memory in this process
masks_ndarray.close()

# Clean up the shared memory in the other process
shared_memory_module.clean()

# Avoid resource_tracker warnings
try:
    resource_tracker.unregister(masks_ndarray.shm._name, "shared_memory")  # type: ignore
except Exception:
    pass  # Silently ignore if unregister fails

# Clean up and exit the environment
env.exit()
