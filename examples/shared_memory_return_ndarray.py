from wetlands.ndarray import NDArray

import getting_started

# Create a Conda environment from getting_started.py
image_path, env = getting_started.initialize(["wetlands>=0.4.1"])

# Import shared_memory_module in the environment
shared_memory_module = env.import_module("shared_memory_return_ndarray_module.py")

# run env.execute(module_name, function_name, args)
masks_ndarray: NDArray = shared_memory_module.segment(str(image_path), return_segmentation=True)

# Save the segmentation from the shared memory
segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.bin"
masks_ndarray.array.tofile(segmentation_path)

# Clean up the shared memory in this process
masks_ndarray.close()

# Clean up the shared memory in the other process
shared_memory_module.clean()
# Warning: masks_ndarray.array is now freed, do not access it
# masks_ndarray.array[0] causes a segmentation fault

# Clean up and exit the environment
env.exit()
