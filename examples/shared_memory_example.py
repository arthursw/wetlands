from multiprocessing import shared_memory
from multiprocessing import resource_tracker

# Note: you need numpy to run this example, since it is used to save the resulting masks stored in the shared memory
import numpy as np

import getting_started

# Create a Conda environment from getting_started.py
image_path, segmentation_path, env = getting_started.initialize()

# Import shared_memory_module in the environment
shared_memory_module = env.import_module("shared_memory_module.py")

# run env.execute(module_name, function_name, args)
masks_shape, masks_dtype, shm_name = shared_memory_module.segment(str(image_path))

# Save the segmentation from the shared memory
shm = shared_memory.SharedMemory(name=shm_name)
masks = np.ndarray(masks_shape, dtype=masks_dtype, buffer=shm.buf)
segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.bin"
masks.tofile(segmentation_path)

# Clean up the shared memory in this process
shm.close()

# Clean up the shared memory in the other process
shared_memory_module.clean()

# Avoid resource_tracker warnings
try:
    resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore
except Exception:
    pass  # Silently ignore if unregister fails

# Clean up and exit the environment
env.exit()
