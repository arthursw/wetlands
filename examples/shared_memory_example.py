from multiprocessing import shared_memory
from multiprocessing import resource_tracker

# Note: you need numpy to run this example, since it is used to save the resulting masks stored in the shared memory
import numpy as np

import getting_started

# Create a Conda environment from getting_started.py
imagePath, segmentationPath, env = getting_started.initialize()

# Import shared_memory_module in the environment
sharedMemoryModule = env.importModule("shared_memory_module.py")
# run env.execute(module_name, function_name, args)
masksShape, masksDtype, shmName = sharedMemoryModule.segment(str(imagePath))

# Save the segmentation from the shared memory
shm = shared_memory.SharedMemory(name=shmName)
masks = np.ndarray(masksShape, dtype=masksDtype, buffer=shm.buf)
segmentationPath = imagePath.parent / f"{imagePath.stem}_segmentation.bin"
masks.tofile(segmentationPath)

# Clean up the shared memory in this process
shm.close()

# Clean up the shared memory in the other process
sharedMemoryModule.clean()

# Avoid resource_tracker warnings
try:
    resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore
except Exception:
    pass  # Silently ignore if unregister fails

# Clean up and exit the environment
env.exit()
