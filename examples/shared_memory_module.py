import numpy as np
from pathlib import Path
from multiprocessing import shared_memory

import example_module

shm: shared_memory.SharedMemory | None = None


def segment(image_path: Path | str):
    global shm
    # Segment the image with example_module.py
    masks, flows, styles, diams = example_module.segment(image_path)
    # Create the shared memory
    shm = shared_memory.SharedMemory(create=True, size=masks.nbytes)
    # Create a NumPy array backed by shared memory
    masks_shared = np.ndarray(masks.shape, dtype=masks.dtype, buffer=shm.buf)
    # Copy the masks into the shared memory
    masks_shared[:] = masks[:]
    # Return the shape, dtype and shared memory name to recreate the numpy array on the other side
    return masks.shape, masks.dtype, shm.name


def clean():
    global shm
    if shm is None:
        return
    # Clean up the shared memory in this process
    shm.close()
    # Free and release the shared memory block
    shm.unlink()
