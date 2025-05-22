import numpy as np
from pathlib import Path
from multiprocessing import shared_memory

import example_module

shm: shared_memory.SharedMemory | None = None


def segment(imagePath: Path | str):
    global shm
    # Segment the image with example_module.py
    masks, flows, styles, diams = example_module.segment(imagePath)
    # Create the shared memory
    shm = shared_memory.SharedMemory(create=True, size=masks.nbytes)
    # Create a NumPy array backed by shared memory
    masksShared = np.ndarray(masks.shape, dtype=masks.dtype, buffer=shm.buf)
    # Copy the masks into the shared memory
    masksShared[:] = masks[:]
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
