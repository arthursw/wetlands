from contextlib import contextmanager, suppress
from multiprocessing import resource_tracker, shared_memory

import numpy as np


def create_shared_array(shape: tuple, dtype: str | type):
    # Create the shared memory
    shm = shared_memory.SharedMemory(create=True, size=int(np.prod(shape) * np.dtype(dtype).itemsize))
    # Create a NumPy array backed by shared memory
    shared = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    return shared, shm


def share_array(
    array: np.ndarray,
) -> tuple[np.ndarray, shared_memory.SharedMemory]:
    # Create the shared memory and numpy array
    shared, shm = create_shared_array(array.shape, dtype=array.dtype)
    # Copy the array into the shared memory
    shared[:] = array[:]
    # Return the shape, dtype and shared memory name to recreate the numpy array on the other side
    return shared, shm


def wrap(shared: np.ndarray, shm: shared_memory.SharedMemory):
    return {"name": shm.name, "shape": shared.shape, "dtype": shared.dtype}


def unwrap(shmw: dict):
    shm = shared_memory.SharedMemory(name=shmw["name"])
    shared_array = np.ndarray(shmw["shape"], dtype=shmw["dtype"], buffer=shm.buf)
    return shared_array, shm


def release_shared_memory(
    shm: shared_memory.SharedMemory | None,
    unlink: bool = True,
):
    if shm is None:
        return
    if unlink:
        shm.unlink()
    shm.close()


@contextmanager
def share_manage_array(original_array: np.ndarray, unlink_on_exit: bool = True):
    shm = None
    try:
        shared, shm = share_array(original_array)
        yield wrap(shared, shm)
    finally:
        release_shared_memory(shm, unlink_on_exit)


@contextmanager
def get_shared_array(wrapper: dict):
    shm = None
    try:
        shared_array, shm = unwrap(wrapper)
        yield shared_array
    finally:
        if shm is not None:
            shm.close()


def unregister(shm: shared_memory.SharedMemory):
    # Avoid resource_tracker warnings
    # Silently ignore if unregister fails
    with suppress(Exception):
        resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore
