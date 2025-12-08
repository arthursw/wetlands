import copyreg
from contextlib import contextmanager, suppress
from multiprocessing import resource_tracker, shared_memory

import numpy as np


class NDArray:
    """
    NDArray: A wrapper around a numpy array stored in shared memory.
    Pickles into a small dict containing shared memory metadata.
    """

    def __init__(self, array: np.ndarray, shm: shared_memory.SharedMemory | None = None):
        if shm is None:
            # Allocate shared memory
            shm = shared_memory.SharedMemory(create=True, size=array.nbytes)
            # Copy array data into shared memory
            shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
            shm_arr[:] = array[:]
        else:
            # Use existing shared memory
            shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)

        self.array = shm_arr
        self.shm = shm

    def __reduce__(self):
        """
        What the object becomes when pickled.
        Returns a tuple describing how to reconstruct the object:
         (callable, args)
        """
        state = {"name": self.shm.name, "shape": self.array.shape, "dtype": str(self.array.dtype)}

        return (self._reconstruct, (state,))

    @staticmethod
    def _reconstruct(state):
        """
        Rebuilds the NDArray when unpickled.
        """
        shm = shared_memory.SharedMemory(name=state["name"])
        array = np.ndarray(state["shape"], dtype=np.dtype(state["dtype"]), buffer=shm.buf)
        return NDArray(array, shm=shm)

    def close(self):
        """Close shared memory view (but keep block alive)."""
        self.shm.close()

    def unlink(self, close=True):
        """Free the shared memory block."""
        if close:
            self.shm.close()
        self.shm.unlink()

    def unregister(self):
        # Avoid resource_tracker warnings
        # Silently ignore if unregister fails
        with suppress(Exception):
            resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore
    
    def dispose(self, unregister=True):
        """Close, free, and unregister the shared memory block.
            Best-effort teardown.
            Intended for shutdown, not for regular resource management.
        """
        # close should run first
        with suppress(Exception):
            self.close()

        # then unlink
        with suppress(Exception):
            self.unlink()

        # only unregister if requested
        if unregister:
            with suppress(Exception):
                self.unregister()

    def __repr__(self):
        return f"NDArray(shape={self.array.shape}, dtype={self.array.dtype}, shm={self.shm.name})"


_registered = False


def register_ndarray_pickle():
    """
    Register NDArray pickling with the Python copyreg framework.
    Users call this manually when they want NDArray to be picklable.
    """

    global _registered
    if _registered:
        return

    copyreg.pickle(NDArray, _pickle_ndarray)
    _registered = True


def _pickle_ndarray(obj: NDArray):
    """
    Returns (callable, args) for reconstructing NDArray during unpickling.
    """
    state = {
        "name": obj.shm.name,
        "shape": obj.array.shape,
        "dtype": str(obj.array.dtype),
    }
    return NDArray._reconstruct, (state,)

def initialize_ndarray(array: np.ndarray, ndarray:NDArray):
    if ndarray is not None:
        if ndarray.array.dtype == array.dtype and ndarray.array.shape == array.shape:
            ndarray.array[:] = array[:]
            return
        else:
            ndarray.dispose()
        return NDArray(array)
    
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
