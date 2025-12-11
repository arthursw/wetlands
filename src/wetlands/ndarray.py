import copyreg
from contextlib import contextmanager, suppress
from multiprocessing import resource_tracker, shared_memory

import numpy as np


class NDArray:
    """
    NDArray: A wrapper around a numpy array stored in shared memory.
    Pickles into a small dict containing shared memory metadata.

    Can be initialized in two ways:
    1. With an array: NDArray(array=my_array) - creates shared memory and copies data
    2. With shape and dtype: NDArray(shape=(100, 100), dtype='float32') -
       creates shared memory but defers numpy array creation until first access (lazy)

    Args:
        array: numpy array to wrap (mutually exclusive with shape/dtype)
        shm: existing SharedMemory object to use (optional)
        shape: shape of array for lazy initialization (requires dtype)
        dtype: dtype of array for lazy initialization (requires shape)
        unregister_on_exit: whether to unregister shared memory on context exit
    """

    def __init__(
        self,
        array: np.ndarray | None = None,
        shm: shared_memory.SharedMemory | None = None,
        shape: tuple | None = None,
        dtype: str | type | None = None,
        unregister_on_exit: bool = True,
    ):
        if array is not None:
            if shm is None:
                # Allocate shared memory
                shm = shared_memory.SharedMemory(create=True, size=array.nbytes)
                # Copy array data into shared memory
                shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
                shm_arr[:] = array[:]
            else:
                # Use existing shared memory
                shm_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
            self._array = shm_arr
            self._shape = array.shape
            self._dtype = array.dtype
        else:
            # Lazy initialization with shape and dtype
            if shape is None or dtype is None:
                raise ValueError("Either 'array' or both 'shape' and 'dtype' must be provided")
            resolved_dtype = np.dtype(dtype) if not isinstance(dtype, np.dtype) else dtype
            if shm is None:
                # Allocate shared memory
                size = int(np.prod(shape) * resolved_dtype.itemsize)
                shm = shared_memory.SharedMemory(create=True, size=size)
            self._array = None
            self._shape = shape
            self._dtype = resolved_dtype

        self.shm = shm
        self.unregister_on_exit = unregister_on_exit

    @property
    def array(self) -> np.ndarray:
        """Lazily create the numpy array from shared memory on first access."""
        if self._array is None:
            self._array = np.ndarray(self._shape, dtype=self._dtype, buffer=self.shm.buf)
        return self._array

    def __reduce__(self):
        """
        What the object becomes when pickled.
        Returns a tuple describing how to reconstruct the object:
         (callable, args)
        """
        assert self.shm is not None, "shm must not be None for pickling"
        state = {"name": self.shm.name, "shape": self._shape, "dtype": str(self._dtype)}

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
        if self.shm is not None:
            self.shm.close()

    def unlink(self, close=True):
        """Free the shared memory block."""
        if self.shm is not None:
            if close:
                self.shm.close()
            self.shm.unlink()

    def unregister(self):
        # Avoid resource_tracker warnings
        # Silently ignore if unregister fails
        if self.shm is not None:
            with suppress(Exception):
                resource_tracker.unregister(self.shm._name, "shared_memory")  # type: ignore

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

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Exit context manager and cleanup resources."""
        self.dispose(unregister=self.unregister_on_exit)
        return False

    def __repr__(self):
        shm_name = self.shm.name if self.shm is not None else "None"
        return f"NDArray(shape={self._shape}, dtype={self._dtype}, shm={shm_name})"


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


def update_ndarray(array: np.ndarray, ndarray: NDArray | None):
    """updates ndarray from array:
    if ndarray is None: create an NDArray from array
    else:
        if array has the same shape and size as ndarray:
            update the ndarray values and return it
        else: delete the ndarray and create a new one from array
    """
    if ndarray is not None:
        if ndarray.array.dtype == array.dtype and ndarray.array.shape == array.shape:
            ndarray.array[:] = array[:]
            return ndarray
        ndarray.dispose(unregister=False)
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
