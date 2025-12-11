# Sharing memory among processes

The Python [`multiprocessing.shared_memory`](https://docs.python.org/3/library/multiprocessing.shared_memory.html) module enables to share memory among processes.
The `shared_memory_example.py` script below demonstrates this.

Wetlands provides [NDArray][wetlands.ndarray.NDArray] to automatically convert numpy arrays to shared memory objects and send them between environments. 
This requires to install Wetlands in the external environment. The `shared_memory_example_minimal.py` script below demonstrates this.

In short:
```python

import numpy as np
from wetlands.environment_manager import EnvironmentManager
from wetlands.ndarray import NDArray

environment_manager = EnvironmentManager()

env = environment_manager.create("numpy", {"pip":["wetlands>=0.4.8", "numpy==2.2.4"]})
env.launch()

minimal_module = env.import_module("minimal_module.py")

# Create the shared_memory.SharedMemory shm from the numpy array
array = NDArray(np.array([1,2,3]))
result = minimal_module.sum(array)

print(f"Sum of {array} is {result}.")

# Release the shared memory: calls shm.unlink(), shm.close() (optionally unregisters it)
array.dispose()
env.exit()
```

with `example_module.py` as follow:

```python
def sum(x):
    import numpy as np
    result = int(np.sum(x.array))
    x.close()
    return result
```


!!!note
    You can find more shared memory helpers in the [`ndarray` module][wetlands.ndarray] module.

## Standalone example

Fist, let see the use of shared memory without the Wetlands NDArray helper. No need to install wetlands in the external environment.

!!!note

    You need to install `numpy` to run this example, since it is used to save the resulting masks stored in the shared memory.

It will use `shared_memory_module.py` to create the segmentation and the shared memory holding the resulting masks.

This module defines two functions:

- a `segment` function which uses the `segment` function of `example_module.py` and creates a NumPy array backed by a shared memory to store the resulting masks,

- a `clean` function to clean up, free and release the shared memory block.

```python
{% include "../examples/shared_memory_module.py" %}
```

The `shared_memory_example.py` script creates an environment using the initialization function from `getting_started.py`.

`shared_memory_example.py`
```python
from multiprocessing import shared_memory
import numpy as np

import getting_started

# Create a Conda environment from getting_started.py
image_path, segmentation_path, env = getting_started.initialize()
```

Then, it imports `shared_memory_module.py` to perform a `cellpose` segmentation, and creates a shared memory for the resulting masks.

```python

# Import shared_memory_module in the environment
shared_memory_module = env.import_module("shared_memory_module.py")
# run env.execute(module_name, function_name, args)
diameters, masks_shape, masks_dtype, shm_name = shared_memory_module.segment(image_path)
```

This shared memory can now be used in the main process, for example to save the masks as a numpy binary file:

```python
# Save the segmentation from the shared memory
shm = shared_memory.SharedMemory(name=shm_name)
masks = np.ndarray(masks_shape, dtype=masks_dtype, buffer=shm.buf)
segmentation_path = image_path.parent / f"{image_path.stem}_segmentation.bin"
masks.tofile(segmentation_path)
    
print(f"Found diameters of {diameters} pixels.")

# Clean up and exit the environment
env.exit()
```

!!!note

    Changes to the shared memory made by one process will be refelcted in the other process. You can update the memory from both processes and perform more sofisticated operations.

## NDArray example

This requires to install Wetlands in the external environment.

NDArray is a helper class that:
- Stores a NumPy array backed by a SharedMemory block.
- On pickling, becomes a small JSON-serializable dict `{"name": shm.name, "shape": ..., "dtype": ...}`.
- On unpickling, automatically recreates the NDArray instance and re-attaches to the shared memory buffer.
- It can also be initialized with a shape and a dtype, in which case the underlying numpy array will be created on demand (when accessing the `NDArray.array` property) but not initialized!

!!! warning "Always initialize the values!"

    When initialized with a shape and a dtype, the values of the numpy array will be UNDEFINED!
    you MUST set `array.fill(0)` or `array[:] = otherArray` before using it.

The equivalent example with the NDArray helper is pretty straight-forward:

`shared_memory_example_minimal.py`:
```python
{% include "../examples/shared_memory_example_minimal.py" %}
```

And `shared_memory_module_minimal.py`:
```python
{% include "../examples/shared_memory_module_minimal.py" %}
```