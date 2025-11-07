# Sharing memory among processes

The Python [`multiprocessing.shared_memory`](https://docs.python.org/3/library/multiprocessing.shared_memory.html) module enables to share memory among processes.

The `shared_memory_example.py` script demonstrate this.

!!!note
    Please see the shared memory helpers in the [`shared_memory` module][wetlands.shared_memory], even though there are not used in the examples.

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
imagePath, segmentationPath, env = getting_started.initialize()
```

Then, it imports `shared_memory_module.py` to perform a `cellpose` segmentation, and creates a shared memory for the resulting masks.

```python

# Import shared_memory_module in the environment
sharedMemoryModule = env.importModule("shared_memory_module.py")
# run env.execute(module_name, function_name, args)
diameters, masksShape, masksDtype, shmName = sharedMemoryModule.segment(imagePath)
```

This shared memory can now be used in the main process, for example to save the masks as a numpy binary file:

```python
# Save the segmentation from the shared memory
shm = shared_memory.SharedMemory(name=shmName)
masks = np.ndarray(masksShape, dtype=masksDtype, buffer=shm.buf)
segmentationPath = imagePath.parent / f"{imagePath.stem}_segmentation.bin"
masks.tofile(segmentationPath)
    
print(f"Found diameters of {diameters} pixels.")

# Clean up and exit the environment
env.exit()
```

!!!note

    Changes to the shared memory made by one process will be refelcted in the other process. You can update the memory from both processes and perform more sofisticated operations.