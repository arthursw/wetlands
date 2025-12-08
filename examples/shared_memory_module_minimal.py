from pathlib import Path
from wetlands.ndarray import NDArray

import example_module

ndarray: NDArray | None = None


def segment(image_path: Path | str):
    global ndarray
    # Segment the image with example_module.py
    masks, flows, styles, diams = example_module.segment(image_path)

    # Or, if the image was send with NDArray (image_path would be a NDArray)
    # masks, flows, styles, diams = example_module.segment(image_path.array)

    # Create the shared memory
    ndarray = NDArray(masks)
    # Return the ndarray
    return ndarray


def clean():
    global ndarray
    if ndarray is None:
        return
    ndarray.dispose()
    ndarray = None
