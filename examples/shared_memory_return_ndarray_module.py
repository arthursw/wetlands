from pathlib import Path
from wetlands.ndarray import NDArray

import example_module

ndarray: NDArray | None = None


def segment(image_path: Path | str):
    global ndarray
    # Segment the image with example_module.py
    masks = example_module.segment(image_path, return_segmentation=True)

    # Create and return shared memory from masks
    ndarray = NDArray(masks)
    return ndarray


def clean():
    global ndarray
    if ndarray is None:
        return
    ndarray.dispose()
    ndarray = None
