from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np


def threshold(image: "np.ndarray", value: float) -> "np.ndarray":
    return image > value
