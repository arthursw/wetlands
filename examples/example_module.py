import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np


def threshold(image: "np.ndarray", value: float) -> "np.ndarray":
    return image > value


def process_items(items, task=None):
    """Process a sequence while demonstrating the injected task context."""
    results = []
    for index, item in enumerate(items, start=1):
        time.sleep(0.05)
        results.append(item * 2)
        if task is not None:
            task.update("Processing items", current=index, maximum=len(items))
    if task is not None:
        task.set_output("items_processed", len(results))
        task.log("Worker finished processing items", logging.INFO)
    return results


def cooperative_work(task=None):
    """Wait until the caller requests cancellation, then acknowledge it."""
    if task is not None:
        task.update("Cooperative task is running", current=0, maximum=1)
    while task is None or not task.cancel_requested:
        time.sleep(0.02)
    task.cancel()
    return "stopped"


def ignore_cancellation(task=None):
    """Remain busy long enough for Wetlands to replace this worker."""
    if task is not None:
        task.update("Non-cooperative task is running", current=0, maximum=1)
    time.sleep(300)
    return "unreachable"


def raise_example_error():
    raise ValueError("The worker could not process this input")


def slow_sum(values):
    time.sleep(2)
    return sum(values)
