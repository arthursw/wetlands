# Values and NumPy ownership

Wetlands transports NumPy arrays automatically through its value codec protocol.
Application and worker code exchange normal `numpy.ndarray` objects and never manage shared-memory handles.

## Example

```python
import numpy as np

image = np.arange(256, dtype=np.float32).reshape(16, 16)

with environment.start() as pool:
    masks = pool.execute_import(
        "worker_package.segmentation:segment",
        kwargs={"image": image, "threshold": 0.5},
    )

assert isinstance(masks, np.ndarray)
```

Worker code is ordinary Python:

```python
def segment(image: "numpy.ndarray", threshold: float) -> "numpy.ndarray":
    image[...] = image / image.max()
    return image > threshold
```

Worker mutation does not modify the caller's original array.
The result returned to the host is independently owned and remains valid after task and pool cleanup.

## Supported values

Wetlands accepts simple Python values, nested containers, and NumPy arrays at the task boundary.
See [Supported platforms and values](reference/supported.md) for the exact list and validation rules.

Unsupported objects raise `ValueEncodingError` and identify where the unsupported value appeared in the submitted arguments.

## NumPy rules

- Object-dtype arrays are rejected.
- Non-contiguous arrays are converted to C-contiguous transport storage.
- Structured dtypes are preserved.
- Empty arrays do not allocate shared memory.
- Workers receive writable, private C-contiguous copies detached from input transport storage.
- Host results are copied before the output transport resource is released.

Install NumPy in both the host environment and any worker environment that exchanges arrays.

## Ownership

For an input array:

1. the host allocates and owns shared memory;
2. the worker attaches to it and closes its view after execution;
3. the host unlinks it when the task becomes terminal.

For a result array:

1. the worker allocates and owns shared memory;
2. the host attaches, copies the array, and acknowledges receipt;
3. the worker unlinks the segment and acknowledges release.

The same lease cleanup paths cover dispatch failure, cancellation, timeout, disconnection, and worker death.
Wetlands handles Python resource-tracker behavior internally, including the different attachment API available in newer Python releases.

## Intermediate values

Progress metadata and named intermediate outputs should remain small simple values.
Array-valued intermediate outputs are not supported in Wetlands 2 because their lifetime is not tied to a single terminal result acknowledgement.

## Extensibility

The execution envelope identifies codecs by ID and version.
Worker startup reports its supported codec capabilities and execution-protocol version.
The host fails before dispatch when required capabilities are unavailable.

The initial codec boundary is intentionally small.
Additional array or storage technologies can be introduced later without changing task semantics.
