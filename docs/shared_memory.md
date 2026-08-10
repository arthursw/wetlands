# NumPy arrays and shared-memory transport

Wetlands passes ordinary `numpy.ndarray` objects between your application and worker processes.
The array data travels automatically through operating-system shared memory instead of being placed in the worker's ordinary control messages.
Only a small descriptor containing information such as the array's shape and dtype travels through the control connection.

You do not create, attach, close, or unlink shared-memory segments yourself.
Application and worker code only handle normal NumPy arrays.

> Shared memory is the transport mechanism, not the ownership model.
> Wetlands copies data between your arrays and temporary shared-memory segments, so this is not a shared mutable array and is not a zero-copy API.

## Set up both environments

Install Wetlands with NumPy transport support in the application environment:

```sh
pip install "wetlands[shared-memory]"
```

Also include NumPy in every managed environment whose worker functions receive or return arrays:

```python
from wetlands import EnvironmentSpec


spec = EnvironmentSpec(
    python="3.12.*",
    conda=("numpy>=2",),
)
```

The extra installs NumPy in the application environment so Wetlands can encode and decode arrays.
The dependency in `EnvironmentSpec` installs NumPy independently inside the worker environment.

## Pass an array like any other value

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

## What happens during a call

For an input array, Wetlands performs this flow:

```text
application array
    -> copy into host-owned shared memory
    -> worker reads the shared-memory segment
    -> copy into a private, writable worker array
```

For a result array, the flow runs in the other direction:

```text
worker result array
    -> copy into worker-owned shared memory
    -> application reads the shared-memory segment
    -> copy into an independently owned application array
```

Wetlands removes the temporary shared-memory segments after transfer.
This design keeps large array bytes out of the control-message stream while giving each process a normal array with an unambiguous lifetime.

## Supported values

Wetlands accepts simple Python values, nested containers, and NumPy arrays at the task boundary.
See [Supported platforms and values](reference/supported.md) for the exact list and validation rules.

Unsupported objects raise `ValueEncodingError` and identify where the unsupported value appeared in the submitted arguments.

## Array behavior and limitations

- Object-dtype arrays are rejected.
- Non-contiguous arrays are converted to C-contiguous transport storage.
- Structured dtypes are preserved.
- Empty arrays do not allocate shared memory.
- Workers receive writable, private C-contiguous arrays detached from the input transport storage.
- Results returned to the application are independent arrays, copied before the output transport storage is released.

## Shared-memory lifetime and cleanup

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

For the lower-level protocol design, see [Transport codecs](developer/codecs.md).
