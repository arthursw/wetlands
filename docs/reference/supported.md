# Supported platforms and values

## Platforms

Wetlands 2 supports:

- Python 3.9 through 3.14;
- Linux, macOS, and Windows;
- the Pixi version registered and verified by the installed Wetlands release.

The test suite covers supported Python and operating-system combinations.
Real-Pixi acceptance tests run on all three operating systems.

## Task values

Arguments and results may contain:

- `None`;
- booleans, integers, floats, strings, and bytes;
- nested lists and tuples;
- dictionaries whose keys are `None`, booleans, integers, floats, strings, or bytes;
- NumPy arrays without object dtype.

Cyclic containers and unsupported objects fail explicitly at the execution boundary.

Install `wetlands[shared-memory]` in the host and NumPy in the worker environment when arrays cross the boundary.
Non-contiguous arrays are copied into contiguous transport storage.
NumPy arrays may have at most 64 dimensions, and one transported array must fit in the platform's shared-memory size limit.

Intermediate outputs support the simple Python values above but not NumPy arrays.

See [Values and NumPy ownership](../shared_memory.md) for mutation and ownership rules.
