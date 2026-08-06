# Configure workers and GPUs

Use `worker_environment` when workers in one pool need different process environment variables.
A common use is assigning one GPU to each worker.

```python
with environment.start(
    workers=2,
    worker_environment=lambda index: {
        "CUDA_VISIBLE_DEVICES": str(index),
    },
) as workers:
    # Worker 0 sees CUDA_VISIBLE_DEVICES=0.
    # Worker 1 sees CUDA_VISIBLE_DEVICES=1.
    ...
```

Wetlands calls the callback once for each zero-based worker index before launching any worker and copies the returned mappings.
A replacement worker keeps the same index and receives the same copied mapping.

Each mapping must contain only string keys and string values.
Names cannot be empty or contain `=` or null bytes, and values cannot contain null bytes.

The case-insensitive `WETLANDS_*` namespace is reserved.
`PYTHONEXECUTABLE`, `PYTHONHOME`, and `PYTHONPATH` also cannot be set because Wetlands controls the worker interpreter and import environment.

Indexed environments cannot be combined with `persistent=True`.
A later controller would not have the original callback configuration to validate a reconnected worker.
