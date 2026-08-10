# Wetlands and Appose

[Appose](https://github.com/apposed/appose) and Wetlands both let an application run work in isolated local processes and exchange data with them.
The projects emphasize different use cases, so the better choice depends on the application.

| | Wetlands | Appose |
| --- | --- | --- |
| Primary use case | Call installed Python functions or explicit Python source paths from a Python application | Cooperate across processes and languages through tasks, services, and custom workers |
| Languages | Python host and Python workers | Python and Java APIs with Python, Java, Groovy, and custom workers |
| Environments | Managed Pixi projects | Pixi, Conda/Mamba, uv, the system environment, or an existing environment |
| Arrays and shared memory | Pass ordinary NumPy arrays automatically; inputs use copy-in semantics and results are independently owned | Pass explicit `NDArray` objects for zero-copy tensor sharing between processes and languages |
| Long-lived workers | Managed worker pools, automatic replacement, and optional detach-and-reconnect persistence | Reusable services that run repeated or concurrent tasks in a worker process |
| Interactive debugging | Attach VS Code or another Debug Adapter Protocol client to an already-running Python worker | Documented diagnostics focus on verbose logging and the JSON worker protocol |

## Choosing between them

Consider Wetlands when the host and worker code are Python, callers should pass ordinary Python values and NumPy arrays, or the application benefits from managed pools, reproducible `pixi.lock` provisioning, and post-hoc debugger attachment.

Consider Appose when Python must call Java or Groovy, Java must call Python, tensors should remain in explicitly managed zero-copy shared memory, or the application needs a choice of environment backends or a custom worker protocol.

Both projects support asynchronous tasks, progress updates, cancellation, isolated environments, and reuse of a running worker for multiple tasks.
Neither project's process or environment boundary is a security sandbox for untrusted code.

See the [Appose documentation](https://docs.apposed.org/) for its current APIs and supported features.
For Wetlands, continue with [Run your first task](getting_started.md), [NumPy arrays and shared-memory transport](shared_memory.md), or [Debug a running worker](debugging.md).
