# Run your first task

This tutorial creates an isolated Python environment and asks a worker in that environment to add some numbers.

You will finish with a working Wetlands installation and the result `42`.

## Prerequisites

You need Python 3.9 or newer and internet access for the first run.
Wetlands downloads Pixi and the requested Python environment when they are not already available.

## Install Wetlands

```sh
pip install wetlands
```

## Create and run the example

Save this as `first_task.py`:

```python
from wetlands import EnvironmentManager, EnvironmentSpec


with EnvironmentManager(root="wetlands") as manager:
    environment = manager.provision(
        "first-example",
        EnvironmentSpec(python="3.12.*"),
    ).wait_for()

    with environment.start() as workers:
        result = workers.execute_import(
            "builtins:sum",
            args=([20, 22],),
        )

print(result)
```

Run it:

```console
$ python first_task.py
42
```

The first run may take several minutes while Wetlands downloads Pixi and Python.
Later runs reuse the ready environment and are faster.

## What happened?

`EnvironmentManager` owns the `wetlands` directory and the resources stored below it.

`EnvironmentSpec` describes the environment to create.
This example requests Python 3.12 and no third-party packages.

`manager.provision()` creates or reuses the environment.
Provisioning means preparing an environment so it is ready to run code.

`environment.start()` starts a worker process inside that environment.
The context manager stops the worker when the block ends.

`execute_import()` imports `builtins:sum` inside the worker, calls it with `[20, 22]`, waits for completion, and returns the result to the application.

The outer context manager closes the manager even when an error occurs.

## What remains on disk?

Closing the manager and worker pool stops their processes, but it keeps the ready environment below the `wetlands` directory.
The next run can reuse it instead of downloading and installing Python again.

When you no longer need an environment, follow [Discover, replace, and remove environments](how-to/environment_management.md) to remove it safely.

## Next step

Continue with [Run code from your own package](tutorials/own_package.md).
It replaces `builtins:sum` with a function that you provide and introduces NumPy arrays.

If something failed during setup, read [Handle execution and provisioning errors](how-to/errors.md).
