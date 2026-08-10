# Discover, replace, and remove environments

An `EnvironmentManager` can list the environments it owns below its root.

```python
from wetlands import ManagedEnvironmentState

for info in manager.managed_environments():
    if info.state is ManagedEnvironmentState.READY:
        print(f"{info.name} is ready at {info.path}")
    else:
        print(f"{info.name} is incomplete")
```

Discovery returns immutable snapshots and ignores directories that Wetlands cannot prove it owns.

## Replace a different recipe

Provisioning reuses a ready environment only when its recipe and lockfile match.
Use `replace_existing=True` to remove a different ready recipe under the same name before rebuilding:

```python
environment = manager.provision(
    "analysis",
    new_spec,
    replace_existing=True,
).wait_for()
```

This removes the old environment before the replacement succeeds.
For rollback-safe upgrades, provision under a new managed environment name and switch your application's own logical mapping only after success.

## Remove an environment

Close its worker pools and managed processes, then wait for the removal operation:

```python
removal = manager.remove("analysis")
removal.listen(lambda event: print(event.message))
removed = removal.wait_for()
print(removed.name)
```

Wetlands refuses to remove unmanaged targets or environments with live worker pools or managed processes.
It raises `EnvironmentNotFoundError` when the name does not exist.

Completion means the original path is gone and the name can be reused.
Recursive disk reclamation may continue safely in the background and resume during a later manager operation if interrupted.

Read [Environment identity, reuse, and rebuilding](../concepts/environment_identity.md) for the underlying rules.
