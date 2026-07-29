# Persistent workers and reconnection

Ordinary worker pools stop when their controller closes them.
A persistent pool can instead remain alive while one controller exits and a later controller reconnects.

Persistence is useful when environment imports or model initialization are expensive and a trusted local application needs warm workers across controller restarts.

## Start and detach

The first controller starts a persistent pool and explicitly relinquishes it:

```python
from wetlands import EnvironmentManager

manager = EnvironmentManager(root="wetlands")
environment = manager.environment("cellpose")

pool = environment.start(workers=1, persistent=True)
pool.execute_import("worker_package.models:load")
pool.detach()
manager.close()
```

`detach()` closes the controller connection without stopping the workers.
After detaching, that `WorkerPool` object is closed and cannot submit more tasks.

Calling `pool.close()` instead stops the worker processes.
Leaving an attached or newly started pool's context manager also calls `close()` unless the pool was explicitly detached first.

## Attach from a later controller

A later process can open the same ready environment and claim the detached pool:

```python
from wetlands import EnvironmentManager

manager = EnvironmentManager(root="wetlands")
environment = manager.environment("cellpose")
pool = environment.attach_pool()

try:
    result = pool.execute_import(
        "worker_package.segment:run",
        args=(image,),
    )
finally:
    pool.detach()
    manager.close()
```

Use `pool.close()` in the `finally` block when the workers are no longer needed.

## Exclusive execution ownership

A persistent pool has exactly one execution controller.
`attach_pool()` fails while the original controller is still alive or another controller owns the pool.
It succeeds only after the current controller calls `detach()` or is proven dead.

The attaching manager must use the same Wetlands root.
Wetlands also verifies the environment path and generation, recipe identity, worker-runtime version, execution-protocol version, authentication key, and complete commissioned pool membership.
A pool from a replaced environment or an incompatible Wetlands runtime is never reused.

Only persistent pools may be attached.
Non-persistent workers appear in runtime diagnostics while alive but are owned by their original controller and stop with it.

## Failure and crash behavior

Wetlands records process identity rather than trusting a process ID alone.
Before attachment, it reconciles the runtime registry, removes workers that are proven dead, and rejects ambiguous or unsafe process identity.

If a controller crashes without detaching, a later controller may attach after Wetlands proves that the previous controller is dead.
If a worker has died or the commissioned pool is incomplete, attachment fails rather than returning a partially healthy pool.

Provisioning or replacing an environment is rejected while workers from that environment are live.
Stop or detach and then explicitly close the old pool before replacing its environment.

## Debugger reconnection is separate

Execution attachment transfers ownership of task dispatch.
Debugger attachment does not.

The [`wetlands debug`](debugging.md) command uses a separate authenticated management connection and can start or locate a debug adapter while the application still controls the worker pool.
An editor may disconnect and reconnect without detaching the execution controller.
