# How Wetlands works

Wetlands separates environment lifecycle operations from worker execution.

## Manager construction

`EnvironmentManager` resolves and validates configuration in memory.
It does not prepare Pixi or materialize runtime state.

## Pixi preparation

`prepare()` returns an observable operation.
The operation serializes preparation under a root-local lock, discovers or installs the registered Pixi release, verifies its version and artifact digest, and returns `PixiInfo`.

Concurrent managers targeting the same root coordinate through the same lock.

## Provisioning

`provision(name, spec)` coordinates the full lifecycle of one physical environment.

The ordered stages are:

1. acquire the environment lifecycle lock;
2. prepare Pixi;
3. inspect and remove incomplete state;
4. materialize `pixi.toml` and an optional supplied `pixi.lock`;
5. resolve or validate the lockfile;
6. install Conda, PyPI, and local dependencies;
7. run explicit post-install commands;
8. validate the environment;
9. publish ready metadata atomically.

Provisioning and worker lifecycle operations share per-environment coordination.
An environment is never published ready while provisioning is incomplete.

Failures and cancellation stop the active process tree and remove the incomplete target.
If the host crashes before cleanup, the next provisioning attempt treats missing ready metadata as incomplete and rebuilds.

## Workers

`ManagedEnvironment.start()` launches one or more warm Python worker processes from the Pixi project.

Each worker exposes separate authenticated local `multiprocessing.connection` endpoints for execution control and narrowly scoped runtime management.
The execution controller owns task dispatch, while the management endpoint allows worker discovery and lazy debugger startup without taking over the pool.
Both channels use OS-assigned loopback TCP ports for consistent Linux, macOS, and Windows behavior.

Startup performs a capability handshake containing:

- execution-protocol version;
- worker runtime version;
- worker Python version;
- supported codec IDs and versions;
- managed environment identity.

The pool rejects incompatible workers before accepting execution.

Starting a debugger through the management endpoint launches a loopback `debugpy` adapter in that worker.
The resulting Debug Adapter Protocol endpoint is not authenticated by Wetlands and is intended only for trusted local use.

## Execution

`submit_import()` creates a versioned execution envelope containing a task ID, qualified module target, encoded positional arguments, encoded keyword arguments, and codec requirements.

The worker imports the requested module in its own environment, resolves the qualified callable, decodes ordinary values, invokes the function, and encodes its result.

`submit_path()` carries a canonical path and qualified attribute instead.
Path modules are cached by canonical path and content identity, preventing collisions between equal filename stems.

The host has one reader per worker and routes messages to tasks by task ID.
Queued work is dispatched to idle workers.
Health monitoring fails affected tasks and replaces workers after a crash, disconnect, or configured inactivity timeout.

## NumPy transfer

NumPy payloads travel out of band in shared memory while descriptors travel in the execution envelope.

Creator ownership is explicit.
Inputs are created and unlinked by the host.
Results are created and unlinked by the worker after a host acknowledgement.
The host copies returned arrays before acknowledgement.

Terminal cleanup is idempotent so competing completion, cancellation, disconnection, and worker-death paths cannot release a lease twice.

## Async integration

Wetlands uses threads and subprocesses internally, so synchronous applications do not need an event loop.

Awaiting an operation or task adapts its completion to the caller's current `asyncio` loop.
Async event streams use thread-safe loop callbacks.
Wetlands never assumes ownership of the application's event loop.

## Trust model

Worker targets execute arbitrary Python with the current user's privileges.
Installers and post-install commands have those privileges as well.
They can generally access the user's files, environment variables, network, local services, processes, CPU, memory, and disk.

Authentication prevents accidental or unauthenticated connection to a local worker.
It does not restrict code already running as the same user and does not turn the worker into a sandbox.
Use Wetlands only with trusted code and dependency sources.
See the [security policy](https://github.com/arthursw/wetlands/security/policy) for reporting instructions and supported releases.
