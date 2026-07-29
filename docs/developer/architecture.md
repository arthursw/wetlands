# Architecture and execution protocol

This section describes the boundaries that contributors must preserve when changing Wetlands.
The public user model remains environments, operations, worker pools, and execution tasks.

## Components

`EnvironmentManager` owns one Wetlands root and coordinates Pixi preparation, provisioning operations, managed-environment handles, runtime state, and shutdown.
Its constructor validates and stores configuration without network access, subprocess activity, filesystem writes, or runtime-state creation.
It may perform limited filesystem inspection while resolving and validating configured paths.

`ManagedEnvironment` represents one successfully published environment generation.
It can start a new worker pool or attach to a detached persistent pool from the same generation.

`WorkerPool` owns task scheduling and the execution-controller connection.
It dispatches work to warm workers, reports worker failure, and replaces unhealthy workers.

Each worker imports qualified module targets or canonical path targets inside the Pixi environment.
Worker code receives ordinary Python values and returns ordinary Python values.

## Provisioning publication

Provisioning serializes work for one physical environment and performs preparation, project materialization, lock validation or resolution, installation, validation, and ready-metadata publication.
The ready metadata is written only after every preceding stage succeeds.

Failure or cancellation terminates the active subprocess tree and removes the incomplete environment.
After a host crash, the next attempt treats an environment without matching ready metadata as incomplete and rebuilds it.

## Execution connections

Workers and controllers communicate through authenticated loopback `multiprocessing.connection` channels.
Loopback TCP provides one consistent transport on Linux, macOS, and Windows.

Worker startup sends a capability handshake containing:

- execution-protocol version;
- worker-runtime version;
- Python version;
- managed-environment identity;
- supported codec IDs and versions.

The host rejects incompatible capabilities before dispatch.

Task dispatch uses a versioned execution envelope containing the task ID, qualified target, encoded positional arguments, encoded keyword arguments, and required codecs.
Control messages carry progress, cancellation, results, failures, acknowledgements, and health information.

The worker's management connection is separate from execution ownership.
Worker discovery reads the durable runtime registry.
The management connection verifies the selected live worker's exact identity and supports lazy debugger startup while the application continues controlling execution.
It must not grow into an alternative task-dispatch channel.
Debugger startup disables automatic child-process instrumentation so target code does not unexpectedly turn subprocesses into additional debug servers.

## Runtime state and ownership

The root-local runtime registry records live worker and controller process identities, pool commissioning, protocol identity, and management endpoints.
Updates are serialized and published atomically.
Authentication material is stored separately with restrictive permissions and is never written to logs or diagnostic output.

A persistent pool has one execution controller.
Detaching releases that claim without killing workers, while closing terminates them.
Debug access is independent and does not claim the execution connection.

## Async integration

Wetlands uses threads and subprocesses internally and does not own an application event loop.
Operations and tasks adapt completion and event delivery to the caller's current `asyncio` loop when awaited.

Any new callback path must remain safe when invoked from a Wetlands background thread.
Any new terminal path must complete mandatory process and transfer-resource cleanup before publishing its terminal state.

## Trust boundary

Pixi environments isolate dependencies, not privileges.
Worker targets, installers, debuggers, and post-install commands run with the current user's access to files, processes, environment variables, local services, and the network.
Authentication prevents accidental or unauthenticated control-channel connections, but it does not make trusted local execution a security sandbox.
