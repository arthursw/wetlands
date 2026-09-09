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

Each provisioning output reader owns its pipe and closes it when draining finishes or fails.
The runner closes pipes whose readers never started, including failures while establishing process ownership.
Process-tree and Windows Job Object cleanup precede bounded reader joins so descendant pipe handles can reach EOF.
A reader that outlives its join retains responsibility for closing its pipe; the runner reports incomplete cleanup without attempting a potentially blocking close from another thread.
Cleanup diagnostics accompany the original command or setup failure.

## Subprocess ownership

All subprocess launch sites follow the same pipe handoff implemented by the internal `PipeOwnership` helper.
The launcher owns stdout and stderr until each reader claims its stream; reader completion or failure closes that stream in the reader thread.
If thread construction or startup fails, cleanup terminates the owned process tree, joins any started readers, and closes only unclaimed pipes.
The handoff also covers a thread-start call interrupted after the reader began running.
Worker retirement and pool shutdown use the same output cleanup path as failed worker startup.
They retain cleanup ownership when a process or output reader cannot be verified as finished, allowing a later close attempt to retry.

| Launch site | Process and output owner | Terminal cleanup |
| --- | --- | --- |
| Pixi preparation, provisioning, and post-install commands | `ProcessTreeRunner` | Verify tree termination, release the Windows Job, reap the child, join readers, close unclaimed pipes. |
| Fallback managed-Python discovery | `ProcessTreeRunner` | The same cleanup applies after success, nonzero exit, or the 30-second probe timeout. |
| Launched workers | Worker pool and `ProcessLogger` | Terminate and reap on failed startup, retirement, or close; detached persistent workers retain daemon readers and a reaper until they exit. |
| Managed commands and services | `ManagedProcess`, retained by its environment | Complete tree and pipe cleanup before publishing the result; report incomplete cleanup and retain ownership for close retries. |
| VS Code command-line launcher | Daemon reaper | No pipes are created; retain the `Popen` until its exit is reaped without blocking CLI shutdown. |

Provisioning reader failures trigger process cleanup even when the command remains running.
Managed-command supervision also attempts cleanup after an unexpected polling failure and publishes that failure to waiters.
POSIX group verification reaps the launched leader after inspecting group liveness, so a concurrent transition to zombie state cannot leave its exit status uncollected.
Windows Job creation and closure share pointer-safe native API declarations; setup failures close allocated handles, and a failed close of an owned Job retains its handle for retry.

These guarantees apply to owned processes and normal operating-system cleanup facilities.
Wetlands does not signal an unverified process identity or claim successful cleanup when termination or a reader join fails.
A descendant that deliberately escapes its process group, an external process retaining a pipe handle, or an operating-system refusal to terminate cannot be treated as proof of a leak-free shutdown.
Bounded cleanup reports the failure instead of closing another thread's blocked stream or pretending the resource was released.

## Environment removal

Logical removal and physical storage reclamation are separate phases.
While holding the per-environment lifecycle gate, the manager proves ownership and worker liveness, persists a quarantine sidecar, and atomically renames the target into a manager-owned quarantine directory beside the environment root.
The original name is reusable only after the rename and cache-epoch invalidation complete.

One lazy reclaimer thread per manager scans and purges durable tombstones under a cross-process lock.
The quarantine container, strict sidecar schema, random mirrored token, and exact filesystem identity authorize resumable deletion even when an interrupted purge has already removed the target's ordinary owner marker.
Unknown or inconsistent state is never deleted automatically.
Manager shutdown stops reclamation cooperatively rather than draining every tombstone, so a later mutating operation may resume the work.

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
The launching interpreter retains each detached worker's `Popen` in a daemon waiter until its exit is reaped, independently of later controllers.
Output readers close their own pipes at EOF or on failure; detachment does not close pipes while readers are active.
These local cleanup threads do not prevent the launcher from exiting while persistent workers remain available.
Debug access is independent and does not claim the execution connection.

## Async integration

Wetlands uses threads and subprocesses internally and does not own an application event loop.
Operations and tasks adapt completion and event delivery to the caller's current `asyncio` loop when awaited.

Any new callback path must remain safe when invoked from a Wetlands background thread.
Any new terminal path must complete mandatory process and transfer-resource cleanup before publishing its terminal state.
Environment removal is terminal after its namespace detachment commits; recursive storage reclamation is explicitly deferred and does not emit events on the completed removal operation.
If directory durability cannot be confirmed after the rename, Wetlands logs the condition and retains the recovery record instead of reporting a misleading pre-commit failure.

## Trust boundary

Pixi environments isolate dependencies, not privileges.
Worker targets, installers, debuggers, and post-install commands run with the current user's access to files, processes, environment variables, local services, and the network.
Authentication prevents accidental or unauthenticated control-channel connections, but it does not make trusted local execution a security sandbox.
