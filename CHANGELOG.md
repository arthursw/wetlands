# Changelog

All notable changes to Wetlands are documented here.

## 2.3.3

- Added content identities and immutable environment-local staging for local packages so managed environments install and validate stable source snapshots instead of depending on a live source tree.
- Kept default manager shutdown bounded during background physical reclamation while preserving one shared deadline for explicit finite close timeouts.
- Made first-time managed Pixi setup observable through `on_mutation_started` before its install-root mutation and download, without notifying when valid Pixi and environment state is reused.

## 2.3.2

- Fixed process-exit races at POSIX group enumeration and Windows wait timeout boundaries so completed shutdown is not reported as unverified.
- Made dead worker-record reconciliation atomic and kept failed worker-pool close attempts retryable without retaining a stale fatal state after successful cleanup.

## 2.3.1

- Fixed a POSIX process-group exit race that could report worker cleanup failure after graceful shutdown had already completed.
- Made operation state transitions and event publication atomic so late listeners and replay iterators cannot miss terminal events.

## 2.3.0

- Made managed-environment removal complete after an identity-safe atomic detach so the name is immediately reusable.
- Added durable background disk reclamation with crash recovery, cross-process serialization, and fail-closed handling of malformed quarantine state.
- Applied deferred reclamation to environment replacement and incomplete provisioning cleanup.

## 2.2.0

- Fixed local-package and pinned Git source provisioning by materializing complete Pixi manifests before installation.
- Added validation and documentation for immutable Git package references pinned to full commit SHAs.
- Enabled long-path handling for Git subprocesses launched by Pixi on Windows without changing the user's global Git configuration.

## 2.1.0

- Added validated per-index worker environment variables through `ManagedEnvironment.start(worker_environment=...)`.
- Preserved worker indices and their snapshotted environment mappings across automatic replacement.
- Defined indexed worker environments as incompatible with persistent pools until their configuration can be durably verified during attachment.

## 2.0.0

Wetlands 2 is a breaking redesign focused on reliable Pixi environment provisioning and ordinary Python function execution.

- Added side-effect-light manager construction and observable, cancellable preparation and provisioning operations.
- Standardized on Pixi projects and reproducible `pixi.lock` files.
- Added qualified installed-package targets, warm worker pools, protocol capability checks, health monitoring, and worker replacement.
- Added automatic transport for simple Python values and NumPy arrays with managed shared-memory cleanup.
- Added blocking, callback-based, and `asyncio`-friendly APIs.
- Added post-hoc debugger attachment to live workers and restored a Wetlands CLI for worker discovery and VS Code attachment.
- Added explicit persistent-pool detachment and reconnection documentation.
- Added a compact developer guide for the execution protocol and internal codec boundary.
- Removed the Wetlands 1 manager backends, execution APIs, and explicit shared-memory array types.
- Removed preconfigured debug mode and the host-side debug extra; Wetlands now manages the worker debugger runtime.

See the [Wetlands 2 migration guide](docs/migration_v2.md) for replacement APIs and examples.
