# Changelog

All notable changes to Wetlands are documented here.

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
