![Wetland](Wetland.svg)

# Wetlands

Wetlands provisions isolated Pixi environments and runs Python callables in managed worker processes.

The library is application-neutral and designed for trusted local dependency isolation.
It does not provide a security boundary.

## What Wetlands manages

- A verified Pixi installation prepared explicitly with `EnvironmentManager.prepare()`.
- Reproducible Pixi projects described by immutable `EnvironmentSpec` values.
- Cancellable provisioning operations with structured stages, output, and failures.
- Warm worker pools with health monitoring and replacement.
- A versioned execution protocol with qualified import and source-path targets.
- Automatic transport of simple Python values and NumPy arrays.
- Synchronous, callback-based, and `asyncio`-friendly observation.

## Start here

Follow [Getting started](getting_started.md) for a complete provision-and-execute example.

Read [Environment specifications](dependencies.md) for dependency and lockfile behavior.

Read [Operations and tasks](tasks.md) for cancellation, events, and async integration.

Existing Wetlands 1 users should start with the [Wetlands 2 migration guide](migration_v2.md).

## Design boundary

Wetlands owns environment preparation, provisioning, workers, transport resources, and cleanup.

The embedding application owns manifests, user-facing naming and policy, GUI integration, and presentation of operation events.

Worker code remains ordinary Python and does not need to import Wetlands.
