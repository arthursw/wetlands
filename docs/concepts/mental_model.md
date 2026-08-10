# The Wetlands mental model

Wetlands separates dependency management from function execution.
Four objects form the main path through the library.

## Manager

An `EnvironmentManager` owns one directory called its root.
It coordinates Pixi, managed environments, live worker information, and cleanup below that root.

Constructing a manager only validates configuration.
Network access and subprocess work begin when an operation such as provisioning starts.

## Environment specification

An `EnvironmentSpec` is the recipe for an environment.
It names the Python version, Conda and PyPI dependencies, local packages, post-install commands, and optional lockfile.

The specification is immutable so Wetlands can compare it reliably with a ready environment.

## Managed environment, worker pools, and commands

A `ManagedEnvironment` represents one successfully prepared environment.
Starting it creates a `WorkerPool` containing one or more Python processes inside that environment.

Workers stay warm between calls, which avoids paying process startup and import cost for every function.
The pool replaces workers that crash, disconnect, or ignore cancellation.

A managed environment can also run installed external commands.
Use `run()` for a short-lived CLI and `spawn()` for a service, GUI, or other independently supervised process.
Unlike a worker pool, a managed process does not provide Python-call or Python-value transport.

## Execution task

Submitting a function call creates an `ExecutionTask`.
The pool queues the task, sends it to an available worker, and returns its result or structured failure to the application.

Installed functions use `module.path:qualified.attribute` names.
The module is imported in the worker environment, not in the host application.

## One complete flow

```text
EnvironmentManager
    └── provisions EnvironmentSpec
            └── returns ManagedEnvironment
                    └── starts WorkerPool
                            └── runs ExecutionTask
                                    └── returns value or failure
```

## Values crossing the boundary

Simple Python values are encoded into protocol messages.
NumPy array data is copied through operating-system shared memory instead of being embedded in those messages.
Shared memory is an automatic transport detail: application and worker code receive independent, ordinary arrays rather than a shared mutable or zero-copy view.
See [NumPy arrays and shared-memory transport](../shared_memory.md) for the complete flow.

The boundary isolates dependencies, not permissions.
Read [Trusted-code security model](security.md) before running third-party code.
