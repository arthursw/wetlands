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

## Managed environment and worker pool

A `ManagedEnvironment` represents one successfully prepared environment.
Starting it creates a `WorkerPool` containing one or more Python processes inside that environment.

Workers stay warm between calls, which avoids paying process startup and import cost for every function.
The pool replaces workers that crash, disconnect, or ignore cancellation.

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
NumPy array data uses shared memory internally, but application and worker code still receive ordinary arrays.

The boundary isolates dependencies, not permissions.
Read [Trusted-code security model](security.md) before running third-party code.
