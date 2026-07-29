![Wetland](Wetland.svg)

# Wetlands

Wetlands is a Python library for creating isolated environments with [Pixi](https://pixi.sh/) and running Python functions inside them.

Applications sometimes need libraries whose dependencies cannot be installed together.
For example, [Cellpose](https://www.cellpose.org/) and [StarDist](https://github.com/stardist/stardist) can each run in their own environment while the main application exchanges ordinary Python values and NumPy arrays with both.

Wetlands creates environments when needed, installs their dependencies, keeps workers ready for repeated calls, and cleans up their resources.
It can be embedded in desktop applications, servers, and plugin systems.

> Wetlands is intended for code you trust.
> Isolated environments prevent dependency conflicts, but they do not restrict what code can access on your computer.

## What Wetlands does

- Downloads and verifies Pixi, or uses the Pixi executable you provide.
- Creates reproducible environments from clear dependency recipes and `pixi.lock`.
- Reports preparation and installation progress and supports cancellation.
- Runs installed Python functions without importing their dependencies into the main environment.
- Keeps workers warm and replaces them when they fail or ignore cancellation.
- Attaches a debugger to an already-running worker when a problem needs investigation.
- Transfers simple Python values and NumPy arrays automatically.
- Works in blocking, callback-based, and `asyncio` applications.

## Start here

Follow [Getting started](getting_started.md) for a complete provision-and-execute example.

Read [Environment specifications](dependencies.md) for dependency and lockfile behavior.

Read [Operations and tasks](tasks.md) for cancellation, events, and async integration.

Read [Debugging running workers](debugging.md) to attach after a problem appears, and [Persistent workers and reconnection](persistent_workers.md) to keep warm workers across controller processes.

Existing Wetlands 1 users should start with the [Wetlands 2 migration guide](migration_v2.md).

## Appose

[Appose](https://github.com/apposed/appose) is an alternative for applications that need interprocess cooperation across Python, Java, or Groovy, including explicit zero-copy tensor sharing between languages.
Wetlands is focused on running Python functions and adds automatic NumPy transport, managed worker pools, and post-hoc debugger attachment.
Read [Wetlands and Appose](appose.md) for a short comparison, or visit the [Appose documentation](https://docs.apposed.org/).

## Before the first run

Wetlands may need to download Pixi and your declared packages.
The first provisioning can therefore require network access and take several minutes.
Pixi, environments, locks, and runtime state are stored below the `EnvironmentManager` root you configure.

## Trusted code only

Worker functions, package installers, and post-install commands run with the permissions of the current operating-system user.
They can generally read and modify that user's files, inspect environment variables, use the network, start processes, and consume system resources.
Authenticated local worker connections prevent accidental or unauthenticated connections; they do not restrict the worker code itself.

Use an operating-system sandbox, container, virtual machine, or separate user account if you need to run untrusted code.
See the [security policy](https://github.com/arthursw/wetlands/security/policy) for the supported threat model and reporting instructions.
