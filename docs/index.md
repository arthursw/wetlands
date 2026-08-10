# Wetlands

![Wetland](Wetland.svg)

Wetlands lets a Python application run functions and external commands in separate environments without mixing their dependencies into the main application.

For example, an application can use Cellpose and StarDist even when those libraries require incompatible versions of another package.
Each library runs in its own environment while Wetlands moves ordinary Python values and NumPy arrays between the application and worker processes.

> Wetlands runs worker functions, managed commands, and installers with your user account's permissions.
> Use it only with code and dependencies you trust.

## New to Wetlands?

Follow [Run your first task](getting_started.md) to install Wetlands, create one environment, and get a result.
The tutorial assumes that you know basic Python but does not assume knowledge of Pixi, worker processes, or asynchronous APIs.

Then follow [Run code from your own package](tutorials/own_package.md) to understand how application code and worker code fit together.

## Solving a specific problem?

- [Report progress, intermediate output, and worker logs](how-to/progress.md)
- [Cancel a task](how-to/cancel_tasks.md)
- [Handle execution and provisioning errors](how-to/errors.md)
- [Handle timeouts](how-to/timeouts.md)
- [Use Wetlands with asyncio](how-to/asyncio.md)
- [Run commands and services](how-to/managed_processes.md)
- [Debug a running worker](debugging.md)
- [Define environment dependencies](dependencies.md)

Use the navigation or search for the complete list of guides and reference pages.

If you are unsure where to look, use a tutorial to learn a complete workflow, a how-to guide to solve one problem, a concept page to understand why Wetlands behaves as it does, or a reference page to look up exact details.

## Contributing?

Start with [Contributor setup and validation](developer/contributing.md).
The developer section explains the architecture, execution protocol, and transport codecs separately from user documentation.

## What Wetlands manages

A manager owns a directory called its **root**.
Below that root, Wetlands stores its Pixi executable, managed environments, locks, and worker runtime state.

The first operation may download Pixi, and the first environment creation downloads its declared packages.
Those operations require network access and may take several minutes.

Read [The Wetlands mental model](concepts/mental_model.md) when you want a fuller explanation of managers, environments, workers, and tasks.
