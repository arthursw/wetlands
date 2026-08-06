# Command-line reference

Wetlands provides commands for discovering live workers and attaching a debugger.

## List workers

```console
wetlands workers [--root ROOT] --environment NAME
```

The command reports worker identity, environment, pool, process ID, persistence, and debugger status.

## Start or find a debugger

```console
wetlands debug [--root ROOT] --environment NAME [--worker WORKER_ID]
```

When only one worker matches, `--worker` may be omitted.
The command prints the local `debugpy` host and port.

Use `--editor vscode --source PATH` to create and open a VS Code workspace with an attach configuration.
Use `--no-launch` to create the workspace without opening VS Code.

Run `wetlands --help`, `wetlands workers --help`, or `wetlands debug --help` for all options.
See [Debug a running worker](../debugging.md) for the complete workflow and trust considerations.
