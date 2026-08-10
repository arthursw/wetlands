# Run commands and services

Use a managed process for an external command, command-line tool, GUI, or service installed in a provisioned environment.
Use a [`WorkerPool`][wetlands.WorkerPool] instead when you need to call Python functions repeatedly: workers stay warm, transport Python values, support task cancellation, and replace unhealthy workers.

Managed processes run trusted code with your operating-system account's permissions.
They are process-lifecycle isolation, not a security sandbox.

## Run a short command

[`ManagedEnvironment.run()`][wetlands.ManagedEnvironment.run] invokes Pixi internally with the immutable manifest and lockfile for the exact environment generation:

```python
environment = manager.provision("tools", spec).wait_for()

result = environment.run(["python", "--version"], timeout=30)
print(result.stdout)
```

Pass an argument vector, not a shell string.
Wetlands never invokes a shell or performs argument splitting.
The result is an immutable [`ManagedProcessResult`][wetlands.ManagedProcessResult] with the requested `argv`, `returncode`, separate `stdout` and `stderr`, and start and end timestamps.

`check=True` is the default.
A non-zero exit raises [`ProcessExitError`][wetlands.ProcessExitError], whose `result` contains the same output and return code.
Set `check=False` to receive that result directly:

```python
result = environment.run(["example-cli", "inspect"], check=False)
if result.returncode != 0:
    print(result.stderr)
```

Invalid argv, working-directory, environment-overlay, timeout, and output-limit values fail before a command is launched with the corresponding ordinary validation or path exception.
Operating-system launch errors preserve their native exception type.
An environment generation changed before launch still raises [`EnvironmentGenerationChangedError`][wetlands.EnvironmentGenerationChangedError].
If a child was created but Wetlands cannot complete or clean up the remaining launch setup, [`ProcessCleanupError`][wetlands.ProcessCleanupError] takes precedence and chains the launch error.

## Select a working directory and environment variables

`cwd` may name any existing host directory and defaults to the managed environment directory.
`env` is an overlay on the host environment: a string sets or replaces a variable, while `None` removes an inherited variable.

```python
result = environment.run(
    ["example-cli", "inspect", "."],
    cwd=project_directory,
    env={
        "EXAMPLE_SETTING": "strict",
        "PYTHONPATH": None,
    },
)
```

The overlay does not modify `os.environ`.
Pixi still uses the manager's configured proxy settings and managed cache/home directories unless the overlay explicitly replaces or removes their environment variables.
Removing platform essentials such as `PATH` or `SystemRoot` can prevent a command from starting correctly and is the caller's responsibility.

## Launch a long-running service

[`ManagedEnvironment.spawn()`][wetlands.ManagedEnvironment.spawn] returns after the child, output readers, and operating-system containment have been installed:

```python
from wetlands import OutputStream

with environment.spawn(["example-server", "--port", "0"]) as process:
    ready = process.wait_for_line(
        lambda event: event.stream is OutputStream.STDOUT and event.text.startswith("Listening on "),
        timeout=30,
    )
    print(ready.text, end="")

    # Use the service here. Leaving the block closes it.
```

The predicate receives an immutable [`OutputEvent`][wetlands.OutputEvent], so readiness checks retain whether a line came from stdout or stderr.
Each event also has a monotonic sequence number and a wall-clock Unix timestamp.
Event text includes its line ending when one was present.
UTF-8 decoding uses replacement characters for invalid input.
Ordering is preserved within each stream; concurrent stdout and stderr have only the sequence order assigned when Wetlands receives them.

`wait_for_line()` observes output without consuming it.
By default it first checks retained events and then follows new ones.
It raises [`ProcessLineTimeoutError`][wetlands.ProcessLineTimeoutError] without stopping the command if no matching line arrives before its deadline, and `EOFError` if both streams close without a match.
Call `wait()` afterward when you also need the process outcome.

The process exposes its requested `argv`, environment name, generation ID, and operating-system `pid` as read-only identity.
`running` reports whether the direct child remains active, and `returncode` is `None` until that child is reaped.
An exited direct child may still have owned descendants; cleanup and generation release therefore depend on the full containment set, not `running` alone.

## Consume output with asyncio

There is one live-output model: independent observers use the asynchronous event stream.

```python
process = environment.spawn(["example-server", "--port", "0"])
try:
    async for event in process.events():
        render(event.stream.value, event.text)
finally:
    process.close()
```

`events(replay=True)` starts with the oldest event still retained; `replay=False` starts with the next event published after subscription.
Iteration ends after both output streams close and all events available to that observer have been yielded.
It does not raise merely because the command exits unsuccessfully; use `wait()` or `await process` for the outcome.

Wetlands retains at most 1,024 output events for replay.
A lagging observer raises [`ProcessEventLagError`][wetlands.ProcessEventLagError] if its next event has already been evicted, without affecting the process or other observers.
Cancelling an observer also leaves the process running.

`ManagedProcess` is awaitable.
`await process` is equivalent to `await process.wait_async(check=True)`; use `wait_async(..., check=False)` when a non-zero result should not raise.
Cancelling a task that is awaiting the process requests cleanup and waits for it before propagating `asyncio.CancelledError`, so the command is not left running invisibly.

## Timeouts and bounded output

`run(..., timeout=...)` and `process.wait(timeout=...)` treat the timeout as a command deadline.
If the timeout wins the process's terminal-state race, Wetlands terminates the owned process tree, completes output cleanup, and raises [`ProcessTimeoutError`][wetlands.ProcessTimeoutError].
Its `result` contains the partial stdout and stderr retained before termination.
Unlike an [`ExecutionTask.wait_for()` timeout](timeouts.md), a managed-process wait timeout stops the managed command.

`output_limit` defaults to 1 MiB across stdout and stderr combined and counts raw bytes before UTF-8 decoding.
If the next byte would exceed the limit, Wetlands retains only the accepted prefix, terminates the process tree, and raises [`ProcessOutputLimitError`][wetlands.ProcessOutputLimitError] with the partial result and the stream that crossed the limit.
Set a deliberate larger bound for commands expected to be noisy; setting zero permits only a silent command.

## Terminate and clean up

Use `terminate(timeout=...)` to request graceful shutdown and then escalate to forced termination when its grace period expires.
Use `kill()` when immediate forced shutdown is required.
`close()` uses the manager's configured `termination_grace` and is the usual ownership operation for application shutdown.

`wait()`, `terminate()`, `kill()`, and `close()` are safe to call repeatedly.
If Wetlands cannot verify termination, output-reader completion, or child reaping, cleanup raises [`ProcessCleanupError`][wetlands.ProcessCleanupError] and retains internal ownership so a later close can retry.

Every process is associated with the exact environment generation that spawned it until the complete owned tree is proven terminal.
While that association is active, removing the environment or provisioning a replacement under the same name raises [`EnvironmentInUseError`][wetlands.EnvironmentInUseError].
Natural exit, successful termination, and successfully cleaned launch failure release the association automatically.
Multiple concurrent processes may use the same generation.

Closing the [`EnvironmentManager`][wetlands.EnvironmentManager] closes all application-owned managed processes and worker pools under one total cleanup deadline.
Managed processes cannot detach or survive a manager restart; keep the manager open for their entire lifetime.

## Cross-platform containment

On POSIX, Wetlands launches the Pixi wrapper in a new session and terminates its process group.
On Windows, it requires assignment to a kill-on-close Job Object before returning a managed process.
This normally includes the command behind the Pixi wrapper and descendants that remain in the containment boundary.

A program that deliberately daemonizes, creates a new POSIX session, breaks away from its Windows Job, or delegates work to another already-running service can escape this ownership boundary.
Wetlands cannot promise to terminate such escaped work.
Do not use this API for detached or persistent-across-manager-restart processes.
