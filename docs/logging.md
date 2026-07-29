# Events and logging

Wetlands exposes lifecycle and execution activity through operation events, task events, and standard Python logging.

Applications should use structured events for user-facing activity and logging for diagnostics.

## Provisioning activity

```python
from wetlands import OperationEventKind


def report(event) -> None:
    if event.kind is OperationEventKind.OUTPUT:
        print(f"[{event.stage}:{event.stream}] {event.line}")
    else:
        print(f"[{event.kind.value}] {event.message}")


operation = manager.provision("analysis", spec)
operation.listen(report)
environment = operation.wait_for()
```

Each event includes its operation ID, sequence, timestamp, state, kind, and message.
Provisioning events may also identify a logical stage, step, stream, output line, or progress position.

Listener callbacks may run on Wetlands background threads.
GUI applications must forward them through their toolkit's thread-safe signaling mechanism.

## Async activity

```python
operation = manager.provision("analysis", spec)

async for event in operation.events():
    activity_model.update(event)

environment = await operation
```

The stream replays available operation history by default and closes after its terminal event.

## Execution task events

```python
task = pool.submit_import("analysis_package.pipeline:run", args=(data,))


def on_task_event(event) -> None:
    print(event.kind.value, event.message, event.progress)


task.listen(on_task_event)
result = task.wait_for()
```

Worker code can publish progress through an explicitly requested runtime context:

```python
def run(items, task=None):
    for index, item in enumerate(items):
        process(item)
        if task is not None:
            task.update(
                "Processing",
                current=index + 1,
                maximum=len(items),
            )
```

Pass `context_keyword="task"` to `submit_import()` or `submit_path()` to request this injection.

## Python logging

Wetlands uses the `wetlands` logger hierarchy.
The host application decides which handlers, formatters, and destinations to install.

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logging.getLogger("wetlands").setLevel(logging.DEBUG)
```

Do not attach handlers that assume they run on the application's UI thread.

## Sensitive values

Provisioning events and structured failures use sanitized command displays.
Common credentials in URLs, tokens, passwords, authorization values, and proxies are redacted from captured commands and subprocess output.

Applications must still avoid printing arbitrary secrets from their own post-install commands or worker functions.
Use a `PostInstallCommand.display` value whenever a shell command contains sensitive expansion.
