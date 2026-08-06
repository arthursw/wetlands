# Use Wetlands with asyncio

Provisioning operations and execution tasks can be awaited directly.
They adapt completion to the event loop that is currently running.

Worker-pool startup and shutdown and manager shutdown are blocking lifecycle calls.
Run those calls with `asyncio.to_thread()` so they do not block the event loop.

## Complete example

<!-- fmt: off -->
```python
--8<-- "examples/async_example.py"
```
<!-- fmt: on -->

Wetlands does not create, run, or stop the application's event loop.

Provisioning messages vary by installation, but the example ends by printing this mask:

```text
[[False False False False]
 [False False False False]
 [ True  True  True  True]
 [ True  True  True  True]]
```

## Consume events asynchronously

Operations and tasks expose an `events()` async iterator:

```python
operation = manager.provision("analysis", spec)

async for event in operation.events():
    render_activity(event)

environment = await operation
```

The iterator replays available history by default and ends after the terminal event.

## Cancellation

If the application cancels a coroutine that is awaiting an operation or task, Wetlands requests cancellation of the underlying work.
It waits for mandatory process and transfer cleanup before propagating `asyncio.CancelledError`.

This may make coroutine cancellation take longer than an ordinary in-process awaitable.
The delay prevents cleanup from continuing invisibly after the caller sees cancellation as complete.
