from __future__ import annotations

import asyncio
import threading
import time

import pytest

from wetlands.operation import (
    Operation,
    OperationCanceled,
    OperationEventKind,
    OperationState,
)


def test_operation_wait_and_await_return_the_value() -> None:
    synchronous: Operation[int] = Operation()
    synchronous._start_runner(lambda: 42, thread_name="test-sync-operation")
    assert synchronous.wait_for() == 42
    assert synchronous.state is OperationState.COMPLETED

    async def run() -> None:
        asynchronous: Operation[int] = Operation()
        asynchronous._start_runner(lambda: 43, thread_name="test-async-operation")
        assert await asynchronous == 43

    asyncio.run(run())


def test_operation_events_are_replayed_and_async_iterable() -> None:
    release = threading.Event()
    operation: Operation[str] = Operation()

    def runner() -> str:
        operation._emit(OperationEventKind.STEP, "working", stage="test")
        release.wait()
        return "done"

    operation._start_runner(runner, thread_name="test-event-operation")

    async def observe() -> list[str]:
        messages = []
        async for event in operation.events():
            messages.append(event.message)
            if event.message == "working":
                release.set()
        return messages

    messages = asyncio.run(observe())
    assert messages[0] == "Operation started"
    assert "working" in messages
    assert messages[-1] == "Operation completed"


def test_operation_cancellation_is_terminal_only_after_runner_cleanup() -> None:
    cleanup_finished = threading.Event()
    runner_started = threading.Event()
    operation: Operation[None] = Operation()

    def runner() -> None:
        runner_started.set()
        while not operation.cancellation_requested:
            time.sleep(0.01)
        time.sleep(0.05)
        cleanup_finished.set()
        raise OperationCanceled(operation.id)

    operation._start_runner(runner, thread_name="test-cancel-operation")
    assert runner_started.wait(timeout=1)
    assert operation.cancel()
    with pytest.raises(OperationCanceled):
        operation.wait_for()
    assert cleanup_finished.is_set()
    assert operation.state is OperationState.CANCELED
    assert not operation.cancel()


def test_canceling_await_requests_operation_cancellation_and_waits_for_cleanup() -> None:
    cleanup_finished = threading.Event()
    operation: Operation[None] = Operation()

    def runner() -> None:
        while not operation.cancellation_requested:
            time.sleep(0.01)
        time.sleep(0.05)
        cleanup_finished.set()
        raise OperationCanceled(operation.id)

    operation._start_runner(runner, thread_name="test-await-cancel-operation")

    async def run() -> None:
        waiter = asyncio.ensure_future(operation)
        await asyncio.sleep(0.02)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert cleanup_finished.is_set()

    asyncio.run(run())
    assert cleanup_finished.is_set()
    assert operation.state is OperationState.CANCELED


def test_wait_timeout_does_not_cancel_operation() -> None:
    release = threading.Event()
    operation: Operation[str] = Operation()
    operation._start_runner(lambda: (release.wait(), "done")[1], thread_name="test-timeout-operation")
    with pytest.raises(TimeoutError):
        operation.wait_for(0.01)
    assert not operation.cancellation_requested
    release.set()
    assert operation.wait_for() == "done"


def test_listener_failure_does_not_fail_operation() -> None:
    operation: Operation[int] = Operation()

    def broken_listener(event) -> None:
        raise RuntimeError("listener failed")

    operation.listen(broken_listener)
    operation._start_runner(lambda: 1, thread_name="test-listener-operation")
    assert operation.wait_for() == 1


def test_completed_operation_can_be_awaited_from_separate_caller_owned_loops() -> None:
    operation: Operation[int] = Operation()
    operation._start_runner(lambda: 5, thread_name="test-multiple-loops-operation")
    assert operation.wait_for() == 5

    async def run() -> int:
        return await operation

    assert asyncio.run(run()) == 5
    assert asyncio.run(run()) == 5


def test_terminal_commit_seal_rejects_late_cancellation() -> None:
    sealed = threading.Event()
    release = threading.Event()
    operation: Operation[str] = Operation()

    def runner() -> str:
        assert operation._seal_cancellation()
        sealed.set()
        release.wait()
        return "published"

    operation._start_runner(runner, thread_name="test-terminal-seal")
    assert sealed.wait(timeout=1)
    assert not operation.cancel()
    release.set()
    assert operation.wait_for() == "published"


def test_async_event_replay_is_bounded_and_retains_terminal_event() -> None:
    operation: Operation[int] = Operation()

    def runner() -> int:
        for index in range(operation._EVENT_HISTORY_LIMIT + 100):
            operation._emit(OperationEventKind.OUTPUT, str(index), line=str(index))
        return 1

    operation._start_runner(runner, thread_name="test-bounded-events")
    assert operation.wait_for() == 1

    async def collect():
        return [event async for event in operation.events()]

    events = asyncio.run(collect())
    assert len(events) == operation._EVENT_HISTORY_LIMIT
    assert events[-1].state is OperationState.COMPLETED


def test_terminal_operation_events_without_replay_close_immediately() -> None:
    operation: Operation[int] = Operation()
    operation._start_runner(lambda: 1, thread_name="test-terminal-no-replay")
    assert operation.wait_for() == 1

    async def collect():
        return [event async for event in operation.events(replay=False)]

    assert asyncio.run(asyncio.wait_for(collect(), timeout=1)) == []


def test_listener_replay_precedes_events_emitted_concurrently() -> None:
    operation: Operation[None] = Operation()
    operation._emit(OperationEventKind.STEP, "old")
    replay_started = threading.Event()
    allow_replay = threading.Event()
    received: list[str] = []

    def listener(event) -> None:
        if event.message == "old":
            replay_started.set()
            assert allow_replay.wait(timeout=1)
        received.append(event.message)

    listening = threading.Thread(target=operation.listen, args=(listener,))
    listening.start()
    assert replay_started.wait(timeout=1)
    operation._emit(OperationEventKind.STEP, "new")
    allow_replay.set()
    listening.join(timeout=1)

    assert not listening.is_alive()
    assert received == ["old", "new"]


def test_terminal_state_and_event_are_published_atomically(monkeypatch: pytest.MonkeyPatch) -> None:
    runner_started = threading.Event()
    allow_runner_to_finish = threading.Event()
    publication_started = threading.Event()
    allow_publication = threading.Event()
    sync_observer_attempted = threading.Event()
    async_observer_attempted = threading.Event()
    sync_observer_finished = threading.Event()
    async_observer_finished = threading.Event()
    operation: Operation[int] = Operation()

    def runner() -> int:
        runner_started.set()
        assert allow_runner_to_finish.wait(timeout=1)
        return 1

    operation._start_runner(runner, thread_name="test-atomic-terminal-operation")
    assert runner_started.wait(timeout=1)

    underlying_lock = operation._lock

    class ObservedLock:
        def __enter__(self):
            if threading.current_thread().name == "test-sync-terminal-observer":
                sync_observer_attempted.set()
            elif threading.current_thread().name == "test-async-terminal-observer":
                async_observer_attempted.set()
            return underlying_lock.__enter__()

        def __exit__(self, *args):
            return underlying_lock.__exit__(*args)

    monkeypatch.setattr(operation, "_lock", ObservedLock())
    queue_event = operation._queue_event_locked

    def pause_terminal_publication(kind, message, **kwargs):
        if message == "Operation completed":
            publication_started.set()
            assert allow_publication.wait(timeout=1)
        return queue_event(kind, message, **kwargs)

    monkeypatch.setattr(operation, "_queue_event_locked", pause_terminal_publication)
    allow_runner_to_finish.set()
    assert publication_started.wait(timeout=1)

    sync_events = []

    def observe_synchronously() -> None:
        operation.listen(sync_events.append)
        sync_observer_finished.set()

    async_events = []
    async_errors: list[BaseException] = []

    def observe_asynchronously() -> None:
        async def collect() -> None:
            async_events.extend([event async for event in operation.events()])

        try:
            asyncio.run(asyncio.wait_for(collect(), timeout=1))
        except BaseException as error:
            async_errors.append(error)
        finally:
            async_observer_finished.set()

    sync_observer = threading.Thread(target=observe_synchronously, name="test-sync-terminal-observer")
    async_observer = threading.Thread(target=observe_asynchronously, name="test-async-terminal-observer")
    sync_observer.start()
    async_observer.start()

    assert sync_observer_attempted.wait(timeout=1)
    assert async_observer_attempted.wait(timeout=1)
    assert not sync_observer_finished.is_set()
    assert not async_observer_finished.is_set()

    allow_publication.set()
    sync_observer.join(timeout=1)
    async_observer.join(timeout=1)

    assert not sync_observer.is_alive()
    assert not async_observer.is_alive()
    assert not async_errors
    assert sync_events[-1].state is OperationState.COMPLETED
    assert async_events[-1].state is OperationState.COMPLETED
    assert operation.wait_for(timeout=1) == 1


def test_cancellation_notification_precedes_terminal_notification(monkeypatch: pytest.MonkeyPatch) -> None:
    runner_started = threading.Event()
    allow_runner_to_finish = threading.Event()
    cancellation_delivery_started = threading.Event()
    allow_cancellation_delivery = threading.Event()
    terminal_event_queued = threading.Event()
    terminal_event_delivered = threading.Event()
    operation: Operation[None] = Operation()
    received: list[OperationEventKind] = []

    def listener(event) -> None:
        if event.kind is OperationEventKind.CANCELLATION_REQUESTED:
            cancellation_delivery_started.set()
            assert allow_cancellation_delivery.wait(timeout=1)
        received.append(event.kind)
        if event.state.terminal:
            terminal_event_delivered.set()

    operation.listen(listener)

    queue_event = operation._queue_event_locked

    def observe_terminal_publication(kind, message, **kwargs):
        publication = queue_event(kind, message, **kwargs)
        if publication[0].state.terminal:
            terminal_event_queued.set()
        return publication

    monkeypatch.setattr(operation, "_queue_event_locked", observe_terminal_publication)

    def runner() -> None:
        runner_started.set()
        assert allow_runner_to_finish.wait(timeout=1)

    operation._start_runner(runner, thread_name="test-cancellation-event-order-operation")
    assert runner_started.wait(timeout=1)

    canceling = threading.Thread(target=operation.cancel, name="test-cancellation-event-order-cancel")
    canceling.start()
    assert cancellation_delivery_started.wait(timeout=1)
    allow_runner_to_finish.set()
    assert terminal_event_queued.wait(timeout=1)
    assert not terminal_event_delivered.is_set()

    allow_cancellation_delivery.set()
    canceling.join(timeout=1)

    assert not canceling.is_alive()
    with pytest.raises(OperationCanceled):
        operation.wait_for(timeout=1)
    assert received[-2:] == [OperationEventKind.CANCELLATION_REQUESTED, OperationEventKind.STATE]
