"""Tests for wetlands.task module."""

import asyncio
import threading
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from wetlands.task import (
    InvalidStateError,
    RemoteTaskHandle,
    Task,
    TaskEvent,
    TaskEventType,
    TaskStatus,
)


class TestTaskStatus:
    def test_is_finished_terminal_states(self):
        assert TaskStatus.COMPLETED.is_finished()
        assert TaskStatus.FAILED.is_finished()
        assert TaskStatus.CANCELED.is_finished()

    def test_is_finished_non_terminal_states(self):
        assert not TaskStatus.PENDING.is_finished()
        assert not TaskStatus.RUNNING.is_finished()


class TestTaskEvent:
    def test_frozen(self):
        task = Task()
        event = TaskEvent(task=task, type=TaskEventType.STARTED)
        with pytest.raises(AttributeError):
            event.type = TaskEventType.COMPLETION  # type: ignore[misc]

    def test_fields(self):
        task = Task()
        event = TaskEvent(task=task, type=TaskEventType.UPDATE)
        assert event.task is task
        assert event.type == TaskEventType.UPDATE


class TestTaskLifecycle:
    def test_initial_state(self):
        task = Task()
        assert task.status == TaskStatus.PENDING
        assert task.message is None
        assert task.current is None
        assert task.maximum is None
        assert task.progress is None
        assert task.outputs == {}
        assert task.error is None
        assert task.traceback is None
        assert task.exception is None

    def test_set_running(self):
        task = Task()
        events = []
        task.listen(lambda e: events.append(e))
        task._set_running()
        assert task.status == TaskStatus.RUNNING
        assert len(events) == 1
        assert events[0].type == TaskEventType.STARTED

    def test_set_completed(self):
        task = Task()
        task._set_running()
        task._set_completed(42)
        assert task.status == TaskStatus.COMPLETED
        assert task.result == 42
        assert task.future.result() == 42

    def test_result_raises_if_not_completed(self):
        task = Task()
        with pytest.raises(InvalidStateError):
            _ = task.result

    def test_set_failed(self):
        task = Task()
        task._set_running()
        task._set_failed("boom", ["line1", "line2"])
        assert task.status == TaskStatus.FAILED
        assert task.error == "boom"
        assert task.traceback == ["line1", "line2"]
        assert task.exception is not None
        with pytest.raises(Exception):
            task.future.result()

    def test_set_canceled(self):
        task = Task()
        task._set_running()
        task._set_canceled()
        assert task.status == TaskStatus.CANCELED
        assert task.future.cancelled()

    def test_progress(self):
        task = Task()
        task._set_update(current=3, maximum=10)
        assert task.progress == pytest.approx(0.3)

    def test_progress_none_when_missing(self):
        task = Task()
        task._set_update(current=3)
        assert task.progress is None

    def test_progress_none_when_max_zero(self):
        task = Task()
        task._set_update(current=0, maximum=0)
        assert task.progress is None

    def test_outputs_accumulate(self):
        task = Task()
        task._set_update(outputs={"a": 1})
        task._set_update(outputs={"b": 2})
        assert task.outputs == {"a": 1, "b": 2}


class TestTaskWaitFor:
    def test_wait_for_completed(self):
        task = Task()
        task._set_running()
        task._set_completed("done")
        result = task.wait_for(timeout=1)
        assert result is task  # chaining

    def test_wait_for_timeout(self):
        task = Task()
        task._set_running()
        with pytest.raises(TimeoutError):
            task.wait_for(timeout=0.01)

    def test_wait_for_from_thread(self):
        task = Task()
        task._set_running()

        def complete_later():
            import time

            time.sleep(0.05)
            task._set_completed("result")

        t = threading.Thread(target=complete_later)
        t.start()
        task.wait_for(timeout=2)
        assert task.result == "result"
        t.join()


class TestTaskListeners:
    def test_multiple_listeners(self):
        task = Task()
        events1, events2 = [], []
        task.listen(lambda e: events1.append(e.type))
        task.listen(lambda e: events2.append(e.type))
        task._set_running()
        assert events1 == [TaskEventType.STARTED]
        assert events2 == [TaskEventType.STARTED]

    def test_terminal_replay(self):
        """Late listeners receive the terminal event."""
        task = Task()
        task._set_running()
        task._set_completed(99)

        events = []
        task.listen(lambda e: events.append(e.type))
        assert events == [TaskEventType.COMPLETION]

    def test_update_not_replayed(self):
        """UPDATE events are transient and not replayed."""
        task = Task()
        task._set_running()
        task._set_update(message="progress")

        events = []
        task.listen(lambda e: events.append(e.type))
        assert events == []

    def test_remove_listener(self):
        task = Task()
        events = []
        def cb(e):
            events.append(e.type)
        task.listen(cb)
        task._set_running()
        task.remove_listener(cb)
        task._set_completed(1)
        assert events == [TaskEventType.STARTED]


class TestTaskStart:
    def test_start_calls_start_fn(self):
        task = Task()
        called = []
        task._set_start_fn(lambda: called.append(True))
        task.start()
        assert called == [True]

    def test_start_noop_when_running(self):
        task = Task()
        called = []
        task._set_start_fn(lambda: called.append(True))
        task._set_running()
        task.start()
        assert called == []  # not called again

    def test_start_raises_without_start_fn(self):
        task = Task()
        with pytest.raises(InvalidStateError):
            task.start()


class TestTaskCancel:
    def test_cancel_calls_cancel_fn(self):
        task = Task()
        task._set_running()
        cancelled = []
        task._set_cancel_fn(lambda: cancelled.append(True))
        task.cancel()
        assert cancelled == [True]

    def test_cancel_noop_when_finished(self):
        task = Task()
        task._set_running()
        cancelled = []
        task._set_cancel_fn(lambda: cancelled.append(True))
        task._set_completed(1)
        task.cancel()
        assert cancelled == []


class TestTaskContextManager:
    def test_context_manager_starts_and_cancels(self):
        task = Task()
        started = []
        cancelled = []
        task._set_start_fn(lambda: (started.append(True), task._set_running()))
        task._set_cancel_fn(lambda: (cancelled.append(True), task._set_canceled()))

        with task:
            assert task.status == TaskStatus.RUNNING
        # exit should have cancelled and waited
        assert cancelled == [True]
        assert task.status == TaskStatus.CANCELED

    def test_context_manager_no_cancel_if_done(self):
        task = Task()
        task._set_start_fn(lambda: task._set_running())
        cancelled = []
        task._set_cancel_fn(lambda: cancelled.append(True))
        task._set_running()
        task._set_completed(42)

        with task:
            pass
        assert cancelled == []


class TestTaskOnMessage:
    def test_on_message_completion(self):
        task = Task()
        task._set_running()
        task._on_message({"action": "execution finished", "result": [1, 2, 3]})
        assert task.status == TaskStatus.COMPLETED
        assert task.result == [1, 2, 3]

    def test_on_message_error(self):
        task = Task()
        task._set_running()
        task._on_message({"action": "error", "exception": "fail", "traceback": ["tb1"]})
        assert task.status == TaskStatus.FAILED
        assert task.error == "fail"
        assert task.traceback == ["tb1"]

    def test_on_message_update(self):
        task = Task()
        task._set_running()
        task._on_message({"action": "update", "message": "working", "current": 5, "maximum": 10})
        assert task.message == "working"
        assert task.current == 5
        assert task.maximum == 10

    def test_on_message_canceled(self):
        task = Task()
        task._set_running()
        task._on_message({"action": "canceled"})
        assert task.status == TaskStatus.CANCELED

    def test_on_message_update_with_outputs(self):
        task = Task()
        task._set_running()
        task._on_message({"action": "update", "outputs": {"key": "val"}})
        assert task.outputs == {"key": "val"}


class TestTaskFuture:
    def test_future_is_standard_future(self):
        task = Task()
        assert isinstance(task.future, Future)

    def test_future_resolves_on_completion(self):
        task = Task()
        task._set_running()
        task._set_completed("hello")
        assert task.future.result(timeout=1) == "hello"

    def test_future_raises_on_failure(self):
        task = Task()
        task._set_running()
        task._set_failed("err")
        with pytest.raises(Exception):
            task.future.result(timeout=1)


class TestTaskAsync:
    def test_await(self):
        task = Task()
        task._set_running()

        async def run():
            # Complete in background
            def complete():
                import time

                time.sleep(0.05)
                task._set_completed(99)

            threading.Thread(target=complete).start()
            return await task

        result = asyncio.run(run())
        assert result == 99

    def test_events_stream(self):
        task = Task()
        task._set_running()

        async def run():
            collected = []

            def emit():
                import time

                time.sleep(0.05)
                task._set_update(message="step1", current=1, maximum=3)
                time.sleep(0.05)
                task._set_completed("done")

            threading.Thread(target=emit).start()

            async for event in task.events():
                collected.append(event.type)

            return collected

        types = asyncio.run(run())
        assert TaskEventType.UPDATE in types
        assert TaskEventType.COMPLETION in types


class TestRemoteTaskHandle:
    def test_cancel_requested_default_false(self):
        handle = RemoteTaskHandle("t1", threading.Lock(), MagicMock())
        assert handle.cancel_requested is False

    def test_set_cancel_requested(self):
        handle = RemoteTaskHandle("t1", threading.Lock(), MagicMock())
        handle._set_cancel_requested()
        assert handle.cancel_requested is True

    def test_update_sends_message(self):
        conn = MagicMock()
        handle = RemoteTaskHandle("t1", threading.Lock(), conn)
        handle.update("progress", current=5, maximum=10)
        conn.send.assert_called_once()
        msg = conn.send.call_args[0][0]
        assert msg["action"] == "update"
        assert msg["task_id"] == "t1"
        assert msg["message"] == "progress"
        assert msg["current"] == 5
        assert msg["maximum"] == 10

    def test_set_output_sends_message(self):
        conn = MagicMock()
        handle = RemoteTaskHandle("t1", threading.Lock(), conn)
        handle.set_output("key", "value")
        conn.send.assert_called_once()
        msg = conn.send.call_args[0][0]
        assert msg["action"] == "update"
        assert msg["outputs"] == {"key": "value"}

    def test_cancel_sends_canceled(self):
        conn = MagicMock()
        handle = RemoteTaskHandle("t1", threading.Lock(), conn)
        handle.cancel()
        conn.send.assert_called_once()
        msg = conn.send.call_args[0][0]
        assert msg["action"] == "canceled"
        assert msg["task_id"] == "t1"

    def test_log_sends_log_message(self):
        conn = MagicMock()
        handle = RemoteTaskHandle("t1", threading.Lock(), conn)
        handle.log("hello", level=20)
        conn.send.assert_called_once()
        msg = conn.send.call_args[0][0]
        assert msg["action"] == "log"
        assert msg["message"] == "hello"
        assert msg["level"] == 20
