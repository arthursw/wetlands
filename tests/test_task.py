"""Tests for wetlands.task module."""

import asyncio
import threading
from unittest.mock import MagicMock

import pytest

from wetlands import ExecutionError, OperationCanceled
from wetlands._internal.diagnostics import TaskFailure, TaskFailureCategory, WorkerInfo
from wetlands.task import (
    ExecutionEvent,
    ExecutionEventKind,
    ExecutionState,
    ExecutionTask,
    InvalidStateError,
    RemoteTaskHandle,
)


class TestExecutionState:
    def test_terminal_states(self):
        assert ExecutionState.COMPLETED.terminal
        assert ExecutionState.FAILED.terminal
        assert ExecutionState.CANCELED.terminal

    def test_non_terminal_states(self):
        assert not ExecutionState.PENDING.terminal
        assert not ExecutionState.RUNNING.terminal


class TestExecutionEvent:
    def test_frozen(self):
        event = ExecutionEvent(
            sequence=1,
            timestamp=1.5,
            task_id="execution-1",
            kind=ExecutionEventKind.STARTED,
            state=ExecutionState.RUNNING,
            message="Execution started",
        )
        with pytest.raises(AttributeError):
            event.kind = ExecutionEventKind.COMPLETION  # type: ignore[misc]

    def test_fields(self):
        task = ExecutionTask("execution-1")
        task._set_running()
        task._set_update(message="Halfway", current=1, maximum=2)
        event = task._events[-1]
        assert event.task_id == task.id
        assert event.kind is ExecutionEventKind.UPDATE
        assert event.state is ExecutionState.RUNNING
        assert event.message == "Halfway"
        assert event.current == 1
        assert event.maximum == 2
        assert event.progress == pytest.approx(0.5)


class TestTaskLifecycle:
    def test_tasks_are_not_context_managers(self):
        task = ExecutionTask()
        assert not hasattr(task, "__enter__")
        assert not hasattr(task, "__exit__")
        assert not hasattr(task, "__aenter__")
        assert not hasattr(task, "__aexit__")

    def test_initial_state(self):
        task = ExecutionTask()
        assert task.state == ExecutionState.PENDING
        assert task.message is None
        assert task.current is None
        assert task.maximum is None
        assert task.progress is None
        assert task.outputs == {}
        assert task.error is None
        assert task.traceback is None
        assert task.exception is None

    def test_set_running(self):
        task = ExecutionTask()
        events = []
        task.listen(lambda e: events.append(e))
        task._set_running()
        assert task.state == ExecutionState.RUNNING
        assert len(events) == 1
        assert events[0].kind == ExecutionEventKind.STARTED

    def test_set_completed(self):
        task = ExecutionTask()
        task._set_running()
        task._set_completed(42)
        assert task.state == ExecutionState.COMPLETED
        assert task.result == 42

    def test_result_raises_if_not_completed(self):
        task = ExecutionTask()
        with pytest.raises(InvalidStateError):
            _ = task.result

    def test_set_failed(self):
        task = ExecutionTask()
        events = []
        task.listen(events.append)
        task._set_running()
        task._set_failed("boom", ["line1", "line2"])
        assert task.state == ExecutionState.FAILED
        assert task.error is not None
        assert task.error.message == "boom"
        assert task.traceback == "line1line2"
        assert task.exception is not None
        assert task.exception.failure is task.error
        assert events[-1].kind is ExecutionEventKind.FAILURE
        assert events[-1].failure is task.error
        assert events[-1].message == str(task.exception)

    def test_set_failed_with_structured_failure(self):
        task = ExecutionTask("task-1")
        task._set_running()
        failure = TaskFailure.worker_died(worker=WorkerInfo(environment="env", index=2, pid=123), returncode=-9)

        task._set_failed(failure)

        assert task.state == ExecutionState.FAILED
        assert task.error is not None
        assert task.error.category == TaskFailureCategory.WORKER_DIED
        assert task.error.signal == 9
        assert task.error.worker is not None
        assert task.error.worker.index == 2
        assert str(task.exception) == "Worker 2 pid 123 died with signal 9"

    def test_set_canceled(self):
        task = ExecutionTask()
        task._set_running()
        task._set_canceled()
        assert task.state == ExecutionState.CANCELED

    def test_progress(self):
        task = ExecutionTask()
        task._set_update(current=3, maximum=10)
        assert task.progress == pytest.approx(0.3)

    def test_progress_none_when_missing(self):
        task = ExecutionTask()
        task._set_update(current=3)
        assert task.progress is None

    def test_progress_none_when_max_zero(self):
        task = ExecutionTask()
        task._set_update(current=0, maximum=0)
        assert task.progress is None

    def test_outputs_accumulate(self):
        task = ExecutionTask()
        task._set_update(outputs={"a": 1})
        task._set_update(outputs={"b": 2})
        assert task.outputs == {"a": 1, "b": 2}


class TestTaskWaitFor:
    def test_wait_for_completed(self):
        task = ExecutionTask()
        task._set_running()
        task._set_completed("done")
        result = task.wait_for(timeout=1)
        assert result == "done"

    def test_wait_for_timeout(self):
        task = ExecutionTask()
        task._set_running()
        with pytest.raises(TimeoutError):
            task.wait_for(timeout=0.01)

    def test_wait_for_failed_raises_public_execution_error(self):
        task = ExecutionTask()
        task._set_running()
        task._set_failed(RuntimeError("remote failure"))

        with pytest.raises(ExecutionError) as captured:
            task.wait_for()

        assert captured.value.failure is task.error

    def test_wait_for_canceled_raises_public_operation_canceled(self):
        task = ExecutionTask()
        task._set_running()
        task._set_canceled()

        with pytest.raises(OperationCanceled):
            task.wait_for()

    def test_wait_for_from_thread(self):
        task = ExecutionTask()
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
        task = ExecutionTask()
        events1, events2 = [], []
        task.listen(lambda e: events1.append(e.kind))
        task.listen(lambda e: events2.append(e.kind))
        task._set_running()
        assert events1 == [ExecutionEventKind.STARTED]
        assert events2 == [ExecutionEventKind.STARTED]

    def test_bounded_history_is_replayed_to_late_listener(self):
        task = ExecutionTask()
        task._set_running()
        task._set_update(message="working")
        task._set_completed(99)

        events = []
        task.listen(lambda e: events.append(e.kind))
        assert events == [
            ExecutionEventKind.STARTED,
            ExecutionEventKind.UPDATE,
            ExecutionEventKind.COMPLETION,
        ]

    def test_replay_can_be_disabled(self):
        task = ExecutionTask()
        task._set_running()
        task._set_update(message="progress")

        events = []
        task.listen(lambda e: events.append(e.kind), replay=False)
        assert events == []

    def test_listener_is_not_retained_after_terminal_state(self):
        task = ExecutionTask()
        task._set_running()
        task._set_completed("done")

        def callback(event):
            return None

        task.listen(callback, replay=False)

        assert callback not in task._listeners

    def test_remove_listener(self):
        task = ExecutionTask()
        events = []

        def cb(e):
            events.append(e.kind)

        task.listen(cb)
        task._set_running()
        task.remove_listener(cb)
        task._set_completed(1)
        assert events == [ExecutionEventKind.STARTED]

    def test_events_are_snapshot_values_not_mutable_task_views(self):
        task = ExecutionTask()
        events = []
        task.listen(events.append)
        task._set_running()
        task._set_update(message="first", current=1, maximum=4)
        first_update = events[-1]
        task._set_update(message="second", current=3, maximum=4)

        assert first_update.message == "first"
        assert first_update.current == 1
        assert first_update.progress == pytest.approx(0.25)
        assert not hasattr(first_update, "task")

    def test_sequences_and_timestamps_are_monotonic(self):
        task = ExecutionTask()
        task._set_running()
        task._set_update(current=1, maximum=2)
        task._set_completed("done")

        assert [event.sequence for event in task._events] == [1, 2, 3]
        assert [event.timestamp for event in task._events] == sorted(event.timestamp for event in task._events)


class TestTaskStart:
    def test_start_calls_start_fn(self):
        task = ExecutionTask()
        called = []
        task._set_start_fn(lambda: called.append(True))
        task.start()
        assert called == [True]

    def test_start_noop_when_running(self):
        task = ExecutionTask()
        called = []
        task._set_start_fn(lambda: called.append(True))
        task._set_running()
        task.start()
        assert called == []  # not called again

    def test_start_raises_without_start_fn(self):
        task = ExecutionTask()
        with pytest.raises(InvalidStateError):
            task.start()


class TestTaskCancel:
    def test_cancel_calls_cancel_fn(self):
        task = ExecutionTask()
        task._set_running()
        cancelled = []
        events = []
        task.listen(events.append, replay=False)
        task._set_cancel_fn(lambda: cancelled.append(True))
        task.cancel()
        assert cancelled == [True]
        assert events[-1].kind is ExecutionEventKind.CANCELLATION_REQUESTED
        assert events[-1].state is ExecutionState.RUNNING

    def test_cancel_noop_when_finished(self):
        task = ExecutionTask()
        task._set_running()
        cancelled = []
        task._set_cancel_fn(lambda: cancelled.append(True))
        task._set_completed(1)
        task.cancel()
        assert cancelled == []


class TestTaskOnMessage:
    def test_on_message_completion(self):
        task = ExecutionTask()
        task._set_running()
        task._on_message({"action": "execution finished", "result": [1, 2, 3]})
        assert task.state == ExecutionState.COMPLETED
        assert task.result == [1, 2, 3]

    def test_on_message_error(self):
        task = ExecutionTask()
        task._set_running()
        task._on_message({"action": "error", "exception": "fail", "traceback": ["tb1"]})
        assert task.state == ExecutionState.FAILED
        assert task.error is not None
        assert task.error.message == "fail"
        assert task.traceback == "tb1"

    def test_on_message_structured_error(self):
        task = ExecutionTask("task-structured")
        task._set_running()
        task._on_message(
            {
                "action": "error",
                "task_id": "task-structured",
                "failure": {
                    "category": "remote_exception",
                    "message": "bad input",
                    "traceback": "Traceback...\nValueError: bad input\n",
                    "remote_exception": {
                        "module": "sample_module",
                        "type_name": "ValueError",
                        "qualified_name": "ValueError",
                        "message": "bad input",
                        "traceback": "ValueError: bad input\n",
                        "cause": None,
                        "context": None,
                        "suppress_context": False,
                    },
                },
            }
        )
        assert task.error is not None
        assert task.error.remote_exception is not None
        assert task.error.remote_exception.module == "sample_module"
        assert task.error.remote_exception.type_name == "ValueError"
        assert task.traceback == "Traceback...\nValueError: bad input\n"
        assert str(task.exception) == "Remote ValueError from sample_module: bad input"

    def test_on_message_update(self):
        task = ExecutionTask()
        task._set_running()
        task._on_message({"action": "update", "message": "working", "current": 5, "maximum": 10})
        assert task.message == "working"
        assert task.current == 5
        assert task.maximum == 10

    def test_on_message_canceled(self):
        task = ExecutionTask()
        task._set_running()
        task._on_message({"action": "canceled"})
        assert task.state == ExecutionState.CANCELED

    def test_on_message_update_with_outputs(self):
        task = ExecutionTask()
        task._set_running()
        task._on_message({"action": "update", "outputs": {"key": "val"}})
        assert task.outputs == {"key": "val"}


class TestTaskAsync:
    def test_await(self):
        task = ExecutionTask()
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
        task = ExecutionTask()
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
                collected.append(event.kind)

            return collected

        types = asyncio.run(run())
        assert ExecutionEventKind.UPDATE in types
        assert ExecutionEventKind.COMPLETION in types

    def test_async_event_replay_is_bounded_and_keeps_terminal_event(self):
        task = ExecutionTask()
        task._set_running()
        for index in range(task._EVENT_HISTORY_LIMIT + 100):
            task._set_update(message=str(index), current=index)
        task._set_completed("done")

        async def collect():
            return [event async for event in task.events()]

        events = asyncio.run(collect())
        assert len(events) == task._EVENT_HISTORY_LIMIT
        assert events[-1].kind is ExecutionEventKind.COMPLETION
        assert events[-1].state is ExecutionState.COMPLETED
        assert [event.sequence for event in events] == sorted(event.sequence for event in events)

    def test_terminal_stream_without_replay_finishes_immediately(self):
        task = ExecutionTask()
        task._set_running()
        task._set_completed("done")

        async def collect():
            return [event async for event in task.events(replay=False)]

        assert asyncio.run(collect()) == []


class TestTerminalStateGuards:
    def test_set_failed_twice_no_exception(self):
        task = ExecutionTask()
        task._set_running()
        task._set_failed("first error")
        task._set_failed("second error")  # should be a no-op
        assert task.state == ExecutionState.FAILED
        assert task.error is not None
        assert task.error.message == "first error"

    def test_set_completed_then_set_failed_no_exception(self):
        task = ExecutionTask()
        task._set_running()
        task._set_completed(42)
        task._set_failed("should be ignored")
        assert task.state == ExecutionState.COMPLETED
        assert task.result == 42

    def test_set_failed_then_set_completed_no_exception(self):
        task = ExecutionTask()
        task._set_running()
        task._set_failed("error")
        task._set_completed(42)  # should be a no-op
        assert task.state == ExecutionState.FAILED
        assert task.error is not None
        assert task.error.message == "error"

    def test_set_canceled_then_set_failed_no_exception(self):
        task = ExecutionTask()
        task._set_running()
        task._set_canceled()
        task._set_failed("should be ignored")
        assert task.state == ExecutionState.CANCELED

    def test_concurrent_set_failed_no_exception(self):
        task = ExecutionTask()
        task._set_running()
        barrier = threading.Barrier(2)
        errors = []

        def fail_task(error_msg):
            try:
                barrier.wait(timeout=2)
                task._set_failed(error_msg)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=fail_task, args=("error1",))
        t2 = threading.Thread(target=fail_task, args=("error2",))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
        assert errors == []
        assert task.state == ExecutionState.FAILED
        assert task.error is not None
        assert task.error.message in ("error1", "error2")


class TestTaskFailureDiagnostics:
    def test_exception_from_chained_exception_preserves_cause(self):
        try:
            try:
                raise KeyError("root")
            except KeyError as e:
                raise ValueError("outer") from e
        except ValueError as e:
            failure = TaskFailure.from_exception(e)

        assert failure.remote_exception is not None
        assert failure.remote_exception.type_name == "ValueError"
        assert failure.remote_exception.cause is not None
        assert failure.remote_exception.cause.type_name == "KeyError"
        assert "The above exception was the direct cause" in (failure.traceback or "")

    def test_failure_payload_round_trips_worker_metadata(self):
        failure = TaskFailure.worker_died(
            worker=WorkerInfo(environment="env", index=1, pid=111, port=5000, persistent=True),
            returncode=-15,
        )

        round_tripped = TaskFailure.from_payload({"failure": failure.to_payload()})

        assert round_tripped.category == TaskFailureCategory.WORKER_DIED
        assert round_tripped.signal == 15
        assert round_tripped.worker is not None
        assert round_tripped.worker.port == 5000
        assert round_tripped.worker.persistent is True


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

    def test_cancel_is_local_and_does_not_emit_a_terminal_message(self):
        conn = MagicMock()
        handle = RemoteTaskHandle("t1", threading.Lock(), conn)
        handle.cancel()
        assert handle.cancel_requested
        conn.send.assert_not_called()

    def test_log_sends_log_message(self):
        conn = MagicMock()
        handle = RemoteTaskHandle("t1", threading.Lock(), conn)
        handle.log("hello", level=20)
        conn.send.assert_called_once()
        msg = conn.send.call_args[0][0]
        assert msg["action"] == "log"
        assert msg["message"] == "hello"
        assert msg["level"] == 20
