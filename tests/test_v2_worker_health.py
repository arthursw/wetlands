from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from wetlands.diagnostics import ExecutionFailureCategory
from wetlands._internal.process_termination import ProcessTerminationError
from wetlands.external_environment import ExternalEnvironment, _validate_worker_environments, _Worker
from wetlands.lifecycle import EnvironmentGenerationChangedError, WorkerStartError
from wetlands.managed_environment import WorkerPool
from wetlands.operation import ExecutionError
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, ProtocolCompatibilityError
from wetlands.task import ExecutionState, ExecutionTask


def _environment(tmp_path: Path) -> ExternalEnvironment:
    manager = MagicMock()
    manager.root = tmp_path / "wetlands"
    manager.environments_root = tmp_path / "wetlands" / "environments"
    manager.state_root = tmp_path / "wetlands" / "state"
    return ExternalEnvironment("example", tmp_path / "pixi.toml", manager)


def _worker(index: int = 0, *, alive: bool = True) -> _Worker:
    process = MagicMock()
    process.pid = 1000 + index
    process.poll.return_value = None if alive else 9
    process.returncode = None if alive else 9
    process.stdout = None
    process.stderr = None
    process._wetlands_job_handle = None
    connection = MagicMock()
    connection.closed = False
    logger = MagicMock()
    return _Worker(index, process, 5000 + index, connection, logger)


def _active_task(task_id: str = "task-1") -> ExecutionTask[Any]:
    task: ExecutionTask[Any] = ExecutionTask(task_id)
    task._payload = {"_call_target": "sample.module:run"}  # type: ignore[attr-defined]
    task._set_running()
    return task


def test_worker_count_and_liveness_follow_the_current_pool(tmp_path):
    environment = _environment(tmp_path)
    live = _worker(0)
    dead = _worker(1, alive=False)

    environment._workers = [live]
    assert environment.worker_count == 1
    assert environment.launched()

    environment._workers = [dead]
    assert not environment.launched()


def test_task_input_cleanup_is_atomic_across_terminal_races(tmp_path):
    environment = _environment(tmp_path)
    task: ExecutionTask[Any] = ExecutionTask("task-1")
    lease = MagicMock()
    task._input_leases = [lease]  # type: ignore[attr-defined]
    threads = [threading.Thread(target=environment._cleanup_task_inputs, args=(task,)) for _ in range(8)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    lease.dispose.assert_called_once()
    assert task._input_leases is None  # type: ignore[attr-defined]


@patch("wetlands.external_environment.terminate_launched_process_tree")
def test_retiring_live_worker_closes_connection_kills_process_and_is_idempotent(mock_terminate, tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    environment._workers = [worker]

    assert environment._remove_dead_worker(worker)
    assert not environment._remove_dead_worker(worker)

    assert worker not in environment._workers
    worker.connection.close.assert_called_once()
    worker.process_logger.join.assert_called_once()
    mock_terminate.assert_called_once()
    assert mock_terminate.call_args.args == (worker.process,)


@patch(
    "wetlands.external_environment.terminate_launched_process_tree",
    side_effect=ProcessTerminationError("worker survived"),
)
def test_unverified_worker_retirement_retains_ownership_and_fails_pool(
    mock_terminate,
    tmp_path,
):
    environment = _environment(tmp_path)
    worker = _worker()
    environment._workers = [worker]

    with patch("wetlands.external_environment.runtime_state.remove_worker") as remove:
        assert not environment._remove_dead_worker(worker)

    assert worker in environment._workers
    remove.assert_not_called()
    with pytest.raises(WorkerStartError) as caught:
        environment._raise_if_failed()
    assert caught.value.phase == "cleanup"
    assert caught.value.worker_index == worker.index
    assert "durable worker ownership record retained" in str(caught.value)
    mock_terminate.assert_called_once()


@patch("wetlands.external_environment.terminate_launched_process_tree")
def test_worker_record_removal_failure_retains_retryable_ownership(
    mock_terminate,
    tmp_path,
):
    environment = _environment(tmp_path)
    worker = _worker()
    environment._workers = [worker]

    with patch(
        "wetlands.external_environment.runtime_state.remove_worker",
        side_effect=OSError("state write failed"),
    ):
        assert not environment._remove_dead_worker(worker)

    assert worker in environment._workers
    with pytest.raises(WorkerStartError) as caught:
        environment._raise_if_failed()
    assert caught.value.phase == "cleanup"
    assert "durable ownership record could not be removed" in str(caught.value)
    mock_terminate.assert_called_once()


def test_pool_close_is_retryable_until_worker_termination_is_verified(tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    environment._workers = [worker]
    pool = WorkerPool(MagicMock(), environment)

    with (
        patch.object(
            environment,
            "_terminate_launched_worker",
            side_effect=[False, True],
        ) as terminate,
        patch("wetlands.external_environment.runtime_state.remove_worker") as remove,
        patch("wetlands.external_environment.reconcile_shared_memory_leases"),
    ):
        with pytest.raises(WorkerStartError) as caught:
            pool.close()

        assert caught.value.phase == "close"
        assert not pool._closed
        assert environment._workers == [worker]
        remove.assert_not_called()

        pool.close()

    assert pool._closed
    assert environment._workers == []
    remove.assert_called_once()
    assert terminate.call_count == 2
    assert environment._fatal_error is None


@patch("wetlands.external_environment.terminate_launched_process_tree")
def test_health_monitor_fails_dead_worker_task_and_requests_replacement(mock_terminate, tmp_path):
    environment = _environment(tmp_path)
    worker = _worker(alive=False)
    task = _active_task()
    worker._current_task = task
    environment._workers = [worker]
    environment._try_replace_worker = MagicMock()
    environment._shutdown_event = MagicMock()
    environment._shutdown_event.wait.side_effect = [False, True]

    environment._health_monitor_loop()

    assert task.state == ExecutionState.FAILED
    assert task.error is not None
    assert task.error.category == ExecutionFailureCategory.WORKER_DIED
    assert worker not in environment._workers
    environment._try_replace_worker.assert_called_once_with(worker.index)


@patch("wetlands.external_environment.terminate_launched_process_tree")
def test_health_monitor_fails_hung_worker_only_after_timeout(mock_terminate, tmp_path):
    environment = _environment(tmp_path)
    environment._worker_timeout = 0.01
    worker = _worker()
    worker._last_activity = time.time() - 1
    task = _active_task()
    worker._current_task = task
    environment._workers = [worker]
    environment._try_replace_worker = MagicMock()
    environment._shutdown_event = MagicMock()
    environment._shutdown_event.wait.side_effect = [False, True]

    environment._health_monitor_loop()

    assert task.state == ExecutionState.FAILED
    assert task.error is not None
    assert task.error.category == ExecutionFailureCategory.TIMEOUT
    assert worker not in environment._workers
    environment._try_replace_worker.assert_called_once_with(worker.index)


def test_replacement_worker_is_added_and_made_available(tmp_path):
    environment = _environment(tmp_path)
    replacement = _worker()

    with patch.object(environment, "_launch_worker", return_value=replacement):
        environment._try_replace_worker(0)

    assert environment._workers == [replacement]
    assert environment._idle_workers.get_nowait() is replacement


def test_replacement_reuses_snapshotted_environment_for_stable_index(tmp_path):
    environment = _environment(tmp_path)
    source = {"CUDA_VISIBLE_DEVICES": "0"}
    calls: list[int] = []

    def configure(index: int):
        calls.append(index)
        return source

    environment._worker_environments = _validate_worker_environments(1, configure)
    source["CUDA_VISIBLE_DEVICES"] = "changed"
    replacement = _worker()

    with patch.object(environment, "_launch_worker", return_value=replacement) as launch:
        environment._try_replace_worker(0)

    assert calls == [0]
    launch.assert_called_once_with(0, {"CUDA_VISIBLE_DEVICES": "0"})


@pytest.mark.parametrize(
    ("worker_environment", "error", "message"),
    [
        (object(), TypeError, "callable"),
        (lambda _index: None, TypeError, "return a mapping"),
        (lambda _index: {1: "value"}, TypeError, "keys and values must be strings"),
        (lambda _index: {"NAME": 1}, TypeError, "keys and values must be strings"),
        (lambda _index: {"": "value"}, ValueError, "invalid variable name"),
        (lambda _index: {"BAD=NAME": "value"}, ValueError, "invalid variable name"),
        (lambda _index: {"BAD\0NAME": "value"}, ValueError, "null bytes"),
        (lambda _index: {"NAME": "bad\0value"}, ValueError, "null bytes"),
        (lambda _index: {"WETLANDS_STARTUP_TOKEN": "value"}, ValueError, "reserved variable"),
        (lambda _index: {"wetlands_future_value": "value"}, ValueError, "reserved variable"),
        (lambda _index: {"PYTHONPATH": "value"}, ValueError, "reserved variable"),
        (lambda _index: {"pythonhome": "value"}, ValueError, "reserved variable"),
    ],
)
def test_worker_environment_validation_is_failure_atomic(
    tmp_path,
    worker_environment,
    error,
    message,
):
    environment = _environment(tmp_path)

    with (
        patch("wetlands.external_environment.reconcile_shared_memory_leases") as reconcile,
        patch.object(environment, "_launch_worker") as launch,
        pytest.raises(error, match=message),
    ):
        environment.launch(worker_environment=worker_environment)

    reconcile.assert_not_called()
    launch.assert_not_called()


def test_worker_environment_callback_runs_once_per_index_before_launch(tmp_path):
    environment = _environment(tmp_path)
    environments = [
        {"CUDA_VISIBLE_DEVICES": "0"},
        {"CUDA_VISIBLE_DEVICES": "1"},
    ]
    calls: list[int] = []
    workers = [_worker(0), _worker(1)]

    def configure(index: int):
        calls.append(index)
        return environments[index]

    with (
        patch("wetlands.external_environment.reconcile_shared_memory_leases"),
        patch.object(environment, "_launch_worker", side_effect=workers) as launch,
        patch("wetlands.external_environment.runtime_state.load_or_create_root_authkey", return_value=b"key"),
        patch("wetlands.external_environment.threading.Thread") as thread,
    ):
        environment.launch(max_workers=2, worker_environment=configure)

    assert calls == [0, 1]
    assert launch.call_args_list == [
        call(0, {"CUDA_VISIBLE_DEVICES": "0"}),
        call(1, {"CUDA_VISIBLE_DEVICES": "1"}),
    ]
    thread.return_value.start.assert_called_once_with()


def test_worker_environment_callback_error_propagates_before_launch_side_effects(tmp_path):
    environment = _environment(tmp_path)
    failure = RuntimeError("configuration failed")

    with (
        patch("wetlands.external_environment.reconcile_shared_memory_leases") as reconcile,
        patch.object(environment, "_launch_worker") as launch,
        pytest.raises(RuntimeError) as caught,
    ):
        environment.launch(worker_environment=MagicMock(side_effect=failure))

    assert caught.value is failure
    reconcile.assert_not_called()
    launch.assert_not_called()


def test_persistent_worker_environment_is_rejected_without_invoking_callback(tmp_path):
    environment = _environment(tmp_path)
    callback = MagicMock(return_value={})

    with (
        patch("wetlands.external_environment.reconcile_shared_memory_leases") as reconcile,
        pytest.raises(ValueError, match="persistent=True"),
    ):
        environment.launch(persistent=True, worker_environment=callback)

    callback.assert_not_called()
    reconcile.assert_not_called()


def test_replacement_failure_is_structurally_reported_by_pool(tmp_path, caplog):
    environment = _environment(tmp_path)

    with (
        patch.object(environment, "_launch_worker", side_effect=RuntimeError("environment broken")),
        caplog.at_level(logging.ERROR),
    ):
        environment._try_replace_worker(0)

    assert "Failed to launch replacement worker 0" in caplog.text
    with pytest.raises(WorkerStartError) as caught:
        environment._raise_if_failed()
    assert caught.value.phase == "replace"
    assert caught.value.worker_index == 0


def test_dispatch_failure_cannot_finish_task_before_transport_and_worker_cleanup(
    tmp_path,
):
    environment = _environment(tmp_path)
    environment._expected_generation_id = "generation-1"
    worker = _worker()
    worker.process_started_at = 1.0
    worker.connection.send.side_effect = BrokenPipeError("closed")
    environment._workers = [worker]
    task: ExecutionTask[Any] = ExecutionTask("task-1")
    task._payload = {"_call_target": "sample.module:run", "codecs": []}  # type: ignore[attr-defined]
    lease = MagicMock()
    task._input_leases = [lease]  # type: ignore[attr-defined]
    cleanup_started = threading.Event()
    allow_cleanup = threading.Event()

    def remove_worker(_worker, *, retirement_claimed=False):
        assert retirement_claimed
        cleanup_started.set()
        assert allow_cleanup.wait(2)
        return True

    with (
        patch.object(
            environment,
            "_remove_dead_worker",
            side_effect=remove_worker,
        ),
        patch.object(environment, "_try_replace_worker"),
    ):
        dispatch = threading.Thread(
            target=environment._dispatch_to_worker,
            args=(worker, task),
        )
        dispatch.start()
        assert cleanup_started.wait(2)
        lease.dispose.assert_called_once()
        with pytest.raises(TimeoutError):
            task.wait_for(0.01)
        assert task.state is ExecutionState.RUNNING

        allow_cleanup.set()
        dispatch.join(2)

    assert not dispatch.is_alive()
    assert task.state is ExecutionState.FAILED


def test_generation_drift_during_replacement_fails_the_pool(tmp_path):
    environment = _environment(tmp_path)
    error = EnvironmentGenerationChangedError(
        "example",
        expected_generation_id="generation-1",
        expected_recipe_hash="recipe-1",
        actual_generation_id="generation-2",
        actual_recipe_hash="recipe-2",
    )

    with (
        patch.object(environment, "_launch_worker", side_effect=error),
        patch.object(environment, "_exit") as exit_pool,
    ):
        environment._try_replace_worker(0)

    exit_pool.assert_called_once_with()
    with pytest.raises(EnvironmentGenerationChangedError) as caught:
        environment._raise_if_failed()
    assert caught.value is error


def test_launch_wraps_worker_failure_in_public_start_error(tmp_path):
    environment = _environment(tmp_path)

    with (
        patch.object(environment, "_launch_worker", side_effect=RuntimeError("boom")),
        pytest.raises(WorkerStartError) as caught,
    ):
        environment.launch()

    assert caught.value.environment == "example"
    assert caught.value.phase == "launch"
    assert "boom" in str(caught.value)


def test_launch_wraps_protocol_mismatch_in_public_start_error(tmp_path):
    environment = _environment(tmp_path)
    mismatch = ProtocolCompatibilityError("protocol mismatch")

    with (
        patch.object(environment, "_launch_worker", side_effect=mismatch),
        pytest.raises(WorkerStartError) as caught,
    ):
        environment.launch()

    assert caught.value.environment == "example"
    assert caught.value.phase == "launch"
    assert caught.value.__cause__ is mismatch


@pytest.mark.parametrize(
    "worker_timeout",
    [0, -1, float("inf"), float("-inf"), float("nan"), True, "1"],
)
def test_launch_rejects_invalid_worker_timeout(tmp_path, worker_timeout):
    environment = _environment(tmp_path)

    with pytest.raises(ValueError, match="positive finite"):
        environment.launch(worker_timeout=worker_timeout)


@pytest.mark.parametrize(
    ("message", "task_setup"),
    [
        (None, lambda task: None),
        (
            {
                "action": "unknown",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "task-1",
            },
            lambda task: None,
        ),
        (
            {
                "action": "accepted",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "other-task",
            },
            lambda task: None,
        ),
        (
            {
                "action": "accepted",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "task-1",
            },
            lambda task: setattr(task, "_accepted", True),
        ),
        (
            {
                "action": "update",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "task-1",
                "message": "too soon",
            },
            lambda task: None,
        ),
        (
            {
                "action": "released",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "task-1",
                "names": [],
            },
            lambda task: setattr(task, "_accepted", True),
        ),
    ],
)
def test_worker_protocol_violation_fails_task_retires_worker_and_replaces(
    tmp_path,
    message,
    task_setup,
):
    environment = _environment(tmp_path)
    worker = _worker()
    task = _active_task()
    task_setup(task)
    worker._current_task = task
    worker.connection.recv.return_value = message
    environment._workers = [worker]

    with (
        patch.object(environment, "_cleanup_failed_task_worker", return_value=True) as cleanup,
        patch.object(environment, "_try_replace_worker") as replace,
    ):
        environment._worker_reader_loop(worker)

    cleanup.assert_called_once()
    replace.assert_called_once_with(worker.index)
    assert task.state is ExecutionState.FAILED
    assert task.error is not None
    assert task.error.category is ExecutionFailureCategory.WORKER_CONNECTION
    assert "protocol failure" in task.error.message.lower()
    with pytest.raises(ExecutionError):
        task.wait_for()


def test_unexpected_protocol_validator_error_still_fails_closed(tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    task = _active_task()
    worker._current_task = task
    worker.connection.recv.return_value = {
        "action": "error",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": task.id,
    }
    environment._workers = [worker]

    with (
        patch(
            "wetlands.external_environment.validate_worker_task_message",
            side_effect=RecursionError("malicious recursive payload"),
        ),
        patch.object(environment, "_cleanup_failed_task_worker", return_value=True) as cleanup,
        patch.object(environment, "_try_replace_worker") as replace,
    ):
        environment._worker_reader_loop(worker)

    cleanup.assert_called_once()
    replace.assert_called_once_with(worker.index)
    assert task.state is ExecutionState.FAILED
    assert task.error is not None
    assert "RecursionError" in task.error.message
    with pytest.raises(ExecutionError):
        task.wait_for()


def test_forced_cancel_barrier_prevents_late_message_redispatch(tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    task = _active_task()
    task._accepted = True  # type: ignore[attr-defined]
    task._inputs_released = True  # type: ignore[attr-defined]
    worker._current_task = task
    worker.connection.recv.return_value = {
        "action": "canceled",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": task.id,
    }
    environment._workers = [worker]
    queued = ExecutionTask("queued-task")
    environment._task_queue.put(queued)
    validator_entered = threading.Event()
    allow_validation = threading.Event()

    def blocked_validation(message, *, expected_task_id):
        validator_entered.set()
        assert allow_validation.wait(2)
        return "canceled"

    with patch(
        "wetlands.external_environment.validate_worker_task_message",
        side_effect=blocked_validation,
    ):
        reader = threading.Thread(
            target=environment._worker_reader_loop,
            args=(worker,),
        )
        reader.start()
        assert validator_entered.wait(2)
        with environment._lock:
            task._terminal_cleanup_in_progress = True  # type: ignore[attr-defined]
            worker._current_task = None
            worker._retired = True
        task._set_canceled()
        allow_validation.set()
        reader.join(2)

    assert not reader.is_alive()
    assert environment._task_queue.get_nowait() is queued
    assert environment._idle_workers.empty()
    worker.connection.send.assert_not_called()


def test_cleanup_claims_worker_retirement_before_slow_lease_cleanup(tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    task = _active_task()
    worker._current_task = task
    environment._workers = [worker]
    cleanup_entered = threading.Event()
    allow_cleanup = threading.Event()

    def slow_input_cleanup(_task):
        cleanup_entered.set()
        assert allow_cleanup.wait(2)

    with (
        patch.object(environment, "_cleanup_task_inputs", side_effect=slow_input_cleanup),
        patch.object(environment, "_remove_dead_worker", return_value=True) as remove,
    ):
        cleanup = threading.Thread(
            target=environment._cleanup_failed_task_worker,
            args=(worker, task),
        )
        cleanup.start()
        assert cleanup_entered.wait(2)
        assert worker._retired
        assert worker._current_task is None
        assert not environment._worker_owns_task(worker, task)
        allow_cleanup.set()
        cleanup.join(2)

    assert not cleanup.is_alive()
    remove.assert_called_once_with(worker, retirement_claimed=True)


def test_task_is_requeued_when_worker_retires_after_idle_pop(tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    worker.process_started_at = 1.0
    environment._workers = [worker]
    environment._idle_workers.put(worker)
    task: ExecutionTask[Any] = ExecutionTask("task-race")
    task._payload = {"_call_target": "sample.module:run", "codecs": []}  # type: ignore[attr-defined]
    task._input_leases = []  # type: ignore[attr-defined]
    environment._submit_task(task, start=False)
    dispatch_entered = threading.Event()
    allow_dispatch = threading.Event()

    def blocked_lease_context(*args, **kwargs):
        dispatch_entered.set()
        assert allow_dispatch.wait(2)
        return {}

    with patch.object(
        environment,
        "_lease_context",
        side_effect=blocked_lease_context,
    ):
        starter = threading.Thread(target=task._start)
        starter.start()
        assert dispatch_entered.wait(2)
        with environment._lock:
            worker._retired = True
        allow_dispatch.set()
        starter.join(2)

    assert not starter.is_alive()
    assert task.state is ExecutionState.PENDING
    assert environment._task_queue.get_nowait() is task
    worker.connection.send.assert_not_called()


def test_submit_that_passed_open_check_is_failed_during_concurrent_pool_close(
    tmp_path,
):
    environment = _environment(tmp_path)
    environment._expected_generation_id = "generation-1"
    managed = MagicMock()
    managed._manager = MagicMock()
    pool = WorkerPool(managed, environment)
    submit_entered = threading.Event()
    allow_submit = threading.Event()
    close_entered = threading.Event()
    allow_close = threading.Event()
    original_submit = environment._submit_encoded
    submitted = []

    def blocked_submit(*args, **kwargs):
        submit_entered.set()
        assert allow_submit.wait(2)
        submitted.append(original_submit(*args, **kwargs))

    def blocked_reconciliation(_root):
        close_entered.set()
        assert allow_close.wait(2)

    with (
        patch.object(environment, "_submit_encoded", side_effect=blocked_submit),
        patch(
            "wetlands.external_environment.reconcile_shared_memory_leases",
            side_effect=blocked_reconciliation,
        ),
    ):
        submitter = threading.Thread(
            target=pool.submit_import,
            args=("operator:add",),
            kwargs={"args": (1, 2)},
        )
        submitter.start()
        assert submit_entered.wait(2)

        closer = threading.Thread(target=pool.close)
        closer.start()
        assert close_entered.wait(2)

        allow_submit.set()
        allow_close.set()
        submitter.join(2)
        closer.join(2)

    assert not submitter.is_alive()
    assert not closer.is_alive()
    assert len(submitted) == 1
    task = submitted[0]
    assert task.state is ExecutionState.FAILED
    assert task.error is not None
    assert "shutting down" in task.error.message
    assert task._input_leases is None  # type: ignore[attr-defined]
    assert environment._task_queue.empty()
    with pytest.raises(ExecutionError):
        task.wait_for()


def test_worker_message_without_active_task_retires_and_replaces(tmp_path):
    environment = _environment(tmp_path)
    worker = _worker()
    worker.connection.recv.return_value = {
        "action": "accepted",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": "late-task",
    }
    environment._workers = [worker]

    with (
        patch.object(environment, "_remove_dead_worker", return_value=True) as retire,
        patch.object(environment, "_try_replace_worker") as replace,
    ):
        environment._worker_reader_loop(worker)

    retire.assert_called_once_with(worker)
    replace.assert_called_once_with(worker.index)


def test_commission_ack_schema_and_order_are_strict(tmp_path):
    environment = _environment(tmp_path)
    environment._persistent = True
    environment._pool_id = "pool-1"
    worker = _worker()
    message = {
        "action": "commissioned",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "pool_id": "pool-1",
    }

    assert environment._is_valid_commissioned_message(worker, message)
    worker._commissioned.set()
    assert not environment._is_valid_commissioned_message(worker, message)
    assert not environment._is_valid_commissioned_message(
        _worker(),
        {**message, "unexpected": True},
    )


def test_commission_ack_is_processed_while_launch_holds_runtime_lock(tmp_path):
    environment = _environment(tmp_path)
    environment._persistent = True
    environment._pool_id = "pool-1"
    worker = _worker()
    message = {
        "action": "commissioned",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "pool_id": "pool-1",
    }
    allow_eof = threading.Event()
    receive_count = 0

    def receive():
        nonlocal receive_count
        receive_count += 1
        if receive_count == 1:
            return message
        assert allow_eof.wait(2)
        raise EOFError

    worker.connection.recv.side_effect = receive
    with environment._lock:
        reader = threading.Thread(
            target=environment._worker_reader_loop,
            args=(worker,),
        )
        reader.start()
        assert worker._commissioned.wait(1)
        environment._shutdown_event.set()
        allow_eof.set()
    reader.join(2)

    assert not reader.is_alive()
    assert receive_count == 2


def test_failed_recorded_launch_retains_worker_until_death_is_verified(
    tmp_path,
):
    environment = _environment(tmp_path)
    process = _worker().process
    process.pid = 1234
    process._wetlands_started_at = 1.0
    process._wetlands_process_group_id = 1234
    process._wetlands_session_id = 1234
    connection = MagicMock()
    capabilities = object()
    startup_socket = MagicMock()
    startup_socket.getsockname.return_value = ("127.0.0.1", 43210)
    ready = {
        "generation_id": "generation-1",
        "recipe_hash": "recipe-1",
    }

    with (
        patch.object(environment, "_ready_identity", return_value=ready),
        patch.object(
            environment,
            "_environment_python",
            return_value=tmp_path / "python",
        ),
        patch(
            "wetlands.external_environment._open_startup_socket",
            return_value=startup_socket,
        ),
        patch.object(
            environment,
            "_spawn_worker_process",
            return_value=process,
        ),
        patch("wetlands.external_environment.ProcessLogger"),
        patch(
            "wetlands.external_environment._wait_for_startup_payload",
            return_value={"port": 5000, "management_port": 5001},
        ),
        patch(
            "wetlands.external_environment.validate_worker_capabilities",
            return_value=capabilities,
        ),
        patch.object(
            environment,
            "_connect_worker",
            return_value=(connection, capabilities),
        ),
        patch("wetlands.external_environment.runtime_state.record_worker") as record,
        patch("wetlands.external_environment.runtime_state.remove_worker") as remove,
        patch.object(
            environment,
            "_start_reader_thread",
            side_effect=RuntimeError("reader failed"),
        ),
        patch.object(
            environment,
            "_cleanup_failed_worker_launch",
            return_value=False,
        ),
        pytest.raises(WorkerStartError) as caught,
    ):
        environment._launch_worker(0, {})

    record.assert_called_once()
    remove.assert_not_called()
    assert len(environment._workers) == 1
    assert environment._workers[0].process is process
    assert caught.value.worker_index == 0
    assert "durable worker ownership record retained" in str(caught.value)
