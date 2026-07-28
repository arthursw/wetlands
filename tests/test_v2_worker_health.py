from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from wetlands._internal.diagnostics import TaskFailureCategory
from wetlands._internal.process_termination import ProcessTerminationError
from wetlands.external_environment import ExternalEnvironment, _Worker
from wetlands.lifecycle import EnvironmentGenerationChangedError, WorkerStartError
from wetlands.managed_environment import WorkerPool
from wetlands.task import ExecutionState, ExecutionTask


def _environment(tmp_path: Path) -> ExternalEnvironment:
    manager = MagicMock()
    manager.wetlands_instance_path = tmp_path / "wetlands"
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
    assert task.error.category == TaskFailureCategory.WORKER_DIED
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
    assert task.error.category == TaskFailureCategory.TIMEOUT
    assert worker not in environment._workers
    environment._try_replace_worker.assert_called_once_with(worker.index)


def test_replacement_worker_is_added_and_made_available(tmp_path):
    environment = _environment(tmp_path)
    replacement = _worker()

    with patch.object(environment, "_launch_worker", return_value=replacement):
        environment._try_replace_worker(0)

    assert environment._workers == [replacement]
    assert environment._idle_workers.get_nowait() is replacement


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

    def remove_worker(_worker):
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
            return_value={"port": 5000},
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
        environment._launch_worker(0, None)

    record.assert_called_once()
    remove.assert_not_called()
    assert len(environment._workers) == 1
    assert environment._workers[0].process is process
    assert caught.value.worker_index == 0
    assert "durable worker ownership record retained" in str(caught.value)
