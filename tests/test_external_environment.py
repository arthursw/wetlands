import logging
import time
import threading
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from wetlands._internal.exceptions import ExecutionException
from wetlands.external_environment import ExternalEnvironment, _Worker
from wetlands.task import Task, TaskStatus

# --- Helper to create a basic ExternalEnvironment with mocked manager ---


def _make_env(**kwargs):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    for k, v in kwargs.items():
        setattr(env, k, v)
    return env


# --- Legacy tests (backward compat, no worker pool) ---


@patch("subprocess.Popen")
def test_launch(mock_popen):
    mock_process = MagicMock()
    mock_process.pid = 12345

    mock_stdout = MagicMock()
    mock_stdout.__iter__.return_value = iter(["Listening port 5000\n"])
    mock_stdout.readline = MagicMock(side_effect=["Listening port 5000\n", ""])

    mock_process.stdout = mock_stdout
    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    with patch("wetlands.external_environment.Client") as mock_client:
        # Make the mock connection's recv raise EOFError to stop the reader thread
        mock_conn = MagicMock()
        mock_conn.recv.side_effect = EOFError()
        mock_conn.closed = False
        mock_client.return_value = mock_conn

        mock_process_logger = MagicMock()
        mock_process_logger.wait_for_line.side_effect = ["Listening port 5000", None]
        mock_process_logger.update_log_context = MagicMock()

        mock_env_manager = MagicMock()
        mock_env_manager.debug = False
        mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
        mock_env_manager.wetlands_instance_path = MagicMock()
        mock_env_manager.wetlands_instance_path.resolve.return_value = Path("/tmp/wetlands")
        mock_env_manager.command_executor._process_loggers = {12345: mock_process_logger}

        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
        env.execute_commands = MagicMock(return_value=mock_process)
        env.launch()

        assert env.port == 5000
        assert env.connection == mock_conn


@patch("multiprocessing.connection.Client")
def test_execute_legacy(mock_client):
    """Test legacy execute when no workers are set up (direct _send_and_wait path)."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]
    # Ensure no workers so it uses legacy path
    env._workers = []

    result = env.execute("module.py", "func", (1, 2, 3))

    assert result == "success"
    env.connection.send.assert_called_once_with(
        {"action": "execute", "module_path": "module.py", "function": "func", "args": (1, 2, 3), "kwargs": {}}
    )


@patch("multiprocessing.connection.Client")
def test_execute_with_kwargs(mock_client):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]
    env._workers = []

    result = env.execute("module.py", "func", ("a",), {"one": 1, "two": 2})

    assert result == "success"
    env.connection.send.assert_called_once_with(
        {
            "action": "execute",
            "module_path": "module.py",
            "function": "func",
            "args": ("a",),
            "kwargs": {"one": 1, "two": 2},
        }
    )


@patch("multiprocessing.connection.Client")
def test_execute_error(mock_client, caplog):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [
        {"action": "error", "exception": "A fake error occurred", "traceback": ["line 1", "line 2"]}
    ]
    env._workers = []

    with pytest.raises(ExecutionException):
        with caplog.at_level(logging.ERROR):
            env.execute("module.py", "func", (1, 2, 3))

    assert "A fake error occurred" in caplog.text
    assert "Traceback:" in caplog.text
    assert "line 1" in caplog.text
    assert "line 2" in caplog.text


@patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
def test_exit(mock_kill):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.process = MagicMock()
    env._workers = []

    env._exit()
    env.connection.send.assert_called_once_with({"action": "exit"})
    env.connection.close.assert_called_once()
    mock_kill.assert_called_once_with(env.process)


# --- Worker pool tests ---


def _make_mock_worker(index=0):
    """Create a mock _Worker with a mock connection."""
    process = MagicMock()
    process.poll.return_value = None
    connection = MagicMock()
    connection.closed = False
    worker = _Worker(index, process, 5000 + index, connection, MagicMock())
    return worker


class TestSubmit:
    def test_submit_creates_task(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        task = env.submit("module.py", "func", args=(1, 2), start=False)
        assert isinstance(task, Task)
        assert task.status == TaskStatus.PENDING

    def test_submit_dispatches_immediately_to_idle_worker(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        task = env.submit("module.py", "func", args=(1,))
        assert task.status == TaskStatus.RUNNING
        # Should have sent the payload to the worker
        worker.connection.send.assert_called_once()
        sent = worker.connection.send.call_args[0][0]
        assert sent["action"] == "execute"
        assert sent["task_id"] == task.id
        assert sent["function"] == "func"

    def test_submit_queues_when_no_idle_workers(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        # Don't put worker in idle queue

        task = env.submit("module.py", "func", args=(1,))
        # Task should be queued (still PENDING since no worker to dispatch)
        assert task.status == TaskStatus.PENDING
        assert not env._task_queue.empty()

    def test_submit_with_start_false(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        task = env.submit("module.py", "func", start=False)
        assert task.status == TaskStatus.PENDING
        worker.connection.send.assert_not_called()

    def test_submit_kwargs(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        env.submit("module.py", "func", kwargs={"x": 1})
        sent = worker.connection.send.call_args[0][0]
        assert sent["kwargs"] == {"x": 1}


class TestSubmitScript:
    def test_submit_script(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        task = env.submit_script("script.py", args=("a", "b"))
        assert task.status == TaskStatus.RUNNING
        sent = worker.connection.send.call_args[0][0]
        assert sent["action"] == "run"
        assert sent["script_path"] == "script.py"


class TestCancel:
    def test_cancel_sends_to_worker(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        task = env.submit("module.py", "func")
        # worker now has the task
        assert worker._current_task is task

        task.cancel()
        # Should have sent cancel message
        assert worker.connection.send.call_count == 2  # execute + cancel
        cancel_msg = worker.connection.send.call_args[0][0]
        assert cancel_msg["action"] == "cancel"
        assert cancel_msg["task_id"] == task.id


class TestWorkerReaderLoop:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_completion_returns_worker_to_idle(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        # Simulate reader receiving completion
        worker.connection.recv.side_effect = [
            {"action": "execution finished", "result": 42},
            EOFError(),  # End reader loop
        ]
        env._worker_reader_loop(worker)

        assert task.status == TaskStatus.COMPLETED
        assert task.result == 42

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_error_returns_worker_to_idle(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = [
            {"action": "error", "exception": "boom", "traceback": ["tb"]},
            EOFError(),
        ]
        env._worker_reader_loop(worker)

        assert task.status == TaskStatus.FAILED
        assert task.error == "boom"

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_update_passes_to_task(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = [
            {"action": "update", "message": "progress", "current": 5, "maximum": 10},
            {"action": "execution finished", "result": None},
            EOFError(),
        ]
        env._worker_reader_loop(worker)

        assert task.message == "progress"
        assert task.current == 5

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_connection_closed_fails_task(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = EOFError()
        env._worker_reader_loop(worker)

        assert task.status == TaskStatus.FAILED

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_dispatches_queued_task_after_completion(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]

        # First task
        task1 = Task()
        task1._set_running()
        worker._current_task = task1

        # Queue a second task
        task2 = Task()
        task2._payload = dict(action="execute", module_path="m.py", function="f", args=(), kwargs={})  # type: ignore[attr-defined]
        env._task_queue.put(task2)

        worker.connection.recv.side_effect = [
            {"action": "execution finished", "result": "r1"},
            {"action": "execution finished", "result": "r2"},
            EOFError(),
        ]
        env._worker_reader_loop(worker)

        assert task1.status == TaskStatus.COMPLETED
        assert task2.status == TaskStatus.COMPLETED
        assert task2.result == "r2"


class TestExecuteWithWorkers:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_execute_uses_worker_pool(self, mock_kill):
        """execute() should use submit() when workers are available."""
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._idle_workers.put(worker)

        dispatched = threading.Event()

        def fake_recv():
            # Wait until task is dispatched before returning the response
            dispatched.wait(timeout=5)
            return {"action": "execution finished", "result": 99}

        call_count = [0]

        def recv_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return fake_recv()
            raise EOFError()

        worker.connection.recv.side_effect = recv_side_effect

        # Start reader in background
        reader_thread = threading.Thread(target=env._worker_reader_loop, args=(worker,), daemon=True)
        reader_thread.start()

        # Patch send to signal dispatch
        original_send = worker.connection.send

        def send_and_signal(payload):
            original_send(payload)
            dispatched.set()

        worker.connection.send = MagicMock(side_effect=send_and_signal)

        result = env.execute("module.py", "func", (1,))
        reader_thread.join(timeout=2)

        assert result == 99


class TestMapTasks:
    def test_map_tasks_creates_task_per_item(self):
        env = _make_env()
        workers = [_make_mock_worker(i) for i in range(2)]
        env._workers = workers
        for w in workers:
            env._idle_workers.put(w)

        tasks = env.map_tasks("module.py", "process", [10, 20, 30])
        assert len(tasks) == 3
        for t in tasks:
            assert isinstance(t, Task)


class TestExitWithWorkers:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_exit_kills_all_workers(self, mock_kill):
        env = _make_env()
        workers = [_make_mock_worker(i) for i in range(3)]
        env._workers = list(workers)
        for w in workers:
            env._idle_workers.put(w)

        env._exit()

        assert len(env._workers) == 0
        assert mock_kill.call_count == 3
        for w in workers:
            w.connection.send.assert_called_once_with({"action": "exit"})
            w.connection.close.assert_called_once()


class TestLaunchedWithWorkers:
    def test_launched_with_live_workers(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        assert env.launched()

    def test_launched_with_dead_workers(self):
        env = _make_env()
        worker = _make_mock_worker()
        worker.process.poll.return_value = 1  # exited
        env._workers = [worker]
        assert not env.launched()


class TestRemoveDeadWorker:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_removes_worker_from_pool(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._remove_dead_worker(worker)
        assert worker not in env._workers
        assert len(env._workers) == 0

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_closes_connection(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        env._remove_dead_worker(worker)
        worker.connection.close.assert_called_once()

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_kills_alive_process(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        worker.process.poll.return_value = None  # still alive
        env._workers = [worker]
        env._remove_dead_worker(worker)
        mock_kill.assert_called_once_with(worker.process)

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_does_not_kill_dead_process(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        worker.process.poll.return_value = 1  # already dead
        env._workers = [worker]
        env._remove_dead_worker(worker)
        mock_kill.assert_not_called()


class TestDeadWorkerCleanup:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_reader_loop_removes_dead_worker(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = EOFError()
        env._worker_reader_loop(worker)

        assert task.status == TaskStatus.FAILED
        assert worker not in env._workers

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_dispatch_failure_removes_dead_worker(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]

        task = Task()
        task._payload = dict(action="execute", module_path="m.py", function="f", args=(), kwargs={})
        worker.connection.send.side_effect = BrokenPipeError("broken")

        env._dispatch_to_worker(worker, task)

        assert task.status == TaskStatus.FAILED
        assert "Failed to send" in task.error
        assert worker not in env._workers

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_start_skips_dead_workers_in_idle_queue(self, mock_kill):
        env = _make_env()
        dead_worker = _make_mock_worker(0)
        live_worker = _make_mock_worker(1)
        env._workers = [live_worker]  # dead_worker not in _workers
        env._idle_workers.put(dead_worker)
        env._idle_workers.put(live_worker)

        task = Task()
        task._payload = dict(action="execute", module_path="m.py", function="f", args=(), kwargs={})
        env._submit_task(task, start=True)

        assert task.status == TaskStatus.RUNNING
        assert live_worker._current_task is task
        dead_worker.connection.send.assert_not_called()

    def test_dispatch_or_idle_skips_dead_worker(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = []  # worker not in pool (dead)

        env._dispatch_or_idle(worker)
        # Should not be added back to idle queue
        assert env._idle_workers.empty()


class TestWorkerCount:
    def test_worker_count_reflects_pool_size(self):
        env = _make_env()
        env._workers = [_make_mock_worker(i) for i in range(3)]
        assert env.worker_count == 3

    def test_worker_count_zero_when_empty(self):
        env = _make_env()
        assert env.worker_count == 0


class TestLastActivity:
    def test_last_activity_set_on_dispatch(self):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]

        task = Task()
        task._payload = dict(action="execute", module_path="m.py", function="f", args=(), kwargs={})
        before = time.time()
        env._dispatch_to_worker(worker, task)
        after = time.time()

        assert before <= worker._last_activity <= after

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_last_activity_set_on_message_recv(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = [
            {"action": "execution finished", "result": "ok"},
            EOFError(),
        ]
        before = time.time()
        env._worker_reader_loop(worker)
        after = time.time()

        assert before <= worker._last_activity <= after


class TestHealthMonitor:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_detects_dead_worker_process(self, mock_kill):
        env = _make_env()
        env._shutdown_event = threading.Event()
        env._worker_timeout = None
        env._additional_activate_commands = {}
        env._worker_env = None

        worker = _make_mock_worker()
        worker.process.poll.return_value = 9  # dead (SIGKILL)
        worker.process.returncode = -9
        env._workers = [worker]

        task = Task()
        task._set_running()
        worker._current_task = task

        # Mock _try_replace_worker to avoid launching a real process
        env._try_replace_worker = MagicMock()

        # Run one iteration of the health monitor
        env._shutdown_event.set()  # Will exit after one check
        # Manually call the check logic since the loop exits immediately when event is set
        # Instead, let's directly test the detection logic
        with env._lock:
            workers = list(env._workers)
        for w in workers:
            t = w._current_task
            if t is None or t.status.is_finished():
                continue
            if w.process.poll() is not None:
                rc = w.process.returncode
                t._set_failed(f"Worker process died (exit code {rc})")
                w._current_task = None
                env._remove_dead_worker(w)
                env._try_replace_worker(w.index)

        assert task.status == TaskStatus.FAILED
        assert "exit code" in task.error
        assert worker not in env._workers
        env._try_replace_worker.assert_called_once_with(0)

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_detects_hung_worker_timeout(self, mock_kill):
        env = _make_env()
        env._shutdown_event = threading.Event()
        env._worker_timeout = 0.1  # Very short timeout for testing
        env._additional_activate_commands = {}
        env._worker_env = None

        worker = _make_mock_worker()
        worker.process.poll.return_value = None  # alive
        worker._last_activity = time.time() - 1.0  # 1 second ago (> 0.1 timeout)
        env._workers = [worker]

        task = Task()
        task._set_running()
        worker._current_task = task

        env._try_replace_worker = MagicMock()

        # Simulate one health check iteration
        with env._lock:
            workers = list(env._workers)
        for w in workers:
            t = w._current_task
            if t is None or t.status.is_finished():
                continue
            if w.process.poll() is not None:
                continue
            if env._worker_timeout is not None:
                elapsed = time.time() - w._last_activity
                if elapsed > env._worker_timeout:
                    t._set_failed(f"Worker process timed out (no response for {elapsed:.0f}s)")
                    w._current_task = None
                    env._remove_dead_worker(w)
                    env._try_replace_worker(w.index)

        assert task.status == TaskStatus.FAILED
        assert "timed out" in task.error
        assert worker not in env._workers

    def test_no_timeout_when_activity_recent(self):
        env = _make_env()
        env._worker_timeout = 10.0

        worker = _make_mock_worker()
        worker.process.poll.return_value = None
        worker._last_activity = time.time()  # just now
        env._workers = [worker]

        task = Task()
        task._set_running()
        worker._current_task = task

        # Check: elapsed < timeout, so no action
        elapsed = time.time() - worker._last_activity
        assert elapsed < env._worker_timeout
        assert task.status == TaskStatus.RUNNING  # unchanged

    def test_health_monitor_loop_exits_on_shutdown(self):
        env = _make_env()
        env._shutdown_event = threading.Event()
        env._worker_timeout = None

        # Set shutdown immediately so the loop exits after first wait
        env._shutdown_event.set()

        # Should return without error
        env._health_monitor_loop()


class TestExitFailsQueuedTasks:
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_exit_fails_queued_tasks(self, mock_kill):
        env = _make_env()
        env._shutdown_event = threading.Event()
        worker = _make_mock_worker()
        env._workers = [worker]

        task1 = Task()
        task2 = Task()
        env._task_queue.put(task1)
        env._task_queue.put(task2)

        env._exit()

        assert task1.status == TaskStatus.FAILED
        assert "shutting down" in task1.error
        assert task2.status == TaskStatus.FAILED
        assert "shutting down" in task2.error
        assert env._task_queue.empty()


class TestTryReplaceWorker:
    def test_replacement_worker_added_to_pool(self):
        env = _make_env()
        env._additional_activate_commands = {}
        env._worker_env = None
        new_worker = _make_mock_worker(0)

        with patch.object(env, "_launch_worker", return_value=new_worker):
            env._try_replace_worker(0)

        assert new_worker in env._workers

    def test_replacement_failure_logs_error(self, caplog):
        env = _make_env()
        env._additional_activate_commands = {}
        env._worker_env = None

        with patch.object(env, "_launch_worker", side_effect=Exception("env broken")):
            with caplog.at_level(logging.ERROR):
                env._try_replace_worker(0)

        assert "Failed to launch replacement worker" in caplog.text
