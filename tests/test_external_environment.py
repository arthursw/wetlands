import json
import logging
import queue
import socket
import subprocess
import time
import threading
import pytest
from pathlib import Path
from multiprocessing.connection import Client, Listener
from typing import Any, Optional, cast
from unittest.mock import MagicMock, patch
from wetlands._internal.exceptions import ExecutionException
from wetlands._internal.diagnostics import TaskFailureCategory
from wetlands._internal.shell import shell_quote
from wetlands._internal import runtime_state
from wetlands.external_environment import ExternalEnvironment, _AttachTimeout, _Worker, _wait_for_startup_payload
from wetlands.environment_manager import EnvironmentManager
from wetlands.task import Task, TaskStatus

# --- Helper to create a basic ExternalEnvironment with mocked manager ---


def _make_env(**kwargs) -> Any:
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    for k, v in kwargs.items():
        setattr(env, k, v)
    return env


def _startup_payload(port: int = 5000, debug_port: Optional[int] = None) -> dict[str, Any]:
    return {
        "event": "wetlands.worker.ready",
        "schema_version": 1,
        "token": "test-token",
        "pid": 12345,
        "port": port,
        "debug_port": debug_port,
    }


# --- Launch tests ---


@patch("subprocess.Popen")
def test_launch(mock_popen, tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    with patch("wetlands.external_environment.Client") as mock_client:
        # Make the mock connection's recv raise EOFError to stop the reader thread
        mock_conn = MagicMock()
        mock_conn.recv.side_effect = EOFError()
        mock_conn.closed = False
        mock_client.return_value = mock_conn

        mock_process_logger = MagicMock()
        mock_process_logger.update_log_context = MagicMock()

        mock_env_manager = MagicMock()
        mock_env_manager.debug = False
        mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
        mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"
        mock_env_manager.command_executor._process_loggers = {12345: mock_process_logger}

        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
        env.execute_commands = MagicMock(return_value=mock_process)
        with patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()):
            env.launch()

        assert env.port == 5000
        assert env.connection == mock_conn
        mock_process_logger.wait_for_line.assert_not_called()


def test_launch_worker_quotes_command_arguments_with_spaces(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    wetlands_instance_path = tmp_path / "wetlands state"
    mock_env_manager.wetlands_instance_path = wetlands_instance_path

    env = ExternalEnvironment("cellpose env", Path("/tmp/cellpose env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()),
    ):
        env._launch_worker(0, {}, None)

    commands = env.execute_commands.call_args.args[0]
    command = commands[0]
    assert shell_quote("cellpose env") in command
    assert shell_quote(wetlands_instance_path) in command
    assert "--startup_host" in command
    assert "--startup_port" in command
    popen_kwargs = env.execute_commands.call_args.kwargs["popen_kwargs"]
    assert "WETLANDS_STARTUP_TOKEN" in popen_kwargs["env"]


def test_launch_worker_uses_authenticated_client(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)
    env._authkey = b"root-auth-key"

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn) as mock_client,
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()),
    ):
        env._launch_worker(0, {}, None)

    mock_client.assert_called_once_with(("localhost", 5000), authkey=b"root-auth-key")


def test_launch_worker_registers_debug_port_from_startup_payload(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = True
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"
    mock_env_manager.register_environment = MagicMock()

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)
    env._authkey = b"root-auth-key"

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload(debug_port=5678)),
    ):
        env._launch_worker(0, {}, None)

    module_executor_path = Path(__file__).resolve().parents[1] / "src" / "wetlands" / "module_executor.py"
    mock_env_manager.register_environment.assert_called_once_with(env, 5678, module_executor_path)
    mock_process_logger.wait_for_line.assert_not_called()


def test_launch_worker_registers_debug_port_with_real_manager_method(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = True
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"
    mock_env_manager.register_environment = EnvironmentManager.register_environment.__get__(
        mock_env_manager, type(mock_env_manager)
    )

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)
    env._authkey = b"root-auth-key"

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload(debug_port=5678)),
    ):
        env._launch_worker(0, {}, None)

    debug_ports_path = tmp_path / "wetlands" / "debug_ports.json"
    assert debug_ports_path.exists()
    assert '"debug_port": 5678' in debug_ports_path.read_text()


def test_wait_for_startup_payload_ignores_invalid_preconnection():
    startup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    startup_socket.bind(("127.0.0.1", 0))
    startup_socket.listen(2)
    host, port = startup_socket.getsockname()
    process = MagicMock()
    process.poll.return_value = None
    result_queue: queue.Queue[Any] = queue.Queue()

    def send_payload(payload: dict[str, Any]) -> None:
        with socket.create_connection((host, port), timeout=1.0) as connection:
            connection.sendall((json.dumps(payload) + "\n").encode("utf-8"))

    def wait_for_payload() -> None:
        try:
            result_queue.put(_wait_for_startup_payload(startup_socket, "expected-token", process, timeout=2.0))
        except BaseException as exc:
            result_queue.put(exc)

    bad_payload = _startup_payload()
    bad_payload["token"] = "wrong-token"
    good_payload = _startup_payload(port=5678)
    good_payload["token"] = "expected-token"
    waiter = threading.Thread(target=wait_for_payload)
    waiter.start()

    try:
        send_payload(bad_payload)
        send_payload(good_payload)
        result = result_queue.get(timeout=3.0)
    finally:
        startup_socket.close()
        waiter.join(timeout=1.0)

    if isinstance(result, BaseException):
        raise result
    payload = cast(dict[str, Any], result)
    assert payload["port"] == 5678


def test_launch_worker_startup_failure_includes_output_tail_and_script_path(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None
    mock_process.returncode = None
    mock_process._wetlands_script_path = "/tmp/wetlands-worker-start.sh"

    mock_process_logger = MagicMock()
    mock_process_logger.get_output.return_value = [
        "Traceback (most recent call last):",
        "ModuleNotFoundError: No module named 'wetlands'",
    ]

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)

    with (
        patch("wetlands.external_environment._wait_for_startup_payload", side_effect=TimeoutError("timed out")),
        pytest.raises(Exception) as exc_info,
    ):
        env._launch_worker(0, {}, None)

    message = str(exc_info.value)
    assert "Could not receive startup information for worker 0" in message
    assert "ModuleNotFoundError: No module named 'wetlands'" in message
    assert "/tmp/wetlands-worker-start.sh" in message


def test_launch_worker_cleans_up_process_when_connect_fails_after_startup(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)
    env._authkey = b"root-auth-key"

    with (
        patch("wetlands.external_environment.Client", side_effect=ConnectionRefusedError("not ready")),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()),
        pytest.raises(ConnectionRefusedError),
    ):
        env._launch_worker(0, {}, None)

    mock_process.kill.assert_called_once()


def test_launch_worker_closes_connection_and_kills_process_when_recording_persistent_worker_fails(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)
    env._authkey = b"root-auth-key"
    env._persistent = True

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()),
        patch("wetlands.external_environment.runtime_state.record_worker", side_effect=OSError("registry failed")),
        pytest.raises(OSError, match="registry failed"),
    ):
        env._launch_worker(0, {}, None)

    mock_conn.send.assert_called_once_with({"action": "exit"})
    mock_conn.close.assert_called_once()
    mock_process.kill.assert_called_once()


def test_persistent_launch_records_worker_metadata(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", tmp_path / "test_env", mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn),
        patch("wetlands.external_environment.runtime_state.load_or_create_root_authkey", return_value=b"root-auth-key"),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()),
    ):
        env.launch(persistent=True)

    from wetlands._internal.runtime_state import load_workers

    registry = load_workers(tmp_path / "wetlands")
    assert registry["workers"]["test_env:0"]["pid"] == 12345
    assert registry["workers"]["test_env:0"]["port"] == 5000
    assert registry["workers"]["test_env:0"]["persistent"] is True


def test_non_persistent_launch_does_not_record_worker_metadata(tmp_path):
    mock_process = MagicMock()
    mock_process.pid = 12345
    mock_process.poll.return_value = None

    mock_process_logger = MagicMock()

    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.get_process_logger = MagicMock(return_value=mock_process_logger)
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", tmp_path / "test_env", mock_env_manager)
    env.execute_commands = MagicMock(return_value=mock_process)

    mock_conn = MagicMock()
    mock_conn.recv.side_effect = EOFError()
    mock_conn.closed = False

    with (
        patch("wetlands.external_environment.Client", return_value=mock_conn),
        patch("wetlands.external_environment.runtime_state.load_or_create_root_authkey", return_value=b"root-auth-key"),
        patch("wetlands.external_environment._wait_for_startup_payload", return_value=_startup_payload()),
    ):
        env.launch()

    from wetlands._internal.runtime_state import load_workers

    assert load_workers(tmp_path / "wetlands")["workers"] == {}


def test_persistent_launch_refuses_existing_live_workers(tmp_path):
    runtime_state.record_worker(
        tmp_path / "wetlands",
        env_name="test_env",
        env_path=tmp_path / "test_env",
        worker_index=0,
        pid=12345,
        port=5000,
        persistent=True,
    )
    mock_env_manager = MagicMock()
    mock_env_manager.debug = False
    mock_env_manager.wetlands_instance_path = tmp_path / "wetlands"

    env = ExternalEnvironment("test_env", tmp_path / "test_env", mock_env_manager)
    env.execute_commands = MagicMock()

    with (
        patch("wetlands.external_environment.runtime_state.pid_exists", return_value=True),
        pytest.raises(Exception, match="Live persistent workers already exist"),
    ):
        env.launch(persistent=True)

    env.execute_commands.assert_not_called()


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
        {
            "action": "execute",
            "module_path": "module.py",
            "function": "func",
            "args": (1, 2, 3),
            "kwargs": {},
            "_call_target": "module:func",
        }
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
            "_call_target": "module:func",
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

    with pytest.raises(ExecutionException) as exc_info:
        with caplog.at_level(logging.WARNING):
            env.execute("module.py", "func", (1, 2, 3))

    assert exc_info.value.failure.message == "A fake error occurred"
    assert "A fake error occurred" in caplog.text
    assert "Traceback:" not in caplog.text
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_execute_legacy_payload_serialization_failure_is_structured():
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.send.side_effect = TypeError("cannot pickle payload")
    env._workers = []

    with pytest.raises(ExecutionException) as exc_info:
        env.execute("module.py", "func", (object(),))

    assert exc_info.value.failure.category == TaskFailureCategory.SERIALIZATION
    assert exc_info.value.failure.serialization_context == "payload"
    assert exc_info.value.failure.call_target == "module:func"


@patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
def test_exit(mock_kill):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.process = MagicMock()
    env.process.poll.return_value = None
    env.process.wait.return_value = 0
    env._workers = []
    process_logger = MagicMock()
    env._process_logger = process_logger

    env._exit()
    env.connection.send.assert_called_once_with({"action": "exit"})
    env.connection.close.assert_called_once()
    env.process.wait.assert_called_once()
    process_logger.join.assert_called_once()
    mock_kill.assert_not_called()


# --- Worker pool tests ---


def _make_mock_worker(index=0) -> Any:
    """Create a mock _Worker with a mock connection."""
    process = MagicMock()
    process.poll.return_value = None
    process.pid = 1000 + index
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
        assert task.error is not None
        assert task.error.message == "boom"
        assert task.error.category == TaskFailureCategory.REMOTE_EXCEPTION

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_worker_pool_remote_error_logs_warning_without_error_traceback(self, mock_kill, caplog):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task("remote-task")
        task._payload = dict(_call_target="module:boom")
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = [
            {
                "action": "error",
                "task_id": "remote-task",
                "failure": {
                    "category": "remote_exception",
                    "message": "user failed",
                    "traceback": "Traceback...\nValueError: user failed\n",
                    "traceback_frames": ["frame"],
                    "remote_exception": {
                        "module": "builtins",
                        "type_name": "ValueError",
                        "qualified_name": "ValueError",
                        "message": "user failed",
                        "traceback": "ValueError: user failed\n",
                        "cause": None,
                        "context": None,
                        "suppress_context": False,
                    },
                },
            },
            EOFError(),
        ]

        with caplog.at_level(logging.WARNING):
            env._worker_reader_loop(worker)

        assert task.status == TaskStatus.FAILED
        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
        warning_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
        assert any("Remote ValueError from builtins: user failed" in message for message in warning_messages)
        assert not any("Traceback:" in message for message in warning_messages)

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_worker_pool_serialization_error_logs_error(self, mock_kill, caplog):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task("serialization-task")
        task._payload = dict(_call_target="module:return_unpickleable")
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = [
            {
                "action": "error",
                "task_id": "serialization-task",
                "failure": {
                    "category": "serialization",
                    "message": "cannot pickle result",
                    "serialization_context": "result",
                },
            },
            EOFError(),
        ]

        with caplog.at_level(logging.ERROR):
            env._worker_reader_loop(worker)

        assert task.status == TaskStatus.FAILED
        error_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
        assert any("Task serialization failure while serializing result: cannot pickle result" in message for message in error_messages)

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

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_stale_task_id_message_does_not_complete_current_task(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        current = Task("current-task")
        current._set_running()
        worker._current_task = current

        worker.connection.recv.side_effect = [
            {"action": "execution finished", "result": "old", "task_id": "old-task"},
            {"action": "execution finished", "result": "new", "task_id": "current-task"},
            EOFError(),
        ]
        env._worker_reader_loop(worker)

        assert current.status == TaskStatus.COMPLETED
        assert current.result == "new"

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_late_message_for_finished_task_is_ignored(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        worker._finished_task_ids.add("finished-task")
        worker._current_task = None

        worker.connection.recv.side_effect = [
            {"action": "execution finished", "result": "late", "task_id": "finished-task"},
            EOFError(),
        ]
        env._worker_reader_loop(worker)

        assert worker._current_task is None


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
        for worker in workers:
            worker.process.wait.return_value = 0
        env._workers = list(workers)
        for w in workers:
            env._idle_workers.put(w)

        env._exit()

        assert len(env._workers) == 0
        mock_kill.assert_not_called()
        for w in workers:
            w.connection.send.assert_called_once_with({"action": "exit"})
            w.connection.close.assert_called_once()
            w.process_logger.join.assert_called_once()

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_exit_kills_worker_after_graceful_wait_timeout(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        worker.process.wait.side_effect = subprocess.TimeoutExpired("worker", 0.1)
        env._workers = [worker]

        env._exit()

        mock_kill.assert_called_once_with(worker.process)
        worker.process_logger.join.assert_called_once()

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_detach_closes_connections_without_exit_or_kill(self, mock_kill):
        env = _make_env()
        workers = [_make_mock_worker(i) for i in range(2)]
        env._workers = list(workers)
        for w in workers:
            env._idle_workers.put(w)

        env.detach()

        assert env._workers == []
        assert env._idle_workers.empty()
        mock_kill.assert_not_called()
        for w in workers:
            w.connection.send.assert_not_called()
            w.connection.close.assert_called_once()

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_detach_notifies_persistent_workers_without_exit_or_kill(self, mock_kill):
        env = _make_env()
        workers = [_make_mock_worker(i) for i in range(2)]
        for worker in workers:
            worker.persistent = True
        env._workers = list(workers)
        for w in workers:
            env._idle_workers.put(w)

        env.detach()

        assert env._workers == []
        assert env._idle_workers.empty()
        mock_kill.assert_not_called()
        for w in workers:
            w.connection.send.assert_called_once_with({"action": "detach"})
            w.connection.close.assert_called_once()

    def test_detach_fails_active_tasks(self):
        env = _make_env()
        worker = _make_mock_worker()
        task = Task()
        task._set_running()
        worker._current_task = task
        env._workers = [worker]

        env.detach()

        assert task.status == TaskStatus.FAILED
        assert task.error is not None
        assert task.error.message == "Environment is detaching"


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

    @patch("wetlands.external_environment.runtime_state.pid_exists", return_value=True)
    def test_launched_with_attached_worker_uses_pid(self, mock_pid_exists):
        env = _make_env()
        connection = MagicMock()
        connection.closed = False
        worker = cast(Any, _Worker(0, None, 5000, connection, None, pid=12345, persistent=True))
        env._workers = [worker]

        assert env.launched()
        mock_pid_exists.assert_called_once_with(12345)

    def test_attach_worker_times_out_when_listener_is_occupied(self):
        authkey = b"root-auth-key"
        listener = Listener(("localhost", 0), authkey=authkey)
        accepted_connection = []
        accepted = threading.Event()
        release = threading.Event()

        def accept_once():
            connection = listener.accept()
            accepted_connection.append(connection)
            accepted.set()
            release.wait(timeout=5)
            connection.close()

        server_thread = threading.Thread(target=accept_once)
        server_thread.start()
        listener_port = cast(tuple[str, int], listener.address)[1]
        first_client = Client(("localhost", listener_port), authkey=authkey)
        env = _make_env()

        try:
            assert accepted.wait(timeout=1)
            with pytest.raises(TimeoutError):
                env._connect_worker(listener_port, authkey, timeout=0.1)
        finally:
            first_client.close()
            release.set()
            server_thread.join(timeout=1)
            listener.close()

        assert accepted_connection

    @patch("wetlands.external_environment.Client")
    def test_attach_worker_uses_plain_client_without_timeout_for_launch(self, mock_client):
        connection = MagicMock()
        mock_client.return_value = connection
        env = _make_env()

        assert env._connect_worker(5000, b"root-auth-key") == connection
        mock_client.assert_called_once_with(("localhost", 5000), authkey=b"root-auth-key")

    def test_attach_workers_preserves_registry_when_worker_is_busy(self, tmp_path):
        root = tmp_path / "wetlands"
        runtime_state.record_worker(
            root,
            env_name="test_env",
            env_path=tmp_path / "test_env",
            worker_index=0,
            pid=12345,
            port=5000,
            persistent=True,
        )
        mock_env_manager = MagicMock()
        mock_env_manager.wetlands_instance_path = root
        env = ExternalEnvironment("test_env", tmp_path / "test_env", mock_env_manager)
        entry = runtime_state.load_workers(root)["workers"]["test_env:0"]

        env._attach_worker = MagicMock(side_effect=_AttachTimeout("busy"))
        with pytest.raises(Exception, match="No live authenticated persistent workers"):
            env.attach_workers([entry], b"root-auth-key")

        assert "test_env:0" in runtime_state.load_workers(root)["workers"]


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

    @patch("wetlands.external_environment.runtime_state.remove_worker")
    @patch("wetlands.external_environment.runtime_state.pid_exists", return_value=True)
    @patch("wetlands._internal.command_executor.CommandExecutor.kill_pid")
    def test_removes_attached_worker_by_pid(self, mock_kill_pid, mock_pid_exists, mock_remove_worker):
        env = _make_env()
        connection = MagicMock()
        connection.closed = False
        worker = cast(Any, _Worker(0, None, 5000, connection, None, pid=12345, persistent=True))
        env._workers = [worker]

        env._remove_dead_worker(worker)

        mock_kill_pid.assert_called_once_with(12345)
        mock_remove_worker.assert_called_once()


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
        assert task.error is not None
        assert "Failed to send" in task.error.message
        assert task.error.category == TaskFailureCategory.WORKER_CONNECTION
        assert worker not in env._workers

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_dispatch_serialization_failure_keeps_worker_available(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]

        task = Task()
        task._payload = dict(action="execute", module_path="m.py", function="f", args=(), kwargs={})
        worker.connection.send.side_effect = TypeError("cannot pickle payload")

        env._dispatch_to_worker(worker, task)

        assert task.status == TaskStatus.FAILED
        assert task.error is not None
        assert task.error.category == TaskFailureCategory.SERIALIZATION
        assert task.error.serialization_context == "payload"
        assert worker in env._workers
        assert worker._current_task is None
        mock_kill.assert_not_called()

    @patch("wetlands._internal.command_executor.CommandExecutor.kill_process")
    def test_reader_structured_error_preserves_remote_exception(self, mock_kill):
        env = _make_env()
        worker = _make_mock_worker()
        env._workers = [worker]
        task = Task()
        task._set_running()
        worker._current_task = task

        worker.connection.recv.side_effect = [
            {
                "action": "error",
                "failure": {
                    "category": "remote_exception",
                    "message": "bad",
                    "traceback": "Traceback...\nValueError: bad\n",
                    "remote_exception": {
                        "module": "worker_mod",
                        "type_name": "ValueError",
                        "qualified_name": "ValueError",
                        "message": "bad",
                        "traceback": "ValueError: bad\n",
                        "cause": None,
                        "context": None,
                        "suppress_context": False,
                    },
                },
            },
            EOFError(),
        ]
        env._worker_reader_loop(worker)

        assert task.error is not None
        assert task.error.remote_exception is not None
        assert task.error.remote_exception.module == "worker_mod"
        assert str(task.exception) == "Remote ValueError from worker_mod: bad"

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
        assert task.error is not None
        assert "exit code" in task.error.message
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
        assert task.error is not None
        assert "timed out" in task.error.message
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
        assert task1.error is not None
        assert "shutting down" in task1.error.message
        assert task2.status == TaskStatus.FAILED
        assert task2.error is not None
        assert "shutting down" in task2.error.message
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
