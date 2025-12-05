import logging
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from wetlands._internal.exceptions import ExecutionException
from wetlands.external_environment import ExternalEnvironment


@patch("subprocess.Popen")
def test_launch(mock_popen):
    mock_process = MagicMock()
    mock_process.pid = 12345

    mock_stdout = MagicMock()
    mock_stdout.__iter__.return_value = iter(["Listening port 5000\n"])  # For iteration
    mock_stdout.readline = MagicMock(side_effect=["Listening port 5000\n", ""])  # For readline()

    mock_process.stdout = mock_stdout
    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    with patch("wetlands.external_environment.Client") as mock_client:
        # Create a mock ProcessLogger that returns expected lines
        mock_process_logger = MagicMock()
        mock_process_logger.wait_for_line.side_effect = ["Listening port 5000", None]
        # Mock the update_log_context method so it doesn't fail
        mock_process_logger.update_log_context = MagicMock()

        # Mock the environment manager with a mock command executor
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
        assert env.connection == mock_client.return_value


@patch("multiprocessing.connection.Client")
def test_execute(mock_client):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]

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

    env._exit()
    env.connection.send.assert_called_once_with({"action": "exit"})
    env.connection.close.assert_called_once()
    mock_kill.assert_called_once_with(env.process)
