import logging
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from wetlands._internal.exceptions import ExecutionException
from wetlands.external_environment import ExternalEnvironment


@patch("subprocess.Popen")
def test_launch(mock_popen):
    mock_process = MagicMock()

    mock_stdout = MagicMock()
    mock_stdout.__iter__.return_value = iter(["Listening port 5000\n"])  # For iteration
    mock_stdout.readline = MagicMock(side_effect=["Listening port 5000\n", ""])  # For readline()

    mock_process.stdout = mock_stdout

    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    with patch("wetlands.external_environment.Client") as mock_client:
        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())

        # Mock executeCommands to call the callback with the port line
        def mock_execute_commands(commands, additionalActivateCommands=None, log_callback=None):  # noqa: ARG001
            if log_callback:
                log_callback("Listening port 5000")
            return mock_process

        env.executeCommands = MagicMock(side_effect=mock_execute_commands)

        # Launch with a log callback
        log_lines = []
        env.launch(log_callback=lambda line: log_lines.append(line))

        assert env.port == 5000
        assert env.connection == mock_client.return_value
        # Verify the callback was called
        assert "Listening port 5000" in log_lines


@patch("multiprocessing.connection.Client")
def test_execute(mock_client):  # noqa: ARG001
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]

    result = env.execute("module.py", "func", (1, 2, 3))

    assert result == "success"
    env.connection.send.assert_called_once_with(
        {"action": "execute", "modulePath": "module.py", "function": "func", "args": (1, 2, 3), "kwargs": {}}
    )


@patch("multiprocessing.connection.Client")
def test_execute_with_kwargs(mock_client):  # noqa: ARG001
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]

    result = env.execute("module.py", "func", ("a",), {"one": 1, "two": 2})

    assert result == "success"
    env.connection.send.assert_called_once_with(
        {
            "action": "execute",
            "modulePath": "module.py",
            "function": "func",
            "args": ("a",),
            "kwargs": {"one": 1, "two": 2},
        }
    )


@patch("multiprocessing.connection.Client")
def test_execute_error(mock_client, caplog):  # noqa: ARG001
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


@patch("wetlands._internal.command_executor.CommandExecutor.killProcess")
def test_exit(mock_kill):
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.process = MagicMock()

    env._exit()
    env.connection.send.assert_called_once_with({"action": "exit"})
    env.connection.close.assert_called_once()
    mock_kill.assert_called_once_with(env.process)


@patch("subprocess.Popen")
def test_launch_with_global_callback(mock_popen):  # noqa: ARG001
    """Test that launch captures log output via the global callback."""
    mock_process = MagicMock()
    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    with patch("wetlands.external_environment.Client"):
        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())

        def mock_execute_commands(commands, additionalActivateCommands=None, log_callback=None):  # noqa: ARG001, ARG002, ARG003
            if log_callback:
                log_callback("Listening port 5000")
                log_callback("Some other output")
            return mock_process

        env.executeCommands = MagicMock(side_effect=mock_execute_commands)

        log_lines = []
        env.launch(log_callback=lambda line: log_lines.append(line))

        # Both the port line and other output should be captured
        assert "Listening port 5000" in log_lines
        assert "Some other output" in log_lines


@patch("subprocess.Popen")
def test_launch_without_callback(mock_popen):  # noqa: ARG001
    """Test that launch works without a log callback."""
    mock_process = MagicMock()
    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    with patch("wetlands.external_environment.Client"):
        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())

        def mock_execute_commands(commands, additionalActivateCommands=None, log_callback=None):  # noqa: ARG001, ARG002, ARG003
            if log_callback:
                log_callback("Listening port 5000")
            return mock_process

        env.executeCommands = MagicMock(side_effect=mock_execute_commands)

        # Should not raise any exception
        env.launch(log_callback=None)

        assert env.port == 5000


@patch("multiprocessing.connection.Client")
def test_execute_with_per_execution_callback(mock_client):  # noqa: ARG001
    """Test that per-execution callbacks are used during execute."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]

    # Set a global callback
    global_logs = []
    env._global_log_callback = lambda line: global_logs.append(("global", line))

    # Execute with a per-execution callback
    execution_logs = []
    result = env.execute(
        "module.py",
        "func",
        (1, 2, 3),
        log_callback=lambda line: execution_logs.append(("execution", line)),
    )

    assert result == "success"
    # After execution, the per-execution callback should be cleared
    assert env._execution_log_callback is None


@patch("multiprocessing.connection.Client")
def test_runscript_with_callback(mock_client):  # noqa: ARG001
    """Test that runScript accepts and uses callbacks."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": {"output": "data"}}]

    execution_logs = []

    result = env.runScript(
        "script.py",
        (1, 2),
        log_callback=lambda line: execution_logs.append(line),
    )

    assert result == {"output": "data"}
    env.connection.send.assert_called_once()
    # Verify the payload was sent correctly
    call_args = env.connection.send.call_args[0][0]
    assert call_args["action"] == "run"
    assert call_args["scriptPath"] == "script.py"
    assert call_args["args"] == (1, 2)


def test_create_log_callback_combines_callbacks():
    """Test that _createLogCallback combines global and execution callbacks."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())

    global_calls = []
    execution_calls = []

    env._global_log_callback = lambda line: global_calls.append(line)
    env._execution_log_callback = lambda line: execution_calls.append(line)

    combined = env._createLogCallback()
    combined("test line")

    assert "test line" in global_calls
    assert "test line" in execution_calls


def test_create_log_callback_handles_missing_callbacks():
    """Test that _createLogCallback handles None callbacks gracefully."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())

    env._global_log_callback = None
    env._execution_log_callback = None

    combined = env._createLogCallback()
    # Should not raise any exception
    combined("test line")


def test_create_log_callback_handles_callback_exceptions():
    """Test that _createLogCallback handles exceptions in callbacks gracefully."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), MagicMock())

    def failing_callback(line):  # noqa: ARG001
        raise ValueError("Callback error")

    env._global_log_callback = failing_callback
    env._execution_log_callback = None

    combined = env._createLogCallback()
    # Should not raise exception even though global callback raises
    combined("test line")
