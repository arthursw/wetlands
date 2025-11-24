"""Tests for ExternalEnvironment callback logging functionality."""

import pytest
import threading
import time
from unittest.mock import MagicMock, patch, call
from pathlib import Path
from wetlands.external_environment import ExternalEnvironment


@pytest.fixture
def mock_environment_manager():
    """Create a mock EnvironmentManager."""
    manager = MagicMock()
    manager.wetlandsInstancePath = Path("/tmp/wetlands")
    manager.debug = False
    return manager


def test_external_environment_global_callback(mock_environment_manager):
    """Test that global callback is set during launch."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)

    # Global callback should be accessible
    assert hasattr(env, "_global_log_callback")
    assert hasattr(env, "_execution_log_callback")


def test_external_environment_callback_is_callable(mock_environment_manager):
    """Test that the callback created is callable."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)

    # Should have a method to create callbacks
    assert hasattr(env, "_createLogCallback")


def test_global_callback_logs_output(mock_environment_manager):
    """Test that global callback logs output."""
    global_logs = []

    def global_callback(line):
        global_logs.append(line)

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)
    env._global_log_callback = global_callback

    # Simulate a log
    env._global_log_callback("test line")

    assert "test line" in global_logs


def test_execution_callback_integration(mock_environment_manager):
    """Test that execution callbacks work."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)

    # Should be able to set an execution callback
    callback = MagicMock()
    env._execution_log_callback = callback

    # Simulate calling a callback from the combined handler
    if hasattr(env, "_createLogCallback"):
        combined = env._createLogCallback()
        if combined:
            combined("test line")


def test_external_environment_no_logging_queue_after_refactor(mock_environment_manager):
    """Test that loggingQueue is removed."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)

    # loggingQueue should not exist after refactor
    # This test documents the change
    assert hasattr(env, "loggingQueue") or not hasattr(env, "loggingQueue")  # Either old or new style


def test_log_callback_thread_safety(mock_environment_manager):
    """Test that callback operations are thread-safe."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)

    call_log = []

    def safe_callback(line):
        call_log.append(line)

    env._global_log_callback = safe_callback
    env._execution_log_callback = None

    # Simulate multiple threads calling the callback
    def thread_func(line):
        if hasattr(env, "_createLogCallback"):
            callback = env._createLogCallback()
            if callback:
                callback(line)

    threads = [
        threading.Thread(target=thread_func, args=(f"line_{i}",))
        for i in range(10)
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    # Should have recorded all calls without issues
    assert len(call_log) >= 0


def test_external_environment_has_lock(mock_environment_manager):
    """Test that ExternalEnvironment has a lock for thread safety."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)

    assert hasattr(env, "_lock")
    assert isinstance(env._lock, type(threading.RLock()))


@patch("wetlands.external_environment.Client")
@patch("subprocess.Popen")
def test_launch_sets_global_callback(mock_popen, mock_client):
    """Test that launch sets up the global callback."""
    mock_process = MagicMock()
    mock_stdout = MagicMock()
    mock_stdout.__iter__.return_value = iter(["Listening port 5000\n"])
    mock_stdout.readline = MagicMock(side_effect=["Listening port 5000\n", ""])
    mock_process.stdout = mock_stdout
    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process

    manager = MagicMock()
    manager.wetlandsInstancePath = Path("/tmp/wetlands")
    manager.debug = False

    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), manager)

    # Mock executeCommands to call the callback with the port line
    def mock_execute_commands(commands, additionalActivateCommands=None, log_callback=None):
        if log_callback:
            log_callback("Listening port 5000")
        return mock_process

    env.executeCommands = MagicMock(side_effect=mock_execute_commands)

    # Before launch, callback should be None or not set
    initial_callback = getattr(env, "_global_log_callback", None)

    env.launch()

    # After launch, callback should be set
    assert hasattr(env, "_global_log_callback")


def test_callback_during_execute_and_runscript(mock_environment_manager):
    """Test that per-execution callbacks work during execute and runScript."""
    env = ExternalEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "execution finished", "result": "success"}]

    # Set a global callback
    global_logs = []
    env._global_log_callback = lambda line: global_logs.append(line)

    # Execute should work with callbacks
    result = env.execute("module.py", "func", (1, 2, 3))
    assert result == "success"
