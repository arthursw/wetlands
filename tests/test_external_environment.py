import pytest
from unittest.mock import MagicMock, patch
from cema.exceptions import ExecutionException
from cema.external_environment import ExternalEnvironment

@patch("subprocess.Popen")
def test_launch(mock_popen):
    mock_process = MagicMock()

    mock_stdout = MagicMock()
    mock_stdout.__iter__.return_value = iter(["Listening port 5000\n"])  # For iteration
    mock_stdout.readline = MagicMock(side_effect=["Listening port 5000\n", ""])  # For readline()
    
    mock_process.stdout = mock_stdout

    mock_process.poll.return_value = None
    mock_popen.return_value = mock_process
    
    with patch("cema.external_environment.Client") as mock_client:
        env = ExternalEnvironment("test_env", MagicMock())
        env.executeCommands = MagicMock(return_value=mock_process)
        env.launch()
        
        assert env.port == 5000
        assert env.connection == mock_client.return_value

@patch("multiprocessing.connection.Client")
def test_execute(mock_client):
    env = ExternalEnvironment("test_env", MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [
        {"action": "execution finished", "result": "success"}
    ]
    
    result = env.execute("module.py", "func", (1, 2, 3))
    
    assert result == "success"
    env.connection.send.assert_called_once_with(
        {"action": "execute", "modulePath": "module.py", "function": "func", "args": (1, 2, 3)}
    )

@patch("multiprocessing.connection.Client")
def test_execute_error(mock_client):
    env = ExternalEnvironment("test_env", MagicMock())
    env.connection = MagicMock()
    env.connection.closed = False
    env.connection.recv.side_effect = [{"action": "error", "message": "Some error"}]
    
    with pytest.raises(ExecutionException):
        env.execute("module.py", "func", (1, 2, 3))

@patch("cema.command_executor.CommandExecutor.killProcess")
def test_exit(mock_kill):
    env = ExternalEnvironment("test_env", MagicMock())
    env.connection = MagicMock()
    env.process = MagicMock()
    
    env._exit()
    env.connection.send.assert_called_once_with({"action": "exit"})
    env.connection.close.assert_called_once()
    mock_kill.assert_called_once_with(env.process)
