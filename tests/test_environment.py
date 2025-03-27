import pytest
from unittest.mock import Mock, patch
from multiprocessing.connection import Connection
from subprocess import Popen
from cema.exceptions import ExecutionException
from cema.environment import ExternalEnvironment, InternalEnvironment


def test_external_environment_initialization():
    process_mock = Mock(spec=Popen)
    env = ExternalEnvironment("test_env", 5000, process_mock)
    assert env.name == "test_env"
    assert env.port == 5000
    assert env.process == process_mock
    assert env.connection is None


def test_external_environment_initialize():
    with patch("cema.environment.Client", autospec=True) as client_mock:
        connection_mock = Mock(spec=Connection)
        client_mock.return_value = connection_mock

        env = ExternalEnvironment("test_env", 5000, Mock(spec=Popen))
        env.initialize()

        assert env.connection == connection_mock
        client_mock.assert_called_once_with(("localhost", 5000))


def test_external_environment_execute_with_no_connection():
    env = ExternalEnvironment("test_env", 5000, Mock(spec=Popen))
    with patch("cema.logger.warning") as logger_mock:
        result = env.execute("test_module.py", "test_function", [1, 2, 3])
        assert result is None
        logger_mock.assert_called()


def test_external_environment_execute_with_mock_connection():
    env = ExternalEnvironment("test_env", 5000, Mock(spec=Popen))
    connection_mock = Mock(spec=Connection)
    connection_mock.closed = False
    env.connection = connection_mock

    connection_mock.recv.side_effect = [{"action": "execution finished", "result": 42}]

    result = env.execute("test_module.py", "test_function", [1, 2, 3])
    assert result == 42
    connection_mock.send.assert_called_with(
        {
            "action": "execute",
            "modulePath": "test_module.py",
            "function": "test_function",
            "args": [1, 2, 3],
        }
    )


def test_external_environment_execute_with_error():
    env = ExternalEnvironment("test_env", 5000, Mock(spec=Popen))
    connection_mock = Mock(spec=Connection)
    connection_mock.closed = False
    env.connection = connection_mock

    connection_mock.recv.side_effect = [{"action": "error", "message": "Test error"}]

    with pytest.raises(ExecutionException):
        env.execute("test_module.py", "test_function", [1, 2, 3])


def test_external_environment_exit():
    env = ExternalEnvironment("test_env", 5000, Mock(spec=Popen))
    connection_mock = Mock(spec=Connection)
    connection_mock.closed = False
    env.connection = connection_mock

    with patch("cema.command_executor.CommandExecutor.killProcess") as kill_mock:
        env._exit()
        connection_mock.send.assert_called_with({"action": "exit"})
        connection_mock.close.assert_called()
        kill_mock.assert_called_with(env.process)


def test_internal_environment_execute():
    env = InternalEnvironment("test_env")
    with patch("cema.environment.import_module") as import_mock:
        module_mock = Mock()
        module_mock.test_function.return_value = 99
        import_mock.return_value = module_mock

        result = env.execute("/fake/path/test_module.py", "test_function", [5, 10])

        assert result == 99
        import_mock.assert_called_once_with("test_module")
        module_mock.test_function.assert_called_once_with(5, 10)


def test_internal_environment_execute_function_not_found():
    env = InternalEnvironment("test_env")
    with patch("cema.environment.import_module") as import_mock:
        module_mock = Mock(spec={})
        import_mock.return_value = module_mock

        with pytest.raises(
            Exception, match="Module test_module has no function test_function."
        ):
            env.execute("/fake/path/test_module.py", "test_function", [5, 10])
