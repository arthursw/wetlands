from unittest.mock import MagicMock, patch
import threading
import pytest

from wetlands import module_executor


class TestSendMessage:
    def test_send_message_with_lock(self):
        """Test that send_message sends through connection with lock"""
        mock_lock = MagicMock()
        mock_lock.__enter__ = MagicMock(return_value=None)
        mock_lock.__exit__ = MagicMock(return_value=None)
        mock_connection = MagicMock()
        message = {"action": "test"}

        module_executor.send_message(mock_lock, mock_connection, message)

        mock_lock.__enter__.assert_called_once()
        mock_connection.send.assert_called_once_with(message)


class TestHandleExecutionError:
    def test_handle_execution_error_sends_error_message(self):
        """Test that handle_execution_error sends error message"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        exception = Exception("Test error")

        with patch("wetlands.module_executor.send_message") as mock_send:
            module_executor.handle_execution_error(mock_lock, mock_connection, exception)
            mock_send.assert_called_once()
            call_args = mock_send.call_args
            assert call_args[0][0] == mock_lock
            assert call_args[0][1] == mock_connection
            assert call_args[0][2]["action"] == "error"
            assert "Test error" in call_args[0][2]["exception"]


class TestExecuteFunction:
    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_success(self, mock_import):
        """Test executing a function from imported module"""
        mock_module = MagicMock()
        mock_module.test_func = MagicMock(return_value=42)
        mock_import.return_value = mock_module

        message = {
            "module_path": "/path/to/module.py",
            "function": "test_func",
            "args": [1, 2],
            "kwargs": {"key": "value"},
        }

        result = module_executor.execute_function(message)

        assert result == 42
        mock_module.test_func.assert_called_once_with(1, 2, key="value")

    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_missing_function(self, mock_import):
        """Test error when function doesn't exist in module"""
        mock_module = MagicMock(spec=[])  # No attributes
        mock_import.return_value = mock_module

        message = {
            "module_path": "/path/to/module.py",
            "function": "nonexistent_func",
        }

        with pytest.raises(Exception, match="has no function"):
            module_executor.execute_function(message)

    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_system_exit(self, mock_import):
        """Test error when function raises SystemExit"""
        mock_module = MagicMock()
        mock_module.test_func = MagicMock(side_effect=SystemExit(1))
        mock_import.return_value = mock_module

        message = {
            "module_path": "/path/to/module.py",
            "function": "test_func",
        }

        with pytest.raises(Exception, match="SystemExit"):
            module_executor.execute_function(message)


class TestRunScript:
    @patch("wetlands.module_executor.runpy.run_path")
    def test_run_script_success(self, mock_run_path):
        """Test running a script with runpy"""
        message = {"script_path": "/path/to/script.py", "args": ["arg1", "arg2"], "run_name": "__main__"}

        result = module_executor.run_script(message)

        assert result is None
        # Normalize path for cross-platform comparison
        from pathlib import Path

        actual_path = mock_run_path.call_args[0][0]
        assert Path(actual_path).as_posix() == "/path/to/script.py"
        assert mock_run_path.call_args[1] == {"run_name": "__main__"}

    @patch("wetlands.module_executor.runpy.run_path")
    def test_run_script_default_run_name(self, mock_run_path):
        """Test running script with default run_name"""
        message = {"script_path": "/path/to/script.py", "args": []}

        module_executor.run_script(message)

        # Normalize path for cross-platform comparison
        from pathlib import Path

        actual_path = mock_run_path.call_args[0][0]
        assert Path(actual_path).as_posix() == "/path/to/script.py"
        assert mock_run_path.call_args[1] == {"run_name": "__main__"}


class TestExecutionWorker:
    @patch("wetlands.module_executor.execute_function")
    @patch("wetlands.module_executor.send_message")
    def test_execution_worker_execute_action(self, mock_send, mock_execute):
        """Test worker handles execute action"""
        mock_execute.return_value = "result"
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "execute", "module_path": "/path/to/module.py"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        mock_execute.assert_called_once()
        mock_send.assert_called_once()

    @patch("wetlands.module_executor.run_script")
    @patch("wetlands.module_executor.send_message")
    def test_execution_worker_run_action(self, mock_send, mock_run):
        """Test worker handles run action"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "run", "script_path": "/path/to/script.py"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        mock_run.assert_called_once()
        mock_send.assert_called_once()

    @patch("wetlands.module_executor.handle_execution_error")
    def test_execution_worker_unknown_action(self, mock_error):
        """Test worker handles unknown action"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "unknown"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        mock_error.assert_called_once()


class TestGetMessage:
    def test_get_message_receives_from_connection(self):
        """Test get_message receives from connection"""
        mock_connection = MagicMock()
        mock_connection.recv.return_value = {"action": "test"}

        result = module_executor.get_message(mock_connection)

        assert result == {"action": "test"}
        mock_connection.recv.assert_called_once()


class TestLaunchListener:
    def test_launch_listener_exit_action(self):
        """Test listener exits on exit action"""
        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch("wetlands.module_executor.get_message", side_effect=[{"action": "exit"}]),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_connection = mock_listener.accept.return_value.__enter__.return_value

            module_executor.launch_listener()

            mock_connection.send.assert_called_with({"action": "exited"})

    def test_launch_listener_execute_action(self):
        """Test listener handles execute action"""
        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch(
                "wetlands.module_executor.get_message",
                side_effect=[{"action": "execute", "module_path": "/path/to/module.py"}, {"action": "exit"}],
            ),
            patch("wetlands.module_executor.execution_worker"),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_connection = mock_listener.accept.return_value.__enter__.return_value

            module_executor.launch_listener()

            # Should have sent exit response
            assert mock_connection.send.called
