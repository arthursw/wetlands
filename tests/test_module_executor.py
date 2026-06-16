from unittest.mock import MagicMock, patch
import logging
import threading
import pytest
from multiprocessing.context import AuthenticationError

from wetlands import module_executor
from wetlands.task import RemoteTaskSerializationError


class TestConfigureLogging:
    def test_configure_logging_writes_environments_log_under_instance_path(self, tmp_path, monkeypatch):
        cwd = tmp_path / "cwd"
        cwd.mkdir()
        wetlands_instance_path = tmp_path / "wetlands"
        monkeypatch.chdir(cwd)
        root_logger = logging.getLogger()
        previous_handlers = list(root_logger.handlers)
        previous_level = root_logger.level
        for handler in previous_handlers:
            root_logger.removeHandler(handler)

        log_path = module_executor.configure_logging(wetlands_instance_path)

        try:
            logging.getLogger("test_env").info("hello from worker")
            for handler in root_logger.handlers:
                handler.flush()

            assert log_path == wetlands_instance_path.resolve() / "environments.log"
            assert log_path.exists()
            assert "hello from worker" in log_path.read_text()
            assert not (cwd / "environments.log").exists()
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                handler.close()
            for handler in previous_handlers:
                root_logger.addHandler(handler)
            root_logger.setLevel(previous_level)

    def test_configure_logging_splits_console_streams_and_preserves_file_logging(
        self, tmp_path, monkeypatch, capsys
    ):
        cwd = tmp_path / "cwd"
        cwd.mkdir()
        wetlands_instance_path = tmp_path / "wetlands"
        monkeypatch.chdir(cwd)
        root_logger = logging.getLogger()
        previous_handlers = list(root_logger.handlers)
        previous_level = root_logger.level
        for handler in previous_handlers:
            root_logger.removeHandler(handler)

        log_path = module_executor.configure_logging(wetlands_instance_path)

        try:
            worker_logger = logging.getLogger("test_env")
            worker_logger.info("worker progress")
            worker_logger.error("worker failure")
            for handler in root_logger.handlers:
                handler.flush()

            captured = capsys.readouterr()
            assert "worker progress" in captured.out
            assert "worker progress" not in captured.err
            assert "worker failure" in captured.err
            assert "worker failure" not in captured.out

            log_content = log_path.read_text()
            assert "worker progress" in log_content
            assert "worker failure" in log_content
        finally:
            for handler in list(root_logger.handlers):
                root_logger.removeHandler(handler)
                handler.close()
            for handler in previous_handlers:
                root_logger.addHandler(handler)
            root_logger.setLevel(previous_level)


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
            assert call_args[0][2]["failure"]["category"] == "remote_exception"
            assert call_args[0][2]["failure"]["remote_exception"]["type_name"] == "Exception"
            assert "Test error" in call_args[0][2]["exception"]
            assert "task_id" not in call_args[0][2]

    def test_handle_execution_error_with_task_id(self):
        """Test that handle_execution_error includes task_id when provided"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        exception = Exception("Test error")

        with patch("wetlands.module_executor.send_message") as mock_send:
            module_executor.handle_execution_error(mock_lock, mock_connection, exception, task_id="task-123")
            call_args = mock_send.call_args
            assert call_args[0][2]["task_id"] == "task-123"
            assert call_args[0][2]["failure"]["task_id"] == "task-123"

    def test_handle_execution_error_without_task_id(self):
        """Test backward compat: no task_id field when not provided"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        exception = Exception("err")

        with patch("wetlands.module_executor.send_message") as mock_send:
            module_executor.handle_execution_error(mock_lock, mock_connection, exception)
            msg = mock_send.call_args[0][2]
            assert "task_id" not in msg

    def test_handle_execution_error_preserves_chained_exception(self):
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        try:
            try:
                raise RuntimeError("root")
            except RuntimeError as e:
                raise ValueError("outer") from e
        except ValueError as exception:
            with patch("wetlands.module_executor.send_message") as mock_send:
                module_executor.handle_execution_error(mock_lock, mock_connection, exception, task_id="task-chain")

        failure = mock_send.call_args[0][2]["failure"]
        assert failure["remote_exception"]["type_name"] == "ValueError"
        assert failure["remote_exception"]["cause"]["type_name"] == "RuntimeError"
        assert "The above exception was the direct cause" in failure["traceback"]

    def test_handle_execution_error_can_report_serialization_context(self):
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        exception = TypeError("cannot pickle result")

        with patch("wetlands.module_executor.send_message") as mock_send:
            module_executor.handle_execution_error(
                mock_lock,
                mock_connection,
                exception,
                task_id="task-result",
                category="serialization",
                serialization_context="result",
            )

        failure = mock_send.call_args[0][2]["failure"]
        assert failure["category"] == "serialization"
        assert failure["serialization_context"] == "result"

    def test_remote_task_handle_serialization_error_is_serialization_failure(self):
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        exception = RemoteTaskSerializationError("output", TypeError("cannot pickle output"))

        with patch("wetlands.module_executor.send_message") as mock_send:
            module_executor.handle_execution_error(mock_lock, mock_connection, exception, task_id="task-output")

        failure = mock_send.call_args[0][2]["failure"]
        assert failure["category"] == "serialization"
        assert failure["serialization_context"] == "output"


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

    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_injects_task_handle(self, mock_import):
        """Test RemoteTaskHandle injection when function has 'task' parameter"""

        def my_func(a, task=None):
            return task

        mock_module = MagicMock()
        mock_module.my_func = my_func
        mock_import.return_value = mock_module

        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()

        message = {
            "module_path": "/path/to/module.py",
            "function": "my_func",
            "args": [1],
            "kwargs": {},
            "task_id": "task-abc",
        }

        result = module_executor.execute_function(message, mock_lock, mock_connection)

        # Result should be the injected RemoteTaskHandle
        assert result is not None
        assert result._task_id == "task-abc"

    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_no_injection_without_task_param(self, mock_import):
        """Test no injection when function lacks 'task' parameter"""

        def my_func(a):
            return a

        mock_module = MagicMock()
        mock_module.my_func = my_func
        mock_import.return_value = mock_module

        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()

        message = {
            "module_path": "/path/to/module.py",
            "function": "my_func",
            "args": [42],
            "kwargs": {},
            "task_id": "task-abc",
        }

        result = module_executor.execute_function(message, mock_lock, mock_connection)
        assert result == 42

    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_no_injection_without_task_id(self, mock_import):
        """Test no injection when message has no task_id (backward compat)"""

        def my_func(a, task=None):
            return task

        mock_module = MagicMock()
        mock_module.my_func = my_func
        mock_import.return_value = mock_module

        message = {
            "module_path": "/path/to/module.py",
            "function": "my_func",
            "args": [1],
            "kwargs": {},
        }

        result = module_executor.execute_function(message)
        assert result is None  # task should not be injected

    @patch("wetlands.module_executor.importlib.import_module")
    def test_execute_function_cleans_up_active_tasks(self, mock_import):
        """Test that _active_tasks is cleaned up after execution"""

        def my_func(a, task=None):
            # During execution, the task should be in _active_tasks
            assert "task-cleanup" in module_executor._active_tasks
            return 1

        mock_module = MagicMock()
        mock_module.my_func = my_func
        mock_import.return_value = mock_module

        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()

        message = {
            "module_path": "/path/to/module.py",
            "function": "my_func",
            "args": [1],
            "kwargs": {},
            "task_id": "task-cleanup",
        }

        module_executor.execute_function(message, mock_lock, mock_connection)
        assert "task-cleanup" not in module_executor._active_tasks


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

        mock_execute.assert_called_once_with(message, mock_lock, mock_connection)
        mock_send.assert_called_once()
        sent_msg = mock_send.call_args[0][2]
        assert sent_msg["action"] == "execution finished"
        assert "task_id" not in sent_msg  # backward compat

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
        sent_msg = mock_send.call_args[0][2]
        assert "task_id" not in sent_msg  # backward compat

    @patch("wetlands.module_executor.handle_execution_error")
    def test_execution_worker_unknown_action(self, mock_error):
        """Test worker handles unknown action"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "unknown"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        mock_error.assert_called_once()

    @patch("wetlands.module_executor.execute_function")
    @patch("wetlands.module_executor.send_message")
    def test_execution_worker_execute_with_task_id(self, mock_send, mock_execute):
        """Test worker includes task_id in response when message has task_id"""
        mock_execute.return_value = "result"
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "execute", "module_path": "/path/to/module.py", "task_id": "task-456"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        sent_msg = mock_send.call_args[0][2]
        assert sent_msg["action"] == "execution finished"
        assert sent_msg["task_id"] == "task-456"
        assert sent_msg["result"] == "result"

    @patch("wetlands.module_executor.run_script")
    @patch("wetlands.module_executor.send_message")
    def test_execution_worker_run_with_task_id(self, mock_send, mock_run):
        """Test worker includes task_id in run response"""
        mock_run.return_value = None
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "run", "script_path": "/path/to/script.py", "task_id": "task-789"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        sent_msg = mock_send.call_args[0][2]
        assert sent_msg["task_id"] == "task-789"

    @patch("wetlands.module_executor.handle_execution_error")
    def test_execution_worker_error_with_task_id(self, mock_error):
        """Test worker passes task_id to error handler"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "unknown", "task_id": "task-err"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        mock_error.assert_called_once()
        _, kwargs = mock_error.call_args
        assert kwargs["task_id"] == "task-err"


class TestGetMessage:
    def test_get_message_receives_from_connection(self):
        """Test get_message receives from connection"""
        mock_connection = MagicMock()
        mock_connection.recv.return_value = {"action": "test"}

        result = module_executor.get_message(mock_connection)

        assert result == {"action": "test"}
        mock_connection.recv.assert_called_once()


class TestLaunchListener:
    def test_launch_listener_uses_authkey(self):
        """Test listener is authenticated with the provided authkey."""
        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch("wetlands.module_executor.get_message", side_effect=[{"action": "exit"}]),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_listener.address = ("localhost", 5000)
            mock_listener.accept.return_value.__enter__.return_value = MagicMock()

            module_executor.launch_listener(authkey=b"root-auth-key")

            MockListener.assert_called_once_with(("localhost", module_executor.port), authkey=b"root-auth-key")

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

    def test_persistent_launch_listener_returns_to_accept_on_eof(self):
        first_connection = MagicMock()
        second_connection = MagicMock()
        first_context = MagicMock()
        second_context = MagicMock()
        first_context.__enter__.return_value = first_connection
        second_context.__enter__.return_value = second_connection

        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch("wetlands.module_executor.get_message", side_effect=[EOFError(), {"action": "exit"}]),
            patch("wetlands.module_executor._detach_standard_streams"),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_listener.address = ("localhost", 5000)
            mock_listener.accept.side_effect = [first_context, second_context]

            module_executor.launch_listener(persistent=True)

            assert mock_listener.accept.call_count == 2
            second_connection.send.assert_called_with({"action": "exited"})

    def test_persistent_launch_listener_survives_auth_failure(self):
        connection = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = connection

        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch("wetlands.module_executor.get_message", side_effect=[{"action": "exit"}]),
            patch("wetlands.module_executor._detach_standard_streams"),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_listener.address = ("localhost", 5000)
            mock_listener.accept.side_effect = [AuthenticationError("bad key"), context]

            module_executor.launch_listener(authkey=b"root-auth-key", persistent=True)

            assert mock_listener.accept.call_count == 2
            connection.send.assert_called_with({"action": "exited"})

    def test_persistent_launch_listener_survives_abandoned_auth_connection(self):
        connection = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = connection

        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch("wetlands.module_executor.get_message", side_effect=[{"action": "exit"}]),
            patch("wetlands.module_executor._detach_standard_streams"),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_listener.address = ("localhost", 5000)
            mock_listener.accept.side_effect = [EOFError(), context]

            module_executor.launch_listener(authkey=b"root-auth-key", persistent=True)

            assert mock_listener.accept.call_count == 2
            connection.send.assert_called_with({"action": "exited"})

    def test_persistent_launch_listener_waits_for_active_task_before_reaccept(self):
        first_connection = MagicMock()
        second_connection = MagicMock()
        first_context = MagicMock()
        second_context = MagicMock()
        first_context.__enter__.return_value = first_connection
        second_context.__enter__.return_value = second_connection
        task_started = threading.Event()
        release_task = threading.Event()

        def fake_execution_worker(*_args):
            task_started.set()
            release_task.wait(timeout=2)

        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch(
                "wetlands.module_executor.get_message",
                side_effect=[{"action": "execute", "module_path": "/path/to/module.py"}, EOFError(), {"action": "exit"}],
            ),
            patch("wetlands.module_executor.execution_worker", side_effect=fake_execution_worker),
            patch("wetlands.module_executor._detach_standard_streams"),
        ):
            mock_listener = MockListener.return_value.__enter__.return_value
            mock_listener.address = ("localhost", 5000)
            mock_listener.accept.side_effect = [first_context, second_context]

            listener_thread = threading.Thread(
                target=module_executor.launch_listener,
                kwargs={"persistent": True},
            )
            listener_thread.start()

            assert task_started.wait(timeout=1)
            assert mock_listener.accept.call_count == 1
            release_task.set()
            listener_thread.join(timeout=2)

            assert not listener_thread.is_alive()
            assert mock_listener.accept.call_count == 2
            second_connection.send.assert_called_with({"action": "exited"})

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

    def test_launch_listener_cancel_action(self):
        """Test listener handles cancel action by setting cancel_requested on active task"""
        mock_handle = MagicMock()
        module_executor._active_tasks["task-cancel-1"] = mock_handle

        try:
            with (
                patch("wetlands.module_executor.Listener") as MockListener,
                patch(
                    "wetlands.module_executor.get_message",
                    side_effect=[
                        {"action": "cancel", "task_id": "task-cancel-1"},
                        {"action": "exit"},
                    ],
                ),
            ):
                MockListener.return_value.__enter__.return_value

                module_executor.launch_listener()

                mock_handle._set_cancel_requested.assert_called_once()
        finally:
            module_executor._active_tasks.pop("task-cancel-1", None)

    def test_launch_listener_cancel_unknown_task(self):
        """Test listener handles cancel for unknown task_id gracefully"""
        with (
            patch("wetlands.module_executor.Listener") as MockListener,
            patch(
                "wetlands.module_executor.get_message",
                side_effect=[
                    {"action": "cancel", "task_id": "nonexistent"},
                    {"action": "exit"},
                ],
            ),
        ):
            MockListener.return_value.__enter__.return_value

            # Should not raise
            module_executor.launch_listener()


class TestBackwardCompatibility:
    """Ensure messages without task_id work exactly as before."""

    @patch("wetlands.module_executor.execute_function")
    @patch("wetlands.module_executor.send_message")
    def test_no_task_id_in_execute_response(self, mock_send, mock_execute):
        """Response should not contain task_id when message has none"""
        mock_execute.return_value = 42
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "execute", "module_path": "/path/to/module.py"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        sent_msg = mock_send.call_args[0][2]
        assert "task_id" not in sent_msg
        assert sent_msg["action"] == "execution finished"
        assert sent_msg["result"] == 42

    @patch("wetlands.module_executor.run_script")
    @patch("wetlands.module_executor.send_message")
    def test_no_task_id_in_run_response(self, mock_send, mock_run):
        """Response should not contain task_id when message has none"""
        mock_run.return_value = None
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "run", "script_path": "/path/to/script.py"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        sent_msg = mock_send.call_args[0][2]
        assert "task_id" not in sent_msg

    @patch("wetlands.module_executor.handle_execution_error")
    def test_no_task_id_in_error(self, mock_error):
        """Error handler should receive task_id=None when message has none"""
        mock_lock = MagicMock(spec=threading.Lock)
        mock_connection = MagicMock()
        message = {"action": "unknown"}

        module_executor.execution_worker(mock_lock, mock_connection, message)

        _, kwargs = mock_error.call_args
        assert kwargs["task_id"] is None
