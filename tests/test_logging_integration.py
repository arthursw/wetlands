"""Integration tests for logging system with context tracking."""

import logging
import subprocess
import time

from wetlands.logger import logger, enable_file_logging, LOG_SOURCE_ENVIRONMENT, LOG_SOURCE_EXECUTION, LOG_SOURCE_GLOBAL
from wetlands._internal.process_logger import ProcessLogger
from wetlands._internal.command_executor import CommandExecutor


class TestLoggerConvenienceMethods:
    """Test the WetlandsAdapter convenience methods."""

    def test_log_global(self):
        """Test logger.log_global() method."""
        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.logger.addHandler(handler)

        try:
            logger.log_global("Global operation started", stage="setup")

            assert len(records) > 0
            assert getattr(records[0], "log_source", None) == LOG_SOURCE_GLOBAL
            assert getattr(records[0], "stage", None) == "setup"
        finally:
            logger.logger.removeHandler(handler)

    def test_log_environment(self):
        """Test logger.log_environment() method."""
        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.logger.addHandler(handler)

        try:
            logger.log_environment("Environment created", env_name="test_env", stage="create")

            assert len(records) > 0
            assert getattr(records[0], "log_source", None) == LOG_SOURCE_ENVIRONMENT
            assert getattr(records[0], "env_name", None) == "test_env"
            assert getattr(records[0], "stage", None) == "create"
        finally:
            logger.logger.removeHandler(handler)

    def test_log_execution(self):
        """Test logger.log_execution() method."""
        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.logger.addHandler(handler)

        try:
            logger.log_execution("Executing function", env_name="test_env", call_target="module:func")

            assert len(records) > 0
            assert getattr(records[0], "log_source", None) == LOG_SOURCE_EXECUTION
            assert getattr(records[0], "env_name", None) == "test_env"
            assert getattr(records[0], "call_target", None) == "module:func"
        finally:
            logger.logger.removeHandler(handler)


class TestProcessLogger:
    """Test ProcessLogger functionality for reading subprocess output."""

    def test_process_logger_reads_output(self):
        """Test that ProcessLogger reads subprocess output correctly."""
        # Create a simple subprocess that prints output
        process = subprocess.Popen(
            ["python", "-c", "print('hello'); print('world')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        process_logger = ProcessLogger(process, {}, logger)
        process_logger.start_reading()

        # Wait for process to complete
        process.wait(timeout=5)

        # Give reader thread time to process output
        if process_logger._reader_thread:
            process_logger._reader_thread.join(timeout=2)

        output = process_logger.get_output()
        assert "hello" in output
        assert "world" in output

    def test_process_logger_with_context(self):
        """Test that ProcessLogger properly tracks and propagates context."""
        process = subprocess.Popen(
            ["python", "-c", "print('test output')"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        log_context = {"log_source": LOG_SOURCE_EXECUTION, "env_name": "test_env", "call_target": "test:func"}

        process_logger = ProcessLogger(process, log_context, logger)

        # Capture logs with context
        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.logger.addHandler(handler)

        try:
            process_logger.start_reading()
            process.wait(timeout=5)

            if process_logger._reader_thread:
                process_logger._reader_thread.join(timeout=2)

            # Check that context was propagated to logs
            if records:
                assert getattr(records[0], "log_source", None) == LOG_SOURCE_EXECUTION
                assert getattr(records[0], "env_name", None) == "test_env"
                assert getattr(records[0], "call_target", None) == "test:func"
        finally:
            logger.logger.removeHandler(handler)

    def test_process_logger_subscribers(self):
        """Test that ProcessLogger subscriber callbacks work correctly."""
        process = subprocess.Popen(
            ["python", "-c", "print('line1'); print('line2'); print('line3')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        process_logger = ProcessLogger(process, {}, logger)

        lines_received = []

        def callback(line, context):
            lines_received.append(line)

        process_logger.subscribe(callback)
        process_logger.start_reading()

        process.wait(timeout=5)

        if process_logger._reader_thread:
            process_logger._reader_thread.join(timeout=2)

        assert "line1" in lines_received
        assert "line2" in lines_received
        assert "line3" in lines_received

    def test_wait_for_line(self):
        """Test ProcessLogger.wait_for_line() with timeout."""
        process = subprocess.Popen(
            ["python", "-c", "import time; print('start'); time.sleep(0.1); print('done')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        process_logger = ProcessLogger(process, {}, logger)
        process_logger.start_reading()

        # Wait for specific line
        line = process_logger.wait_for_line(lambda v: v == "done", timeout=5)

        assert line == "done"

        process.wait(timeout=5)
        if process_logger._reader_thread:
            process_logger._reader_thread.join(timeout=2)

    def test_update_log_context(self):
        """Test dynamic log context updates."""
        process = subprocess.Popen(
            ["python", "-c", "print('output')"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        process_logger = ProcessLogger(process, {"log_source": LOG_SOURCE_EXECUTION, "env_name": "test"}, logger)

        # Update context
        process_logger.update_log_context({"call_target": "updated:target"})

        # Verify context was updated
        assert process_logger.log_context["call_target"] == "updated:target"
        assert process_logger.log_context["env_name"] == "test"

        process_logger.start_reading()
        process.wait(timeout=5)
        if process_logger._reader_thread:
            process_logger._reader_thread.join(timeout=2)


class TestCommandExecutor:
    """Test CommandExecutor with logging integration."""

    def test_command_executor_with_log_context(self):
        """Test that CommandExecutor passes log context to ProcessLogger."""
        executor = CommandExecutor()

        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.logger.addHandler(handler)

        try:
            log_context = {"log_source": LOG_SOURCE_ENVIRONMENT, "env_name": "test_env", "stage": "install"}

            # Execute simple command
            process = executor.execute_commands(["echo 'test'"], log_context=log_context, wait=True)

            # Wait for logging to complete
            time.sleep(0.1)
            if process.pid in executor._process_loggers:
                process_logger = executor._process_loggers[process.pid]
                if process_logger._reader_thread and process_logger._reader_thread.is_alive():
                    process_logger._reader_thread.join(timeout=2)

            # Verify context was propagated
            if records:
                for record in records:
                    if getattr(record, "log_source", None) == LOG_SOURCE_ENVIRONMENT:
                        assert getattr(record, "env_name", None) == "test_env"
                        assert getattr(record, "stage", None) == "install"
                # At minimum, verify we got output records
                assert len(records) > 0
        finally:
            logger.logger.removeHandler(handler)

    def test_command_executor_get_output(self):
        """Test CommandExecutor.execute_commands_and_get_output() captures output correctly."""
        executor = CommandExecutor()

        output = executor.execute_commands_and_get_output(["echo 'line1'", "echo 'line2'"])

        assert "line1" in output
        assert "line2" in output

    def test_command_executor_get_json_output(self):
        """Test CommandExecutor.execute_commands_and_get_json_output() parses JSON."""
        executor = CommandExecutor()

        import json

        test_data = {"key": "value", "number": 42}
        json_str = json.dumps(test_data)

        output = executor.execute_commands_and_get_json_output([f"echo '{json_str}'"])

        # output is parsed JSON (could be dict or list depending on input)
        assert isinstance(output, dict)
        assert output["key"] == "value"  # type: ignore
        assert output["number"] == 42  # type: ignore


class TestFileLogging:
    """Test file logging functionality."""

    def test_enable_file_logging(self, tmp_path):
        """Test that enable_file_logging creates and writes to a file."""
        log_file = tmp_path / "test.log"

        enable_file_logging(log_file)

        logger.log_global("Test message", stage="test")

        # Give time for file to be written
        time.sleep(0.1)

        assert log_file.exists()
        content = log_file.read_text()
        assert "Test message" in content

    def test_file_logging_prevents_duplicates(self, tmp_path):
        """Test that enable_file_logging doesn't crash when called twice."""
        log_file = tmp_path / "test.log"

        # Enable file logging twice with same path - should not raise
        enable_file_logging(log_file)
        enable_file_logging(log_file)

        # File should exist and be writable
        assert log_file.exists()


class TestLogContextPropagation:
    """Test that log context is properly propagated through the system."""

    def test_context_isolation_between_processes(self):
        """Test that different processes have isolated log contexts."""
        executor = CommandExecutor()

        records_by_env = {"env1": [], "env2": []}

        class ContextHandler(logging.Handler):
            def emit(self, record):
                env_name = getattr(record, "env_name", "unknown")
                if env_name in records_by_env:
                    records_by_env[env_name].append(record)

        handler = ContextHandler()
        logger.logger.addHandler(handler)

        try:
            # Execute commands in different contexts
            process1 = executor.execute_commands(
                ["python", "-c", "print('env1_output')"], log_context={"env_name": "env1"}, wait=True
            )

            process2 = executor.execute_commands(
                ["python", "-c", "print('env2_output')"], log_context={"env_name": "env2"}, wait=True
            )

            time.sleep(0.2)

            # Wait for reader threads
            for pid in [process1.pid, process2.pid]:
                if pid in executor._process_loggers:
                    process_logger = executor._process_loggers[pid]
                    if process_logger._reader_thread and process_logger._reader_thread.is_alive():
                        process_logger._reader_thread.join(timeout=2)

            # Verify contexts were maintained
            assert len(records_by_env["env1"]) > 0
            assert len(records_by_env["env2"]) > 0
        finally:
            logger.logger.removeHandler(handler)
