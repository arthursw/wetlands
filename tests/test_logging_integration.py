"""Integration tests for logging system with context tracking."""

import logging
import subprocess
import time

from wetlands.logger import (
    logger,
    enable_file_logging,
    enable_console_logging,
    LOG_SOURCE_ENVIRONMENT,
    LOG_SOURCE_EXECUTION,
    LOG_SOURCE_GLOBAL,
)
from wetlands._internal.process_logger import ProcessLogger


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

    def test_enable_console_logging_splits_info_and_error_streams(self, capsys):
        """Test Wetlands console logging sends routine logs to stdout and errors to stderr."""
        base_logger = logger.logger
        root_logger = logging.getLogger()
        previous_handlers = list(base_logger.handlers)
        previous_level = base_logger.level
        previous_propagate = base_logger.propagate
        root_previous_handlers = list(root_logger.handlers)
        root_previous_level = root_logger.level
        for handler in previous_handlers:
            base_logger.removeHandler(handler)

        try:
            root_logger.addHandler(logging.StreamHandler())
            root_logger.setLevel(logging.INFO)
            enable_console_logging(level=logging.DEBUG)

            logger.info("routine progress")
            logger.error("action failed")
            for handler in base_logger.handlers:
                handler.flush()

            captured = capsys.readouterr()
            assert "routine progress" in captured.out
            assert "routine progress" not in captured.err
            assert "action failed" in captured.err
            assert "action failed" not in captured.out
        finally:
            for handler in list(base_logger.handlers):
                base_logger.removeHandler(handler)
                handler.close()
            for handler in list(root_logger.handlers):
                if handler not in root_previous_handlers:
                    root_logger.removeHandler(handler)
                    handler.close()
            for handler in previous_handlers:
                base_logger.addHandler(handler)
            root_logger.setLevel(root_previous_level)
            base_logger.setLevel(previous_level)
            base_logger.propagate = previous_propagate


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

    def test_process_logger_progress_uses_info_stdout_path(self, capsys):
        """Test subprocess progress emitted by ProcessLogger follows the INFO/stdout route."""
        base_logger = logger.logger
        previous_handlers = list(base_logger.handlers)
        previous_level = base_logger.level
        previous_propagate = base_logger.propagate
        for handler in previous_handlers:
            base_logger.removeHandler(handler)

        process = subprocess.Popen(
            ["python", "-c", "print('subprocess progress')"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            base_logger.propagate = False
            enable_console_logging(level=logging.INFO)
            process_logger = ProcessLogger(process, {"log_source": LOG_SOURCE_EXECUTION}, logger)
            process_logger.start_reading()
            process.wait(timeout=5)
            if process_logger._reader_thread:
                process_logger._reader_thread.join(timeout=2)

            captured = capsys.readouterr()
            assert "subprocess progress" in captured.out
            assert "subprocess progress" not in captured.err
        finally:
            process.wait(timeout=5)
            for handler in list(base_logger.handlers):
                base_logger.removeHandler(handler)
                handler.close()
            for handler in previous_handlers:
                base_logger.addHandler(handler)
            base_logger.setLevel(previous_level)
            base_logger.propagate = previous_propagate

    def test_process_logger_stderr_uses_info_stdout_path(self, capsys):
        """Test subprocess stderr emitted by ProcessLogger follows the INFO/stdout route."""
        base_logger = logger.logger
        previous_handlers = list(base_logger.handlers)
        previous_level = base_logger.level
        previous_propagate = base_logger.propagate
        for handler in previous_handlers:
            base_logger.removeHandler(handler)

        process = subprocess.Popen(
            [
                "python",
                "-c",
                "import sys; print('subprocess progress'); print('subprocess failure', file=sys.stderr)",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            enable_console_logging(level=logging.INFO)
            process_logger = ProcessLogger(process, {"log_source": LOG_SOURCE_EXECUTION}, logger)
            process_logger.start_reading()
            process.wait(timeout=5)
            if process_logger._reader_thread:
                process_logger._reader_thread.join(timeout=2)
            if process_logger._stderr_reader_thread:
                process_logger._stderr_reader_thread.join(timeout=2)

            captured = capsys.readouterr()
            assert "subprocess progress" in captured.out
            assert "subprocess progress" not in captured.err
            assert "subprocess failure" in captured.out
            assert "subprocess failure" not in captured.err
        finally:
            process.wait(timeout=5)
            for handler in list(base_logger.handlers):
                base_logger.removeHandler(handler)
                handler.close()
            for handler in previous_handlers:
                base_logger.addHandler(handler)
            base_logger.setLevel(previous_level)
            base_logger.propagate = previous_propagate


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
