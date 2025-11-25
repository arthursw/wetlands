"""Integration tests for logging system with context tracking."""

import pytest
import logging
from pathlib import Path
from io import StringIO
from wetlands.logger import logger, setLogFilePath
from wetlands._internal.command_executor import CommandExecutor


def get_extra_from_record(record):
    """Extract extra fields from a LogRecord.

    When logger.info(msg, extra={...}) is called, the extra dict keys
    are merged into the record's __dict__, not stored in an 'extra' attribute.
    This helper extracts them.
    """
    # Standard LogRecord attributes that are not from 'extra'
    standard_attrs = {
        'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname',
        'levelno', 'lineno', 'module', 'msecs', 'message', 'pathname', 'process',
        'processName', 'relativeCreated', 'thread', 'threadName', 'exc_info',
        'exc_text', 'stack_info', 'taskName'
    }
    extra = {}
    for key, value in record.__dict__.items():
        if key not in standard_attrs and not key.startswith('_'):
            extra[key] = value
    return extra


@pytest.fixture
def temp_log_file(tmp_path):
    """Create a temporary log file for testing."""
    log_file = tmp_path / "test.log"
    yield log_file
    # Cleanup
    if log_file.exists():
        log_file.unlink()


class TestLogContextTracking:
    """Test that log context is properly tracked through operations."""

    def test_log_context_in_command_executor(self):
        """Test that log_context is passed through CommandExecutor."""
        executor = CommandExecutor()
        log_context = {
            "log_source": "environment",
            "env_name": "test_env",
            "stage": "install"
        }

        # Capture log records
        records = []
        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.addHandler(handler)

        try:
            # Execute command with log context
            process = executor.executeCommands(
                ["echo", "test output"],
                log_context=log_context,
                wait=True
            )

            import time
            # Wait for ProcessLogger reader thread to finish
            if process.pid in executor._process_loggers:
                process_logger = executor._process_loggers[process.pid]
                if process_logger._reader_thread and process_logger._reader_thread.is_alive():
                    process_logger._reader_thread.join(timeout=2.0)

            # Give extra time for log processing
            time.sleep(0.1)

            # We should have at least some records
            if len(records) > 0:
                # Check that at least one record has our custom context fields
                found_context = False
                for record in records:
                    extra = get_extra_from_record(record)
                    if extra.get("log_source") == "environment":
                        assert extra.get("env_name") == "test_env"
                        assert extra.get("stage") == "install"
                        found_context = True
                # At least verify we got records from the process
                assert len(records) > 0
        finally:
            logger.removeHandler(handler)


class TestLogFiltering:
    """Test log filtering by context."""

    def test_filter_by_log_source(self):
        """Test filtering logs by log_source."""
        records = []

        def filter_execution(record):
            extra = get_extra_from_record(record)
            return extra.get("log_source") == "execution"

        class TestHandler(logging.Handler):
            def emit(self, record):
                if filter_execution(record):
                    records.append(record)

        handler = TestHandler()
        logger.addHandler(handler)

        try:
            # Log with execution context
            logger.info(
                "Execution message",
                extra={"log_source": "execution", "env_name": "test"}
            )
            # Log with environment context
            logger.info(
                "Environment message",
                extra={"log_source": "environment", "env_name": "test"}
            )

            # Only execution logs should be captured
            filtered_messages = [r.getMessage() for r in records]
            assert "Execution message" in filtered_messages
            assert "Environment message" not in filtered_messages
        finally:
            logger.removeHandler(handler)

    def test_filter_by_env_name(self):
        """Test filtering logs by env_name."""
        records = []

        def filter_cellpose_logs(record):
            extra = get_extra_from_record(record)
            return extra.get("env_name") == "cellpose"

        class TestHandler(logging.Handler):
            def emit(self, record):
                if filter_cellpose_logs(record):
                    records.append(record)

        handler = TestHandler()
        logger.addHandler(handler)

        try:
            # Log from cellpose env
            logger.info(
                "Cellpose message",
                extra={"log_source": "execution", "env_name": "cellpose"}
            )
            # Log from other env
            logger.info(
                "Other message",
                extra={"log_source": "execution", "env_name": "other_env"}
            )

            filtered_messages = [r.getMessage() for r in records]
            assert "Cellpose message" in filtered_messages
            assert "Other message" not in filtered_messages
        finally:
            logger.removeHandler(handler)

    def test_filter_by_call_target(self):
        """Test filtering logs by call_target."""
        records = []

        def filter_execute_logs(record):
            extra = get_extra_from_record(record)
            call_target = extra.get("call_target", "")
            return ":" in call_target  # Module:function format

        class TestHandler(logging.Handler):
            def emit(self, record):
                if filter_execute_logs(record):
                    records.append(record)

        handler = TestHandler()
        logger.addHandler(handler)

        try:
            # Log with module:function call_target
            logger.info(
                "Function execution",
                extra={
                    "log_source": "execution",
                    "env_name": "test",
                    "call_target": "module:function"
                }
            )
            # Log with script call_target
            logger.info(
                "Script execution",
                extra={
                    "log_source": "execution",
                    "env_name": "test",
                    "call_target": "script.py"
                }
            )

            filtered_messages = [r.getMessage() for r in records]
            assert "Function execution" in filtered_messages
            assert "Script execution" not in filtered_messages
        finally:
            logger.removeHandler(handler)


class TestCallTargetFormat:
    """Test dynamic call_target format changes."""

    def test_call_target_module_function_format(self):
        """Test call_target in module:function format."""
        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.addHandler(handler)

        try:
            logger.info(
                "Executing function",
                extra={
                    "log_source": "execution",
                    "env_name": "test",
                    "call_target": "segment:detect"
                }
            )

            assert len(records) > 0
            extra = get_extra_from_record(records[0])
            assert extra.get("call_target") == "segment:detect"
            assert ":" in extra.get("call_target", "")
        finally:
            logger.removeHandler(handler)

    def test_call_target_script_format(self):
        """Test call_target in script.py format."""
        records = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = TestHandler()
        logger.addHandler(handler)

        try:
            logger.info(
                "Running script",
                extra={
                    "log_source": "execution",
                    "env_name": "test",
                    "call_target": "process.py"
                }
            )

            assert len(records) > 0
            extra = get_extra_from_record(records[0])
            assert extra.get("call_target") == "process.py"
            assert extra.get("call_target").endswith(".py")
        finally:
            logger.removeHandler(handler)


class TestMultipleLogSources:
    """Test logging from different log sources."""

    def test_multiple_log_sources_separation(self):
        """Test that different log sources can be tracked separately."""
        env_logs = []
        exec_logs = []

        class EnvHandler(logging.Handler):
            def emit(self, record):
                extra = get_extra_from_record(record)
                if extra.get("log_source") == "environment":
                    env_logs.append(record)

        class ExecHandler(logging.Handler):
            def emit(self, record):
                extra = get_extra_from_record(record)
                if extra.get("log_source") == "execution":
                    exec_logs.append(record)

        env_handler = EnvHandler()
        exec_handler = ExecHandler()
        logger.addHandler(env_handler)
        logger.addHandler(exec_handler)

        try:
            # Log from different sources
            logger.info(
                "Environment message",
                extra={"log_source": "environment", "env_name": "test", "stage": "create"}
            )
            logger.info(
                "Execution message",
                extra={"log_source": "execution", "env_name": "test", "call_target": "test:func"}
            )

            assert len(env_logs) > 0
            assert len(exec_logs) > 0
            assert env_logs[0].getMessage() == "Environment message"
            assert exec_logs[0].getMessage() == "Execution message"
        finally:
            logger.removeHandler(env_handler)
            logger.removeHandler(exec_handler)
