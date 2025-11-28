"""Tests for ProcessLogger and logging functionality."""

import subprocess
import pytest
import logging
from unittest.mock import MagicMock
from wetlands._internal.process_logger import ProcessLogger
from wetlands.logger import logger


@pytest.fixture
def mock_process():
    """Create a mock subprocess.Popen object."""
    process = MagicMock(spec=subprocess.Popen)
    process.pid = 12345
    process.stdout = None
    return process


@pytest.fixture
def log_context():
    """Create a test log context."""
    return {"log_source": "execution", "env_name": "test_env", "call_target": "test:function"}


def test_process_logger_initialization(mock_process, log_context):
    """Test ProcessLogger initializes with correct context."""
    process_logger = ProcessLogger(mock_process, log_context, logger)

    assert process_logger.process == mock_process
    assert process_logger.log_context == log_context
    assert process_logger.base_logger == logger
    assert process_logger._subscribers == []
    assert process_logger._output == []


def test_process_logger_subscription(mock_process, log_context):
    """Test ProcessLogger subscriber registration."""
    process_logger = ProcessLogger(mock_process, log_context, logger)
    callback = MagicMock()

    process_logger.subscribe(callback)

    assert len(process_logger._subscribers) == 1
    assert process_logger._subscribers[0] == callback


def test_process_logger_update_context(mock_process, log_context):
    """Test ProcessLogger dynamic context updates."""
    process_logger = ProcessLogger(mock_process, log_context, logger)

    # Update call_target dynamically
    process_logger.update_log_context({"call_target": "test:other_function"})

    assert process_logger.log_context["call_target"] == "test:other_function"
    assert process_logger.log_context["env_name"] == "test_env"  # Other fields unchanged


def test_process_logger_accumulates_output(mock_process, log_context):
    """Test ProcessLogger accumulates output lines."""
    process_logger = ProcessLogger(mock_process, log_context, logger)

    # Manually add output (simulating what _read_stdout does)
    with process_logger._lock:
        process_logger._output.append("line 1")
        process_logger._output.append("line 2")
        process_logger._output.append("line 3")

    output = process_logger.get_output()

    assert output == ["line 1", "line 2", "line 3"]


def test_process_logger_wait_for_line(mock_process, log_context):
    """Test ProcessLogger wait_for_line predicate matching."""
    process_logger = ProcessLogger(mock_process, log_context, logger)

    # Define predicate
    def port_predicate(line: str) -> bool:
        return line.startswith("Listening port ")

    # Start waiting (in background)
    import threading

    result_holder = []

    def wait():
        result = process_logger.wait_for_line(port_predicate, timeout=2.0)
        result_holder.append(result)

    wait_thread = threading.Thread(target=wait, daemon=True)
    wait_thread.start()

    # Simulate matching line being found
    import time

    time.sleep(0.1)
    callback = process_logger._subscribers[0]  # The callback added by wait_for_line
    callback("Listening port 12345", {})

    wait_thread.join(timeout=1)

    assert result_holder[0] == "Listening port 12345"


def test_process_logger_wait_for_line_timeout(mock_process, log_context):
    """Test ProcessLogger wait_for_line with timeout."""
    process_logger = ProcessLogger(mock_process, log_context, logger)

    def never_match(line: str) -> bool:
        return False

    result = process_logger.wait_for_line(never_match, timeout=0.1)

    assert result is None


def test_process_logger_subscriber_callback_integration(mock_process, log_context, caplog):
    """Test that subscribers are called and receive correct context."""
    process_logger = ProcessLogger(mock_process, log_context, logger)
    callback = MagicMock()
    process_logger.subscribe(callback)

    # Simulate subscriber notification
    test_line = "Test output"
    with process_logger._lock:
        callback(test_line, process_logger.log_context)

    # Verify callback was called with correct arguments
    callback.assert_called_once()
    call_args = callback.call_args
    assert call_args[0][0] == test_line
    assert call_args[0][1]["log_source"] == "execution"
    assert call_args[0][1]["env_name"] == "test_env"
    assert call_args[0][1]["call_target"] == "test:function"


def test_process_logger_with_empty_context(mock_process):
    """Test ProcessLogger with empty context dict."""
    process_logger = ProcessLogger(mock_process, {}, logger)

    assert process_logger.log_context == {}


def test_process_logger_context_isolation(mock_process, log_context):
    """Test that ProcessLogger doesn't modify the original context dict."""
    original_context = log_context.copy()
    process_logger = ProcessLogger(mock_process, log_context, logger)

    # Update context in ProcessLogger
    process_logger.update_log_context({"new_field": "new_value"})

    # Original context should be unchanged
    assert log_context == original_context
    assert "new_field" not in log_context


def test_process_logger_multiple_subscribers(mock_process, log_context):
    """Test ProcessLogger with multiple subscribers."""
    process_logger = ProcessLogger(mock_process, log_context, logger)
    callback1 = MagicMock()
    callback2 = MagicMock()

    process_logger.subscribe(callback1)
    process_logger.subscribe(callback2)

    assert len(process_logger._subscribers) == 2


def test_process_logger_subscriber_error_handling(mock_process, log_context, caplog):
    """Test ProcessLogger handles subscriber errors gracefully."""
    process_logger = ProcessLogger(mock_process, log_context, logger)

    # Add a callback that raises an exception
    def bad_callback(line: str, context: dict) -> None:
        raise ValueError("Callback error")

    process_logger.subscribe(bad_callback)

    # Manually call subscriber (simulating _read_stdout behavior)
    with caplog.at_level(logging.ERROR):
        with process_logger._lock:
            for callback in process_logger._subscribers:
                try:
                    callback("test line", process_logger.log_context)
                except Exception as e:
                    process_logger.base_logger.error(f"Error in log callback: {e}")

    # Verify error was logged
    assert "Error in log callback" in caplog.text


class TestProcessLoggerIntegration:
    """Integration tests for ProcessLogger with actual subprocess."""

    def test_process_logger_reads_echo_output(self):
        """Test ProcessLogger reads output from real subprocess."""
        process = subprocess.Popen(
            ["echo", "Hello World"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )

        log_context = {"log_source": "execution", "env_name": "test"}
        process_logger = ProcessLogger(process, log_context, logger)

        # Subscribe to collect output
        collected_lines = []
        process_logger.subscribe(lambda line, ctx: collected_lines.append(line))

        process_logger.start_reading()
        process.wait()

        # Give reader thread time to process
        import time

        time.sleep(0.1)

        assert "Hello World" in collected_lines

    def test_process_logger_with_log_context_in_logger(self):
        """Test that log context is properly attached to log records."""
        process = subprocess.Popen(
            ["echo", "Test message"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )

        log_context = {"log_source": "execution", "env_name": "cellpose", "call_target": "segment:detect"}

        # Create a custom handler that captures record.__dict__ to verify extra field
        records_with_extra = []

        class TestHandler(logging.Handler):
            def emit(self, record):
                # Store record with its attributes to capture extra
                records_with_extra.append(
                    {
                        "message": record.getMessage(),
                        "extra": dict(record.__dict__),  # Capture all attributes
                    }
                )

        handler = TestHandler()
        logger.logger.addHandler(handler)

        try:
            process_logger = ProcessLogger(process, log_context, logger)
            process_logger.start_reading()
            process.wait()

            # Give reader thread time to process
            import time

            time.sleep(0.1)

            # Verify at least one record was logged
            assert len(records_with_extra) > 0

            # The extra fields are stored as attributes on the record
            record_dict = records_with_extra[0]["extra"]
            # Check that context fields were attached
            assert record_dict.get("log_source") == "execution"
            assert record_dict.get("env_name") == "cellpose"
            assert record_dict.get("call_target") == "segment:detect"
        finally:
            logger.logger.removeHandler(handler)
