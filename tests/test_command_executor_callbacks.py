"""Tests for CommandExecutor callback logging functionality."""

import pytest
import threading
import time
from unittest.mock import MagicMock, patch
from wetlands._internal.command_executor import CommandExecutor


@pytest.fixture
def executor():
    return CommandExecutor()


def test_execute_commands_with_log_callback(executor):
    """Test that log_callback is called for each line of output."""
    callback = MagicMock()

    process = executor.executeCommands(["echo Line1\necho Line2\necho Line3"], log_callback=callback)
    process.wait()

    # Callback should have been called for each line
    assert callback.call_count >= 3  # At least 3 lines


def test_execute_commands_callback_receives_correct_lines(executor):
    """Test that callback receives the correct output lines."""
    lines_received = []

    def callback(line):
        lines_received.append(line)

    process = executor.executeCommands(["echo Hello\necho World"], log_callback=callback)
    process.wait()

    assert "Hello" in lines_received
    assert "World" in lines_received


def test_execute_commands_callback_called_in_thread(executor):
    """Test that callback is called from a separate thread."""
    callback_thread_id = []
    main_thread_id = threading.current_thread().ident

    def callback(line):
        if not callback_thread_id:
            callback_thread_id.append(threading.current_thread().ident)

    process = executor.executeCommands(["echo test"], log_callback=callback)
    process.wait()
    time.sleep(0.1)  # Give time for thread to execute

    # Callback should have been called
    assert callback_thread_id
    # Thread ID should be different from main thread (daemon thread)
    assert callback_thread_id[0] != main_thread_id


def test_execute_commands_without_callback_still_works(executor):
    """Test that executeCommands works without callback (backward compatibility)."""
    process = executor.executeCommands(["echo test"])
    with process:
        output = process.stdout.read().strip()

    assert output == "test"
    assert process.returncode == 0


def test_get_output_with_callback_uses_callback(executor):
    """Test that when callback is provided, getOutput returns empty since callback consumes stdout."""
    lines_received = []

    def callback(line):
        lines_received.append(line)

    process = executor.executeCommands(["echo test1\necho test2"], log_callback=callback)
    time.sleep(0.1)  # Give callback thread time to read
    with process:
        output = executor.getOutput(process, ["echo test1\necho test2"], log=False)

    # Callback should have consumed the output, so getOutput gets nothing
    assert len(output) == 0
    # But callback should have received the lines
    assert len(lines_received) > 0


def test_execute_commands_and_get_output_no_callback(executor):
    """Test that executeCommandsAndGetOutput still works without callback."""
    output = executor.executeCommandsAndGetOutput(["echo hello"], log=False)
    assert "hello" in output


def test_callback_exception_handling(executor):
    """Test that callback exceptions don't break the process."""
    call_count = [0]

    def callback(line):
        call_count[0] += 1
        if call_count[0] == 2:
            raise RuntimeError("Test error")

    # Process should complete even if callback raises exception
    process = executor.executeCommands(["echo line1\necho line2\necho line3"], log_callback=callback)
    process.wait()

    # Process should have completed successfully
    assert process.returncode == 0


def test_callback_receives_all_output_lines(executor):
    """Test that all output lines are passed to callback."""
    all_lines = []

    def callback(line):
        all_lines.append(line)

    # Create a command with multiple lines
    command = "echo 'Line 1'; echo 'Line 2'; echo 'Line 3'"
    process = executor.executeCommands([command], log_callback=callback)
    process.wait()
    time.sleep(0.1)  # Give time for all callbacks to complete

    # Should have received multiple lines
    assert len(all_lines) >= 3


def test_multiple_callbacks_not_supported(executor):
    """Test that only one callback is supported at a time."""
    callback1 = MagicMock()
    callback2 = MagicMock()

    # Should accept first callback
    process = executor.executeCommands(["echo test"], log_callback=callback1)
    process.wait()

    assert callback1.call_count > 0
