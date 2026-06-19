from unittest.mock import MagicMock
from pathlib import Path
import codecs
import logging

import pytest

from wetlands._internal.command_executor import CommandExecutor
from wetlands.logger import logger


@pytest.fixture
def executor():
    return CommandExecutor()


def test_execute_commands_success(executor):
    # Use log=False to prevent ProcessLogger from consuming stdout
    process = executor.execute_commands(["echo HelloWorld"], log=False)
    with process:
        output = process.stdout.read().strip()
    assert output == "HelloWorld"
    assert process.returncode == 0


def test_execute_commands_does_not_shell_out_to_chmod(executor, monkeypatch):
    run = MagicMock()
    monkeypatch.setattr("wetlands._internal.command_executor.subprocess.run", run)

    process = executor.execute_commands(["echo HelloWorld"], log=False)
    with process:
        output = process.stdout.read().strip()

    assert output == "HelloWorld"
    run.assert_not_called()


def test_execute_commands_failure(executor):
    process = executor.execute_commands(["exit 1"])
    process.wait()
    assert process.returncode == 1


def test_execute_commands_wait_failure_raises(executor):
    with pytest.raises(Exception, match="failed"):
        executor.execute_commands(["echo 'dependency solve failed'", "exit 1"], wait=True)


def test_execute_commands_writes_windows_script_with_unicode_encoding(tmp_path, monkeypatch):
    executor = CommandExecutor(scripts_path=tmp_path)
    process = MagicMock(pid=123, stdout=None, stderr=None, returncode=0)
    process._conda_exit_detected = False
    popen = MagicMock(return_value=process)
    monkeypatch.setattr(executor, "_is_windows", lambda: True)
    monkeypatch.setattr("wetlands._internal.command_executor.subprocess.Popen", popen)

    executor.execute_commands(["python -c \"print('✔')\""], wait=True, log=False)

    script_bytes = Path(process._wetlands_script_path).read_bytes()
    assert script_bytes.startswith(codecs.BOM_UTF8)
    assert "✔".encode() in script_bytes


def test_execute_commands_successful_stderr_logs_info_and_remains_captured(executor, caplog):
    with caplog.at_level(logging.INFO, logger=logger.logger.name):
        process = executor.execute_commands(
            ["python -c \"import sys; sys.stderr.buffer.write('✔ Added python=3.12\\n'.encode())\""],
            wait=True,
        )

    process_logger = executor.get_process_logger(process)
    assert process_logger.get_stderr_output() == ["✔ Added python=3.12"]

    progress_records = [record for record in caplog.records if "Added python=3.12" in record.getMessage()]
    assert progress_records
    assert all(record.levelno == logging.INFO for record in progress_records)
    assert all(getattr(record, "stream", None) == "stderr" for record in progress_records)


def test_execute_commands_wait_failure_logs_summary_and_bounded_stderr_tail(executor, caplog):
    stderr_lines = [f"err-{index}" for index in range(25)]
    command = "python -c \"import sys; [print(f'err-{i}', file=sys.stderr) for i in range(25)]\""

    with pytest.raises(Exception, match="failed"):
        with caplog.at_level(logging.INFO, logger=logger.logger.name):
            executor.execute_commands([command, "exit 3"], exit_if_command_error=False, wait=True)

    error_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert any("failed with exit code 3" in message for message in error_messages)
    assert "err-0" not in error_messages
    assert stderr_lines[-20:] == [message for message in error_messages if message.startswith("err-")]


def test_get_output_success(executor):
    output = executor.execute_commands_and_get_output(["echo Hello"])
    assert output == ["Hello"]


def test_get_output_success_keeps_stderr_output_available(executor):
    process = executor.execute_commands(
        [
            "echo stdout-line",
            "python -c \"import sys; print('stderr-line', file=sys.stderr)\"",
        ],
        wait=True,
    )

    process_logger = executor.get_process_logger(process)
    assert process_logger.get_stdout_output() == ["stdout-line"]
    assert process_logger.get_stderr_output() == ["stderr-line"]


def test_get_output_failure(executor):
    with pytest.raises(Exception, match="failed"):
        executor.execute_commands_and_get_output(["exit 1"])


def test_conda_system_exit(executor):
    with pytest.raises(Exception, match="failed"):
        executor.execute_commands_and_get_output(["echo CondaSystemExit"])
