from unittest.mock import MagicMock

import pytest

from wetlands._internal.command_executor import CommandExecutor


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


def test_get_output_success(executor):
    output = executor.execute_commands_and_get_output(["echo Hello"])
    assert output == ["Hello"]


def test_get_output_failure(executor):
    with pytest.raises(Exception, match="failed"):
        executor.execute_commands_and_get_output(["exit 1"])


def test_conda_system_exit(executor):
    with pytest.raises(Exception, match="failed"):
        executor.execute_commands_and_get_output(["echo CondaSystemExit"])
