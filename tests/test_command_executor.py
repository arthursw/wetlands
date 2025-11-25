import pytest
from wetlands._internal.command_executor import CommandExecutor


@pytest.fixture
def executor():
    return CommandExecutor()


def test_execute_commands_success(executor):
    # Use log=False to prevent ProcessLogger from consuming stdout
    process = executor.executeCommands(["echo HelloWorld"], log=False)
    with process:
        output = process.stdout.read().strip()
    assert output == "HelloWorld"
    assert process.returncode == 0


def test_execute_commands_failure(executor):
    process = executor.executeCommands(["exit 1"])
    process.wait()
    assert process.returncode == 1


def test_get_output_success(executor):
    output = executor.executeCommandsAndGetOutput(["echo Hello"], log=False)
    assert output == ["Hello"]


def test_get_output_failure(executor):
    with pytest.raises(Exception, match="failed"):
        executor.executeCommandsAndGetOutput(["exit 1"], log=False)


def test_conda_system_exit(executor):
    with pytest.raises(Exception, match="failed"):
        executor.executeCommandsAndGetOutput(["echo CondaSystemExit"], log=False)
