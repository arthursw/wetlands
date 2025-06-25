import pytest
from wetlands._internal.command_executor import CommandExecutor


@pytest.fixture
def executor():
    return CommandExecutor()


def test_execute_commands_success(executor):
    process = executor.executeCommands(["echo HelloWorld"])
    with process:
        output = process.stdout.read().strip()
    assert output == "HelloWorld"
    assert process.returncode == 0


def test_execute_commands_failure(executor):
    process = executor.executeCommands(["exit 1"])
    process.wait()
    assert process.returncode == 1


def test_get_output_success(executor):
    process = executor.executeCommands(["echo Hello"])
    with process:
        output = executor.getOutput(process, ["echo Hello"], log=False)
    assert output == ["Hello"]


def test_get_output_failure(executor):
    process = executor.executeCommands(["exit 1"])
    with pytest.raises(Exception, match="failed"):
        with process:
            executor.getOutput(process, ["exit 1"], log=False)


def test_conda_system_exit(executor):
    process = executor.executeCommands(["echo CondaSystemExit"])  # Simulate Conda exit
    with pytest.raises(Exception, match="failed"):
        with process:
            executor.getOutput(process, ["echo CondaSystemExit"], log=False)
