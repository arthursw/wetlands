from pathlib import Path
from unittest.mock import MagicMock
import subprocess

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment
from wetlands._internal.command_generator import Commands


@pytest.fixture
def mock_command_executor(monkeypatch):
    """Mocks the CommandExecutor methods."""
    mock_execute = MagicMock(spec=subprocess.Popen)
    mock_execute_output = MagicMock(return_value=["output line 1", "output line 2"])

    mocks = {
        "executeCommands": mock_execute,
        "executeCommandsAndGetOutput": mock_execute_output,
    }
    return mocks


@pytest.fixture
def environment_manager_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    dummy_micromamba_path = tmp_path_factory.mktemp("conda_root")
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance")
    main_env_path = dummy_micromamba_path / "envs" / "main_test_env"

    monkeypatch.setattr(EnvironmentManager, "installConda", MagicMock())

    manager = EnvironmentManager(
        wetlandsInstancePath=wetlands_instance_path,
        condaPath=dummy_micromamba_path,
        manager="micromamba",
        mainCondaEnvironmentPath=main_env_path,
    )

    monkeypatch.setattr(manager.commandExecutor, "executeCommands", mock_command_executor["executeCommands"])
    monkeypatch.setattr(
        manager.commandExecutor, "executeCommandsAndGetOutput", mock_command_executor["executeCommandsAndGetOutput"]
    )

    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    return manager, mock_command_executor["executeCommandsAndGetOutput"], mock_command_executor["executeCommands"]


# ---- executeCommands Tests ----


def test_execute_commands_in_specific_env(environment_manager_fixture):
    manager, _, mock_execute = environment_manager_fixture
    env_name = "exec-env"
    commands_to_run: Commands = {"all": ["python script.py", "echo done"]}
    popen_kwargs = {"cwd": "/some/path"}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settingsManager.getEnvironmentPathFromName(env_name), manager)
    manager.environments[env_name] = env

    manager.executeCommands(env, commands_to_run, popenKwargs=popen_kwargs)

    mock_execute.assert_called_once()
    called_args, called_kwargs = mock_execute.call_args
    command_list = called_args[0]

    # Check activation for the specific environment
    assert any(f"activate {env.path}" in cmd for cmd in command_list)
    # Check user commands are present
    assert "python script.py" in command_list
    assert "echo done" in command_list
    # Check popenKwargs are passed through
    assert called_kwargs.get("popenKwargs") == popen_kwargs


def test_execute_commands_in_main_env(environment_manager_fixture):
    manager, _, mock_execute = environment_manager_fixture
    manager.mainEnvironment.path = Path("/path/to/main/env")  # Give it a path
    commands_to_run: Commands = {"all": ["ls -l"]}

    # Pass the main environment
    manager.executeCommands(manager.mainEnvironment, commands_to_run)

    mock_execute.assert_called_once()
    called_args, _ = mock_execute.call_args
    command_list = called_args[0]

    # Check user command
    assert "ls -l" in command_list
