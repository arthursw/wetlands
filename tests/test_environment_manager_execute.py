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
        "execute_commands": mock_execute,
        "execute_commands_and_get_output": mock_execute_output,
    }
    return mocks


@pytest.fixture
def environment_manager_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    dummy_micromamba_path = tmp_path_factory.mktemp("conda_root")
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance")
    main_env_path = dummy_micromamba_path / "envs" / "main_test_env"

    monkeypatch.setattr(EnvironmentManager, "install_conda", MagicMock())

    manager = EnvironmentManager(
        wetlands_instance_path=wetlands_instance_path,
        conda_path=dummy_micromamba_path,
        manager="micromamba",
        main_conda_environment_path=main_env_path,
    )

    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_command_executor["execute_commands"])
    monkeypatch.setattr(
        manager.command_executor,
        "execute_commands_and_get_output",
        mock_command_executor["execute_commands_and_get_output"],
    )

    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    return manager, mock_command_executor["execute_commands_and_get_output"], mock_command_executor["execute_commands"]


# ---- execute_commands Tests ----


def test_execute_commands_in_specific_env(environment_manager_fixture):
    manager, _, mock_execute = environment_manager_fixture
    env_name = "exec-env"
    commands_to_run: Commands = {"all": ["python script.py", "echo done"]}
    popen_kwargs = {"cwd": "/some/path"}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settings_manager.get_environment_path_from_name(env_name), manager)
    manager.environments[env_name] = env

    manager.execute_commands(env, commands_to_run, popen_kwargs=popen_kwargs)

    mock_execute.assert_called_once()
    called_args, called_kwargs = mock_execute.call_args
    command_list = called_args[0]

    # Check activation for the specific environment
    assert any(f"activate {env.path}" in cmd for cmd in command_list)
    # Check user commands are present
    assert "python script.py" in command_list
    assert "echo done" in command_list
    # Check popen_kwargs are passed through
    assert called_kwargs.get("popen_kwargs") == popen_kwargs


def test_execute_commands_in_main_env(environment_manager_fixture):
    manager, _, mock_execute = environment_manager_fixture
    manager.main_environment.path = Path("/path/to/main/env")  # Give it a path
    commands_to_run: Commands = {"all": ["ls -l"]}

    # Pass the main environment
    manager.execute_commands(manager.main_environment, commands_to_run)

    mock_execute.assert_called_once()
    called_args, _ = mock_execute.call_args
    command_list = called_args[0]

    # Check user command
    assert "ls -l" in command_list
