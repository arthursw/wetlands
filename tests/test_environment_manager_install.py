import platform
from unittest.mock import MagicMock
import subprocess

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment
from wetlands._internal.dependency_manager import Dependencies
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
    main_env_path = dummy_micromamba_path / "envs" / "main_test_env"

    monkeypatch.setattr(EnvironmentManager, "installConda", MagicMock())

    manager = EnvironmentManager(
        condaPath=dummy_micromamba_path, manager="micromamba", mainCondaEnvironmentPath=main_env_path
    )

    monkeypatch.setattr(manager.commandExecutor, "executeCommands", mock_command_executor["executeCommands"])
    monkeypatch.setattr(
        manager.commandExecutor, "executeCommandsAndGetOutput", mock_command_executor["executeCommandsAndGetOutput"]
    )

    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    return manager, mock_command_executor["executeCommandsAndGetOutput"], mock_command_executor["executeCommands"]


@pytest.fixture
def environment_manager_pixi_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor for Pixi."""
    dummy_pixi_path = tmp_path_factory.mktemp("pixi_root")

    monkeypatch.setattr(EnvironmentManager, "installConda", MagicMock())

    manager = EnvironmentManager(condaPath=dummy_pixi_path, manager="pixi")

    monkeypatch.setattr(manager.commandExecutor, "executeCommands", mock_command_executor["executeCommands"])
    monkeypatch.setattr(
        manager.commandExecutor, "executeCommandsAndGetOutput", mock_command_executor["executeCommandsAndGetOutput"]
    )

    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    return manager, mock_command_executor["executeCommandsAndGetOutput"], mock_command_executor["executeCommands"]


# ---- install Tests (micromamba) ----


def test_install_in_existing_env(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "target-env"
    dependencies: Dependencies = {"conda": ["new_dep==1.0"]}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settingsManager.getEnvironmentPathFromName(env_name), manager)
    manager.environments[env_name] = env

    manager.install(env, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Check for install commands targeting the environment
    assert any("new_dep==1.0" in cmd for cmd in command_list if "install" in cmd)
    # Check activation commands are present (usually part of install dependencies)
    assert any(
        "micromamba activate" in cmd or ". /path/to/micromamba" in cmd for cmd in command_list
    )  # Check general activation pattern


def test_install_in_main_env(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["another_pip_dep"]}

    # Pass the main environment
    manager.install(manager.mainEnvironment, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Install commands should NOT have "-n env_name"
    assert not any(f"install -n" in cmd for cmd in command_list if "install" in cmd)
    # Check pip install command is present
    assert any("pip install" in cmd and "another_pip_dep" in cmd for cmd in command_list)


def test_install_with_additional_commands(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "install-env-extras"
    dependencies: Dependencies = {"conda": ["dep1"]}
    additional_commands: Commands = {"all": ["post-install script"]}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settingsManager.getEnvironmentPathFromName(env_name), manager)
    manager.environments[env_name] = env

    manager.install(env, dependencies, additional_commands)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Check install command
    assert any("install" in cmd and "dep1" in cmd for cmd in command_list)
    # Check additional command
    assert "post-install script" in command_list


# ---- install Tests (Pixi) ----


def test_install_in_existing_env_pixi(environment_manager_pixi_fixture):
    manager, mock_execute_output, _ = environment_manager_pixi_fixture
    env_name = "target-env"
    dependencies: Dependencies = {"conda": ["new_dep==1.0"]}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settingsManager.getEnvironmentPathFromName(env_name), manager)
    manager.environments[env_name] = env

    manager.install(env, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    pixi_bin = "pixi.exe" if platform.system() == "Windows" else "pixi"

    # Check for install commands targeting the environment
    assert any("new_dep==1.0" in cmd for cmd in command_list if f"{pixi_bin} add" in cmd)
