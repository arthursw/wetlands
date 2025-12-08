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
    mock_process = MagicMock(spec=subprocess.Popen)
    mock_process.returncode = 0
    mock_process.pid = 12345
    # Make wait() set returncode to 0 when called
    mock_process.wait.return_value = 0

    mock_execute = MagicMock(return_value=mock_process)
    mock_execute_output = MagicMock(return_value=["output line 1", "output line 2"])

    mocks = {
        "execute_commands": mock_execute,
        "execute_commands_and_get_output": mock_execute_output,
        "mock_process": mock_process,
    }
    return mocks


@pytest.fixture
def environment_manager_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    dummy_micromamba_path = tmp_path_factory.mktemp("conda_root")
    main_env_path = dummy_micromamba_path / "envs" / "main_test_env"

    monkeypatch.setattr(EnvironmentManager, "install_conda", MagicMock())

    manager = EnvironmentManager(
        conda_path=dummy_micromamba_path, manager="micromamba", main_conda_environment_path=main_env_path
    )

    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_command_executor["execute_commands"])
    monkeypatch.setattr(
        manager.command_executor,
        "execute_commands_and_get_output",
        mock_command_executor["execute_commands_and_get_output"],
    )

    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    return manager, mock_command_executor["execute_commands"], mock_command_executor["execute_commands_and_get_output"]


@pytest.fixture
def environment_manager_pixi_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor for Pixi."""
    dummy_pixi_path = tmp_path_factory.mktemp("pixi_root")

    monkeypatch.setattr(EnvironmentManager, "install_conda", MagicMock())

    manager = EnvironmentManager(conda_path=dummy_pixi_path, manager="pixi")

    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_command_executor["execute_commands"])
    monkeypatch.setattr(
        manager.command_executor,
        "execute_commands_and_get_output",
        mock_command_executor["execute_commands_and_get_output"],
    )

    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    return manager, mock_command_executor["execute_commands"], mock_command_executor["execute_commands_and_get_output"]


# ---- install Tests (micromamba) ----


def test_install_in_existing_env(environment_manager_fixture):
    manager, _, mock_execute_and_get_output = environment_manager_fixture
    mock_execute_output = mock_execute_and_get_output
    env_name = "target-env"
    dependencies: Dependencies = {"conda": ["new_dep==1.0"]}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settings_manager.get_environment_path_from_name(env_name), manager)
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
    manager, _, mock_execute_and_get_output = environment_manager_fixture
    mock_execute_output = mock_execute_and_get_output
    dependencies: Dependencies = {"pip": ["another_pip_dep"]}

    # Pass the main environment
    manager.install(manager.main_environment, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Install commands should NOT have "-n env_name"
    assert not any(f"install -n" in cmd for cmd in command_list if "install" in cmd)
    # Check pip install command is present
    assert any("pip install" in cmd and "another_pip_dep" in cmd for cmd in command_list)


def test_install_with_additional_commands(environment_manager_fixture):
    manager, _, mock_execute_and_get_output = environment_manager_fixture
    mock_execute_output = mock_execute_and_get_output
    env_name = "install-env-extras"
    dependencies: Dependencies = {"conda": ["dep1"]}
    additional_commands: Commands = {"all": ["post-install script"]}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settings_manager.get_environment_path_from_name(env_name), manager)
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
    manager, _, mock_execute_and_get_output = environment_manager_pixi_fixture
    mock_execute_output = mock_execute_and_get_output
    env_name = "target-env"
    dependencies: Dependencies = {"conda": ["new_dep==1.0"]}

    # Create an environment object
    env = ExternalEnvironment(env_name, manager.settings_manager.get_environment_path_from_name(env_name), manager)
    manager.environments[env_name] = env

    manager.install(env, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    pixi_bin = "pixi.exe" if platform.system() == "Windows" else "pixi"

    # Check for install commands targeting the environment
    assert any("new_dep==1.0" in cmd for cmd in command_list if f"{pixi_bin} add" in cmd)
