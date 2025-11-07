import platform
import re
from pathlib import Path
from unittest.mock import MagicMock
import subprocess

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.internal_environment import InternalEnvironment
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


@pytest.fixture
def environment_manager_pixi_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor for Pixi."""
    dummy_pixi_path = tmp_path_factory.mktemp("pixi_root")
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance_pixi")

    monkeypatch.setattr(EnvironmentManager, "installConda", MagicMock())

    manager = EnvironmentManager(
        wetlandsInstancePath=wetlands_instance_path, condaPath=dummy_pixi_path, manager="pixi"
    )

    monkeypatch.setattr(manager.commandExecutor, "executeCommands", mock_command_executor["executeCommands"])
    monkeypatch.setattr(
        manager.commandExecutor, "executeCommandsAndGetOutput", mock_command_executor["executeCommandsAndGetOutput"]
    )

    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    return manager, mock_command_executor["executeCommandsAndGetOutput"], mock_command_executor["executeCommands"]


# ---- create Tests (micromamba) ----


def test_create_dependencies_met_use_main_environment(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "new-env-dont-create"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    # Mock _dependenciesAreInstalled to return True
    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=True))

    env = manager.create(env_name, dependencies=dependencies, forceExternal=False)

    assert env is manager.mainEnvironment  # Should return the main environment instance
    assert isinstance(env, InternalEnvironment)
    manager._dependenciesAreInstalled.assert_called_once_with(dependencies)
    mock_execute_output.assert_not_called()  # No commands should be run


def test_create_dependencies_met_force_external(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "forced-external-env"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    # Mock _dependenciesAreInstalled to return True, but forceExternal=True overrides it
    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=True))

    env = manager.create(env_name, dependencies=dependencies, forceExternal=True)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    assert env is manager.environments[env_name]
    mock_execute_output.assert_called()  # Creation commands should be run

    # Check for key commands
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"create -n {env_name}" in cmd for cmd in command_list)
    # Check install commands are present (assuming numpy leads to some install command)
    assert any("install" in cmd for cmd in command_list if "create" not in cmd)


def test_create_dependencies_not_met_create_external(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "new-external-env"
    dependencies: Dependencies = {"conda": ["requests"], "pip": ["pandas"]}

    # Mock _dependenciesAreInstalled to return False
    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies, forceExternal=False)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    assert env is manager.environments[env_name]
    manager._dependenciesAreInstalled.assert_called_once_with(dependencies)
    mock_execute_output.assert_called()  # Creation commands should be run

    # Check for key commands
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    current_py_version = platform.python_version()
    assert any(f"create -n {env_name} python={current_py_version} -y" in cmd for cmd in command_list)
    assert any(f"install" in cmd for cmd in command_list if "micromamba" in cmd)  # Check for install commands
    assert any("requests" in cmd for cmd in command_list if "install" in cmd)  # Check dep is mentioned
    assert any("pandas" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_python_version(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "py-versioned-env"
    py_version = "3.10.5"
    dependencies: Dependencies = {"python": f"={py_version}", "pip": ["toolz"]}  # Use exact match format

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    # Check python version is in create command
    assert any(f"create -n {env_name} python={py_version} -y" in cmd for cmd in command_list)
    # Check install command for toolz
    assert any("toolz" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_additional_commands(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "env-with-extras"
    dependencies: Dependencies = {"pip": ["tiny-package"]}
    additional_commands: Commands = {
        "all": ["echo 'hello world'"],
        "linux": ["specific command"],  # e.g., 'linux', 'darwin', 'windows'
    }

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))

    monkeypatch.setattr(platform, "system", MagicMock(return_value="Linux"))

    manager.create(env_name, dependencies=dependencies, additionalInstallCommands=additional_commands)

    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Check create and install commands are present
    assert any(f"create -n {env_name}" in cmd for cmd in command_list)
    assert any("tiny-package" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)

    # Check additional commands are present
    assert "echo 'hello world'" in command_list
    assert "specific command" in command_list


def test_create_invalid_python_version_raises(environment_manager_fixture, monkeypatch):
    manager, _, _ = environment_manager_fixture
    env_name = "invalid-py-env"
    dependencies: Dependencies = {"python": "=3.8"}  # Below 3.9 limit

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))

    with pytest.raises(Exception, match="Python version must be greater than 3.8"):
        manager.create(env_name, dependencies=dependencies)


# ---- create Tests (Pixi) ----


def test_create_with_python_version_pixi(environment_manager_pixi_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_pixi_fixture
    env_name = "py-versioned-env"
    py_version = "3.10.5"
    dependencies: Dependencies = {
        "python": f"={py_version}",
        "pip": ["toolz"],
        "conda": ["dep==1.0"],
    }  # Use exact match format

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    pixi_bin = "pixi.exe" if platform.system() == "Windows" else "pixi"
    assert any(f"{pixi_bin} init" in cmd for cmd in command_list)
    # Check python version is in create command
    assert any(re.match(rf"{pixi_bin} add .* python={py_version}", cmd) is not None for cmd in command_list)
    # Check install command for dependencies
    assert any("toolz" in cmd and "--pypi" in cmd for cmd in command_list if f"{pixi_bin} add" in cmd)
    assert any("dep" in cmd for cmd in command_list if f"{pixi_bin} add" in cmd)
