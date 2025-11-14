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

    manager = EnvironmentManager(wetlandsInstancePath=wetlands_instance_path, condaPath=dummy_pixi_path, manager="pixi")

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

    # Mock _environmentValidatesRequirements to return True for main env
    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=True))

    env = manager.create(env_name, dependencies=dependencies, useExisting=True)

    assert env is manager.mainEnvironment
    assert isinstance(env, InternalEnvironment)
    mock_execute_output.assert_not_called()


def test_create_always_creates_new_when_use_existing_false(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "always-new-env"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    # Mock _environmentValidatesRequirements to return True (but should be ignored when useExisting=False)
    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=True))

    env = manager.create(env_name, dependencies=dependencies, useExisting=False)

    # Should create new environment, not return existing one
    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    mock_execute_output.assert_called()


def test_create_dependencies_not_met_create_external(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "new-external-env"
    dependencies: Dependencies = {"conda": ["requests"], "pip": ["pandas"]}

    # Mock _environmentValidatesRequirements to return False
    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    assert env is manager.environments[env_name]
    mock_execute_output.assert_called()

    # Check for key commands
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    current_py_version = platform.python_version()
    assert any(f"create -n {env_name} python={current_py_version} -y" in cmd for cmd in command_list)
    assert any(f"install" in cmd for cmd in command_list if "micromamba" in cmd)
    assert any("requests" in cmd for cmd in command_list if "install" in cmd)
    assert any("pandas" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_python_version(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "py-versioned-env"
    py_version = "3.10.5"
    dependencies: Dependencies = {"python": f"={py_version}", "pip": ["toolz"]}

    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"create -n {env_name} python={py_version} -y" in cmd for cmd in command_list)
    assert any("toolz" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_additional_commands(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "env-with-extras"
    dependencies: Dependencies = {"pip": ["tiny-package"]}
    additional_commands: Commands = {
        "all": ["echo 'hello world'"],
        "linux": ["specific command"],
    }

    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=False))
    monkeypatch.setattr(platform, "system", MagicMock(return_value="Linux"))

    manager.create(env_name, dependencies=dependencies, additionalInstallCommands=additional_commands)

    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    assert any(f"create -n {env_name}" in cmd for cmd in command_list)
    assert any("tiny-package" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)
    assert "echo 'hello world'" in command_list
    assert "specific command" in command_list


def test_create_invalid_python_version_raises(environment_manager_fixture, monkeypatch):
    manager, _, _ = environment_manager_fixture
    env_name = "invalid-py-env"
    dependencies: Dependencies = {"python": "=3.8"}

    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=False))

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

    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=False))
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


# ---- create Tests (useExisting parameter) ----


def test_create_with_use_existing_returns_main_env(environment_manager_fixture, monkeypatch):
    """Test that useExisting=True returns main environment if it satisfies dependencies."""
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["numpy>=1.20"]}

    # Mock _environmentValidatesRequirements to return True for main environment
    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=True))

    env = manager.create("new_env", dependencies=dependencies, useExisting=True)

    # Should return main environment instead of creating new one
    assert env is manager.mainEnvironment
    # Should not execute any creation commands
    mock_execute_output.assert_not_called()


def test_create_with_use_existing_returns_existing_env(environment_manager_fixture, monkeypatch):
    """Test that useExisting=True returns an existing environment if it satisfies dependencies."""
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["numpy>=1.20"]}

    # Create an existing environment
    existing_env = ExternalEnvironment("existing_env", Path("some/path"), manager)
    manager.environments["existing_env"] = existing_env

    # Mock _environmentValidatesRequirements: return False for main, True for existing
    def mock_validates(env, deps):
        return env is existing_env

    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(side_effect=mock_validates))

    env = manager.create("new_env", dependencies=dependencies, useExisting=True)

    # Should return the existing environment
    assert env is existing_env
    # Should not execute any creation commands
    mock_execute_output.assert_not_called()


def test_create_with_use_existing_creates_new_if_none_satisfy(environment_manager_fixture, monkeypatch):
    """Test that useExisting=True creates new env if no existing env satisfies dependencies."""
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["numpy>=2.0"]}

    # Create an existing environment that doesn't satisfy
    existing_env = ExternalEnvironment("existing_env", Path("some/path"), manager)
    manager.environments["existing_env"] = existing_env

    # Mock _environmentValidatesRequirements to always return False
    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    env = manager.create("new_env", dependencies=dependencies, useExisting=True)

    # Should create a new environment
    assert isinstance(env, ExternalEnvironment)
    assert env.name == "new_env"
    # Should execute creation commands
    mock_execute_output.assert_called()


def test_create_with_use_existing_false_skips_environment_checks(environment_manager_fixture, monkeypatch):
    """Test that useExisting=False always creates a new environment."""
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["numpy"]}

    # Create existing environments
    env1 = ExternalEnvironment("env1", Path("path1"), manager)
    manager.environments["env1"] = env1

    # Mock _environmentValidatesRequirements to return True (would match if checked)
    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(return_value=True))

    env = manager.create("new_env", dependencies=dependencies, useExisting=False)

    # Should create a new environment, not return existing ones
    assert isinstance(env, ExternalEnvironment)
    assert env.name == "new_env"
    mock_execute_output.assert_called()


def test_create_with_use_existing_returns_first_match(environment_manager_fixture, monkeypatch):
    """Test that useExisting=True returns the first environment that satisfies deps."""
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["numpy"]}

    # Create multiple environments, the second one satisfies deps
    env1 = ExternalEnvironment("env1", Path("path1"), manager)
    env2 = ExternalEnvironment("env2", Path("path2"), manager)
    manager.environments["env1"] = env1
    manager.environments["env2"] = env2

    # Mock to return True only for env2
    def mock_validates(env, deps):
        return env is env2

    monkeypatch.setattr(manager, "_environmentValidatesRequirements", MagicMock(side_effect=mock_validates))

    env = manager.create("new_env", dependencies=dependencies, useExisting=True)

    # Should return the first (or one of the) matching environment
    assert env is env2
    mock_execute_output.assert_not_called()
