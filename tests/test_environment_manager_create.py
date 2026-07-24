import platform
import re
import shutil
from pathlib import Path
from unittest.mock import MagicMock
import subprocess

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment
from wetlands.exceptions import EnvironmentReuseError
from wetlands._internal.dependency_manager import Dependencies
from wetlands._internal.command_generator import Commands
from wetlands._internal.environment_metadata import (
    ENVIRONMENT_METADATA_SCHEMA_VERSION,
    MANAGED_STATUS,
    environment_metadata_path,
    read_environment_metadata,
    write_environment_metadata,
)
from wetlands._internal.shell import shell_quote


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

    return manager, mock_command_executor["execute_commands"], mock_command_executor["execute_commands_and_get_output"]


@pytest.fixture
def environment_manager_pixi_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor for Pixi."""
    dummy_pixi_path = tmp_path_factory.mktemp("pixi_root")
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance_pixi")

    monkeypatch.setattr(EnvironmentManager, "install_conda", MagicMock())

    manager = EnvironmentManager(
        wetlands_instance_path=wetlands_instance_path, conda_path=dummy_pixi_path, manager="pixi"
    )

    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_command_executor["execute_commands"])
    monkeypatch.setattr(
        manager.command_executor,
        "execute_commands_and_get_output",
        mock_command_executor["execute_commands_and_get_output"],
    )

    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    return manager, mock_command_executor["execute_commands"], mock_command_executor["execute_commands_and_get_output"]


# ---- create Tests (micromamba) ----


def test_create_does_not_scan_other_environments_for_matching_dependencies(environment_manager_fixture, monkeypatch):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "always-new-env"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=True))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    manager._environment_validates_requirements.assert_not_called()
    mock_execute.assert_called()


def test_create_dependencies_not_met_create_external(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "new-external-env"
    dependencies: Dependencies = {"conda": ["requests"], "pip": ["pandas"]}

    # Mock _environment_validates_requirements to return False
    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    assert env is manager.environments[env_name]
    mock_execute_output.assert_called()

    # Check for key commands
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    current_py_version = platform.python_version()
    assert any(f"create -n {shell_quote(env_name)} python={current_py_version} -y" in cmd for cmd in command_list)
    assert any(f"install" in cmd for cmd in command_list if "micromamba" in cmd)
    assert any("requests" in cmd for cmd in command_list if "install" in cmd)
    assert any("pandas" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_includes_local_dependency_commands(environment_manager_fixture, monkeypatch, tmp_path):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "local-package-env"
    local_path = tmp_path / "local package"
    local_path.mkdir()
    dependencies: Dependencies = {
        "local": [
            {"name": "local-package", "path": local_path},
        ],
    }

    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

    manager.create(env_name, dependencies=dependencies)

    mock_execute.assert_called()
    called_args, _ = mock_execute.call_args
    command_list = called_args[0]
    assert any(f"pip install  -e {shell_quote(local_path.resolve())}" == cmd for cmd in command_list)


def test_create_raises_and_does_not_report_success_when_install_commands_fail(
    environment_manager_fixture, monkeypatch, caplog
):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "broken-env"
    dependencies: Dependencies = {"pip": ["missing-package==0"]}
    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))
    mock_execute.side_effect = Exception("dependency solving failed")

    with caplog.at_level("INFO"):
        with pytest.raises(Exception, match="dependency solving failed"):
            manager.create(env_name, dependencies=dependencies)

    assert env_name not in manager.environments
    assert f"Environment '{env_name}' created successfully" not in caplog.text


def test_create_writes_metadata_after_success(environment_manager_fixture):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "metadata-env"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    assert env.path is not None
    mock_execute.assert_called_once()
    metadata, reason = read_environment_metadata(env.path, use_pixi=False)
    assert reason is None
    assert metadata is not None
    assert metadata["status"] == MANAGED_STATUS
    assert metadata["name"] == env_name
    assert metadata["recipe_hash"].startswith("sha256:")
    assert metadata["recipe"]["dependencies"]["pip"] == ["numpy==1.2.3"]


def test_create_same_name_same_recipe_reuses_existing_environment(environment_manager_fixture, monkeypatch):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "reuse-same-recipe"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}
    monkeypatch.setattr(
        manager,
        "get_installed_packages",
        MagicMock(return_value=[{"name": "numpy", "version": "1.2.3", "kind": "pypi"}]),
    )

    env = manager.create(env_name, dependencies=dependencies)
    mock_execute.reset_mock()

    reused = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    assert reused is env
    mock_execute.assert_not_called()


def test_create_same_name_same_recipe_reuses_disk_environment_after_manager_forgets_it(
    environment_manager_fixture, monkeypatch
):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "reuse-disk-recipe"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}
    monkeypatch.setattr(
        manager,
        "get_installed_packages",
        MagicMock(return_value=[{"name": "numpy", "version": "1.2.3", "kind": "pypi"}]),
    )

    env = manager.create(env_name, dependencies=dependencies)
    manager.environments.pop(env_name)
    manager.environment_exists.return_value = True
    mock_execute.reset_mock()

    reused = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    assert isinstance(reused, ExternalEnvironment)
    assert reused is manager.environments[env_name]
    assert reused is not env
    assert reused.path == env.path
    mock_execute.assert_not_called()


def test_create_same_recipe_rejects_incompatible_micromamba_environment(environment_manager_fixture, monkeypatch):
    manager, _, _ = environment_manager_fixture
    env_name = "incompatible-micromamba-env"
    dependencies: Dependencies = {"pip": ["bioimageflow-core>=0.2"]}
    monkeypatch.setattr(
        manager,
        "get_installed_packages",
        MagicMock(return_value=[{"name": "bioimageflow-core", "version": "0.1.7", "kind": "pypi"}]),
    )

    manager.create(env_name, dependencies=dependencies)

    with pytest.raises(EnvironmentReuseError, match="installed packages no longer satisfy"):
        manager.create(env_name, dependencies=dependencies)


def test_create_same_recipe_rejects_incompatible_pixi_environment(environment_manager_pixi_fixture, monkeypatch):
    manager, _, _ = environment_manager_pixi_fixture
    env_name = "incompatible-pixi-env"
    dependencies: Dependencies = {"pip": ["bioimageflow-core>=0.2"]}
    monkeypatch.setattr(
        manager,
        "get_installed_packages",
        MagicMock(return_value=[{"name": "bioimageflow-core", "version": "0.1.7", "kind": "pypi"}]),
    )

    environment = manager.create(env_name, dependencies=dependencies)
    assert environment.path is not None
    environment.path.touch()

    with pytest.raises(EnvironmentReuseError, match="installed packages no longer satisfy"):
        manager.create(env_name, dependencies=dependencies)


def test_create_same_name_different_recipe_raises(environment_manager_fixture):
    manager, _, _ = environment_manager_fixture
    env_name = "reuse-different-recipe"

    manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    with pytest.raises(EnvironmentReuseError, match="already exists"):
        manager.create(env_name, dependencies={"pip": ["numpy==2.0.0"]})


def test_create_same_name_missing_metadata_raises(environment_manager_fixture):
    manager, _, _ = environment_manager_fixture
    env_name = "missing-metadata"
    env = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})
    environment_metadata_path(env.path, use_pixi=False).unlink()

    with pytest.raises(EnvironmentReuseError, match="metadata"):
        manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})


def test_create_unreusable_disk_environment_is_not_cached(environment_manager_fixture):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "uncached-missing-metadata"
    manager.environment_exists.return_value = True

    with pytest.raises(EnvironmentReuseError, match="metadata"):
        manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    assert env_name not in manager.environments

    manager.environment_exists.return_value = False
    replacement = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    assert replacement is manager.environments[env_name]
    mock_execute.assert_called_once()


def test_create_recreates_registered_default_environment_removed_on_disk(environment_manager_fixture):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "removed-registered-env"
    env = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})
    shutil.rmtree(env.path)
    mock_execute.reset_mock()

    replacement = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    assert replacement is manager.environments[env_name]
    assert replacement is not env
    mock_execute.assert_called_once()


def test_create_same_name_unmanaged_metadata_raises(environment_manager_fixture):
    manager, _, _ = environment_manager_fixture
    env_name = "unmanaged-env"
    env = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})
    metadata, reason = read_environment_metadata(env.path, use_pixi=False)
    assert reason is None
    assert metadata is not None
    metadata["status"] = "unmanaged"
    write_environment_metadata(env.path, use_pixi=False, metadata=metadata)

    with pytest.raises(EnvironmentReuseError, match="unmanaged"):
        manager.create(env_name, dependencies={"pip": ["numpy==2.0.0"]})


def test_create_same_name_invalid_metadata_raises(environment_manager_fixture):
    manager, _, _ = environment_manager_fixture
    env_name = "invalid-metadata-env"
    env = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})
    write_environment_metadata(
        env.path,
        use_pixi=False,
        metadata={"schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION},
    )

    with pytest.raises(EnvironmentReuseError, match="invalid_metadata"):
        manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})


def test_load_default_path_reuses_environment_without_recipe_validation(environment_manager_fixture):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "load-unmanaged-env"
    env = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})
    write_environment_metadata(
        env.path,
        use_pixi=False,
        metadata={"schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION},
    )
    manager.environments.pop(env_name)
    manager.environment_exists.return_value = True
    mock_execute.reset_mock()

    loaded = manager.load(env_name)

    assert isinstance(loaded, ExternalEnvironment)
    assert loaded.path == env.path
    assert loaded is manager.environments[env_name]
    mock_execute.assert_not_called()


def test_create_replace_existing_recreates_default_environment(environment_manager_fixture, monkeypatch):
    manager, mock_execute, _ = environment_manager_fixture
    env_name = "replace-env"
    env = manager.create(env_name, dependencies={"pip": ["numpy==1.2.3"]})

    def delete_existing():
        del manager.environments[env_name]

    delete_mock = MagicMock(side_effect=delete_existing)
    monkeypatch.setattr(env, "delete", delete_mock)
    mock_execute.reset_mock()

    replacement = manager.create(env_name, dependencies={"pip": ["numpy==2.0.0"]}, replace_existing=True)

    assert replacement is manager.environments[env_name]
    assert replacement is not env
    delete_mock.assert_called_once()
    mock_execute.assert_called_once()
    metadata, reason = read_environment_metadata(replacement.path, use_pixi=False)
    assert reason is None
    assert metadata is not None
    assert metadata["recipe"]["dependencies"]["pip"] == ["numpy==2.0.0"]


def test_create_replace_existing_refuses_loaded_non_default_path(environment_manager_fixture):
    manager, _, _ = environment_manager_fixture
    env_name = "loaded-env"
    manager.environments[env_name] = ExternalEnvironment(env_name, Path("/other/location/env"), manager)

    with pytest.raises(EnvironmentReuseError, match="non-default path"):
        manager.create(env_name, dependencies={}, replace_existing=True)


def test_create_with_python_version(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "py-versioned-env"
    py_version = "3.10.5"
    dependencies: Dependencies = {"python": f"={py_version}", "pip": ["toolz"]}

    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"create -n {shell_quote(env_name)} python={py_version} -y" in cmd for cmd in command_list)
    assert any("toolz" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_additional_commands(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "env-with-extras"
    dependencies: Dependencies = {"pip": ["tiny-package"]}
    additional_commands: Commands = {
        "all": ["echo 'hello world'"],
        "linux": ["specific command"],
    }

    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))
    monkeypatch.setattr(platform, "system", MagicMock(return_value="Linux"))

    manager.create(env_name, dependencies=dependencies, additional_install_commands=additional_commands)

    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    assert any(f"create -n {shell_quote(env_name)}" in cmd for cmd in command_list)
    assert any("tiny-package" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)
    assert "echo 'hello world'" in command_list
    assert "specific command" in command_list


def test_create_invalid_python_version_raises(environment_manager_fixture, monkeypatch):
    manager, _, _ = environment_manager_fixture
    env_name = "invalid-py-env"
    dependencies: Dependencies = {"python": "=3.8"}

    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))

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

    monkeypatch.setattr(manager, "_environment_validates_requirements", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

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
