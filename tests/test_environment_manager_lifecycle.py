from pathlib import Path
from unittest.mock import MagicMock

import json5
import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment
from wetlands._internal.dependency_manager import Dependencies
from wetlands._internal.command_generator import Commands, CommandsDict


@pytest.fixture
def environment_manager_fixture(tmp_path_factory, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    import subprocess

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

    mock_execute = MagicMock(spec=subprocess.Popen)
    mock_execute_output = MagicMock(return_value=["output line 1", "output line 2"])

    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_execute)
    monkeypatch.setattr(manager.command_executor, "execute_commands_and_get_output", mock_execute_output)
    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    return manager


@pytest.fixture
def environment_manager_pixi_fixture(tmp_path_factory, monkeypatch):
    """Provides a Pixi EnvironmentManager instance with mocked CommandExecutor."""
    import subprocess

    dummy_pixi_path = tmp_path_factory.mktemp("pixi_root")
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance_pixi")

    monkeypatch.setattr(EnvironmentManager, "install_conda", MagicMock())

    manager = EnvironmentManager(
        wetlands_instance_path=wetlands_instance_path, conda_path=dummy_pixi_path, manager="pixi"
    )

    mock_execute = MagicMock(spec=subprocess.Popen)
    mock_execute_output = MagicMock(return_value=["output line 1", "output line 2"])

    monkeypatch.setattr(manager.command_executor, "execute_commands", mock_execute)
    monkeypatch.setattr(manager.command_executor, "execute_commands_and_get_output", mock_execute_output)
    monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

    return manager


# ---- register_environment Tests ----


class TestRegisterEnvironment:
    def test_register_environment_creates_debug_ports_file(self, environment_manager_fixture, tmp_path, monkeypatch):
        """Test that register_environment creates debug_ports.json"""
        manager = environment_manager_fixture
        manager.wetlands_instance_path = tmp_path / "wetlands"

        mock_process = MagicMock()
        mock_process.pid = 12345
        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), manager)
        env.process = mock_process

        debug_port = 5678
        executor_path = Path("/path/to/module_executor.py")

        manager.register_environment(env, debug_port, executor_path)

        debug_ports_file = tmp_path / "wetlands" / "debug_ports.json"
        assert debug_ports_file.exists()

        with open(debug_ports_file, "r") as f:
            content = json5.load(f)

        assert "test_env" in content
        assert content["test_env"]["debug_port"] == 5678
        assert content["test_env"]["module_executor_path"] == "/path/to/module_executor.py"

    def test_register_environment_appends_to_existing_file(self, environment_manager_fixture, tmp_path):
        """Test that register_environment appends to existing debug_ports.json"""
        manager = environment_manager_fixture
        manager.wetlands_instance_path = tmp_path / "wetlands"

        # Create existing file with one env
        debug_ports_dir = tmp_path / "wetlands"
        debug_ports_dir.mkdir(exist_ok=True, parents=True)
        debug_ports_file = debug_ports_dir / "debug_ports.json"

        existing_data = {"env1": {"debug_port": 1111, "module_executor_path": "/path/to/exec1"}}
        with open(debug_ports_file, "w") as f:
            json5.dump(existing_data, f, index=4, quote_keys=True)

        # Register new environment
        mock_process = MagicMock()
        env = ExternalEnvironment("env2", Path("/tmp/env2"), manager)
        env.process = mock_process

        manager.register_environment(env, 2222, Path("/path/to/exec2"))

        # Verify both envs are in file
        with open(debug_ports_file, "r") as f:
            content = json5.load(f)

        assert len(content) == 2
        assert content["env1"]["debug_port"] == 1111
        assert content["env2"]["debug_port"] == 2222

    def test_register_environment_overwrites_existing_env(self, environment_manager_fixture, tmp_path):
        """Test that register_environment overwrites entry for existing environment"""
        manager = environment_manager_fixture
        manager.wetlands_instance_path = tmp_path / "wetlands"

        debug_ports_dir = tmp_path / "wetlands"
        debug_ports_dir.mkdir(exist_ok=True, parents=True)
        debug_ports_file = debug_ports_dir / "debug_ports.json"

        # Create file with old data
        old_data = {"test_env": {"debug_port": 9999, "module_executor_path": "/old/path"}}
        with open(debug_ports_file, "w") as f:
            json5.dump(old_data, f, index=4, quote_keys=True)

        # Register with new data
        mock_process = MagicMock()
        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), manager)
        env.process = mock_process

        manager.register_environment(env, 5555, Path("/new/path"))

        with open(debug_ports_file, "r") as f:
            content = json5.load(f)

        assert content["test_env"]["debug_port"] == 5555
        assert content["test_env"]["module_executor_path"] == "/new/path"

    def test_register_environment_with_no_process(self, environment_manager_fixture):
        """Test that register_environment returns early if process is None"""
        manager = environment_manager_fixture

        env = ExternalEnvironment("test_env", Path("/tmp/test_env"), manager)
        env.process = None  # No process

        # Should return without error
        manager.register_environment(env, 5678, Path("/path/to/executor"))


# ---- _remove_environment Tests ----


class TestRemoveEnvironment:
    def test_remove_environment_existing(self, environment_manager_fixture):
        """Test that _remove_environment removes environment from dict"""
        manager = environment_manager_fixture
        env_name = "test_env"
        env = ExternalEnvironment(env_name, Path("/tmp/test_env"), manager)
        manager.environments[env_name] = env

        assert env_name in manager.environments
        manager._remove_environment(env)
        assert env_name not in manager.environments

    def test_remove_environment_non_existing(self, environment_manager_fixture):
        """Test that _remove_environment handles non-existing environment gracefully"""
        manager = environment_manager_fixture
        env = ExternalEnvironment("non_existing", Path("/tmp/non_existing"), manager)

        # Should not raise error
        manager._remove_environment(env)


# ---- delete Tests ----


class TestDeleteEnvironment:
    """Tests for deleting environments."""

    def test_delete_nonexistent_environment_micromamba(self, environment_manager_fixture, monkeypatch):
        """Test that delete raises an error when trying to delete a nonexistent environment."""
        manager = environment_manager_fixture
        env_name = "nonexistent-env"

        # Mock environment_exists to return False
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

        # Create external environment and call delete on it
        env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)

        # Should raise exception
        with pytest.raises(Exception, match="does not exist"):
            env.delete()

    def test_delete_existing_environment_micromamba(self, environment_manager_fixture, monkeypatch, tmp_path):
        """Test that delete removes an existing environment (micromamba)."""
        manager = environment_manager_fixture
        env_name = "test-env"
        env_path = tmp_path / env_name

        # Mock environment_exists to return True
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Mock send2trash
        mock_send2trash = MagicMock()
        monkeypatch.setattr("wetlands.external_environment.send2trash", mock_send2trash)

        # Create external environment
        env = ExternalEnvironment(env_name, env_path, manager)
        manager.environments[env_name] = env

        env.delete()

        # Should call send2trash with environment path (not parent for micromamba)
        mock_send2trash.assert_called_once_with(env_path)
        # Should be removed from environments dict
        assert env_name not in manager.environments

    def test_delete_existing_environment_pixi(self, environment_manager_pixi_fixture, monkeypatch, tmp_path):
        """Test that delete removes an existing environment (pixi)."""
        manager = environment_manager_pixi_fixture
        env_name = "test-env"
        env_path = tmp_path / env_name

        # Mock environment_exists to return True
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Mock send2trash
        mock_send2trash = MagicMock()
        monkeypatch.setattr("wetlands.external_environment.send2trash", mock_send2trash)

        # Create external environment
        env = ExternalEnvironment(env_name, env_path, manager)
        manager.environments[env_name] = env

        env.delete()

        # For pixi, it should call send2trash with parent directory
        mock_send2trash.assert_called_once_with(env_path.parent)

    def test_delete_environment_exits_external_environment(self, environment_manager_fixture, monkeypatch, tmp_path):
        """Test that delete properly exits an external environment if it's running."""
        manager = environment_manager_fixture
        env_name = "running-env"
        env_path = tmp_path / env_name

        # Mock environment_exists
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Mock send2trash
        mock_send2trash = MagicMock()
        monkeypatch.setattr("wetlands.external_environment.send2trash", mock_send2trash)

        # Create external environment
        env = ExternalEnvironment(env_name, env_path, manager)
        manager.environments[env_name] = env

        # Mock launched to return True
        mock_launched = MagicMock(return_value=True)
        monkeypatch.setattr(env, "launched", mock_launched)

        # Mock _exit
        mock_exit = MagicMock()
        monkeypatch.setattr(env, "_exit", mock_exit)

        env.delete()

        # Should call _exit on the environment
        mock_exit.assert_called_once()
        # Should call send2trash with environment path
        mock_send2trash.assert_called_once_with(env_path)
        # Should be removed from environments dict
        assert env_name not in manager.environments


# ---- update Tests ----


class TestUpdateEnvironment:
    """Tests for updating environments."""

    def test_update_nonexistent_environment_raises_error(self, environment_manager_fixture, monkeypatch):
        """Test that update raises an error when trying to update a nonexistent environment."""
        manager = environment_manager_fixture
        env_name = "nonexistent-env"
        dependencies: Dependencies = {"pip": ["numpy"]}

        # Mock environment_exists to return False
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=False))

        # Create external environment and call update on it
        env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)

        # Should raise exception
        with pytest.raises(Exception, match="does not exist"):
            env.update(dependencies)

    def test_update_environment_with_dependencies_dict(self, environment_manager_fixture, monkeypatch):
        """Test that update deletes and recreates an environment with new dependencies."""
        manager = environment_manager_fixture
        env_name = "test-env"
        new_dependencies: Dependencies = {"pip": ["numpy==1.0"]}

        # Mock environment_exists to return True
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Create external environment
        env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        manager.environments[env_name] = env

        # Mock delete on the environment
        delete_mock = MagicMock()
        monkeypatch.setattr(env, "delete", delete_mock)

        # Mock manager.create to return a new environment
        new_env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        create_mock = MagicMock(return_value=new_env)
        monkeypatch.setattr(manager, "create", create_mock)

        result = env.update(new_dependencies)

        # Should call delete
        delete_mock.assert_called_once()
        # Should call create with new dependencies
        create_mock.assert_called_once()
        call_args, call_kwargs = create_mock.call_args
        assert call_args[0] == env_name
        assert call_kwargs.get("dependencies") == new_dependencies
        # Should return the created environment
        assert result == new_env

    def test_update_environment_with_additional_install_commands(self, environment_manager_fixture, monkeypatch):
        """Test that update works with additional install commands."""
        manager = environment_manager_fixture
        env_name = "test-env"
        dependencies: Dependencies = {"pip": ["requests>=2.25"]}
        additional_commands: Commands = CommandsDict({"all": ["echo 'installing'"], "mac": ["echo 'macOS only'"]})

        # Mock environment_exists
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Create external environment
        env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        manager.environments[env_name] = env

        # Mock delete on the environment
        delete_mock = MagicMock()
        monkeypatch.setattr(env, "delete", delete_mock)

        # Mock manager.create to return a new environment
        new_env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        create_mock = MagicMock(return_value=new_env)
        monkeypatch.setattr(manager, "create", create_mock)

        result = env.update(dependencies, additional_install_commands=additional_commands)

        # Should call delete
        delete_mock.assert_called_once()
        # Should call create with new dependencies and additional commands
        create_mock.assert_called_once()
        call_args, call_kwargs = create_mock.call_args
        assert call_args[0] == env_name
        assert call_kwargs.get("dependencies") == dependencies
        assert call_kwargs.get("additional_install_commands") == additional_commands
        # Should return the created environment
        assert result == new_env

    def test_update_with_force_external(self, environment_manager_fixture, monkeypatch):
        """Test that update works with use_existing flag to False."""
        manager = environment_manager_fixture
        env_name = "test-env"
        dependencies: Dependencies = {"pip": ["numpy>=1.20"]}

        # Mock environment_exists
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Create external environment
        env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        manager.environments[env_name] = env

        # Mock delete on the environment
        delete_mock = MagicMock()
        monkeypatch.setattr(env, "delete", delete_mock)

        # Mock manager.create to return a new environment
        new_env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        create_mock = MagicMock(return_value=new_env)
        monkeypatch.setattr(manager, "create", create_mock)

        result = env.update(dependencies, use_existing=False)

        # Should call delete
        delete_mock.assert_called_once()
        # Should call create with use_existing flag
        create_mock.assert_called_once()
        call_args, call_kwargs = create_mock.call_args
        assert call_args[0] == env_name
        assert call_kwargs.get("dependencies") == dependencies
        assert call_kwargs.get("use_existing") is False
        # Should return the created environment
        assert result == new_env

    def test_update_with_additional_commands(self, environment_manager_fixture, monkeypatch):
        """Test that update passes additional install commands through to create."""
        manager = environment_manager_fixture
        env_name = "test-env"
        dependencies: Dependencies = {"pip": ["requests"]}
        additional_commands: Commands = {"mac": ["echo 'test'"]}

        # Mock environment_exists
        monkeypatch.setattr(manager, "environment_exists", MagicMock(return_value=True))

        # Create external environment
        env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        manager.environments[env_name] = env

        # Mock delete on the environment
        delete_mock = MagicMock()
        monkeypatch.setattr(env, "delete", delete_mock)

        # Mock manager.create
        new_env = ExternalEnvironment(env_name, Path(f"/tmp/{env_name}"), manager)
        create_mock = MagicMock(return_value=new_env)
        monkeypatch.setattr(manager, "create", create_mock)

        env.update(dependencies, additional_install_commands=additional_commands)

        # Should pass additional commands to create
        create_mock.assert_called_once()
        call_args = create_mock.call_args
        assert call_args[1].get("additional_install_commands") == additional_commands


# ---- Environment Access Tests ----


class TestExistingEnvironmentAccess:
    """Tests for accessing existing environments via Path objects."""

    def test_environment_exists_with_path_not_found(self, tmp_path):
        """Test that environment_exists() returns False for nonexistent Path."""
        nonexistent = tmp_path / "nonexistent"
        manager = EnvironmentManager(
            wetlands_instance_path=tmp_path / "wetlands", conda_path=tmp_path, manager="micromamba"
        )
        assert not manager.environment_exists(nonexistent)

    def test_load_nonexistent_path_raises_error(self, tmp_path_factory):
        """Test that load() raises an error when given a nonexistent Path."""
        tmp = tmp_path_factory.mktemp("conda_root")
        wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance_test")
        nonexistent = tmp / "nonexistent"

        manager = EnvironmentManager(
            wetlands_instance_path=wetlands_instance_path, conda_path=tmp, manager="micromamba"
        )

        # Should raise because the environment doesn't exist
        with pytest.raises(Exception, match="was not found"):
            manager.load(name="test_env", environment_path=nonexistent)
