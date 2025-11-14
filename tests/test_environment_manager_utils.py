from pathlib import Path
from unittest.mock import MagicMock, patch

from wetlands._internal.dependency_manager import Dependencies
from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment


# ---- _removeChannel Tests ----


class TestRemoveChannel:
    def test_remove_channel_with_channel(self):
        """Test that _removeChannel removes channel prefix"""
        manager = MagicMock()
        manager._removeChannel = EnvironmentManager._removeChannel.__get__(manager)

        result = manager._removeChannel("conda-forge::numpy==1.2.3")
        assert result == "numpy==1.2.3"

    def test_remove_channel_without_channel(self):
        """Test that _removeChannel returns unchanged string if no channel"""
        manager = MagicMock()
        manager._removeChannel = EnvironmentManager._removeChannel.__get__(manager)

        result = manager._removeChannel("numpy==1.2.3")
        assert result == "numpy==1.2.3"


# ---- _addDebugpyInDependencies Tests ----


class TestAddDebugpyInDependencies:
    def test_add_debugpy_in_debug_mode(self):
        """Test that debugpy is added when debug mode is enabled"""
        manager = EnvironmentManager(
            wetlandsInstancePath=Path("/tmp/wetlands_test"),
            condaPath=Path("/tmp/test_conda"),
            manager="micromamba",
            debug=True,
        )

        with patch.object(manager, "installConda"):
            dependencies = Dependencies({"pip": ["numpy"]})
            manager._addDebugpyInDependencies(dependencies)

            assert "conda" in dependencies
            assert "debugpy" in dependencies["conda"]

    def test_add_debugpy_not_in_debug_mode(self):
        """Test that debugpy is not added when debug mode is disabled"""
        manager = EnvironmentManager(
            wetlandsInstancePath=Path("/tmp/wetlands_test2"),
            condaPath=Path("/tmp/test_conda"),
            manager="micromamba",
            debug=False,
        )

        with patch.object(manager, "installConda"):
            dependencies = Dependencies({"pip": ["numpy"]})
            manager._addDebugpyInDependencies(dependencies)

            # Conda deps should not be added if not in debug mode
            if "conda" in dependencies:
                assert "debugpy" not in dependencies.get("conda", [])

    def test_add_debugpy_already_present(self):
        """Test that debugpy is not added twice if already present"""
        manager = EnvironmentManager(
            wetlandsInstancePath=Path("/tmp/wetlands_test3"),
            condaPath=Path("/tmp/test_conda"),
            manager="micromamba",
            debug=True,
        )

        with patch.object(manager, "installConda"):
            dependencies = Dependencies({"conda": ["debugpy==1.0.0"]})
            manager._addDebugpyInDependencies(dependencies)

            # Check debugpy appears only once
            count = sum(1 for dep in dependencies["conda"] if "debugpy" in str(dep))  # type: ignore
            assert count == 1


# ---- getInstalledPackages Tests ----


class TestGetInstalledPackages:
    def test_get_installed_packages_conda(self, tmp_path_factory, monkeypatch):
        """Test getting installed packages from conda environment"""
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
        manager.settingsManager.usePixi = False

        mock_packages = [
            {"name": "numpy", "version": "1.2.3", "kind": "conda"},
            {"name": "pandas", "version": "2.0.0", "kind": "conda"},
        ]
        manager.commandExecutor.executeCommandAndGetJsonOutput = MagicMock(return_value=mock_packages)
        manager.commandExecutor.executeCommandsAndGetOutput = MagicMock(return_value=[])

        # Create an environment object
        env_name = "test_env"
        env = ExternalEnvironment(env_name, manager.settingsManager.getEnvironmentPathFromName(env_name), manager)
        manager.environments[env_name] = env

        result = manager.getInstalledPackages(env)

        # Should include conda packages
        assert any(p["name"] == "numpy" for p in result)


# ---- _checkRequirement Tests ----


class TestCheckRequirement:
    def test_check_requirement_conda_installed(self, tmp_path_factory, monkeypatch):
        """Test checking if conda requirement is met"""
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

        installed_packages = [
            {"name": "numpy", "version": "1.2.3", "kind": "conda"},
        ]

        result = manager._checkRequirement("numpy==1.2.3", "conda", installed_packages)
        assert result is True

    def test_check_requirement_conda_version_mismatch(self, tmp_path_factory, monkeypatch):
        """Test checking conda requirement with version mismatch"""
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

        installed_packages = [
            {"name": "numpy", "version": "1.2.3", "kind": "conda"},
        ]

        result = manager._checkRequirement("numpy==2.0.0", "conda", installed_packages)
        assert result is False

    def test_check_requirement_pip_installed(self, tmp_path_factory, monkeypatch):
        """Test checking if pip requirement is met"""
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

        installed_packages = [
            {"name": "requests", "version": "2.28.0", "kind": "pypi"},
        ]

        result = manager._checkRequirement("requests==2.28.0", "pip", installed_packages)
        assert result is True

    def test_check_requirement_removes_channel(self, tmp_path_factory, monkeypatch):
        """Test that channel is removed before checking"""
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

        installed_packages = [
            {"name": "numpy", "version": "1.2.3", "kind": "conda"},
        ]

        result = manager._checkRequirement("conda-forge::numpy==1.2.3", "conda", installed_packages)
        assert result is True
