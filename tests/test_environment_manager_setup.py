from unittest.mock import MagicMock

import pytest

from wetlands.environment_manager import EnvironmentManager


# ---- conda_path Tests ----


def test_environment_manager_conda_path(tmp_path_factory):
    """Check that an exception is raised if the provided CondaPath contains the name of another conda manager."""

    dummy_conda_path = tmp_path_factory.mktemp("path_containing_pixi_and_micromamba").resolve()
    wetlands_instance_path = tmp_path_factory.mktemp("wetlands_instance").resolve()

    # With manager="auto", it should raise because the path is ambiguous
    with pytest.raises(Exception, match="must contain either"):
        manager = EnvironmentManager(
            wetlandsInstancePath=wetlands_instance_path,
            condaPath=tmp_path_factory.mktemp("random_path").resolve(),
            manager="auto",
        )

    # With explicit manager, it should not raise based on path
    manager = EnvironmentManager(
        wetlandsInstancePath=wetlands_instance_path, condaPath=dummy_conda_path, manager="pixi"
    )
    assert manager is not None

    manager = EnvironmentManager(
        wetlandsInstancePath=wetlands_instance_path, condaPath=dummy_conda_path, manager="micromamba"
    )
    assert manager is not None


# ---- setCondaPath Tests ----


class TestSetCondaPath:
    def test_set_conda_path_updates_settings(self, tmp_path_factory, monkeypatch):
        """Test that setCondaPath updates the settings manager"""
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

        new_path = tmp_path_factory.mktemp("new_conda")

        mock_set_path = MagicMock()
        monkeypatch.setattr(manager.settingsManager, "setCondaPath", mock_set_path)

        manager.setCondaPath(new_path, usePixi=True)

        mock_set_path.assert_called_once_with(new_path, True)


# ---- setProxies Tests ----


class TestSetProxies:
    def test_set_proxies_calls_settings_manager(self, tmp_path_factory, monkeypatch):
        """Test that setProxies delegates to settings manager"""
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

        proxies = {"http": "http://proxy.example.com:8080", "https": "https://proxy.example.com:8443"}

        mock_set_proxies = MagicMock()
        monkeypatch.setattr(manager.settingsManager, "setProxies", mock_set_proxies)

        manager.setProxies(proxies)

        mock_set_proxies.assert_called_once_with(proxies)
