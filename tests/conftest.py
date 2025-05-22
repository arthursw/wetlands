from pathlib import Path
from unittest.mock import MagicMock
import pytest
from wetlands._internal.settings_manager import SettingsManager
from wetlands._internal.command_generator import CommandGenerator
from wetlands._internal.dependency_manager import DependencyManager


@pytest.fixture
def mock_settings_manager_micromamba(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("conda_env")  # Creates a unique temp directory
    mock = MagicMock(spec=SettingsManager)
    mock.usePixi = False
    mock.getCondaPaths.return_value = (temp_dir, Path("bin/micromamba"))
    mock.getProxyEnvironmentVariablesCommands.return_value = []
    mock.getProxyString.return_value = None
    mock.condaBin = "micromamba"
    mock.condaBinConfig = "micromamba --rc-file ~/.mambarc"
    return mock


@pytest.fixture
def mock_settings_manager_pixi(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("conda_env")  # Creates a unique temp directory
    mock = MagicMock(spec=SettingsManager)
    mock.usePixi = True
    mock.getCondaPaths.return_value = (temp_dir, Path("bin/pixi"))
    mock.getProxyEnvironmentVariablesCommands.return_value = []
    mock.getProxyString.return_value = None
    mock.condaBin = "pixi"
    mock.condaBinConfig = "pixi --manifest-path pixi.toml"
    return mock


@pytest.fixture
def mock_dependency_manager():
    return MagicMock(spec=DependencyManager)


@pytest.fixture
def mock_command_generator_micromamba(mock_settings_manager_micromamba):
    return CommandGenerator(mock_settings_manager_micromamba)


@pytest.fixture
def mock_command_generator_pixi(mock_settings_manager_pixi):
    return CommandGenerator(mock_settings_manager_pixi)
