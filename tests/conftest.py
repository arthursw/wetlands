from unittest.mock import MagicMock
import pytest
from wetlands._internal.settings_manager import SettingsManager
from wetlands._internal.dependency_manager import DependencyManager


@pytest.fixture
def mock_settings_manager(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("conda_env")  # Creates a unique temp directory
    mock = MagicMock(spec=SettingsManager)
    mock.getCondaPaths.return_value = (temp_dir, "micromamba")
    mock.getProxyEnvironmentVariablesCommands.return_value = []
    mock.getProxyString.return_value = None
    mock.condaBin = "micromamba"
    mock.condaBinConfig = "micromamba --rc-file ~/.mambarc"
    return mock


@pytest.fixture
def mock_dependency_manager():
    return MagicMock(spec=DependencyManager)
