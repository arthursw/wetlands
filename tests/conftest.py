from pathlib import Path
from unittest.mock import MagicMock
import sys
import pytest
from wetlands._internal.settings_manager import SettingsManager
from wetlands._internal.command_generator import CommandGenerator
from wetlands._internal.dependency_manager import DependencyManager
import wetlands._internal.install as install_module
import wetlands.environment_manager as env_mgr_module


def pytest_addoption(parser):
    """Add custom pytest command-line options."""
    parser.addoption(
        "--skip-micromamba",
        action="store_true",
        default=False,
        help="Skip micromamba tests to speed up development (run pixi tests only)",
    )


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: mark test as an integration test that should not be mocked")
    config.addinivalue_line("markers", "micromamba: mark test as micromamba-specific")


def pytest_generate_tests(metafunc):
    """Filter parametrized tests based on --skip-micromamba flag."""
    if metafunc.config.getoption("--skip-micromamba"):
        # If test uses env_manager fixture with micromamba parameter, skip micromamba variants
        if "env_manager" in metafunc.fixturenames:
            # This marks that we should skip micromamba for this test
            metafunc.config._skip_micromamba_tests = True


def pytest_collection_modifyitems(config, items):
    """Skip tests that use micromamba parameter when --skip-micromamba is set."""
    if config.getoption("--skip-micromamba"):
        skip_micromamba = pytest.mark.skip(reason="Skipped with --skip-micromamba flag")
        for item in items:
            # Check if the test is parametrized with micromamba
            if "micromamba_root/" in item.nodeid:
                item.add_marker(skip_micromamba)


# Store original functions before mocking

_original_install_micromamba_env = env_mgr_module.installMicromamba
_original_install_pixi_env = env_mgr_module.installPixi
_original_install_micromamba = install_module.installMicromamba
_original_install_pixi = install_module.installPixi

# Store originals for test_installer module if it's imported
try:
    import tests.test_installer as test_installer_module

    _original_install_micromamba_test = test_installer_module.installMicromamba
    _original_install_pixi_test = test_installer_module.installPixi
except (ImportError, AttributeError):
    _original_install_micromamba_test = None
    _original_install_pixi_test = None


def create_mock_micromamba():
    """Create a mock installMicromamba function."""

    def mock_install_micromamba(install_path: Path, version: str = "2.3.0-1", proxies=None):
        """Mock installation - creates a fake micromamba binary without downloading."""
        install_path.mkdir(exist_ok=True, parents=True)
        bin_path = install_path / "bin" / "micromamba"
        bin_path.parent.mkdir(exist_ok=True, parents=True)
        # Create a fake executable file that mimics the real binary
        bin_path.write_text("#!/bin/bash\necho 'micromamba version 2.3.0'\n")
        bin_path.chmod(0o755)
        return bin_path

    return mock_install_micromamba


def create_mock_pixi():
    """Create a mock installPixi function."""

    def mock_install_pixi(install_path: Path, version: str = "v0.48.2", proxies=None):
        """Mock installation - creates a fake pixi binary without downloading."""
        install_path.mkdir(exist_ok=True, parents=True)
        bin_path = install_path / "bin" / "pixi"
        bin_path.parent.mkdir(exist_ok=True, parents=True)
        # Create a fake executable file that mimics the real binary
        bin_path.write_text("#!/bin/bash\necho 'pixi v0.48.2'\n")
        bin_path.chmod(0o755)
        return bin_path

    return mock_install_pixi


def pytest_runtest_setup(item):
    """Mock download functions before each test, unless marked as integration."""
    # Check if test is marked with integration marker
    if "integration" in item.keywords:
        return

    # Create mock functions
    mock_micromamba = create_mock_micromamba()
    mock_pixi = create_mock_pixi()

    # Apply mocks in all places where the functions are imported
    install_module.installMicromamba = mock_micromamba
    install_module.installPixi = mock_pixi
    env_mgr_module.installMicromamba = mock_micromamba
    env_mgr_module.installPixi = mock_pixi

    # Also mock in test_installer module if it has been imported using sys.modules
    for module_name in ["test_installer", "tests.test_installer"]:
        if module_name in sys.modules:
            test_installer_module = sys.modules[module_name]
            if hasattr(test_installer_module, "installMicromamba"):
                test_installer_module.installMicromamba = mock_micromamba  # type: ignore
            if hasattr(test_installer_module, "installPixi"):
                test_installer_module.installPixi = mock_pixi  # type: ignore


def pytest_runtest_teardown(item):
    """Restore original functions after each test."""
    # Restore original functions in all modules
    install_module.installMicromamba = _original_install_micromamba
    install_module.installPixi = _original_install_pixi
    env_mgr_module.installMicromamba = _original_install_micromamba_env
    env_mgr_module.installPixi = _original_install_pixi_env

    # Restore in test_installer module if it was imported
    if _original_install_micromamba_test is not None:
        for module_name in ["test_installer", "tests.test_installer"]:
            if module_name in sys.modules:
                test_installer_module = sys.modules[module_name]
                if hasattr(test_installer_module, "installMicromamba"):
                    test_installer_module.installMicromamba = _original_install_micromamba_test  # type: ignore
                if hasattr(test_installer_module, "installPixi"):
                    test_installer_module.installPixi = _original_install_pixi_test  # type: ignore


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
