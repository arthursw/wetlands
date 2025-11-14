import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands._internal.dependency_manager import Dependencies

# --- Fixtures (shared from conftest if needed) ---

conda_list_json = """
[
    {
        "base_url": "https://repo.anaconda.com/pkgs/main",
        "build_number": 1,
        "build_string": "h18a0788_1",
        "channel": "pkgs/main",
        "dist_name": "zlib-1.2.13-h18a0788_1",
        "name": "zlib",
        "platform": "osx-arm64",
        "version": "1.2.13"
    },
    {
        "base_url": "https://repo.anaconda.com/pkgs/main",
        "build_number": 0,
        "build_string": "py312h1a4646a_0",
        "channel": "pkgs/main",
        "dist_name": "zstandard-0.22.0-py312h1a4646a_0",
        "name": "zstandard",
        "platform": "osx-arm64",
        "version": "0.22.0"
    },
    {
        "base_url": "https://repo.anaconda.com/pkgs/main",
        "build_number": 2,
        "build_string": "hd90d995_2",
        "channel": "pkgs/main",
        "dist_name": "zstd-1.5.5-hd90d995_2",
        "name": "zstd",
        "platform": "osx-arm64",
        "version": "1.5.5"
    }
]
    """.splitlines()


@pytest.fixture
def mock_command_executor(monkeypatch):
    """Mocks the CommandExecutor methods."""
    import subprocess

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


# ---- _dependenciesAreInstalled Tests ----


def test_environment_validates_requirements_main_env_python_mismatch(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    # Ensure the version string format causes a mismatch
    different_py_version = "99.99"
    assert not sys.version.startswith(different_py_version)

    dependencies: Dependencies = {"python": f"={different_py_version}"}

    result = manager._environmentValidatesRequirements(manager.mainEnvironment, dependencies)

    assert result is False
    mock_execute_output.assert_not_called()


def test_environment_validates_requirements_main_env_empty_deps(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {}

    result = manager._environmentValidatesRequirements(manager.mainEnvironment, dependencies)

    assert result is True
    mock_execute_output.assert_not_called()


def test_environment_validates_requirements_main_env_no_path_conda_fails(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = None
    dependencies: Dependencies = {"conda": ["some_package"]}

    result = manager._environmentValidatesRequirements(manager.mainEnvironment, dependencies)

    assert result is False
    mock_execute_output.assert_not_called()


def test_environment_validates_requirements_main_env_no_path_pip_uses_metadata(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = None
    dependencies: Dependencies = {"pip": ["pytest"]}

    result = manager._environmentValidatesRequirements(manager.mainEnvironment, dependencies)

    # This depends on whether 'pytest' is ACTUALLY available via metadata in the test env
    import importlib.metadata

    try:
        importlib.metadata.version("pytest")
        assert result is True
    except importlib.metadata.PackageNotFoundError:
        assert result is False

    mock_execute_output.assert_not_called()


# ---- _checkRequirement Tests ----


def test_check_requirement_no_version_specified(environment_manager_fixture):
    """Test matching package without version constraint."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    result = manager._checkRequirement("numpy", "pip", installed_packages)

    assert result is True


def test_check_requirement_exact_version_match(environment_manager_fixture):
    """Test exact version matching with ==."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    result = manager._checkRequirement("numpy==1.20.0", "pip", installed_packages)

    assert result is True


def test_check_requirement_exact_version_no_match(environment_manager_fixture):
    """Test exact version not matching."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    result = manager._checkRequirement("numpy==1.21.0", "pip", installed_packages)

    assert result is False


def test_check_requirement_greater_than_or_equal(environment_manager_fixture):
    """Test >= version specifier."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    result_true = manager._checkRequirement("numpy>=1.20.0", "pip", installed_packages)
    result_false = manager._checkRequirement("numpy>=1.21.0", "pip", installed_packages)

    assert result_true is True
    assert result_false is False


def test_check_requirement_less_than(environment_manager_fixture):
    """Test < version specifier."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    result_true = manager._checkRequirement("numpy<1.21.0", "pip", installed_packages)
    result_false = manager._checkRequirement("numpy<1.20.0", "pip", installed_packages)

    assert result_true is True
    assert result_false is False


def test_check_requirement_version_range(environment_manager_fixture):
    """Test version range with multiple specifiers."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.5", "kind": "pypi"}]

    result_true = manager._checkRequirement("numpy>=1.20.0,<1.21.0", "pip", installed_packages)
    result_false = manager._checkRequirement("numpy>=1.20.6,<1.21.0", "pip", installed_packages)

    assert result_true is True
    assert result_false is False


def test_check_requirement_compatible_release(environment_manager_fixture):
    """Test ~= (compatible release) specifier.

    ~=2.28 means >= 2.28 and == 2.*
    ~=3.0 means >= 3.0 and == 3.*
    """
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "package", "version": "2.28.5", "kind": "pypi"}]

    result_true = manager._checkRequirement("package~=2.28", "pip", installed_packages)
    result_false = manager._checkRequirement("package~=3.0", "pip", installed_packages)

    assert result_true is True
    assert result_false is False


def test_check_requirement_not_equal(environment_manager_fixture):
    """Test != version specifier."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "package", "version": "1.5.2", "kind": "pypi"}]

    result_true = manager._checkRequirement("package!=1.5.0", "pip", installed_packages)
    result_false = manager._checkRequirement("package!=1.5.2", "pip", installed_packages)

    assert result_true is True
    assert result_false is False


def test_check_requirement_conda_with_channel(environment_manager_fixture):
    """Test conda package with channel prefix."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "zlib", "version": "1.2.13", "kind": "conda"}]

    result = manager._checkRequirement("conda-forge::zlib==1.2.13", "conda", installed_packages)

    assert result is True


def test_check_requirement_package_not_found(environment_manager_fixture):
    """Test when package is not in installed list."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    result = manager._checkRequirement("scipy>=1.0", "pip", installed_packages)

    assert result is False


def test_check_requirement_wrong_package_manager(environment_manager_fixture):
    """Test when package manager type doesn't match."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "1.20.0", "kind": "pypi"}]

    # Try to check as conda when it's pip
    result = manager._checkRequirement("numpy==1.20.0", "conda", installed_packages)

    assert result is False


def test_check_requirement_invalid_version_format(environment_manager_fixture):
    """Test handling of invalid version format in installed packages."""
    manager, _, _ = environment_manager_fixture
    installed_packages = [{"name": "numpy", "version": "not-a-version", "kind": "pypi"}]

    # Should not crash, just return False
    result = manager._checkRequirement("numpy>=1.0", "pip", installed_packages)

    assert result is False


# ---- _environmentValidatesRequirements Tests ----


def test_environment_validates_requirements_conda_only(environment_manager_fixture, monkeypatch):
    """Test that _environmentValidatesRequirements works for conda packages."""
    manager, mock_execute_output, _ = environment_manager_fixture
    from unittest.mock import MagicMock
    from wetlands.external_environment import ExternalEnvironment

    # Create a test environment
    test_env = ExternalEnvironment("test_env", Path("some/path"), manager)
    dependencies: Dependencies = {"conda": ["zlib==1.2.13"]}

    # Create conda packages with the proper format (including "kind" field)
    conda_packages = [
        {"name": "zlib", "version": "1.2.13", "kind": "conda"},
        {"name": "zstandard", "version": "0.22.0", "kind": "conda"},
    ]

    # Mock getInstalledPackages to return conda packages
    monkeypatch.setattr(manager, "getInstalledPackages", MagicMock(return_value=conda_packages))

    result = manager._environmentValidatesRequirements(test_env, dependencies)

    assert result is True


def test_environment_validates_requirements_pip_only(environment_manager_fixture, monkeypatch):
    """Test that _environmentValidatesRequirements works for pip packages."""
    manager, _, _ = environment_manager_fixture
    from unittest.mock import MagicMock
    from wetlands.external_environment import ExternalEnvironment

    # Create a test environment
    test_env = ExternalEnvironment("test_env", Path("some/path"), manager)
    dependencies: Dependencies = {"pip": ["numpy>=1.20.0"]}

    pip_packages = [
        {"name": "numpy", "version": "1.21.0", "kind": "pypi"},
        {"name": "scipy", "version": "1.7.0", "kind": "pypi"},
    ]

    # Mock getInstalledPackages
    monkeypatch.setattr(manager, "getInstalledPackages", MagicMock(return_value=pip_packages))

    result = manager._environmentValidatesRequirements(test_env, dependencies)

    assert result is True


def test_environment_validates_requirements_not_satisfied(environment_manager_fixture, monkeypatch):
    """Test that _environmentValidatesRequirements returns False when dependencies are not satisfied."""
    manager, _, _ = environment_manager_fixture
    from unittest.mock import MagicMock
    from wetlands.external_environment import ExternalEnvironment

    # Create a test environment
    test_env = ExternalEnvironment("test_env", Path("some/path"), manager)
    dependencies: Dependencies = {"pip": ["numpy>=2.0.0"]}

    pip_packages = [
        {"name": "numpy", "version": "1.20.0", "kind": "pypi"},  # Too old
    ]

    # Mock getInstalledPackages
    monkeypatch.setattr(manager, "getInstalledPackages", MagicMock(return_value=pip_packages))

    result = manager._environmentValidatesRequirements(test_env, dependencies)

    assert result is False


def test_environment_validates_requirements_mixed_deps(environment_manager_fixture, monkeypatch):
    """Test _environmentValidatesRequirements with both conda and pip dependencies."""
    manager, _, _ = environment_manager_fixture
    from unittest.mock import MagicMock
    from wetlands.external_environment import ExternalEnvironment

    # Create a test environment
    test_env = ExternalEnvironment("test_env", Path("some/path"), manager)
    dependencies: Dependencies = {"conda": ["zlib>=1.2.0"], "pip": ["numpy>=1.20"]}

    mixed_packages = [
        {"name": "zlib", "version": "1.2.13", "kind": "conda"},
        {"name": "numpy", "version": "1.21.0", "kind": "pypi"},
    ]

    # Mock getInstalledPackages
    monkeypatch.setattr(manager, "getInstalledPackages", MagicMock(return_value=mixed_packages))

    result = manager._environmentValidatesRequirements(test_env, dependencies)

    assert result is True
