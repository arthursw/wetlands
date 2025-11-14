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


def test_dependencies_are_installed_python_mismatch(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    # Ensure the version string format causes a mismatch
    different_py_version = "99.99"
    assert not sys.version.startswith(different_py_version)

    dependencies: Dependencies = {"python": f"={different_py_version}"}  # Exact match required by logic

    installed = manager._dependenciesAreInstalled(dependencies)

    assert not installed
    mock_execute_output.assert_not_called()  # Should return False before checking packages


def test_dependencies_are_installed_empty_deps(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {}  # Python version check passes by default

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is True  # Empty deps means nothing to fail
    mock_execute_output.assert_not_called()  # No packages to check


def test_dependencies_are_installed_conda_only_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = Path("some/valid/path")  # Ensure mainEnv path is not None
    dependencies: Dependencies = {"conda": ["conda-forge::zlib==1.2.13", "zstandard"]}
    # Mock output for 'conda list'
    mock_execute_output.return_value = conda_list_json

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is True
    # Check if conda list command was executed within the main env context
    assert mock_execute_output.call_count >= 1
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"activate {manager.mainEnvironment.path}" in cmd for cmd in command_list)
    assert any("freeze --all" in cmd for cmd in command_list)


def test_dependencies_are_installed_conda_only_not_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = str(Path("some/valid/path"))
    dependencies: Dependencies = {"conda": ["package1", "missing_package"]}
    mock_execute_output.return_value = conda_list_json

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is False


def test_dependencies_are_installed_pip_only_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = Path("some/valid/path")
    dependencies: Dependencies = {"pip": ["package1==1.0", "package2"]}
    # Mock output for 'pip freeze'
    pip_freeze_output = """
package1==1.0
package2==2.5
otherpackage==3.0
    """.splitlines()

    # Mock outputs for both commands, called sequentially
    mock_execute_output.side_effect = [
        conda_list_json,
        pip_freeze_output,
    ]

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is True
    # Check if pip freeze command was executed
    assert mock_execute_output.call_count >= 1
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"activate {manager.mainEnvironment.path}" in cmd for cmd in command_list)
    assert any("pip freeze --all" in cmd for cmd in command_list)


def test_dependencies_are_installed_pip_only_not_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = Path("some/valid/path")
    dependencies: Dependencies = {"pip": ["package1==1.0", "missing_package==3.3"]}
    mock_execute_output.return_value = "[]"

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is False


def test_dependencies_are_installed_conda_and_pip_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = Path("some/valid/path")
    dependencies: Dependencies = {"conda": ["zlib"], "pip": ["p_package==2"]}
    # Mock outputs for both commands, called sequentially
    mock_execute_output.side_effect = [
        conda_list_json,
        ["p_package==2.0"],  # pip freeze output
    ]

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is True
    assert mock_execute_output.call_count >= 1
    # Check first call (conda list)
    call1_args, _ = mock_execute_output.call_args_list[0]
    assert any("list --json" in cmd for cmd in call1_args[0])
    # Check second call (pip freeze)
    call2_args, _ = mock_execute_output.call_args_list[1]
    assert any("pip freeze --all" in cmd for cmd in call2_args[0])


def test_dependencies_are_installed_conda_ok_pip_missing(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = Path("some/valid/path")
    dependencies: Dependencies = {"conda": ["conda-forge::zlib==1.2.13"], "pip": ["p_package==2", "missing_pip==3"]}
    mock_execute_output.side_effect = [
        conda_list_json,
        ["p_package==2.0"],  # pip freeze output (missing one)
    ]

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is False
    assert mock_execute_output.call_count >= 1


def test_dependencies_are_installed_no_main_env_conda_fails(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = None  # No main environment path
    dependencies: Dependencies = {"conda": ["some_package"]}

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is False
    mock_execute_output.assert_not_called()  # Should fail before calling conda list


def test_dependencies_are_installed_no_main_env_pip_uses_metadata(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.path = None  # No main environment path, should use metadata.distributions()
    dependencies: Dependencies = {"pip": ["pytest"]}  # Assume pytest is installed in test runner env

    installed = manager._dependenciesAreInstalled(dependencies)

    # This depends on whether 'pytest' is ACTUALLY available via metadata in the test env
    import importlib.metadata

    try:
        importlib.metadata.version("pytest")
        assert installed is True
    except importlib.metadata.PackageNotFoundError:
        assert installed is False  # Or assert False if you know it won't be found

    mock_execute_output.assert_not_called()  # Should use metadata, not run pip freeze


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
