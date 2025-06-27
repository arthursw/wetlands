import sys
import re
import platform
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.internal_environment import InternalEnvironment
from wetlands.external_environment import ExternalEnvironment
from wetlands._internal.dependency_manager import Dependencies
from wetlands._internal.command_generator import Commands

# --- Fixtures ---

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
    mock_execute = MagicMock(spec=subprocess.Popen)
    mock_execute_output = MagicMock(return_value=["output line 1", "output line 2"])

    # Patch the instance methods after EnvironmentManager is initialized
    # We'll apply these mocks within the main fixture
    mocks = {
        "executeCommands": mock_execute,
        "executeCommandAndGetOutput": mock_execute_output,
    }
    return mocks


@pytest.fixture
def environment_manager_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    dummy_micromamba_path = tmp_path_factory.mktemp("conda_root")
    main_env_path = dummy_micromamba_path / "envs" / "main_test_env"

    # Don't create main_env_path directory, let the manager handle checks

    manager = EnvironmentManager(condaPath=dummy_micromamba_path, usePixi=False, mainCondaEnvironmentPath=main_env_path)

    # Apply the mocks to the specific instance's commandExecutor
    monkeypatch.setattr(manager.commandExecutor, "executeCommands", mock_command_executor["executeCommands"])
    monkeypatch.setattr(
        manager.commandExecutor, "executeCommandAndGetOutput", mock_command_executor["executeCommandAndGetOutput"]
    )

    # Mock environmentExists to simplify create tests
    # We can override this in specific tests if needed
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    return manager, mock_command_executor["executeCommandAndGetOutput"], mock_command_executor["executeCommands"]


@pytest.fixture
def environment_manager_pixi_fixture(tmp_path_factory, mock_command_executor, monkeypatch):
    """Provides an EnvironmentManager instance with mocked CommandExecutor."""
    dummy_pixi_path = tmp_path_factory.mktemp("pixi_root")

    manager = EnvironmentManager(condaPath=dummy_pixi_path, usePixi=True)

    # Apply the mocks to the specific instance's commandExecutor
    monkeypatch.setattr(manager.commandExecutor, "executeCommands", mock_command_executor["executeCommands"])
    monkeypatch.setattr(
        manager.commandExecutor, "executeCommandAndGetOutput", mock_command_executor["executeCommandAndGetOutput"]
    )

    # Mock environmentExists to simplify create tests
    # We can override this in specific tests if needed
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    return manager, mock_command_executor["executeCommandAndGetOutput"], mock_command_executor["executeCommands"]


# --- Test Functions ---

# ---- _dependenciesAreInstalled Tests ----


def test_environment_manager_conda_path(tmp_path_factory):
    """Check that an exception is raised if the provided CondaPath contains the name of another conda manager."""

    dummy_conda_path = tmp_path_factory.mktemp("path_containing_pixi_and_micromamba").resolve()

    with pytest.raises(Exception):
        manager = EnvironmentManager(condaPath=dummy_conda_path, usePixi=False)

    with pytest.raises(Exception):
        manager = EnvironmentManager(condaPath=dummy_conda_path, usePixi=True)

    manager = EnvironmentManager(condaPath=dummy_conda_path, usePixi=True, acceptAllCondaPaths=True)
    assert manager is not None


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

    # Mock sys.version temporarily if needed, or assume test runner Python version is okay
    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is True  # Empty deps means nothing to fail
    mock_execute_output.assert_not_called()  # No packages to check


def test_dependencies_are_installed_conda_only_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.name = str(Path("some/valid/path"))  # Ensure mainEnv path is not None
    dependencies: Dependencies = {"conda": ["conda-forge::zlib==1.2.13", "zstandard"]}
    # Mock output for 'conda list'
    mock_execute_output.return_value = conda_list_json

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is True
    # Check if conda list command was executed within the main env context
    assert mock_execute_output.call_count >= 1
    called_args, called_kwargs = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"activate {manager.mainEnvironment.name}" in cmd for cmd in command_list)
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
    manager.mainEnvironment.name = str(Path("some/valid/path"))
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
    called_args, called_kwargs = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"activate {manager.mainEnvironment.name}" in cmd for cmd in command_list)
    assert any("pip freeze --all" in cmd for cmd in command_list)


def test_dependencies_are_installed_pip_only_not_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.name = str(Path("some/valid/path"))
    dependencies: Dependencies = {"pip": ["package1==1.0", "missing_package==3.3"]}
    mock_execute_output.return_value = "[]"

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is False


def test_dependencies_are_installed_conda_and_pip_installed(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.name = Path("some/valid/path")
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
    manager.mainEnvironment.name = None
    dependencies: Dependencies = {"conda": ["some_package"]}

    installed = manager._dependenciesAreInstalled(dependencies)

    assert installed is False
    mock_execute_output.assert_not_called()  # Should fail before calling conda list


def test_dependencies_are_installed_no_main_env_pip_uses_metadata(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    manager.mainEnvironment.name = None
    dependencies: Dependencies = {"pip": ["pytest"]}  # Assume pytest is installed in test runner env

    # We don't need to mock distributions if pytest is actually installed
    # If not, we would mock importlib.metadata.distributions
    # mock_dist = MagicMock()
    # mock_dist.metadata = {'Name': 'pytest'}
    # mock_dist.version = '1.2.3'
    # monkeypatch.setattr('importlib.metadata.distributions', MagicMock(return_value=[mock_dist]))

    installed = manager._dependenciesAreInstalled(dependencies)

    # This depends on whether 'pytest' is ACTUALLY available via metadata in the test env
    # Let's assume it is for this test case. If not, the mock above is needed.
    import importlib.metadata

    try:
        importlib.metadata.version("pytest")
        assert installed is True
    except importlib.metadata.PackageNotFoundError:
        assert installed is False  # Or assert False if you know it won't be found

    mock_execute_output.assert_not_called()  # Should use metadata, not run pip freeze


# ---- create Tests ----


@pytest.mark.parametrize("force_external", [True, False])
def test_create_environment_already_exists(environment_manager_fixture, monkeypatch, force_external):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "existing-env"

    # Mock environmentExists to return True for this specific name
    mock_exists = MagicMock(return_value=True)
    monkeypatch.setattr(manager, "environmentExists", mock_exists)

    # Add to environments dict to simulate it was *really* created before
    manager.environments[env_name] = ExternalEnvironment(env_name, manager)

    env = manager.create(env_name, dependencies={}, forceExternal=force_external)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    mock_exists.assert_called_once_with(env_name)
    mock_execute_output.assert_not_called()  # No creation/installation commands needed
    assert env is manager.environments[env_name]  # Should return existing instance


def test_create_dependencies_met_use_main_environment(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "new-env-dont-create"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    # Mock _dependenciesAreInstalled to return True
    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=True))
    # Mock environmentExists to return False (it doesn't exist yet)
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies, forceExternal=False)

    assert env is manager.mainEnvironment  # Should return the main environment instance
    assert isinstance(env, InternalEnvironment)
    manager._dependenciesAreInstalled.assert_called_once_with(dependencies)
    manager.environmentExists.assert_called_once_with(env_name)
    mock_execute_output.assert_not_called()  # No commands should be run


def test_create_dependencies_met_force_external(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "forced-external-env"
    dependencies: Dependencies = {"pip": ["numpy==1.2.3"]}

    # Mock _dependenciesAreInstalled to return True, but forceExternal=True overrides it
    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=True))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies, forceExternal=True)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    assert env is manager.environments[env_name]
    manager.environmentExists.assert_called_once_with(env_name)
    mock_execute_output.assert_called()  # Creation commands should be run

    # Check for key commands
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    assert any(f"create -n {env_name}" in cmd for cmd in command_list)
    # Check install commands are present (assuming numpy leads to some install command)
    assert any("install" in cmd for cmd in command_list if "create" not in cmd)


def test_create_dependencies_not_met_create_external(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "new-external-env"
    dependencies: Dependencies = {"conda": ["requests"], "pip": ["pandas"]}

    # Mock _dependenciesAreInstalled to return False
    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies, forceExternal=False)

    assert isinstance(env, ExternalEnvironment)
    assert env.name == env_name
    assert env is manager.environments[env_name]
    manager._dependenciesAreInstalled.assert_called_once_with(dependencies)
    manager.environmentExists.assert_called_once_with(env_name)
    mock_execute_output.assert_called()  # Creation commands should be run

    # Check for key commands
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    current_py_version = platform.python_version()
    assert any(f"create -n {env_name} python={current_py_version} -y" in cmd for cmd in command_list)
    assert any(f"install" in cmd for cmd in command_list if "micromamba" in cmd)  # Check for install commands
    assert any("requests" in cmd for cmd in command_list if "install" in cmd)  # Check dep is mentioned
    assert any("pandas" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_python_version(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "py-versioned-env"
    py_version = "3.10.5"
    dependencies: Dependencies = {"python": f"={py_version}", "pip": ["toolz"]}  # Use exact match format

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    env = manager.create(env_name, dependencies=dependencies)

    assert isinstance(env, ExternalEnvironment)
    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    # Check python version is in create command
    assert any(f"create -n {env_name} python={py_version} -y" in cmd for cmd in command_list)
    # Check install command for toolz
    assert any("toolz" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)


def test_create_with_additional_commands(environment_manager_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "env-with-extras"
    dependencies: Dependencies = {"pip": ["tiny-package"]}
    additional_commands: Commands = {
        "all": ["echo 'hello world'"],
        "linux": ["specific command"],  # e.g., 'linux', 'darwin', 'windows'
    }

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    monkeypatch.setattr(platform, "system", MagicMock(return_value="Linux"))

    manager.create(env_name, dependencies=dependencies, additionalInstallCommands=additional_commands)

    mock_execute_output.assert_called()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Check create and install commands are present
    assert any(f"create -n {env_name}" in cmd for cmd in command_list)
    assert any("tiny-package" in cmd for cmd in command_list if "pip" in cmd and "install" in cmd)

    # Check additional commands are present
    assert "echo 'hello world'" in command_list
    assert "specific command" in command_list


def test_create_invalid_python_version_raises(environment_manager_fixture, monkeypatch):
    manager, _, _ = environment_manager_fixture
    env_name = "invalid-py-env"
    dependencies: Dependencies = {"python": "=3.8"}  # Below 3.9 limit

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))
    monkeypatch.setattr(manager, "environmentExists", MagicMock(return_value=False))

    with pytest.raises(Exception, match="Python version must be greater than 3.8"):
        manager.create(env_name, dependencies=dependencies)


# ---- install Tests ----


def test_install_in_existing_env(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "target-env"
    dependencies: Dependencies = {"conda": ["new_dep==1.0"]}

    manager.install(env_name, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Check for install commands targeting the environment
    assert any("new_dep==1.0" in cmd for cmd in command_list if "install" in cmd)
    # Check activation commands are present (usually part of install dependencies)
    assert any(
        "micromamba activate" in cmd or ". /path/to/micromamba" in cmd for cmd in command_list
    )  # Check general activation pattern


def test_install_in_main_env(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    dependencies: Dependencies = {"pip": ["another_pip_dep"]}

    manager.install(None, dependencies)  # None signifies main/current environment context

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Install commands should NOT have "-n env_name"
    assert not any(f"install -n" in cmd for cmd in command_list if "install" in cmd)
    # Check pip install command is present
    assert any("pip install" in cmd and "another_pip_dep" in cmd for cmd in command_list)


def test_install_with_additional_commands(environment_manager_fixture):
    manager, mock_execute_output, _ = environment_manager_fixture
    env_name = "install-env-extras"
    dependencies: Dependencies = {"conda": ["dep1"]}
    additional_commands: Commands = {"all": ["post-install script"]}

    manager.install(env_name, dependencies, additional_commands)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]

    # Check install command
    assert any("install" in cmd and "dep1" in cmd for cmd in command_list)
    # Check additional command
    assert "post-install script" in command_list


# ---- executeCommands Tests ----


def test_execute_commands_in_specific_env(environment_manager_fixture):
    manager, _, mock_execute = environment_manager_fixture
    env_name = "exec-env"
    commands_to_run: Commands = {"all": ["python script.py", "echo done"]}
    popen_kwargs = {"cwd": "/some/path"}

    manager.executeCommands(env_name, commands_to_run, popenKwargs=popen_kwargs)

    mock_execute.assert_called_once()
    called_args, called_kwargs = mock_execute.call_args
    command_list = called_args[0]

    # Check activation for the specific environment
    assert any(f"activate {env_name}" in cmd for cmd in command_list)
    # Check user commands are present
    assert "python script.py" in command_list
    assert "echo done" in command_list
    # Check popenKwargs are passed through
    assert called_kwargs.get("popenKwargs") == popen_kwargs


def test_execute_commands_in_main_env(environment_manager_fixture):
    manager, _, mock_execute = environment_manager_fixture
    manager.mainEnvironment.name = str(Path("/path/to/main/env"))  # Give it a path
    commands_to_run: Commands = {"all": ["ls -l"]}

    manager.executeCommands(None, commands_to_run)  # None for main env

    mock_execute.assert_called_once()
    called_args, _ = mock_execute.call_args
    command_list = called_args[0]

    # Check user command
    assert "ls -l" in command_list


# Test with Pixi Environment Manager


def test_create_with_python_version_pixi(environment_manager_pixi_fixture, monkeypatch):
    manager, mock_execute_output, _ = environment_manager_pixi_fixture
    env_name = "py-versioned-env"
    py_version = "3.10.5"
    dependencies: Dependencies = {
        "python": f"={py_version}",
        "pip": ["toolz"],
        "conda": ["dep==1.0"],
    }  # Use exact match format

    monkeypatch.setattr(manager, "_dependenciesAreInstalled", MagicMock(return_value=False))
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
    assert any("dep" in cmd for cmd in command_list if f"{pixi_bin} add" in cmd)


# ---- install Tests ----


def test_install_in_existing_env_pixi(environment_manager_pixi_fixture):
    manager, mock_execute_output, _ = environment_manager_pixi_fixture
    env_name = "target-env"
    dependencies: Dependencies = {"conda": ["new_dep==1.0"]}

    manager.install(env_name, dependencies)

    mock_execute_output.assert_called_once()
    called_args, _ = mock_execute_output.call_args
    command_list = called_args[0]
    pixi_bin = "pixi.exe" if platform.system() == "Windows" else "pixi"

    # Check for install commands targeting the environment
    assert any("new_dep==1.0" in cmd for cmd in command_list if f"{pixi_bin} add" in cmd)
