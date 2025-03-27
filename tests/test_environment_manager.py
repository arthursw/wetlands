import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from cema.environment import Environment
from cema.dependency_manager import Dependencies
from cema.environment_manager import EnvironmentManager

from contextlib import contextmanager

@contextmanager
def safe_chdir(path):
    currentPath = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(currentPath)

@pytest.fixture
def environment_manager():
    return EnvironmentManager(condaPath="/path/to/micromamba")

def test_set_conda_path(environment_manager):
    environment_manager.settingsManager.setCondaPath = MagicMock()
    environment_manager.setCondaPath("/new/path/to/micromamba")
    environment_manager.settingsManager.setCondaPath.assert_called_once_with("/new/path/to/micromamba")

def test_set_proxies(environment_manager):
    proxies = {"http": "http://proxy.com", "https": "https://proxy.com"}
    environment_manager.settingsManager.setProxies = MagicMock()
    environment_manager.setProxies(proxies)
    environment_manager.settingsManager.setProxies.assert_called_once_with(proxies)

def test_remove_channel(environment_manager):
    assert environment_manager._removeChannel("conda-forge::numpy") == "numpy"
    assert environment_manager._removeChannel("numpy") == "numpy"

def test_dependencies_are_installed_when_cached_with_installed_dependencies(environment_manager):
    environment_name = "test_env"
    dependencies = Dependencies(conda=["numpy"], pip=["requests"])
    
    mock_env = MagicMock(spec=Environment)
    mock_env.installedDependencies = {
        "conda": ["numpy"],
        "pip": ["requests==2.25.1"]
    }
    
    environment_manager.environments[environment_name] = mock_env
    
    environment_manager.dependencyManager.formatDependencies = MagicMock(
    side_effect=lambda package_type, dependencies, flag: 
        (["numpy"], [], True) if package_type == "conda" else 
        (["requests==2.25.1"], [], True)
    )
    
    assert environment_manager.dependenciesAreInstalled(environment_name, dependencies)

    dependencies_not_installed = Dependencies(conda=["scipy"], pip=["flask"])

    environment_manager.dependencyManager.formatDependencies = MagicMock(
    side_effect=lambda package_type, dependencies, flag: 
        (["scipy"], [], True) if package_type == "conda" else 
        (["flask"], [], True)
    )
    assert not environment_manager.dependenciesAreInstalled(environment_name, dependencies_not_installed)

def test_dependencies_are_installed(environment_manager, mock_settings_manager):
    environment_name = "test_env"
    dependencies = Dependencies(conda=["numpy"], pip=["requests"])
    
    mock_env = MagicMock(spec=Environment)
    mock_env.installedDependencies = {}
    
    environment_manager.environments[environment_name] = mock_env
    
    environment_manager.dependencyManager.formatDependencies = MagicMock(
    side_effect=lambda package_type, dependencies, flag: 
        (["numpy"], [], True) if package_type == "conda" else 
        (["requests==2.25.1"], [], True)
    )
    environment_manager.settingsManager = mock_settings_manager
    
    with patch.object(environment_manager.commandExecutor, "executeCommandAndGetOutput", return_value=["numpy", "requests==2.25.1"]), patch.object(environment_manager.commandGenerator, "getActivateCondaCommands", return_value=[]):
        assert environment_manager.dependenciesAreInstalled(environment_name, dependencies)
    







import os
import platform
from pathlib import Path
import pytest
from cema.environment_manager import EnvironmentManager

@pytest.fixture(scope="function")
def env_manager(tmp_path_factory):
    # Setup temporary conda root
    temp_root = tmp_path_factory.mktemp("conda root")
    
    # Basic environment configuration
    manager = EnvironmentManager(temp_root)
    yield manager
    
    # Teardown - kill all environments
    for env_name in list(manager.environments.keys()):
        manager.exit(env_name)
    # Clean temp directory handled by pytest

def test_basic_environment_creation(env_manager):
    """Verify environment creation and existence check"""
    env_name = "test_env"
    dependencies = {"python": "3.9"}
    
    # Create environment
    result = env_manager.create(env_name, dependencies)
    assert result is True
    assert env_manager.environmentExists(env_name)
    
    # Verify python version
    output = env_manager.executeCommandsInEnvironment(
        env_name, 
        ['python -c "import sys; print(sys.version)"']
    )
    stdout, _ = output.communicate()
    assert "Python 3.9" in stdout.decode()

def test_dependency_checks(env_manager):
    """Verify dependency verification logic"""
    env_name = "dep_env"
    test_package = "numpy"
    
    # Create env with dependency
    env_manager.create(env_name, {"conda": [test_package]})
    
    # Check installed dependencies
    assert env_manager.dependenciesAreInstalled(
        env_name, {"conda": [test_package]}
    )

@pytest.mark.parametrize("package_manager", ["conda", "pip"])
def test_package_installation(env_manager, package_manager):
    """Test both conda and pip package installation"""
    env_name = f"pkg_{package_manager}_env"
    test_package = "six" if package_manager == "conda" else "requests"
    
    env_manager.create(env_name, {
        package_manager: [test_package],
        "python": "3.9"
    })
    
    # Verify installation
    assert env_manager.dependenciesAreInstalled(
        env_name, {package_manager: [test_package]}
    )

def test_environment_launch(env_manager):
    """Verify server environment launch capability"""
    env_name = "server_env"
    env_manager.create(env_name, {"python": "3.9"})
    
    # Launch environment and verify status
    environment = env_manager.launch(env_name)
    assert environment.port > 0
    assert env_manager.environmentIsLaunched(env_name)

    # Clean up
    env_manager.exit(environment)
    assert not env_manager.environmentIsLaunched(env_name)

def test_platform_specific_commands(env_manager, monkeypatch, tmp_path_factory):
    """Test platform-specific command execution"""
    temp_test = tmp_path_factory.mktemp("test")
    with safe_chdir(temp_test):
        env_name = "platform_env"
        commands = {"linux": ["echo linux > test.txt"], "darwin": ["echo mac > test.txt"]}
        
        # Mock platform
        monkeypatch.setattr(platform, "system", lambda: "Linux")
        
        env_manager.create(
            env_name,
            {"python": "3.9"},
            additionalInstallCommands=commands
        )
        
        # Verify command execution
        test_file = Path("test.txt")
        assert test_file.exists()
        assert "linux" in test_file.read_text().lower()
        
        # Clean up
        test_file.unlink()

def test_invalid_environment_handling(env_manager):
    """Test error conditions and validation"""
    # Test Python version validation
    with pytest.raises(Exception) as exc_info:
        env_manager.create("invalid_py_env", {"python": "3.7"})
    assert "greater than 3.8" in str(exc_info.value)

    # Test duplicate environment creation
    env_name = "duplicate_env"
    env_manager.create(env_name, {"python": "3.9"})
    with pytest.raises(Exception):
        env_manager.create(env_name, {}, errorIfExists=True)

def test_real_world_workflow(env_manager):
    """Test complete workflow matching the example code"""
    # Create and launch environment
    environment = env_manager.createAndLaunch(
        "numpy-test",
        {"conda": ["numpy"]}
    )

    expected_list = [0, 5]
    # Execute command in environment
    result = environment.execute(
        "numpy_module.py",
        "generateArray",
        [0, 10, 5]
    )

    # Verify expected behavior
    assert result == expected_list
    env_manager.exit(environment)
    assert not env_manager.environmentIsLaunched("cellpose-test")