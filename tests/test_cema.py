import pytest
from cema.environment_manager import EnvironmentManager, InternalEnvironment
from cema.exceptions import IncompatibilityException
import subprocess
import os
import platform
from pathlib import Path

from cema.external_environment import ExternalEnvironment

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def env_manager(tmp_path_factory):
    # Setup temporary conda root
    temp_root = tmp_path_factory.mktemp("conda root")
    logger.info(f"Creating test directory {temp_root}")
    # Basic environment configuration
    manager = EnvironmentManager(temp_root)
    yield manager
    
    for env_name, env in manager.environments.items():
        logger.info(f"Exiting environment {env_name}")
        env.exit()

    # Clean temp directory handled by pytest
    print(f'Removing {temp_root}')

def test_dependency_installation(env_manager):
    """Test that EnvironmentManager.create() correctly installs dependencies."""
    env_name = "test_env_deps"
    logger.info(f"Testing dependency installation: {env_name}")
    dependencies = {"conda": ["requests"]}
    env = env_manager.create(env_name, dependencies)

    # Get the micromamba path used by the env_manager
    micromamba_path = env_manager.settingsManager.condaPath

    # Verify that 'requests' is installed

    commands = env_manager.commandGenerator.getActivateCondaCommands() + [
        f"{env_manager.settingsManager.condaBin} activate {env_name}",
        f"{env_manager.settingsManager.condaBin} list -y",
    ]
    installedCondaPackages = env_manager.commandExecutor.executeCommandAndGetOutput(commands, log=False)

    assert any("requests" in icp for icp in installedCondaPackages)

    env.exit()


def test_internal_external_environment(env_manager):
    """Test that EnvironmentManager.create() correctly creates internal/external environments."""

    logger.info("Testing internal/external environment creation")
    # No dependencies: InternalEnvironment
    env_internal = env_manager.create("test_env_internal", {})
    assert isinstance(env_internal, InternalEnvironment)

    # With dependencies: ExternalEnvironment
    env_external = env_manager.create("test_env_external", {"conda": ["requests"]})
    assert isinstance(env_external, ExternalEnvironment)

    # force_external=True: ExternalEnvironment
    env_external_forced = env_manager.create("test_env_external_forced", {}, forceExternal=True)
    assert isinstance(env_external_forced, ExternalEnvironment)

    env_internal.exit()
    env_external.exit()
    env_external_forced.exit()


def test_incompatible_dependencies(env_manager):
    """Test that IncompatibilityException is raised for incompatible dependencies."""
    env_name = "test_env_incompatible"
    logger.info(f"Testing incompatible dependencies: {env_name}")
    if platform.system() == "Windows":
        incompatible_dependency = {"conda": [{"name": "unixodbc", "platforms": ["linux-64"], "optional": False}]}
    elif platform.system() == "Darwin":
        incompatible_dependency = {"conda": [{"name": "libxcursor", "platforms": ["linux-64"], "optional": False}]}
    else:
        incompatible_dependency = {"conda": [{"name": "bla", "platforms": ["osx-64"], "optional": False}]}
    with pytest.raises(IncompatibilityException):
        env_manager.create(env_name, incompatible_dependency)


def test_invalid_python_version(env_manager):
    """Test that an exception is raised for invalid Python versions."""
    env_name = "test_env_invalid_python"
    logger.info(f"Testing invalid Python version: {env_name}")
    with pytest.raises(Exception) as excinfo:
        env_manager.create(env_name, {"python": "3.8.0"})
    assert "Python version must be greater than 3.8" in str(excinfo.value)


def test_mambarc_modification(env_manager, tmp_path):
    """Test that proxy settings are correctly written to the .mambarc file."""
    logger.info("Testing .mambarc modification")
    proxies = {"http": "http://proxy.example.com", "https": "https://proxy.example.com"}
    env_manager.setProxies(proxies)
    mambarc_path = Path(env_manager.settingsManager.condaPath) / ".mambarc"
    assert os.path.exists(mambarc_path)

    with open(mambarc_path, "r") as f:
        content = f.read()
        assert "http: http://proxy.example.com" in content
        assert "https: https://proxy.example.com" in content
    
    env_manager.setProxies({})

    with open(mambarc_path, "r") as f:
        content = f.read()
        assert "proxy" not in content
        assert "http: http://proxy.example.com" not in content
        assert "https: https://proxy.example.com" not in content


def test_code_execution(env_manager, tmp_path):
    """Test that Environment.execute() correctly executes code within an environment."""
    env_name = "test_env_code_exec"
    logger.info(f"Testing code execution: {env_name}")
    dependencies = {"conda": ["numpy"]}  # numpy is required to import it

    # Create a simple module in the tmp_path
    module_path = tmp_path / "test_module.py"
    with open(module_path, "w") as f:
        f.write(
            """
import numpy as np
def my_function(x):
    return np.sum(x)
"""
        )
    env = env_manager.create(env_name, dependencies)
    env.launch()
    # Execute the function within the environment
    result = env.execute(str(module_path), "my_function", [[1, 2, 3]])
    assert result == 6

    env.exit()


def test_non_existent_function(env_manager, tmp_path):
    """Test that an exception is raised when executing a non-existent function."""
    env_name = "test_env_non_existent_function"

    logger.info(f"Testing non-existent function: {env_name}")
    # Create a simple module in the tmp_path
    module_path = tmp_path / "test_module.py"
    with open(module_path, "w") as f:
        f.write(
            """
def my_function(x):
    return x * 2
"""
        )

    env = env_manager.create(env_name, {})  # No dependencies needed

    with pytest.raises(Exception) as excinfo:
        env.execute(str(module_path), "non_existent_function", [1])
    assert "has no function" in str(excinfo.value)

    env.exit()


def test_non_existent_module(env_manager):
    """Test that an exception is raised when importing a non-existent module."""
    env_name = "test_env_non_existent_module"
    env = env_manager.create(env_name, {})
    logger.info(f"Testing non-existent module: {env_name}")

    with pytest.raises(ModuleNotFoundError):
        env.execute("non_existent_module.py", "my_function", [1])

    env.exit()