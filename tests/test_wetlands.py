from multiprocessing.connection import Client
from typing import cast
import os
import platform
from pathlib import Path
import logging
import pytest

from wetlands._internal.dependency_manager import Dependencies
from wetlands.internal_environment import InternalEnvironment
from wetlands._internal.exceptions import IncompatibilityException
from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment


# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", params=["micromamba_root/", "pixi_root/"])
def env_manager(request, tmp_path_factory):
    # Setup temporary conda root
    temp_root = tmp_path_factory.mktemp(request.param)
    logger.info(f"Creating test directory {temp_root}")
    # Basic environment configuration
    manager = EnvironmentManager(temp_root, usePixi="pixi" in request.param)
    yield manager

    for env_name, env in manager.environments.copy().items():
        logger.info(f"Exiting environment {env_name}")
        env.exit()

    # Clean temp directory handled by pytest
    print(f"Removing {temp_root}")


def test_environment_creation(env_manager):
    """Test that EnvironmentManager.create() correctly create an environment and installs dependencies."""
    env_name = "test_env_deps"
    logger.info(f"Testing dependency installation: {env_name}")
    dependencies = Dependencies({"conda": ["requests"]})
    env = env_manager.create(env_name, dependencies)

    # Verify that 'requests' is installed
    installedPackages = env_manager.getInstalledPackages(env_name)
    assert any(icp["name"] == "requests" for icp in installedPackages)

    # Verify that recreating the same env returns it
    same_env = env_manager.create(env_name, dependencies)
    assert env == same_env
    env.exit()
    other_env = env_manager.create(env_name, dependencies)
    assert other_env != same_env
    other_env.exit()


def test_dependency_installation(env_manager):
    """Test that EnvironmentManager.install() correctly installs dependencies (in existing env)."""
    env_name = "test_env_deps"
    logger.info(f"Testing dependency installation: {env_name}")
    env = cast(ExternalEnvironment, env_manager.create(env_name, forceExternal=True))
    dependencies = Dependencies({"pip": ["munch==4.0.0"], "conda": ["bioimageit::noise2self==1.0"]})

    env.install(dependencies)

    # Verify that 'numpy' and 'noise2self' is installed
    installedPackages = env_manager.getInstalledPackages(env_name)
    assert any(
        icp["name"] == "noise2self" and icp["version"].startswith("1.0") and icp["kind"] == "conda"
        for icp in installedPackages
    )
    assert any(
        icp["name"] == "munch" and icp["version"].startswith("4.0.0") and icp["kind"] == "pypi"
        for icp in installedPackages
    )

    env.exit()


def test_internal_external_environment(env_manager):
    """Test that EnvironmentManager.create() correctly creates internal/external environments."""

    logger.info("Testing internal/external environment creation")
    # No dependencies: InternalEnvironment
    env_internal = env_manager.create("test_env_internal", {})
    assert isinstance(env_internal, InternalEnvironment)
    assert env_internal == env_manager.mainEnvironment

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
    if env_manager.settingsManager.usePixi:
        assert env_manager.settingsManager.proxies == proxies
        env_manager.setProxies({})
        assert env_manager.settingsManager.proxies == {}
        return
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
try:
    import numpy as np
except ModuleNotFoundError:
    pass

def sum(x):
    return int(np.sum(x))

def prod(x=[], y=1):
    return int(np.prod(x)) * y
"""
        )
    env = env_manager.create(env_name, dependencies)
    env.launch()
    # Execute the function within the environment
    result = env.execute(str(module_path), "sum", [[1, 2, 3]])
    assert result == 6
    result = env.execute(str(module_path), "prod", [[1, 2, 3]], {"y": 2})
    assert result == 12

    # Test with importModule
    module = env.importModule(str(module_path))
    result = module.sum([1, 2, 3])
    assert result == 6
    result = module.prod([1, 2, 3], y=3)
    assert result == 18

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
def double(x):
    return x * 2
"""
        )

    env = env_manager.create(env_name, {})  # No dependencies needed

    with pytest.raises(Exception) as excinfo:
        env.execute(str(module_path), "non_existent_function", [1])
    assert "has no function" in str(excinfo.value)

    module = env.importModule(str(module_path))
    with pytest.raises(Exception) as excinfo:
        module.non_existent_function(1)

    assert "has no attribute" in str(excinfo.value)

    env.exit()


def test_non_existent_module(env_manager):
    """Test that an exception is raised when importing a non-existent module."""
    env_name = "test_env_non_existent_module"
    env = env_manager.create(env_name, {})
    logger.info(f"Testing non-existent module: {env_name}")

    with pytest.raises(ModuleNotFoundError):
        env.execute("non_existent_module.py", "my_function", [1])

    with pytest.raises(ModuleNotFoundError):
        env.importModule("non_existent_module.py")

    env.exit()


def test_advanced_execution(env_manager, tmp_path):
    env = env_manager.create("advanced_test", Dependencies(conda=["numpy"]))

    module_path = tmp_path / "test_module.py"
    with open(module_path, "w") as f:
        f.write("""from multiprocessing.connection import Listener
import sys
import numpy as np

with Listener(("localhost", 0)) as listener:
    print(f"Listening port {listener.address[1]}")
    with listener.accept() as connection:
        while message := connection.recv():
            if message["action"] == "execute_prod":
                connection.send(int(np.prod(message["args"])))
            if message["action"] == "execute_sum":
                connection.send(int(np.sum(message["args"])))
            if message["action"] == "exit":
                connection.send(dict(action="exited"))
                sys.exit()
    """)

    process = env.executeCommands([f"python -u {(tmp_path / 'test_module.py').resolve()}"])

    port = 0
    if process.stdout is not None:
        for line in process.stdout:
            if line.strip().startswith("Listening port "):
                port = int(line.strip().replace("Listening port ", ""))
                break

    connection = Client(("localhost", port))

    connection.send(dict(action="execute_sum", args=[1, 2, 3, 4]))
    result = connection.recv()
    assert result == 10

    connection.send(dict(action="execute_prod", args=[1, 2, 3, 4]))
    result = connection.recv()
    assert result == 24

    connection.send(dict(action="exit"))
    result = connection.recv()
    assert result["action"] == "exited"
