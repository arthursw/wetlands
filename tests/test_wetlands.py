from multiprocessing.connection import Client
import os
import platform
from pathlib import Path
import logging
import pytest
import shutil

from wetlands._internal.dependency_manager import Dependencies
from wetlands.internal_environment import InternalEnvironment
from wetlands._internal.exceptions import IncompatibilityException
from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment


# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def tool_available(tool_name: str) -> bool:
    """Check if a tool is available in PATH."""
    return shutil.which(tool_name) is not None


@pytest.fixture(scope="module", params=["micromamba_root/", "pixi_root/"])
def env_manager(request, tmp_path_factory):
    # Setup temporary conda root
    temp_root = tmp_path_factory.mktemp(request.param)
    wetlands_instance_path = temp_root / "wetlands"
    logger.info(f"Creating test directory {temp_root}")
    # Basic environment configuration
    manager = EnvironmentManager(wetlandsInstancePath=wetlands_instance_path, condaPath=temp_root)
    yield manager

    for env_name, env in manager.environments.copy().items():
        logger.info(f"Exiting environment {env_name}")
        env.exit()

    # Clean temp directory handled by pytest
    print(f"Removing {temp_root}")


@pytest.mark.integration
def test_environment_creation(env_manager):
    """Test that EnvironmentManager.create() correctly create an environment and installs dependencies."""
    env_name = "test_env_deps"
    logger.info(f"Testing dependency installation: {env_name}")
    dependencies = Dependencies({"conda": ["requests"]})
    env = env_manager.create(env_name, dependencies)

    # Verify that 'requests' is installed
    installedPackages = env_manager.getInstalledPackages(env)
    assert any(icp["name"] == "requests" for icp in installedPackages)

    # Verify that recreating the same env returns it
    same_env = env_manager.create(env_name, dependencies)
    assert env == same_env
    env.exit()
    other_env = env_manager.create(env_name, dependencies)
    assert other_env != same_env
    other_env.exit()


@pytest.mark.integration
def test_dependency_installation(env_manager):
    """Test that EnvironmentManager.install() correctly installs dependencies (in existing env)."""
    env_name = "test_env_deps"
    logger.info(f"Testing dependency installation: {env_name}")
    env = env_manager.create(env_name, useExisting=False)
    dependencies = Dependencies({"pip": ["munch==4.0.0"], "conda": ["fastai::fastprogress==1.0.3"]})

    env.install(dependencies)

    # Verify that 'numpy' and 'noise2self' is installed
    installedPackages = env_manager.getInstalledPackages(env)
    assert any(
        icp["name"] == "fastprogress" and icp["version"].startswith("1.0.3") and icp["kind"] == "conda"
        for icp in installedPackages
    )
    assert any(
        icp["name"] == "munch" and icp["version"].startswith("4.0.0") and icp["kind"] == "pypi"
        for icp in installedPackages
    )

    env.exit()


@pytest.mark.integration
def test_internal_external_environment(env_manager):
    """Test that EnvironmentManager.create() correctly creates internal/external environments."""

    logger.info("Testing internal/external environment creation")
    # No dependencies: InternalEnvironment
    env_internal = env_manager.create("test_env_internal", {}, useExisting=True)
    assert isinstance(env_internal, InternalEnvironment)
    assert env_internal == env_manager.mainEnvironment

    # With dependencies: ExternalEnvironment
    env_external = env_manager.create("test_env_external", {"conda": ["requests"]})
    assert isinstance(env_external, ExternalEnvironment)

    env_external_forced = env_manager.create("test_env_external_forced", {}, useExisting=False)
    assert isinstance(env_external_forced, ExternalEnvironment)

    env_internal.exit()
    env_external.exit()
    env_external_forced.exit()


@pytest.mark.integration
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


@pytest.mark.integration
def test_invalid_python_version(env_manager):
    """Test that an exception is raised for invalid Python versions."""
    env_name = "test_env_invalid_python"
    logger.info(f"Testing invalid Python version: {env_name}")
    with pytest.raises(Exception) as excinfo:
        env_manager.create(env_name, {"python": "3.8.0"})
    assert "Python version must be greater than 3.8" in str(excinfo.value)


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
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

    env = env_manager.create(env_name, {}, useExisting=True)  # No dependencies needed
    # env.launch()

    with pytest.raises(Exception) as excinfo:
        env.execute(str(module_path), "non_existent_function", [1])
    assert "has no function" in str(excinfo.value)

    module = env.importModule(str(module_path))
    with pytest.raises(Exception) as excinfo:
        module.non_existent_function(1)

    assert "has no attribute" in str(excinfo.value)

    env.exit()


@pytest.mark.integration
def test_non_existent_module(env_manager):
    """Test that an exception is raised when importing a non-existent module."""
    env_name = "test_env_non_existent_module"
    env = env_manager.create(env_name, {}, useExisting=True)
    # env.launch()
    logger.info(f"Testing non-existent module: {env_name}")

    with pytest.raises(ModuleNotFoundError):
        env.execute("non_existent_module.py", "my_function", [1])

    with pytest.raises(ModuleNotFoundError):
        env.importModule("non_existent_module.py")

    env.exit()


@pytest.mark.integration
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


@pytest.mark.integration
@pytest.mark.skipif(not tool_available("micromamba"), reason="micromamba not available")
def test_existing_environment_access_via_path(tmp_path, tmp_path_factory):
    """Test that users can reference existing environments using Path objects.

    This integration test verifies real-world behavior by:
    - Creating a real environment directly with micromamba via subprocess
    - Installing a dependency in that environment
    - Accessing it via EnvironmentManager using Path object instead of name
    - Verifying the dependency is detected regardless of access method
    """
    import subprocess

    # Setup temporary conda root
    temp_root = tmp_path_factory.mktemp("micromamba_root")
    wetlands_instance_path = temp_root / "wetlands"
    logger.info(f"Creating test directory {temp_root}")
    # Basic environment configuration
    env_manager = EnvironmentManager(wetlandsInstancePath=wetlands_instance_path, condaPath=temp_root)

    env_name = "test_env_access_via_path"
    logger.info(f"Testing existing environment access via Path: {env_name}")

    # Get the conda root path
    conda_root = Path(env_manager.settingsManager.condaPath)

    env_path = tmp_path_factory.mktemp("test_envs") / "test_env"
    conda_bin = env_manager.settingsManager.condaBin

    # Step 1: Create environment directly with micromamba via subprocess
    logger.info(f"Creating environment at {env_path} using subprocess")
    # Need to activate micromamba first
    import os

    env_vars = os.environ.copy()
    env_vars["MAMBA_ROOT_PREFIX"] = str(conda_root)

    # Create command with proper shell activation
    shell_cmd = f"""
    cd "{conda_root}"
    export MAMBA_ROOT_PREFIX="{conda_root}"
    eval "$({str(conda_bin)} shell hook -s posix)"
    {str(conda_bin)} create -p {env_path} python=3.11 requests -y
    """

    result = subprocess.run(shell_cmd, capture_output=True, text=True, shell=True, env=env_vars)
    assert result.returncode == 0, f"Failed to create environment: {result.stderr}\nstdout: {result.stdout}"
    assert env_path.exists(), f"Environment path not created: {env_path}"
    logger.info(f"Successfully created environment at {env_path}")

    # Step 3: Access the same environment using Path object
    env_by_path = env_manager.load(env_name, env_path)
    logger.info(f"Successfully accessed environment via Path: {env_path}")

    # Verify we got a valid environment back
    assert isinstance(env_by_path, ExternalEnvironment)

    # Step 4: Verify dependency is still detected when accessed by path
    installed_by_path = env_manager.getInstalledPackages(env_by_path)
    assert any(pkg["name"] == "requests" for pkg in installed_by_path), (
        f"'requests' not found when accessing by path: {[p['name'] for p in installed_by_path]}"
    )
    logger.info(f"Verified 'requests' is installed when accessing by path")

    env_by_path.exit()
    logger.info(f"Test complete for {env_name}")


@pytest.mark.integration
@pytest.mark.skipif(not tool_available("pixi"), reason="pixi not available")
def test_existing_pixi_environment_access_via_path(tmp_path_factory):
    """Test that users can reference existing pixi environments using workspace Path.

    This test verifies real-world pixi behavior by:
    - Creating a real pixi environment directly with subprocess
    - Installing dependencies via pixi add
    - Accessing it via EnvironmentManager using workspace Path
    - Verifying dependencies are detected regardless of access method
    """
    import subprocess

    # Setup temporary pixi root
    temp_root = tmp_path_factory.mktemp("pixi_root")
    wetlands_instance_path = temp_root / "wetlands"
    logger.info(f"Creating test directory {temp_root}")
    env_manager = EnvironmentManager(wetlandsInstancePath=wetlands_instance_path, condaPath=temp_root)

    env_name = "test_pixi_access_via_path"
    logger.info(f"Testing pixi environment access via Path: {env_name}")

    # Get paths
    pixi_bin = env_manager.settingsManager.condaBin
    workspace_root = tmp_path_factory.mktemp("external_env")
    workspace_path = workspace_root / env_name
    manifest_path = workspace_path / "pixi.toml"

    # Step 1: Create workspace and pixi.toml directly with subprocess
    workspace_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Creating pixi environment at {workspace_path}")

    init_cmd = [str(pixi_bin), "init", "--no-progress", str(workspace_path)]
    result = subprocess.run(init_cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Failed to init pixi environment: {result.stderr}\nstdout: {result.stdout}"
    assert manifest_path.exists(), f"pixi.toml not created: {manifest_path}"
    logger.info(f"Successfully initialized pixi environment")

    # Step 2: Add python and requests directly with subprocess
    add_python_cmd = [str(pixi_bin), "add", "--no-progress", "--manifest-path", str(manifest_path), "python=3.11"]
    result = subprocess.run(add_python_cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Failed to add python: {result.stderr}\nstdout: {result.stdout}"

    add_requests_cmd = [str(pixi_bin), "add", "--manifest-path", str(manifest_path), "requests"]
    result = subprocess.run(add_requests_cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Failed to add requests: {result.stderr}\nstdout: {result.stdout}"
    logger.info(f"Successfully added python and requests to pixi environment")

    # Step 4: Access the environment using workspace Path
    env_by_path = env_manager.load(env_name, manifest_path)
    logger.info(f"Successfully accessed pixi environment via workspace path")

    # Verify we got a valid environment back
    assert isinstance(env_by_path, ExternalEnvironment)

    # Step 5: Verify dependencies are consistent
    installed_by_path = env_manager.getInstalledPackages(env_by_path)
    assert any(pkg["name"] == "requests" for pkg in installed_by_path), (
        f"'requests' not found when accessing via path: {[p['name'] for p in installed_by_path]}"
    )

    env_by_path.exit()
    logger.info(f"Test complete for {env_name}")


@pytest.mark.integration
def test_nonexistent_environment_path_raises_error(env_manager):
    """Test that attempting to use a nonexistent Path as environment raises an error.

    This verifies that the system properly validates paths:
    - Attempting to reference a nonexistent Path raises an exception
    - The system doesn't silently create environments for invalid paths
    """
    logger.info("Testing error handling for nonexistent environment path")

    # Create a path that definitely doesn't exist
    nonexistent_env_path = Path(env_manager.settingsManager.condaPath) / "envs" / "definitely_not_exists_xyz123"
    assert not nonexistent_env_path.exists()
    logger.info(f"Verified nonexistent path: {nonexistent_env_path}")

    # Attempting to use a nonexistent path should raise an error
    with pytest.raises(Exception, match="was not found"):
        env_manager.load("unexisting_env", nonexistent_env_path)
    logger.info("Verified error raised for nonexistent environment path")
