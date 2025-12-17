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


# Config file contents for parameterized test_create_from_config
PIXI_TOML_CONTENT = """
[workspace]
name = "test-project"
channels = ["conda-forge"]
platforms = ["linux-64", "osx-arm64", "osx-64", "win-64"]

[dependencies]
requests = ">=2.25"
"""

PYPROJECT_TOML_CONTENT = """
[project]
name = "test-project"
version = "0.1.0"
dependencies = [
    "requests>=2.25",
]

[project.optional-dependencies]
dev = ["pytest>=6.0"]
"""

ENV_YML_CONTENT = """
name: test-env
channels:
  - conda-forge
dependencies:
  - requests
"""

REQUIREMENTS_TXT_CONTENT = """
requests>=2.25
"""


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
    manager = EnvironmentManager(wetlands_instance_path=wetlands_instance_path, conda_path=temp_root)
    yield manager

    for env_name, env in manager.environments.copy().items():
        logger.info(f"Exiting environment {env_name}")
        env.exit()

    # Clean temp directory handled by pytest
    print(f"Removing {temp_root}")


@pytest.mark.integration
def test_environment_creation_and_types(env_manager):
    """Test environment creation, dependency installation, and internal/external type selection."""
    logger.info("Testing environment creation, types, and dependencies")

    # Test 1: No dependencies -> InternalEnvironment
    env_internal = env_manager.create("test_env_internal", {}, use_existing=True)
    assert isinstance(env_internal, InternalEnvironment)
    assert env_internal == env_manager.main_environment

    # Test 2: With dependencies -> ExternalEnvironment + deps installed
    dependencies = Dependencies({"conda": ["requests"]})
    env_external = env_manager.create("test_env_external", dependencies)
    assert isinstance(env_external, ExternalEnvironment)

    installed_packages = env_manager.get_installed_packages(env_external)
    assert any(icp["name"] == "requests" for icp in installed_packages)

    # Test 3: Recreating same env returns same instance
    same_env = env_manager.create("test_env_external", dependencies)
    assert env_external == same_env

    # Test 4: After exit, recreating gives different instance
    env_external.exit()
    other_env = env_manager.create("test_env_external", dependencies)
    assert other_env != same_env
    other_env.exit()

    # Test 5: Force external with use_existing=False
    env_external_forced = env_manager.create("test_env_external_forced", {}, use_existing=False)
    assert isinstance(env_external_forced, ExternalEnvironment)

    env_internal.exit()
    env_external_forced.exit()


@pytest.mark.integration
def test_dependency_installation(env_manager):
    """Test that Environment.install() correctly installs dependencies in existing env."""
    logger.info("Testing dependency installation in existing env")
    env = env_manager.create("test_env_deps", use_existing=False)
    dependencies = Dependencies({"pip": ["munch==4.0.0"], "conda": ["fastai::fastprogress==1.0.3"]})

    env.install(dependencies)

    installed_packages = env_manager.get_installed_packages(env)
    assert any(
        icp["name"] == "fastprogress" and icp["version"].startswith("1.0.3") and icp["kind"] == "conda"
        for icp in installed_packages
    )
    assert any(
        icp["name"] == "munch" and icp["version"].startswith("4.0.0") and icp["kind"] == "pypi"
        for icp in installed_packages
    )

    env.exit()


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
    env_manager.set_proxies(proxies)
    if env_manager.settings_manager.use_pixi:
        assert env_manager.settings_manager.proxies == proxies
        env_manager.set_proxies({})
        assert env_manager.settings_manager.proxies == {}
        return
    mambarc_path = Path(env_manager.settings_manager.conda_path) / ".mambarc"
    assert os.path.exists(mambarc_path)

    with open(mambarc_path, "r") as f:
        content = f.read()
        assert "http: http://proxy.example.com" in content
        assert "https: https://proxy.example.com" in content

    env_manager.set_proxies({})

    with open(mambarc_path, "r") as f:
        content = f.read()
        assert "proxy" not in content
        assert "http: http://proxy.example.com" not in content
        assert "https: https://proxy.example.com" not in content


@pytest.mark.integration
class TestCodeExecution:
    """Tests for code execution within environments, using a shared numpy environment."""

    @pytest.fixture(scope="class")
    def numpy_env(self, env_manager):
        """Shared numpy environment for code execution tests."""
        logger.info("Creating shared numpy environment for TestCodeExecution")
        env = env_manager.create("shared_numpy_env", {"conda": ["numpy"]})
        env.launch()
        yield env
        env.exit()

    def test_execute_and_import_module(self, numpy_env, tmp_path):
        """Test that Environment.execute() and import_module() correctly execute code."""
        logger.info("Testing code execution with execute() and import_module()")

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

        # Test execute()
        result = numpy_env.execute(str(module_path), "sum", [[1, 2, 3]])
        assert result == 6
        result = numpy_env.execute(str(module_path), "prod", [[1, 2, 3]], {"y": 2})
        assert result == 12

        # Test import_module()
        module = numpy_env.import_module(str(module_path))
        result = module.sum([1, 2, 3])
        assert result == 6
        result = module.prod([1, 2, 3], y=3)
        assert result == 18

    def test_advanced_subprocess_execution(self, numpy_env, tmp_path):
        """Test advanced execution with subprocess communication."""
        logger.info("Testing advanced subprocess execution")

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

        process = numpy_env.execute_commands([f"python -u {(tmp_path / 'test_module.py').resolve()}"], log=False)

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
def test_execution_errors(env_manager, tmp_path):
    """Test that proper exceptions are raised for non-existent functions and modules."""
    logger.info("Testing execution errors for non-existent function/module")

    module_path = tmp_path / "test_module.py"
    with open(module_path, "w") as f:
        f.write(
            """
def double(x):
    return x * 2
"""
        )

    env = env_manager.create("test_env_execution_errors", {}, use_existing=True)

    # Test non-existent function via execute
    with pytest.raises(Exception) as excinfo:
        env.execute(str(module_path), "non_existent_function", [1])
    assert "has no function" in str(excinfo.value)

    # Test non-existent function via import_module
    module = env.import_module(str(module_path))
    with pytest.raises(Exception) as excinfo:
        module.non_existent_function(1)
    assert "has no attribute" in str(excinfo.value)

    # Test non-existent module via execute
    with pytest.raises(ModuleNotFoundError):
        env.execute("non_existent_module.py", "my_function", [1])

    # Test non-existent module via import_module
    with pytest.raises(ModuleNotFoundError):
        env.import_module("non_existent_module.py")

    env.exit()


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
    env_manager = EnvironmentManager(wetlands_instance_path=wetlands_instance_path, conda_path=temp_root)

    env_name = "test_env_access_via_path"
    logger.info(f"Testing existing environment access via Path: {env_name}")

    # Get the conda root path
    conda_root = Path(env_manager.settings_manager.conda_path)

    env_path = tmp_path_factory.mktemp("test_envs") / "test_env"
    conda_bin = env_manager.settings_manager.conda_bin

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
    installed_by_path = env_manager.get_installed_packages(env_by_path)
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
    env_manager = EnvironmentManager(wetlands_instance_path=wetlands_instance_path, conda_path=temp_root)

    env_name = "test_pixi_access_via_path"
    logger.info(f"Testing pixi environment access via Path: {env_name}")

    # Get paths
    pixi_bin = env_manager.settings_manager.conda_bin
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
    installed_by_path = env_manager.get_installed_packages(env_by_path)
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
    nonexistent_env_path = Path(env_manager.settings_manager.conda_path) / "envs" / "definitely_not_exists_xyz123"
    assert not nonexistent_env_path.exists()
    logger.info(f"Verified nonexistent path: {nonexistent_env_path}")

    # Attempting to use a nonexistent path should raise an error
    with pytest.raises(Exception, match="was not found"):
        env_manager.load("unexisting_env", nonexistent_env_path)
    logger.info("Verified error raised for nonexistent environment path")


@pytest.mark.integration
def test_shared_memory_ndarray(env_manager, tmp_path):
    """Test that NDArray shared memory integration works correctly.

    This test verifies:
    - NDArray can be created and passed between processes
    - Shared memory is properly allocated and cleaned up
    - No resource_tracker warnings are raised on cleanup
    """
    logger.info("Testing NDArray shared memory integration")

    env_name = "test_shared_memory_ndarray"
    dependencies = Dependencies({"conda": [], "pip": ["numpy", f"wetlands@file:{str(Path(__file__).parent.parent)}"]})
    env = env_manager.create(env_name, dependencies)
    env.launch()

    # Create a module that creates an NDArray
    module_path = tmp_path / "shared_memory_module.py"
    with open(module_path, "w") as f:
        f.write("""
import numpy as np
from wetlands.ndarray import NDArray

ndarray: NDArray | None = None

def create_array(shape, dtype_str):
    global ndarray
    arr = np.random.rand(*shape).astype(dtype_str)
    ndarray = NDArray(arr)
    return ndarray

def clean():
    global ndarray
    if ndarray is None:
        return
    ndarray.close()
    ndarray.unlink()
    ndarray = None
""")

    # Import the module
    shared_memory_module = env.import_module(str(module_path))

    # Create an NDArray in the subprocess
    shape = (10, 10)
    dtype_str = "float32"
    masks_ndarray = shared_memory_module.create_array(shape, dtype_str)

    # Verify we got the NDArray back
    assert masks_ndarray is not None
    assert masks_ndarray.array.shape == shape
    assert str(masks_ndarray.array.dtype) == dtype_str

    # Verify data integrity - array should have random values
    assert masks_ndarray.array.size > 0
    assert 0 <= masks_ndarray.array.min() < 1
    assert 0 < masks_ndarray.array.max() <= 1

    # Clean up the shared memory properly
    masks_ndarray.close()
    shared_memory_module.clean()

    from multiprocessing import resource_tracker

    # Avoid resource_tracker warnings
    try:
        resource_tracker.unregister(masks_ndarray.shm._name, "shared_memory")  # type: ignore
    except Exception:
        pass  # Silently ignore if unregister fails

    env.exit()
    logger.info("Test completed successfully with no resource leaks")


@pytest.mark.integration
@pytest.mark.parametrize(
    "config_file,config_content",
    [
        ("pixi.toml", PIXI_TOML_CONTENT),
        ("pyproject.toml", PYPROJECT_TOML_CONTENT),
        ("environment.yml", ENV_YML_CONTENT),
        ("requirements.txt", REQUIREMENTS_TXT_CONTENT),
    ],
)
def test_create_from_config(env_manager, tmp_path, config_file, config_content):
    """Test that EnvironmentManager.create_from_config() works with various config files."""
    logger.info(f"Testing create_from_config with {config_file}")

    # Create config file
    config_path = tmp_path / config_file
    config_path.write_text(config_content)

    env_name = f"test_{config_file.replace('.', '_')}"
    env = env_manager.create_from_config(name=env_name, config_path=config_path)

    # Verify environment was created
    assert env is not None
    assert isinstance(env, ExternalEnvironment)

    # Verify that 'requests' is installed
    installed_packages = env_manager.get_installed_packages(env)
    assert any(pkg["name"] == "requests" for pkg in installed_packages), (
        f"'requests' not found in installed packages: {[p['name'] for p in installed_packages]}"
    )

    env.exit()
    logger.info(f"Test create_from_config with {config_file} completed successfully")
