from multiprocessing.connection import Client
from pathlib import Path
import logging
import pytest
import shutil

from wetlands._internal.dependency_manager import Dependencies
from wetlands._internal.exceptions import ExecutionException
from wetlands.internal_environment import InternalEnvironment
from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment
from wetlands.task import Task, TaskFailureCategory, TaskStatus, TaskEventType


# Config file contents for parameterized test_create_from_config
PIXI_TOML_CONTENT = """
[workspace]
name = "test-project"
channels = ["conda-forge"]
platforms = ["linux-64", "osx-arm64", "osx-64", "win-64"]

[dependencies]
requests = ">=2.25"
"""

ENV_YML_CONTENT = """
name: test-env
channels:
  - conda-forge
dependencies:
  - requests
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
@pytest.mark.agent_integration
@pytest.mark.slow
def test_environment_creation_and_types(env_manager):
    """Test environment creation and internal/external type selection."""
    logger.info("Testing environment creation and types")

    # Test 1: No dependencies -> InternalEnvironment
    env_internal = env_manager.create("test_env_internal", {}, use_existing=True)
    assert isinstance(env_internal, InternalEnvironment)
    assert env_internal == env_manager.main_environment

    # Test 2: Forced creation -> ExternalEnvironment without installing extra packages
    env_external = env_manager.create("test_env_external", {}, use_existing=False)
    assert isinstance(env_external, ExternalEnvironment)

    # Test 3: Recreating same env returns same instance
    same_env = env_manager.create("test_env_external", {})
    assert env_external == same_env

    # Test 4: After exit, recreating gives different instance
    env_external.exit()
    other_env = env_manager.create("test_env_external", {}, use_existing=False)
    assert other_env != same_env
    other_env.exit()

    env_internal.exit()


@pytest.mark.integration
@pytest.mark.agent_integration
@pytest.mark.slow
def test_dependency_installation(env_manager):
    """Test that Environment.install() correctly installs dependencies in existing env."""
    logger.info("Testing dependency installation in existing env")
    env = env_manager.create("test_env_deps", use_existing=False)
    dependencies = Dependencies({"pip": ["munch==4.0.0"]})

    env.install(dependencies)

    installed_packages = env_manager.get_installed_packages(env)
    assert any(
        icp["name"] == "munch" and icp["version"].startswith("4.0.0") and icp["kind"] == "pypi"
        for icp in installed_packages
    )

    env.exit()

@pytest.mark.integration
@pytest.mark.agent_integration
@pytest.mark.slow
class TestCodeExecution:
    """Tests for code execution within launched external environments."""

    @pytest.fixture(scope="class")
    def launched_env(self, env_manager):
        """Shared external environment for code execution tests."""
        logger.info("Creating shared external environment for TestCodeExecution")
        env = env_manager.create("shared_execution_env", {}, use_existing=False)
        env.launch()
        yield env
        env.exit()

    def test_execute_and_import_module(self, launched_env, tmp_path):
        """Test that Environment.execute() and import_module() correctly execute code."""
        logger.info("Testing code execution with execute() and import_module()")

        module_path = tmp_path / "test_module.py"
        with open(module_path, "w") as f:
            f.write(
                """
import builtins

def sum(x):
    return builtins.sum(x)

def prod(x=[], y=1):
    result = 1
    for value in x:
        result *= value
    return result * y
"""
            )

        # Test execute()
        result = launched_env.execute(str(module_path), "sum", [[1, 2, 3]])
        assert result == 6
        result = launched_env.execute(str(module_path), "prod", [[1, 2, 3]], {"y": 2})
        assert result == 12

        # Test import_module()
        module = launched_env.import_module(str(module_path))
        result = module.sum([1, 2, 3])
        assert result == 6
        result = module.prod([1, 2, 3], y=3)
        assert result == 18

    def test_advanced_subprocess_execution(self, launched_env, tmp_path):
        """Test advanced execution with subprocess communication."""
        logger.info("Testing advanced subprocess execution")

        module_path = tmp_path / "test_module.py"
        with open(module_path, "w") as f:
            f.write("""from multiprocessing.connection import Listener
import sys

with Listener(("localhost", 0)) as listener:
    print(f"Listening port {listener.address[1]}")
    with listener.accept() as connection:
        while message := connection.recv():
            if message["action"] == "execute_prod":
                result = 1
                for value in message["args"]:
                    result *= value
                connection.send(result)
            if message["action"] == "execute_sum":
                connection.send(sum(message["args"]))
            if message["action"] == "exit":
                connection.send(dict(action="exited"))
                sys.exit()
    """)

        process = launched_env.execute_commands([f"python -u {(tmp_path / 'test_module.py').resolve()}"], log=False)

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
@pytest.mark.manual
@pytest.mark.slow
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
@pytest.mark.manual
@pytest.mark.slow
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
@pytest.mark.manual
@pytest.mark.slow
def test_shared_memory_ndarray(env_manager, tmp_path):
    """Test that NDArray shared memory integration works correctly.

    This test verifies:
    - NDArray can be created and passed between processes
    - Shared memory is properly allocated and cleaned up
    - No resource_tracker warnings are raised on cleanup
    """
    logger.info("Testing NDArray shared memory integration")
    pytest.importorskip("numpy", reason="NDArray round-trip requires the local shared-memory extra")

    env_name = "test_shared_memory_ndarray"
    repo_src = Path(__file__).parent.parent / "src"
    dependencies = Dependencies({"conda": [], "pip": ["numpy"]})
    env = env_manager.create(env_name, dependencies)
    env.launch()

    # Create a module that creates an NDArray
    module_path = tmp_path / "shared_memory_module.py"
    with open(module_path, "w") as f:
        f.write("""
from __future__ import annotations

import sys

sys.path.insert(0, REPO_SRC)

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
""".replace("REPO_SRC", repr(str(repo_src))))

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
@pytest.mark.slow
@pytest.mark.parametrize(
    "config_file,config_content",
    [
        ("pixi.toml", PIXI_TOML_CONTENT),
        ("environment.yml", ENV_YML_CONTENT),
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


# --- Task API integration tests ---


@pytest.mark.integration
@pytest.mark.agent_integration
@pytest.mark.slow
class TestTaskAPI:
    """Integration tests for the task-based API with real conda environments."""

    @pytest.fixture(scope="class")
    def task_env(self, env_manager):
        """Shared environment for task API tests."""
        logger.info("Creating shared environment for TestTaskAPI")
        env = env_manager.create("task_api_env", {}, use_existing=False)
        env.launch()
        yield env
        env.exit()

    def test_task_lifecycle(self, task_env, tmp_path):
        """Task lifecycle APIs work end-to-end against one remote worker environment."""
        module_path = tmp_path / "task_lifecycle.py"
        module_path.write_text(
            """
import time

def add(a, b):
    return a + b

def double(x):
    return x * 2

def identity(x):
    return x

def boom():
    raise ValueError("test error")

def chained_boom():
    try:
        raise KeyError("root cause")
    except KeyError as e:
        raise RuntimeError("outer failure") from e

def maybe_boom(x):
    if x == 2:
        raise TypeError("bad item")
    return x

def square(x):
    return x ** 2

def triple(x):
    return x * 3

def work_with_progress(n, *, task=None):
    total = 0
    for i in range(n):
        total += i
        if task:
            task.update(f"Step {i}", current=i + 1, maximum=n)
    return total

def slow_work(*, task=None):
    for i in range(1000):
        if task and task.cancel_requested:
            task.cancel()
            return None
        time.sleep(0.01)
    return "done"
"""
        )

        submitted = task_env.submit(str(module_path), "add", args=(3, 7))
        assert isinstance(submitted, Task)
        submitted.wait_for(timeout=30)
        assert submitted.status == TaskStatus.COMPLETED
        assert submitted.result == 10

        deferred = task_env.submit(str(module_path), "double", args=(21,), start=False)
        assert deferred.status == TaskStatus.PENDING
        deferred.start()
        deferred.wait_for(timeout=30)
        assert deferred.status == TaskStatus.COMPLETED
        assert deferred.result == 42

        events = []
        listened = task_env.submit(str(module_path), "identity", args=("hello",), start=False)
        listened.listen(lambda e: events.append(e.type))
        listened.start()
        listened.wait_for(timeout=30)
        assert TaskEventType.STARTED in events
        assert TaskEventType.COMPLETION in events
        assert listened.result == "hello"

        failed = task_env.submit(str(module_path), "boom")
        failed.wait_for(timeout=30)
        assert failed.status == TaskStatus.FAILED
        assert failed.error is not None
        assert "test error" in failed.error.message
        assert failed.error.category == TaskFailureCategory.REMOTE_EXCEPTION
        assert failed.error.remote_exception is not None
        assert failed.error.remote_exception.type_name == "ValueError"
        assert failed.traceback is not None
        assert "ValueError: test error" in failed.traceback

        chained = task_env.submit(str(module_path), "chained_boom")
        chained.wait_for(timeout=30)
        assert chained.error is not None
        assert chained.error.remote_exception is not None
        assert chained.error.remote_exception.type_name == "RuntimeError"
        assert chained.error.remote_exception.cause is not None
        assert chained.error.remote_exception.cause.type_name == "KeyError"

        mapped = task_env.map_tasks(str(module_path), "maybe_boom", [1, 2])
        for task in mapped:
            task.wait_for(timeout=30)
        assert mapped[0].status == TaskStatus.COMPLETED
        assert mapped[1].status == TaskStatus.FAILED
        assert mapped[1].error is not None
        assert mapped[1].error.remote_exception is not None
        assert mapped[1].error.remote_exception.type_name == "TypeError"

        with pytest.raises(ExecutionException) as execute_error:
            task_env.execute(str(module_path), "boom")
        assert execute_error.value.failure.remote_exception is not None
        assert execute_error.value.failure.remote_exception.type_name == "ValueError"

        imported = task_env.import_module(str(module_path))
        with pytest.raises(ExecutionException) as imported_error:
            imported.boom()
        assert imported_error.value.failure.remote_exception is not None
        assert imported_error.value.failure.remote_exception.type_name == "ValueError"

        future_task = task_env.submit(str(module_path), "square", args=(9,))
        assert future_task.future.result(timeout=30) == 81

        assert task_env.execute(str(module_path), "triple", (5,)) == 15

        updates = []
        progress = task_env.submit(str(module_path), "work_with_progress", args=(5,), start=False)
        progress.listen(lambda e: updates.append(e.type) if e.type == TaskEventType.UPDATE else None)
        progress.start()
        progress.wait_for(timeout=30)
        assert progress.status == TaskStatus.COMPLETED
        assert progress.result == 10
        assert len(updates) > 0

        cancellable = task_env.submit(str(module_path), "slow_work")
        import time

        time.sleep(0.2)
        cancellable.cancel()
        cancellable.wait_for(timeout=30)
        assert cancellable.status == TaskStatus.CANCELED


@pytest.mark.integration
@pytest.mark.agent_integration
@pytest.mark.slow
class TestTaskAPIConcurrency:
    """Integration tests for multi-worker concurrency."""

    @pytest.fixture(scope="class")
    def parallel_env(self, env_manager):
        """Environment with 3 workers for concurrency tests."""
        logger.info("Creating parallel environment for TestTaskAPIConcurrency")
        env = env_manager.create("parallel_env", {}, use_existing=False)
        env.launch(max_workers=3)
        yield env
        env.exit()

    def test_parallel_task_apis(self, parallel_env, tmp_path):
        """map(), map_tasks(), submit(), and execute() work with a multi-worker pool."""
        import concurrent.futures
        import time

        module_path = tmp_path / "parallel_compute.py"
        module_path.write_text(
            """
import time

def square(x):
    return x ** 2

def slow_double(x):
    time.sleep(0.1)
    return x * 2

def sleep_and_return(x):
    time.sleep(0.3)
    return x

def inc(x):
    return x + 1
"""
        )

        map_results = list(parallel_env.map(str(module_path), "square", [1, 2, 3, 4, 5]))
        assert map_results == [1, 4, 9, 16, 25]

        mapped_tasks = parallel_env.map_tasks(str(module_path), "slow_double", [10, 20, 30])
        assert len(mapped_tasks) == 3
        for task in mapped_tasks:
            task.wait_for(timeout=30)
        assert [task.result for task in mapped_tasks] == [20, 40, 60]

        start = time.monotonic()
        t1 = parallel_env.submit(str(module_path), "sleep_and_return", args=(1,))
        t2 = parallel_env.submit(str(module_path), "sleep_and_return", args=(2,))
        t3 = parallel_env.submit(str(module_path), "sleep_and_return", args=(3,))

        concurrent.futures.wait([t1.future, t2.future, t3.future], timeout=30)
        elapsed = time.monotonic() - start

        assert t1.result == 1
        assert t2.result == 2
        assert t3.result == 3
        # With 3 workers, 3 tasks sleeping 0.3s each should complete in ~0.3s, not ~0.9s
        assert elapsed < 1.0, f"Expected parallel execution but took {elapsed:.1f}s"

        assert parallel_env.execute(str(module_path), "inc", (99,)) == 100
