from pathlib import Path
import pytest
import sys
from unittest.mock import MagicMock, patch
from wetlands.environment_manager import EnvironmentManager
from wetlands.internal_environment import InternalEnvironment
from wetlands.task import Task, TaskStatus


def test_execute_function_success():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    module_path = "fake_module.py"
    function_name = "test_function"
    args = (1, 2, 3)

    mock_module = MagicMock()
    mock_function = MagicMock(return_value="success")
    setattr(mock_module, function_name, mock_function)

    with (
        patch.object(internal_env, "_import_module", return_value=mock_module),
        patch.object(internal_env, "_is_mod_function", return_value=True),
    ):
        result = internal_env.execute(module_path, function_name, args)

    mock_function.assert_called_once_with(*args)
    assert result == "success"


def test_execute_raises_exception_for_missing_function():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    module_path = "fake_module.py"
    function_name = "non_existent_function"

    mock_module = MagicMock()

    with (
        patch.object(internal_env, "_import_module", return_value=mock_module),
        patch.object(internal_env, "_is_mod_function", return_value=False),
    ):
        with pytest.raises(Exception, match=f"Module {module_path} has no function {function_name}."):
            internal_env.execute(module_path, function_name, ())


def test_run_script_success():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    script_path = "/path/to/script.py"

    with patch("runpy.run_path") as mock_run_path:
        result = internal_env.run_script(script_path)

    mock_run_path.assert_called_once_with(script_path, run_name="__main__")
    assert result is None
    assert sys.argv[0] == script_path


def test_run_script_with_arguments():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    script_path = "/path/to/script.py"
    args = ("arg1", "arg2", "arg3")

    with patch("runpy.run_path") as mock_run_path:
        result = internal_env.run_script(script_path, args=args)

    mock_run_path.assert_called_once_with(script_path, run_name="__main__")
    assert result is None
    assert sys.argv == [script_path, "arg1", "arg2", "arg3"]


def test_run_script_with_custom_run_name():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    script_path = "/path/to/script.py"
    run_name = "custom_name"

    with patch("runpy.run_path") as mock_run_path:
        result = internal_env.run_script(script_path, run_name=run_name)

    mock_run_path.assert_called_once_with(script_path, run_name=run_name)
    assert result is None


# --- Task API tests ---


@pytest.fixture
def internal_env():
    env_manager = MagicMock(spec=EnvironmentManager)
    return InternalEnvironment("test_env", Path("test_env"), env_manager)


def test_submit_returns_completed_task(internal_env):
    module_path = "fake_module.py"
    mock_module = MagicMock()
    mock_function = MagicMock(return_value=42)
    setattr(mock_module, "add", mock_function)

    with (
        patch.object(internal_env, "_import_module", return_value=mock_module),
    ):
        task = internal_env.submit(module_path, "add", args=(1, 2))

    task.wait_for(timeout=5)
    assert isinstance(task, Task)
    assert task.status == TaskStatus.COMPLETED
    assert task.result == 42
    mock_function.assert_called_once_with(1, 2)


def test_submit_with_start_false_stays_pending(internal_env):
    module_path = "fake_module.py"
    mock_module = MagicMock()
    mock_function = MagicMock(return_value=10)
    setattr(mock_module, "f", mock_function)

    with patch.object(internal_env, "_import_module", return_value=mock_module):
        task = internal_env.submit(module_path, "f", start=False)

        assert task.status == TaskStatus.PENDING
        mock_function.assert_not_called()

        # Now start it and verify it completes
        task.start()
        task.wait_for(timeout=5)
        assert task.status == TaskStatus.COMPLETED


def test_submit_script_returns_task(internal_env):
    script_path = "/path/to/script.py"

    with patch("runpy.run_path"):
        task = internal_env.submit_script(script_path)
        task.wait_for(timeout=5)

    assert isinstance(task, Task)
    assert task.status == TaskStatus.COMPLETED
    assert task.result is None


def test_map_yields_results_in_order(internal_env):
    module_path = "fake_module.py"
    mock_module = MagicMock()

    def double(x):
        return x * 2

    mock_module.double = double

    with patch.object(internal_env, "_import_module", return_value=mock_module):
        results = list(internal_env.map(module_path, "double", [1, 2, 3, 4]))

    assert results == [2, 4, 6, 8]


def test_map_tasks_returns_task_list(internal_env):
    module_path = "fake_module.py"
    mock_module = MagicMock()

    def square(x):
        return x**2

    mock_module.square = square

    with patch.object(internal_env, "_import_module", return_value=mock_module):
        tasks = internal_env.map_tasks(module_path, "square", [2, 3, 5])

        assert len(tasks) == 3
        for t in tasks:
            assert isinstance(t, Task)
            t.wait_for(timeout=5)
            assert t.status == TaskStatus.COMPLETED

        assert [t.result for t in tasks] == [4, 9, 25]
