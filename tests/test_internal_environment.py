from pathlib import Path
import pytest
import sys
from unittest.mock import MagicMock, patch
from wetlands.environment_manager import EnvironmentManager
from wetlands.internal_environment import InternalEnvironment


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
        patch.object(internal_env, "_importModule", return_value=mock_module),
        patch.object(internal_env, "_isModFunction", return_value=True),
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
        patch.object(internal_env, "_importModule", return_value=mock_module),
        patch.object(internal_env, "_isModFunction", return_value=False),
    ):
        with pytest.raises(Exception, match=f"Module {module_path} has no function {function_name}."):
            internal_env.execute(module_path, function_name, ())


def test_run_script_success():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    script_path = "/path/to/script.py"

    with patch("runpy.run_path") as mock_run_path:
        result = internal_env.runScript(script_path)

    mock_run_path.assert_called_once_with(script_path, run_name="__main__")
    assert result is None
    assert sys.argv[0] == script_path


def test_run_script_with_arguments():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    script_path = "/path/to/script.py"
    args = ("arg1", "arg2", "arg3")

    with patch("runpy.run_path") as mock_run_path:
        result = internal_env.runScript(script_path, args=args)

    mock_run_path.assert_called_once_with(script_path, run_name="__main__")
    assert result is None
    assert sys.argv == [script_path, "arg1", "arg2", "arg3"]


def test_run_script_with_custom_run_name():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("main_env", Path("test_env"), env_manager)
    script_path = "/path/to/script.py"
    run_name = "custom_name"

    with patch("runpy.run_path") as mock_run_path:
        result = internal_env.runScript(script_path, run_name=run_name)

    mock_run_path.assert_called_once_with(script_path, run_name=run_name)
    assert result is None
