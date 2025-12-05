import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from types import ModuleType
from wetlands.environment import Environment


class DummyEnvironment(Environment):
    def launch(self, additional_activate_commands={}, log_output_in_thread=True):
        pass

    def execute(self, module_path, function, args=[], kwargs={}):
        return f"Executed {function} in {module_path} with args {args} and kwargs {kwargs}"


@pytest.fixture
def mock_environment_manager():
    return MagicMock()


@pytest.fixture
def dummy_env(mock_environment_manager):
    return DummyEnvironment("test_env", Path("/tmp/test_env"), mock_environment_manager)


@patch("sys.path", new=[])
@patch("wetlands.environment.import_module")
def test_importModule(mock_import_module, dummy_env):
    mock_mod = ModuleType("test_mod")
    mock_import_module.return_value = mock_mod

    module = dummy_env._import_module("/path/to/test_mod.py")
    assert module == mock_mod
    assert "test_mod" in dummy_env.modules
    assert dummy_env.modules["test_mod"] == mock_mod


@patch("wetlands.environment.Environment._import_module")
@patch("wetlands.environment.Environment._list_functions")
def test_importModule_creates_fake_module(mock_listFunctions, mock_importModule, dummy_env):
    mock_mod = MagicMock()
    mock_importModule.return_value = mock_mod
    mock_listFunctions.return_value = ["func1", "func2"]

    fake_module = dummy_env.import_module("/path/to/test_mod.py")

    assert hasattr(fake_module, "func1")
    assert hasattr(fake_module, "func2")

    result = fake_module.func1("value1")
    assert result == "Executed func1 in /path/to/test_mod.py with args ('value1',) and kwargs {}"

    result = fake_module.func2("value2", arg_name="arg_value")
    assert result == "Executed func2 in /path/to/test_mod.py with args ('value2',) and kwargs {'arg_name': 'arg_value'}"


def test_exit(dummy_env, mock_environment_manager):
    dummy_env.exit()
    mock_environment_manager._remove_environment.assert_called_once_with(dummy_env)
