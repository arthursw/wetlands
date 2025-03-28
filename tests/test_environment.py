import pytest
import sys
from unittest.mock import MagicMock, patch
from types import ModuleType
from cema.environment import Environment

class DummyEnvironment(Environment):
    def launch(self, additionalActivateCommands={}, logOutputInThread=True):
        pass
    
    def execute(self, modulePath, function, args):
        return f"Executed {function} with {args}"

@pytest.fixture
def mock_environment_manager():
    return MagicMock()

@pytest.fixture
def dummy_env(mock_environment_manager):
    return DummyEnvironment("test_env", mock_environment_manager)

@patch("sys.path", new=[])
@patch("cema.environment.import_module")
def test_importModule(mock_import_module, dummy_env):
    mock_mod = ModuleType("test_mod")
    mock_import_module.return_value = mock_mod
    
    module = dummy_env._importModule("/path/to/test_mod.py")
    assert module == mock_mod
    assert "test_mod" in dummy_env.modules
    assert dummy_env.modules["test_mod"] == mock_mod

@patch("cema.environment.Environment._importModule")
@patch("cema.environment.Environment._listFunctions")
def test_importModule_creates_fake_module(mock_listFunctions, mock_importModule, dummy_env):
    mock_mod = MagicMock()
    mock_importModule.return_value = mock_mod
    mock_listFunctions.return_value = ["func1", "func2"]
    
    fake_module = dummy_env.importModule("/path/to/test_mod.py")
    
    assert hasattr(fake_module, "func1")
    assert hasattr(fake_module, "func2")
    
    result = fake_module.func1(param1="value1")
    assert result == "Executed func1 with {'param1': 'value1'}"

def test_exit(dummy_env, mock_environment_manager):
    dummy_env.exit()
    mock_environment_manager._removeEnvironment.assert_called_once_with("test_env")
