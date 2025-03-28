import pytest
from unittest.mock import MagicMock, patch
from cema.environment_manager import EnvironmentManager
from cema.dependency_manager import Dependencies
from cema.internal_environment import InternalEnvironment


def test_install_raises_exception():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("test_env", env_manager)
    dependencies = MagicMock(spec=Dependencies)
    
    with pytest.raises(Exception, match="test_env*"):
        internal_env.install(dependencies)


def test_execute_function_success():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("test_env", env_manager)
    module_path = "fake_module.py"
    function_name = "test_function"
    args = (1, 2, 3)

    mock_module = MagicMock()
    mock_function = MagicMock(return_value="success")
    setattr(mock_module, function_name, mock_function)
    
    with patch.object(internal_env, "_importModule", return_value=mock_module), \
         patch.object(internal_env, "_isModFunction", return_value=True):
        
        result = internal_env.execute(module_path, function_name, args)
    
    mock_function.assert_called_once_with(*args)
    assert result == "success"


def test_execute_raises_exception_for_missing_function():
    env_manager = MagicMock(spec=EnvironmentManager)
    internal_env = InternalEnvironment("test_env", env_manager)
    module_path = "fake_module.py"
    function_name = "non_existent_function"
    
    mock_module = MagicMock()
    
    with patch.object(internal_env, "_importModule", return_value=mock_module), \
         patch.object(internal_env, "_isModFunction", return_value=False):
        
        with pytest.raises(Exception, match=f"Module {module_path} has no function {function_name}."):
            internal_env.execute(module_path, function_name, ())
