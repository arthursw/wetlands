
from pathlib import Path
from typing import Any, TYPE_CHECKING

from cema.dependency_manager import Dependencies
from cema.environment import Environment
if TYPE_CHECKING:
    from cema.environment_manager import EnvironmentManager


class InternalEnvironment(Environment):

    def __init__(self, name: str, environmentManager: 'EnvironmentManager') -> None:
        super().__init__(name, environmentManager)

    def install(self, 
        dependencies: Dependencies,
        additionalInstallCommands: dict[str, list[str]] = {}) -> None:
        """Useful in ExternalEnvironment only"""
        raise Exception(f'{self.name} is not a conda environment, it cannot install dependencies. Create a conda environment first with EnvironmentManager.create(force=True).')
    
    def execute(self, modulePath: str | Path, function: str, args: tuple) -> Any:
        """Executes a function in the given module
        
        Args:
                modulePath: the path to the module to import
                function: the name of the function to execute
                args: the argument list for the function 
        
        Returns:
                The result of the function
        """
        module = self._importModule(modulePath)
        if not self._isModFunction(module, function):
            raise Exception(f"Module {modulePath} has no function {function}.")
        return getattr(module, function)(*args)
