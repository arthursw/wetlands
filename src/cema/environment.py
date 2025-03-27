import sys
from pathlib import Path
from importlib import import_module
from abc import abstractmethod
from typing import Any, TYPE_CHECKING
from types import ModuleType
import inspect

if TYPE_CHECKING:
    from cema.environment_manager import EnvironmentManager

class Environment:
    
    modules: dict[str, ModuleType] = {}

    def __init__(self, name: str, environmentManager: EnvironmentManager) -> None:
        self.name = name
        self.environmentManager = environmentManager

    def _isModFunction(self, mod, func):
        """Checks that func is a function defined in module mod"""
        return inspect.isfunction(func) and inspect.getmodule(func) == mod

    def _listFunctions(self, mod):
        """Returns the list of functions defined in module mod"""
        return [func.__name__ for func in mod.__dict__.values() 
                if self._isModFunction(mod, func)]

    def _importModule(self, modulePath: Path | str):
        """Imports the given module (if necessary) and adds it to the module map."""
        modulePath = Path(modulePath)
        module = modulePath.stem
        if module not in self.modules:
            sys.path.append(str(modulePath.parent))
            self.modules[module] = import_module(module)
        return self.modules[module]

    def importModule(self, modulePath: Path | str):
        """Imports the given module (if necessary) and returns a fake module object
          that contains the same methods of the module which will be executed within the environment."""
        module = self._importModule(modulePath)
        fakeModule = object()
        for f in self._listFunctions(module):
            setattr(fakeModule, f, lambda **args: self.execute(modulePath, f, args))
        return fakeModule
    
    @abstractmethod
    def launch(self,
        additionalActivateCommands: dict[str, list[str]] = {},
        logOutputInThread: bool = True) -> None:
        """See :meth:`ExternalEnvironment.launch`"""
        pass

    @abstractmethod
    def execute(self, modulePath: str | Path, function: str, args: list) -> Any:
        """Execute the given function in the given module. See :meth:`ExternalEnvironment.execute` and :meth:`InternalEnvironment.execute`"""
        pass

    def _exit(self) -> None:
        """Exit the environment, important in ExternalEnvironment"""
        pass
    
    def launched(self) -> bool:
        """Check if the environment is launched, important in ExternalEnvironment"""
        return True

    def exit(self) -> None:
        """Exit the environment"""
        self._exit()
        self.environmentManager._removeEnvironment(self.name)
