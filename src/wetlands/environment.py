import subprocess
import sys
from pathlib import Path
from importlib import import_module
from abc import abstractmethod
from typing import Any, TYPE_CHECKING, Union
from types import ModuleType
import inspect

from wetlands._internal.command_generator import Commands
from wetlands._internal.dependency_manager import Dependencies

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


class Environment:
    modules: dict[str, ModuleType] = {}

    def __init__(self, name: str, path: Path | None, environmentManager: "EnvironmentManager") -> None:
        self.name = name
        self.path = path.resolve() if isinstance(path, Path) else path
        self.environmentManager = environmentManager

    def _isModFunction(self, mod, func):
        """Checks that func is a function defined in module mod"""
        return inspect.isfunction(func) and inspect.getmodule(func) == mod

    def _listFunctions(self, mod):
        """Returns the list of functions defined in module mod"""
        return [func.__name__ for func in mod.__dict__.values() if self._isModFunction(mod, func)]

    def _importModule(self, modulePath: Path | str):
        """Imports the given module (if necessary) and adds it to the module map."""
        modulePath = Path(modulePath)
        module = modulePath.stem
        if module not in self.modules:
            sys.path.append(str(modulePath.parent))
            self.modules[module] = import_module(module)
        return self.modules[module]

    def importModule(self, modulePath: Path | str) -> Any:
        """Imports the given module (if necessary) and returns a fake module object
        that contains the same methods of the module which will be executed within the environment."""
        module = self._importModule(modulePath)

        class FakeModule:
            pass

        for f in self._listFunctions(module):

            def fakeFunction(*args, _wetlands_imported_function=f, **kwargs):
                return self.execute(modulePath, _wetlands_imported_function, args, kwargs)

            setattr(FakeModule, f, fakeFunction)
        return FakeModule

    def install(self, dependencies: Dependencies, additionalInstallCommands: Commands = {}) -> list[str]:
        """Installs dependencies.
        See [`EnvironmentManager.create`][wetlands.environment_manager.EnvironmentManager.create] for more details on the ``dependencies`` and ``additionalInstallCommands`` parameters.

        Args:
                dependencies: Dependencies to install.
                additionalInstallCommands: Platform-specific commands during installation.
        Returns:
                Output lines of the installation commands.
        """
        return self.environmentManager.install(self, dependencies, additionalInstallCommands)

    def launch(self, additionalActivateCommands: Commands = {}, logOutputInThread: bool = True) -> None:
        """Launch the environment, only available in [ExternalEnvironment][wetlands.external_environment.ExternalEnvironment]. Do nothing when InternalEnvironment. See [`ExternalEnvironment.launch`][wetlands.external_environment.ExternalEnvironment.launch]"""
        return

    def executeCommands(
        self,
        commands: Commands,
        additionalActivateCommands: Commands = {},
        popenKwargs: dict[str, Any] = {},
        wait: bool = False,
    ) -> subprocess.Popen:
        """Executes the given commands in this environment.

        Args:
                commands: The commands to execute in the environment.
                additionalActivateCommands: Platform-specific activation commands.
                popenKwargs: Keyword arguments for subprocess.Popen(). See [`EnvironmentManager.executeCommands`][wetlands.environment_manager.EnvironmentManager.executeCommands].
                wait: Whether to wait for the process to complete before returning.

        Returns:
                The launched process.
        """
        return self.environmentManager.executeCommands(
            self, commands, additionalActivateCommands, popenKwargs, wait=wait
        )

    @abstractmethod
    def execute(self, modulePath: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}) -> Any:
        """Execute the given function in the given module. See [`ExternalEnvironment.execute`][wetlands.external_environment.ExternalEnvironment.execute] and [`InternalEnvironment.execute`][wetlands.internal_environment.InternalEnvironment.execute]"""
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
        self.environmentManager._removeEnvironment(self)

    def delete(self) -> None:
        """Delete this environment. Only available in ExternalEnvironment."""
        raise NotImplementedError("delete() is only available in ExternalEnvironment")

    def update(
        self,
        dependencies: Union[Dependencies, None] = None,
        additionalInstallCommands: Commands = {},
        useExisting: bool = False,
    ) -> "Environment":
        """Update this environment with new dependencies. Only available in ExternalEnvironment."""
        raise NotImplementedError("update() in ExternalEnvironment")
