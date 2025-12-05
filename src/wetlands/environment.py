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

    def __init__(self, name: str, path: Path | None, environment_manager: "EnvironmentManager") -> None:
        self.name = name
        self.path = path.resolve() if isinstance(path, Path) else path
        self.environment_manager = environment_manager

    def _is_mod_function(self, mod, func):
        """Checks that func is a function defined in module mod"""
        return inspect.isfunction(func) and inspect.getmodule(func) == mod

    def _list_functions(self, mod):
        """Returns the list of functions defined in module mod"""
        return [func.__name__ for func in mod.__dict__.values() if self._is_mod_function(mod, func)]

    def _import_module(self, module_path: Path | str):
        """Imports the given module (if necessary) and adds it to the module map."""
        module_path = Path(module_path)
        module = module_path.stem
        if module not in self.modules:
            sys.path.append(str(module_path.parent))
            self.modules[module] = import_module(module)
        return self.modules[module]

    def import_module(self, module_path: Path | str) -> Any:
        """Imports the given module (if necessary) and returns a fake module object
        that contains the same methods of the module which will be executed within the environment."""
        module = self._import_module(module_path)

        class FakeModule:
            pass

        for f in self._list_functions(module):

            def fake_function(*args, _wetlands_imported_function=f, **kwargs):
                return self.execute(module_path, _wetlands_imported_function, args, kwargs)

            setattr(FakeModule, f, fake_function)
        return FakeModule

    def install(self, dependencies: Dependencies, additional_install_commands: Commands = {}) -> list[str]:
        """Installs dependencies.
        See [`EnvironmentManager.create`][wetlands.environment_manager.EnvironmentManager.create] for more details on the ``dependencies`` and ``additional_install_commands`` parameters.

        Args:
                dependencies: Dependencies to install.
                additional_install_commands: Platform-specific commands during installation.
        Returns:
                Output lines of the installation commands.
        """
        return self.environment_manager.install(self, dependencies, additional_install_commands)

    def launch(self, additional_activate_commands: Commands = {}, log_output_in_thread: bool = True) -> None:
        """Launch the environment, only available in [ExternalEnvironment][wetlands.external_environment.ExternalEnvironment]. Do nothing when InternalEnvironment. See [`ExternalEnvironment.launch`][wetlands.external_environment.ExternalEnvironment.launch]"""
        return

    def execute_commands(
        self,
        commands: Commands,
        additional_activate_commands: Commands = {},
        popen_kwargs: dict[str, Any] = {},
        wait: bool = False,
        log_context: dict[str, Any] | None = None,
        log: bool = True,
    ) -> subprocess.Popen:
        """Executes the given commands in this environment.

        Args:
                commands: The commands to execute in the environment.
                additional_activate_commands: Platform-specific activation commands.
                popen_kwargs: Keyword arguments for subprocess.Popen(). See [`EnvironmentManager.execute_commands`][wetlands.environment_manager.EnvironmentManager.execute_commands].
                wait: Whether to wait for the process to complete before returning.
                log_context: Optional context dict to attach to logs via ProcessLogger.
                log: Whether to log the process output.

        Returns:
                The launched process.
        """
        return self.environment_manager.execute_commands(
            self, commands, additional_activate_commands, popen_kwargs, wait=wait, log_context=log_context, log=log
        )

    @abstractmethod
    def execute(self, module_path: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}) -> Any:
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
        self.environment_manager._remove_environment(self)

    def delete(self) -> None:
        """Delete this environment. Only available in ExternalEnvironment."""
        raise NotImplementedError("delete() is only available in ExternalEnvironment")

    def update(
        self,
        dependencies: Union[Dependencies, None] = None,
        additional_install_commands: Commands = {},
        use_existing: bool = False,
    ) -> "Environment":
        """Update this environment with new dependencies. Only available in ExternalEnvironment."""
        raise NotImplementedError("update() in ExternalEnvironment")
