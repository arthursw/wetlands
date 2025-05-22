from pathlib import Path
from typing import Any, TYPE_CHECKING

from wetlands._internal.command_generator import Commands
from wetlands.environment import Environment

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


class InternalEnvironment(Environment):
    def __init__(self, path: Path | None, environmentManager: "EnvironmentManager") -> None:
        """Use absolute path as name for micromamba to consider the activation from a folder path, not from a name"""
        name = str(path.resolve()) if isinstance(path, Path) else path
        super().__init__(name, environmentManager)

    def launch(self, additionalActivateCommands: Commands = {}, logOutputInThread: bool = True) -> None:
        """Raise an exception. See [`Environment.launch`][wetlands.environment.Environment.launch] and [`ExternalEnvironment.launch`][wetlands.external_environment.ExternalEnvironment.launch]"""
        raise Exception("Cannot launch the main environment.")

    def execute(self, modulePath: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}) -> Any:
        """Executes a function in the given module

        Args:
                modulePath: the path to the module to import
                function: the name of the function to execute
                args: the argument list for the function
                kwargs: the keyword arguments for the function

        Returns:
                The result of the function
        """
        module = self._importModule(modulePath)
        if not self._isModFunction(module, function):
            raise Exception(f"Module {modulePath} has no function {function}.")
        return getattr(module, function)(*args)
