import runpy
import sys
from pathlib import Path
from typing import Any, TYPE_CHECKING

from wetlands.environment import Environment

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


class InternalEnvironment(Environment):
    def __init__(self, name: str, path: Path | None, environmentManager: "EnvironmentManager") -> None:
        """Use absolute path as name for micromamba to consider the activation from a folder path, not from a name"""
        super().__init__(name, path, environmentManager)

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

    def runScript(self, scriptPath: str | Path, args: tuple = (), run_name: str = "__main__") -> Any:
        """
        Runs a Python script locally using runpy.run_path(), simulating
        'python script.py arg1 arg2 ...'

        Args:
            scriptPath: Path to the script to execute.
            args: List of arguments to pass (becomes sys.argv[1:] locally).
            run_name: Value for runpy.run_path(run_name=...); defaults to "__main__".

        Returns:
            The resulting globals dict from the executed script, or None on failure.
        """
        scriptPath = str(scriptPath)
        sys.argv = [scriptPath] + list(args)
        runpy.run_path(scriptPath, run_name=run_name)
        return None
