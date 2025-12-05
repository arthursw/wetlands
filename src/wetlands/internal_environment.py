import runpy
import sys
from pathlib import Path
from typing import Any, TYPE_CHECKING

from wetlands.environment import Environment

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


class InternalEnvironment(Environment):
    def __init__(self, name: str, path: Path | None, environment_manager: "EnvironmentManager") -> None:
        """Use absolute path as name for micromamba to consider the activation from a folder path, not from a name"""
        super().__init__(name, path, environment_manager)

    def execute(self, module_path: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}) -> Any:
        """Executes a function in the given module

        Args:
                module_path: the path to the module to import
                function: the name of the function to execute
                args: the argument list for the function
                kwargs: the keyword arguments for the function

        Returns:
                The result of the function
        """
        module = self._import_module(module_path)
        if not self._is_mod_function(module, function):
            raise Exception(f"Module {module_path} has no function {function}.")
        return getattr(module, function)(*args)

    def run_script(self, script_path: str | Path, args: tuple = (), run_name: str = "__main__") -> Any:
        """
        Runs a Python script locally using runpy.run_path(), simulating
        'python script.py arg1 arg2 ...'

        Args:
            script_path: Path to the script to execute.
            args: List of arguments to pass (becomes sys.argv[1:] locally).
            run_name: Value for runpy.run_path(run_name=...); defaults to "__main__".

        Returns:
            The resulting globals dict from the executed script, or None on failure.
        """
        script_path = str(script_path)
        sys.argv = [script_path] + list(args)
        runpy.run_path(script_path, run_name=run_name)
        return None
