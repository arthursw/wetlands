import runpy
import sys
import traceback
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, TYPE_CHECKING

from wetlands.environment import Environment
from wetlands.task import Task

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


class InternalEnvironment(Environment):
    def __init__(self, name: str, path: Path | None, environment_manager: "EnvironmentManager") -> None:
        """Use absolute path as name for micromamba to consider the activation from a folder path, not from a name"""
        super().__init__(name, path, environment_manager)
        self._executor: ThreadPoolExecutor | None = None

    @property
    def _pool(self) -> ThreadPoolExecutor:
        """Lazily create and return the thread pool executor."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor()
        return self._executor

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

    def submit(
        self,
        module_path: str | Path,
        function: str,
        args: tuple = (),
        kwargs: dict[str, Any] | None = None,
        *,
        start: bool = True,
    ) -> Task[Any]:
        """Submit a function for non-blocking execution. Returns a Task."""
        kwargs = kwargs or {}
        task: Task[Any] = Task()

        def _dispatch() -> None:
            task._set_running()
            try:
                module = self._import_module(module_path)
                result = getattr(module, function)(*args, **kwargs)
                task._set_completed(result)
            except Exception as e:
                task._set_failed(str(e), traceback.format_tb(e.__traceback__))

        task._set_start_fn(lambda: self._pool.submit(_dispatch))
        if start:
            task.start()
        return task

    def submit_script(
        self,
        script_path: str | Path,
        args: tuple = (),
        run_name: str = "__main__",
        *,
        start: bool = True,
    ) -> Task[None]:
        """Submit a script for non-blocking execution. Returns a Task[None]."""
        task: Task[None] = Task()

        def _dispatch() -> None:
            task._set_running()
            try:
                self.run_script(script_path, args=args, run_name=run_name)
                task._set_completed(None)
            except Exception as e:
                task._set_failed(str(e), traceback.format_tb(e.__traceback__))

        task._set_start_fn(lambda: self._pool.submit(_dispatch))
        if start:
            task.start()
        return task

    def map(
        self,
        module_path: str | Path,
        function: str,
        iterable: Iterable[Any],
        *,
        timeout: float | None = None,
        ordered: bool = True,
    ) -> Iterator[Any]:
        """Execute function once for each item, distributing across workers."""
        tasks = self.map_tasks(module_path, function, iterable)
        for task in tasks:
            task.wait_for(timeout=timeout)
            yield task.result

    def map_tasks(
        self,
        module_path: str | Path,
        function: str,
        iterable: Iterable[Any],
    ) -> list[Task[Any]]:
        """Submit one task per item. Returns Task objects."""
        return [self.submit(module_path, function, args=(item,)) for item in iterable]
