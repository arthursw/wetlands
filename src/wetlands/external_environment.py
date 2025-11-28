import subprocess
from pathlib import Path
from multiprocessing.connection import Client, Connection
import functools
import threading
from typing import Any, TYPE_CHECKING, Union
from send2trash import send2trash

from wetlands.logger import logger, LOG_SOURCE_EXECUTION
from wetlands._internal.command_generator import Commands
from wetlands._internal.dependency_manager import Dependencies
from wetlands.environment import Environment
from wetlands._internal.exceptions import ExecutionException
from wetlands._internal.command_executor import CommandExecutor
from wetlands._internal.process_logger import ProcessLogger

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager

MODULE_EXECUTOR_FILE = "module_executor.py"


def synchronized(method):
    """Decorator to wrap a method call with self._lock."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class ExternalEnvironment(Environment):
    port: int | None = None
    process: subprocess.Popen | None = None
    connection: Connection | None = None

    def __init__(self, name: str, path: Path, environmentManager: "EnvironmentManager") -> None:
        super().__init__(name, path, environmentManager)
        self._lock = threading.RLock()
        self._process_logger: ProcessLogger | None = None

    @synchronized
    def launch(self, additionalActivateCommands: Commands = {}, logOutputInThread: bool = True) -> None:
        """Launches a server listening for orders in the environment.

        Args:
                additionalActivateCommands: Platform-specific activation commands.
                logOutputInThread: Deprecated parameter, kept for backwards compatibility. ProcessLogger always runs in background.
        """

        if self.launched():
            return

        moduleExecutorPath = Path(__file__).parent.resolve() / "_internal" / MODULE_EXECUTOR_FILE

        debugArgs = f" --debugPort 0" if self.environmentManager.debug else ""
        commands = [
            f'python -u "{moduleExecutorPath}" {self.name} --wetlandsInstancePath {self.environmentManager.wetlandsInstancePath.resolve()}{debugArgs}'
        ]

        # Create log context for the module executor process
        log_context = {"log_source": LOG_SOURCE_EXECUTION, "env_name": self.name, "call_target": MODULE_EXECUTOR_FILE}

        # Pass log_context to executeCommands so ProcessLogger is created with proper context
        self.process = self.executeCommands(commands, additionalActivateCommands, log_context=log_context)

        # Retrieve the ProcessLogger that was already created and started by executeCommands
        self._process_logger = self.environmentManager.commandExecutor._process_loggers.get(self.process.pid)
        if self._process_logger is None:
            raise Exception("Failed to retrieve ProcessLogger for module executor process")

        # Wait for port announcement with timeout
        def port_predicate(line: str) -> bool:
            return line.startswith("Listening port ")

        port_line = self._process_logger.wait_for_line(port_predicate, timeout=30)
        if port_line:
            self.port = int(port_line.replace("Listening port ", ""))

        # Handle debug port if needed
        if self.environmentManager.debug:

            def debug_predicate(line: str) -> bool:
                return line.startswith("Listening debug port ")

            debug_line = self._process_logger.wait_for_line(debug_predicate, timeout=5)
            if debug_line:
                debugPort = int(debug_line.replace("Listening debug port ", ""))
                self.environmentManager.registerEnvironment(self, debugPort, moduleExecutorPath)

        if self.process.poll() is not None:
            raise Exception(f"Process exited with return code {self.process.returncode}.")
        if self.port is None:
            raise Exception("Could not find the server port.")

        self.connection = Client(("localhost", self.port))

    def _sendAndWait(self, payload: dict) -> Any:
        """Send a payload to the remote environment and wait for its response."""
        connection = self.connection
        if connection is None or connection.closed:
            raise ExecutionException("Connection not ready.")

        try:
            connection.send(payload)
            while message := connection.recv():
                action = message.get("action")
                if action == "execution finished":
                    logger.info(f"{payload.get('action')} finished")
                    return message.get("result")
                elif action == "error":
                    logger.error(message["exception"])
                    logger.error("Traceback:")
                    for line in message["traceback"]:
                        logger.error(line)
                    raise ExecutionException(message)
                else:
                    logger.warning(f"Got an unexpected message: {message}")

        except EOFError:
            logger.info("Connection closed gracefully by the peer.")
        except BrokenPipeError as e:
            logger.error(f"Broken pipe. The peer process might have terminated. Exception: {e}.")
        except OSError as e:
            if e.errno == 9:  # Bad file descriptor
                logger.error("Connection closed abruptly by the peer.")
            else:
                logger.error(f"Unexpected OSError: {e}")
                raise e
        return None

    @synchronized
    def execute(self, modulePath: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}) -> Any:
        """Executes a function in the given module and return the result.
        Warning: all arguments (args and kwargs) must be picklable (since they will be send with [multiprocessing.connection.Connection.send](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection.send))!

        Args:
                modulePath: the path to the module to import
                function: the name of the function to execute
                args: the argument list for the function
                kwargs: the keyword arguments for the function

        Returns:
                The result of the function if it is defined and the connection is opened ; None otherwise.
        Raises:
            OSError when raised by the communication.
        """
        # Update log context to reflect the function being executed
        module_name = Path(modulePath).stem
        call_target = f"{module_name}:{function}"
        if self._process_logger:
            self._process_logger.update_log_context({"call_target": call_target})

        try:
            payload = dict(
                action="execute",
                modulePath=str(modulePath),
                function=function,
                args=args,
                kwargs=kwargs,
            )
            return self._sendAndWait(payload)
        finally:
            # Reset to module_executor after execution
            if self._process_logger:
                self._process_logger.update_log_context({"call_target": MODULE_EXECUTOR_FILE})

    @synchronized
    def runScript(self, scriptPath: str | Path, args: tuple = (), run_name: str = "__main__") -> Any:
        """
        Runs a Python script remotely using runpy.run_path(), simulating
        'python script.py arg1 arg2 ...'

        Args:
            scriptPath: Path to the script to execute.
            args: List of arguments to pass (becomes sys.argv[1:] remotely).
            run_name: Value for runpy.run_path(run_name=...); defaults to "__main__".

        Returns:
            The resulting globals dict from the executed script, or None on failure.
        """
        # Update log context to reflect the script being executed
        script_name = Path(scriptPath).name
        if self._process_logger:
            self._process_logger.update_log_context({"call_target": script_name})

        try:
            payload = dict(
                action="run",
                scriptPath=str(scriptPath),
                args=args,
                run_name=run_name,
            )
            return self._sendAndWait(payload)
        finally:
            # Reset to module_executor after execution
            if self._process_logger:
                self._process_logger.update_log_context({"call_target": MODULE_EXECUTOR_FILE})

    @synchronized
    def launched(self) -> bool:
        """Return true if the environment server process is launched and the connection is open."""
        return (
            self.process is not None
            and self.process.poll() is None
            and self.connection is not None
            and not self.connection.closed
            and self.connection.writable
            and self.connection.readable
        )

    @synchronized
    def _exit(self) -> None:
        """Close the connection to the environment and kills the process."""
        if self.connection is not None:
            try:
                self.connection.send(dict(action="exit"))
            except OSError as e:
                if e.args[0] == "handle is closed":
                    pass
            self.connection.close()

        if self.process and self.process.stdout:
            self.process.stdout.close()

        # ProcessLogger runs in a daemon thread, so it will be cleaned up automatically
        self._process_logger = None

        CommandExecutor.killProcess(self.process)

    @synchronized
    def delete(self) -> None:
        """Deletes this external environment and cleans up associated resources.

        Raises:
                Exception: If the environment does not exist.

        Side Effects:
                - If the environment is running, calls _exit() on it
                - Removes environment from environmentManager.environments dict
                - Deletes the environment directory using appropriate conda manager
        """
        if self.path is None:
            raise Exception("Cannot delete an environment with no path.")

        if not self.environmentManager.environmentExists(self.path):
            raise Exception(f"The environment {self.name} does not exist.")

        # Exit the environment if it's running
        if self.launched():
            self._exit()

        # Generate delete commands based on conda manager type
        if self.environmentManager.settingsManager.usePixi:
            send2trash(self.path.parent)
        else:
            send2trash(self.path)

        # Remove from environments dict
        if self.name in self.environmentManager.environments:
            del self.environmentManager.environments[self.name]

    @synchronized
    def update(
        self,
        dependencies: Union[Dependencies, None] = None,
        additionalInstallCommands: Commands = {},
        useExisting: bool = False,
    ) -> "Environment":
        """Updates this external environment by deleting it and recreating it with new dependencies.

        Args:
                dependencies: New dependencies to install. Can be one of:
                    - A Dependencies dict: dict(python="3.12.7", conda=["numpy"], pip=["requests"])
                    - None (no dependencies to install)
                additionalInstallCommands: Platform-specific commands during installation.
                useExisting: use existing environment if it exists instead of recreating it.

        Returns:
                The recreated environment.

        Raises:
                Exception: If the environment does not exist.

        Side Effects:
                - Deletes the existing environment
                - Creates a new environment with the same name but new dependencies
        """
        if not self.path:
            raise Exception("Cannot update an environment with no path.")

        if not self.environmentManager.environmentExists(self.path):
            raise Exception(f"The environment {self.name} does not exist.")

        # Delete the existing environment
        self.delete()

        # Use create for direct Dependencies dict
        return self.environmentManager.create(
            str(self.name),
            dependencies=dependencies,
            additionalInstallCommands=additionalInstallCommands,
            useExisting=useExisting,
        )
