import subprocess
from pathlib import Path
from multiprocessing.connection import Client, Connection
import functools
import threading
from typing import Any, TYPE_CHECKING, Union, Callable
from send2trash import send2trash

from wetlands.logger import logger
from wetlands._internal.command_generator import Commands
from wetlands._internal.dependency_manager import Dependencies
from wetlands.environment import Environment
from wetlands._internal.exceptions import ExecutionException
from wetlands._internal.command_executor import CommandExecutor

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


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
        self._global_log_callback: Callable[[str], None] | None = None
        self._execution_log_callback: Callable[[str], None] | None = None

    def _createLogCallback(self) -> Callable[[str], None]:
        """Creates a combined log callback that calls both global and per-execution callbacks.

        Thread-safe: _execution_log_callback is only modified in synchronized execute()/runScript() methods,
        and reads from the callback thread are atomic in Python for simple object references.

        Returns:
                A callback function that calls both global and execution callbacks if they are set.
        """
        def combined_callback(line: str) -> None:
            if self._global_log_callback is not None:
                try:
                    self._global_log_callback(line)
                except Exception as e:
                    logger.error(f"Exception in global log callback: {e}")

            # Safe to read _execution_log_callback without lock: only modified in synchronized methods
            if self._execution_log_callback is not None:
                try:
                    self._execution_log_callback(line)
                except Exception as e:
                    logger.error(f"Exception in execution log callback: {e}")

        return combined_callback

    @synchronized
    def launch(self, additionalActivateCommands: Commands = {}, log_callback: Callable[[str], None] | None = None) -> None:
        """Launches a server listening for orders in the environment.

        Args:
                additionalActivateCommands: Platform-specific activation commands.
                log_callback: Optional callback to receive log messages during launch and subsequent background logging.
        """

        if self.launched():
            return

        moduleExecutorFile = "module_executor.py"
        moduleExecutorPath = Path(__file__).parent.resolve() / "_internal" / moduleExecutorFile

        debugArgs = f" --debugPort 0" if self.environmentManager.debug else ""
        commands = [
            f'python -u "{moduleExecutorPath}" {self.name} --wetlandsInstancePath {self.environmentManager.wetlandsInstancePath.resolve()}{debugArgs}'
        ]

        # Set up global callback for this launch and subsequent background logging
        self._global_log_callback = log_callback

        # Event to signal when port is found
        port_found_event = threading.Event()

        # Create a callback that parses output for port information using the combined callback
        combined = self._createLogCallback()

        def launch_callback(line: str) -> None:
            """Callback to parse ports and call the combined callback."""
            # Call the combined callback (which includes global and per-execution callbacks)
            combined(line)

            # Parse port information
            if self.environmentManager.debug:
                if line.startswith("Listening debug port "):
                    debugPort = int(line.replace("Listening debug port ", ""))
                    self.environmentManager.registerEnvironment(self, debugPort, moduleExecutorPath)

            if line.startswith("Listening port "):
                self.port = int(line.replace("Listening port ", ""))
                port_found_event.set()

        # Launch process with callback
        self.process = self.executeCommands(commands, additionalActivateCommands, log_callback=launch_callback)

        # Wait for port to be found (with timeout)
        if not port_found_event.wait(timeout=30):
            raise Exception(f"Timeout waiting for server port.")

        # Check if process is still running
        if self.process.poll() is not None:
            raise Exception(f"Process exited with return code {self.process.returncode}.")
        if self.port is None:
            raise Exception(f"Could not find the server port.")

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
    def execute(self, modulePath: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}, log_callback: Callable[[str], None] | None = None) -> Any:
        """Executes a function in the given module and return the result.
        Warning: all arguments (args and kwargs) must be picklable (since they will be send with [multiprocessing.connection.Connection.send](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection.send))!

        Args:
                modulePath: the path to the module to import
                function: the name of the function to execute
                args: the argument list for the function
                kwargs: the keyword arguments for the function
                log_callback: Optional callback to receive log messages during this execution

        Returns:
                The result of the function if it is defined and the connection is opened ; None otherwise.
        Raises:
            OSError when raised by the communication.
        """
        # Set per-execution callback for duration of this execution
        self._execution_log_callback = log_callback

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
            # Clear per-execution callback
            self._execution_log_callback = None

    @synchronized
    def runScript(self, scriptPath: str | Path, args: tuple = (), run_name: str = "__main__", log_callback: Callable[[str], None] | None = None) -> Any:
        """
        Runs a Python script remotely using runpy.run_path(), simulating
        'python script.py arg1 arg2 ...'

        Args:
            scriptPath: Path to the script to execute.
            args: List of arguments to pass (becomes sys.argv[1:] remotely).
            run_name: Value for runpy.run_path(run_name=...); defaults to "__main__".
            log_callback: Optional callback to receive log messages during this execution

        Returns:
            The resulting globals dict from the executed script, or None on failure.
        """
        # Set per-execution callback for duration of this execution
        self._execution_log_callback = log_callback

        try:
            payload = dict(
                action="run",
                scriptPath=str(scriptPath),
                args=args,
                run_name=run_name,
            )
            return self._sendAndWait(payload)
        finally:
            # Clear per-execution callback
            self._execution_log_callback = None

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
