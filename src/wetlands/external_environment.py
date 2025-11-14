import queue
import subprocess
from pathlib import Path
from multiprocessing.connection import Client, Connection
import functools
import threading
from typing import Any, TYPE_CHECKING, Union
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
        self._logThread: threading.Thread | None = None
        self.loggingQueue = queue.Queue()

    def logOutput(self) -> None:
        """Logs output from the subprocess."""
        if self.process is None or self.process.stdout is None or self.process.stdout.readline is None:
            return
        try:
            for line in iter(self.process.stdout.readline, ""):  # Use iter to avoid buffering issues:
                # iter(callable, sentinel) repeatedly calls callable (process.stdout.readline) until it returns the sentinel value ("", an empty string).
                # Since readline() is called directly in each iteration, it immediately processes available output instead of accumulating it in a buffer.
                # This effectively forces line-by-line reading in real-time rather than waiting for the subprocess to fill its buffer.
                logger.info(line.strip())
                self.loggingQueue.put(line.strip())
        except Exception as e:
            logger.error(f"Exception in logging thread: {e}")
            self.loggingQueue.put(f"Exception in logging thread: {e}")
        finally:
            self.loggingQueue.put(None)
        return

    @synchronized
    def launch(self, additionalActivateCommands: Commands = {}, logOutputInThread: bool = True) -> None:
        """Launches a server listening for orders in the environment.

        Args:
                additionalActivateCommands: Platform-specific activation commands.
                logOutputInThread: Logs the process output in a separate thread.
        """

        if self.launched():
            return

        moduleExecutorFile = "module_executor.py"
        moduleExecutorPath = Path(__file__).parent.resolve() / "_internal" / moduleExecutorFile

        debugArgs = f" --debugPort 0" if self.environmentManager.debug else ""
        commands = [
            f'python -u "{moduleExecutorPath}" {self.name} --wetlandsInstancePath {self.environmentManager.wetlandsInstancePath.resolve()}{debugArgs}'
        ]

        self.process = self.executeCommands(commands, additionalActivateCommands)

        if self.process.stdout is not None:
            try:
                for line in self.process.stdout:
                    logger.info(line.strip())
                    if self.environmentManager.debug:
                        if line.strip().startswith("Listening debug port "):
                            debugPort = int(line.strip().replace("Listening debug port ", ""))
                            self.environmentManager.registerEnvironment(self, debugPort, moduleExecutorPath)
                    if line.strip().startswith("Listening port "):
                        self.port = int(line.strip().replace("Listening port ", ""))
                        break
            except Exception as e:
                self.process.stdout.close()
                raise e

        if self.process.poll() is not None:
            if self.process.stdout is not None:
                self.process.stdout.close()
            raise Exception(f"Process exited with return code {self.process.returncode}.")
        if self.port is None:
            raise Exception(f"Could not find the server port.")
        self.connection = Client(("localhost", self.port))

        if logOutputInThread:
            self._logThread = threading.Thread(target=self.logOutput, daemon=True)
            self._logThread.start()

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
        payload = dict(
            action="execute",
            modulePath=str(modulePath),
            function=function,
            args=args,
            kwargs=kwargs,
        )
        return self._sendAndWait(payload)

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
        payload = dict(
            action="run",
            scriptPath=str(scriptPath),
            args=args,
            run_name=run_name,
        )
        return self._sendAndWait(payload)

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

        if self._logThread:
            self._logThread.join(timeout=2)

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
