import subprocess
from pathlib import Path
from multiprocessing.connection import Client, Connection
import threading
from typing import Any, TYPE_CHECKING

from cema import logger
from cema.command_generator import Command
from cema.dependency_manager import Dependencies
from cema.environment import Environment
from cema.exceptions import ExecutionException
from cema.command_executor import CommandExecutor

if TYPE_CHECKING:
    from cema.environment_manager import EnvironmentManager

class ExternalEnvironment(Environment):
    
    port: int | None = None
    process: subprocess.Popen | None = None
    connection: Connection | None = None

    def __init__(self, name: str, environmentManager: 'EnvironmentManager') -> None:
        super().__init__(name, environmentManager)

    def executeCommands(self, 
        commands: list[str],
        additionalActivateCommands: Command = {},
        popenKwargs: dict[str, Any] = {}) -> subprocess.Popen:
        """Executes the given commands in this environment.

        Args:
                commands: The commands to execute in the environment.
                additionalActivateCommands: Platform-specific activation commands.
                popenKwargs: Keyword arguments for subprocess.Popen(). See :meth:`CommandExecutor.executeCommands`.

        Returns:
                The launched process.
        """
        commands = (
            self.environmentManager.commandGenerator.getActivateEnvironmentCommands(
                self.name, additionalActivateCommands
            )
            + commands
        )
        return self.environmentManager.commandExecutor.executeCommands(commands, popenKwargs=popenKwargs)
    
    def logOutput(self) -> None:
        """Logs output from the subprocess."""
        if self.process is None or self.process.stdout is None or self.process.stdout.readline is None:
            return
        try:
            for line in iter(
                self.process.stdout.readline, ""
            ):  # Use iter to avoid buffering issues:
                # iter(callable, sentinel) repeatedly calls callable (process.stdout.readline) until it returns the sentinel value ("", an empty string).
                # Since readline() is called directly in each iteration, it immediately processes available output instead of accumulating it in a buffer.
                # This effectively forces line-by-line reading in real-time rather than waiting for the subprocess to fill its buffer.
                logger.info(line.strip())
        except Exception as e:
            logger.error(f"Exception in logging thread: {e}")
        return
    
    def launch(self,
        additionalActivateCommands: Command = {},
        logOutputInThread: bool = True) -> None:
        """Launches a server listening for orders in the environment.

        Args:
                additionalActivateCommands: Platform-specific activation commands.
                logOutputInThread: Logs the process output in a separate thread.
        """

        moduleExecutorPath = Path(__file__).parent.resolve() / "module_executor.py"
        self.process = self.executeCommands(
            [f'python -u "{moduleExecutorPath}" {self.name}'],
            additionalActivateCommands,
        )

        if self.process.stdout is not None:
            try:
                for line in self.process.stdout:
                    logger.info(line.strip())
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
            threading.Thread(target=self.logOutput, args=[]).start()

    def install(self, 
        dependencies: Dependencies,
        additionalInstallCommands: Command = {}) -> None:
        """Installs dependencies.
        See :meth:`EnvironmentManager.create` for more details on the ``dependencies`` and ``additionalInstallCommands`` parameters.

        Args:
                dependencies: Dependencies to install.
                additionalInstallCommands: Platform-specific commands during installation.
        """

        installCommands = self.environmentManager.commandGenerator.getActivateCondaCommands()
        installCommands += self.environmentManager.dependencyManager.getInstallDependenciesCommands(self.name, dependencies)
        installCommands += self.environmentManager.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
        self.environmentManager.commandExecutor.executeCommandAndGetOutput(installCommands)

    def execute(self, modulePath: str | Path, function: str, args: tuple) -> Any:
        """Executes a function in the given module and return the result.
        
        Args:
                modulePath: the path to the module to import
                function: the name of the function to execute
                args: the argument list for the function 
        
        Returns:
                The result of the function if it is defined and the connection is opened ; None otherwise.
        Raises:
            OSError when raised by the communication.
        """
        connection = self.connection
        if connection is None or connection.closed:
            logger.warning(
                f"Connection not ready. Skipping execute {modulePath}.{function}({args})"
            )
            return None
        try:
            connection.send(
                dict(
                    action="execute",
                    modulePath=modulePath,
                    function=function,
                    args=args,
                )
            )
            while message := connection.recv():
                if message["action"] == "execution finished":
                    logger.info("execution finished")
                    return message.get("result")
                elif message["action"] == "error":
                    raise ExecutionException(message)
                else:
                    logger.warning(f"Got an unexpected message: {message}")
        # If the connection was closed (subprocess killed): catch and ignore the exception, otherwise: raise it
        except EOFError:
            print("Connection closed gracefully by the peer.")
        except BrokenPipeError as e:
            logger.error(
                f"Broken pipe. The peer process might have terminated. Exception: {e}."
            )
        except OSError as e:
            if e.errno == 9:  # Bad file descriptor
                logger.error("Connection closed abruptly by the peer.")
            else:
                logger.error(f"Unexpected OSError: {e}")
                raise e
        return None

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

    def _exit(self) -> None:
        """Close the connection to the environment and kills the process."""
        if self.connection is not None:
            try:
                self.connection.send(dict(action="exit"))
            except OSError as e:
                if e.args[0] == "handle is closed":
                    pass
            self.connection.close()

        CommandExecutor.killProcess(self.process)

