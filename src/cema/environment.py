import sys
from pathlib import Path
from subprocess import Popen
from importlib import import_module
from abc import abstractmethod
from multiprocessing.connection import Client, Connection
from typing import Any
from types import ModuleType

from cema import logger
from cema.exceptions import ExecutionException
from cema.command_executor import CommandExecutor

class Environment:
    def __init__(self, name: str) -> None:
        self.name = name
        self.installedDependencies: dict[str, list[str]] = {}

    @abstractmethod
    def execute(self, modulePath: str | Path, function: str, args: list) -> Any:
        pass

    @abstractmethod
    def _exit(self) -> None:
        pass

    def launched(self) -> bool:
        return True


class ClientEnvironment(Environment):
    def __init__(self, name: str, port: int, process: Popen) -> None:
        super().__init__(name)
        self.port = port
        self.process = process
        self.connection: Connection | None = None

    def initialize(self) -> None:
        self.connection = Client(("localhost", self.port))

    def execute(self, modulePath: str | Path, function: str, args: list) -> Any:
        connection = self.connection
        if connection is None or connection.closed:
            logger.warning(
                f"Connection not ready. Skipping execute {modulePath}.{function}({args})"
            )
            return None
        try:
            connection.send(
                dict(action="execute", modulePath=modulePath, function=function, args=args)
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
            print(f"Broken pipe. The peer process might have terminated. Exception: {e}.")

        # except (PicklingError, TypeError) as e:
        # 	print(f"Failed to serialize the message: {e}")
        except OSError as e:
            if e.errno == 9:  # Bad file descriptor
                print("Connection closed abruptly by the peer.")
            else:
                print(f"Unexpected OSError: {e}")
                raise e
        return None

    def launched(self) -> bool:
        return (
            self.process is not None
            and self.process.poll() is None
            and self.connection is not None
            and not self.connection.closed
            and self.connection.writable
            and self.connection.readable
        )

    def _exit(self) -> None:
        if self.connection is not None:
            try:
                self.connection.send(dict(action="exit"))
            except OSError as e:
                if e.args[0] == "handle is closed":
                    pass
            self.connection.close()

        CommandExecutor.killProcess(self.process)

class DirectEnvironment(Environment):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.modules: dict[str, ModuleType] = {}

    def execute(self, modulePath: str | Path, function: str, args: list) -> Any:
        modulePath = Path(modulePath)
        module = modulePath.stem
        if module not in self.modules:
            sys.path.append(str(modulePath.parent))
            self.modules[module] = import_module(module)
        if not hasattr(self.modules[module], function):
            raise Exception(f"Module {module} has no function {function}.")
        return getattr(self.modules[module], function)(*args)

    def _exit(self) -> None:
        pass