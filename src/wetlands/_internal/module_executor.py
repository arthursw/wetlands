"""
This script launches a server inside a specified conda environment. It listens on a dynamically assigned
local port for incoming execution commands sent via a multiprocessing connection.

Clients can send instructions to:
- Dynamically import a Python module from a specified path and execute a function
- Run a Python script via runpy.run_path()
- Receive the result or any errors from the execution

Designed to be run within isolated environments for sandboxed execution of Python code modules.
"""

import sys
import logging
import threading
import traceback
import argparse
import runpy
from pathlib import Path
from importlib import import_module
from multiprocessing.connection import Listener, Connection

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(process)d:%(name)s:%(message)s",
    handlers=[
        logging.FileHandler("environments.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

port = 0
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Wetlands module executor",
        "Module executor is executed in a conda environment. It listens to a port and waits for execution orders. "
        "When instructed, it can import a module and execute one of its functions or run a script with runpy.",
    )
    parser.add_argument("environment", help="The name of the execution environment.")
    parser.add_argument("-p", "--port", help="The port to listen to.", default=0, type=int)
    parser.add_argument(
        "-dp", "--debugPort", help="The debugpy port to listen to. Only provide in debug mode.", default=None, type=int
    )
    parser.add_argument(
        "-wip",
        "--wetlandsInstancePath",
        help="Path to the folder containing the state of the wetlands instance to debug. Only provide in debug mode.",
        default=None,
        type=Path,
    )
    args = parser.parse_args()
    port = args.port
    logger = logging.getLogger(args.environment)
    if args.debugPort is not None:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Starting {args.environment} with python {sys.version}")
        import debugpy

        _, debugPort = debugpy.listen(args.debugPort)
        print(f"Listening debug port {debugPort}")
else:
    logger = logging.getLogger("module_executor")


def sendMessage(lock: threading.Lock, connection: Connection, message: dict):
    """Thread-safe sending of messages."""
    with lock:
        connection.send(message)


def handleExecutionError(lock: threading.Lock, connection: Connection, e: Exception):
    """Common error handling for any execution type."""
    logger.error(str(e))
    logger.error("Traceback:")
    tbftb = traceback.format_tb(e.__traceback__)
    for line in tbftb:
        logger.error(line)
    sys.stderr.flush()
    sendMessage(
        lock,
        connection,
        dict(
            action="error",
            exception=str(e),
            traceback=tbftb,
        ),
    )
    logger.debug("Error sent")


def executeFunction(message: dict):
    """Import a module and execute one of its functions."""
    modulePath = Path(message["modulePath"])
    logger.debug(f"Import module {modulePath}")
    sys.path.append(str(modulePath.parent))
    module = import_module(modulePath.stem)
    if not hasattr(module, message["function"]):
        raise Exception(f"Module {modulePath} has no function {message['function']}.")
    args = message.get("args", [])
    kwargs = message.get("kwargs", {})
    logger.info(f"Execute {message['modulePath']}:{message['function']}({args})")
    try:
        result = getattr(module, message["function"])(*args, **kwargs)
    except SystemExit as se:
        raise Exception(f"Function raised SystemExit: {se}\n\n")
    logger.info("Executed")
    return result


def runScript(message: dict):
    """Run a Python script via runpy.run_path(), simulating 'python script.py args...'."""
    scriptPath = message["scriptPath"]
    args = message.get("args", [])
    run_name = message.get("run_name", "__main__")

    sys.argv = [scriptPath] + list(args)
    logger.info(f"Running script {scriptPath} with args {args} and run_name={run_name}")
    runpy.run_path(scriptPath, run_name=run_name)
    logger.info("Script executed")
    return None


def executionWorker(lock: threading.Lock, connection: Connection, message: dict):
    """
    Worker function handling both 'execute' and 'run' actions.
    """
    try:
        action = message["action"]
        if action == "execute":
            result = executeFunction(message)
        elif action == "run":
            result = runScript(message)
        else:
            raise Exception(f"Unknown action: {action}")

        sendMessage(
            lock,
            connection,
            dict(
                action="execution finished",
                message=f"{action} completed",
                result=result,
            ),
        )
    except Exception as e:
        handleExecutionError(lock, connection, e)


def getMessage(connection: Connection) -> dict:
    logger.debug("Waiting for message...")
    return connection.recv()


def launchListener():
    """
    Launches a listener on a random available port on localhost.
    Waits for client connections and handles 'execute', 'run', or 'exit' messages.
    """
    lock = threading.Lock()
    with Listener(("localhost", port)) as listener:
        while True:
            print(f"Listening port {listener.address[1]}")
            with listener.accept() as connection:
                logger.debug(f"Connection accepted {listener.address}")
                message = ""
                try:
                    while message := getMessage(connection):
                        logger.debug(f"Got message: {message}")

                        if message["action"] in ("execute", "run"):
                            logger.debug(f"Launch thread for action {message['action']}")
                            thread = threading.Thread(
                                target=executionWorker,
                                args=(lock, connection, message),
                            )
                            thread.start()

                        elif message["action"] == "exit":
                            logger.info("exit")
                            sendMessage(lock, connection, dict(action="exited"))
                            listener.close()
                            return
                except Exception as e:
                    handleExecutionError(lock, connection, e)


if __name__ == "__main__":
    launchListener()

logger.debug("Exit")
