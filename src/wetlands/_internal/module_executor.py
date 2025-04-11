"""
This script launches a server inside a specified conda environment. It listens on a dynamically assigned
local port for incoming execution commands sent via a multiprocessing connection.

Clients can send instructions to:
- Dynamically import a Python module from a specified path
- Execute a given function from that module with optional arguments
- Receive the result or any errors from the execution

Designed to be run within isolated environments for sandboxed execution of Python code modules.
"""

import sys
import logging
import threading
import traceback
import argparse
from pathlib import Path
from importlib import import_module
from multiprocessing.connection import Listener, Connection

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("environments.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Wetlands module executor",
        "Module executor is executed in a conda environment. It listens to a port and waits for execution orders. "
        "When instructed, it can import a module and execute one of its functions.",
    )
    parser.add_argument("environment", help="The name of the execution environment.")
    args = parser.parse_args()

    logger = logging.getLogger(args.environment)
else:
    logger = logging.getLogger("module_executor")


def getMessage(connection: Connection) -> dict:
    """
    Waits for and receives a message from the given connection.

    Args:
        connection: A multiprocessing connection object.

    Returns:
        The message received from the connection.
    """
    logger.debug(f"Waiting for message...")
    return connection.recv()


def functionExecutor(lock: threading.Lock, connection: Connection, message: dict):
    """
    Executes a specified function from a dynamically imported module in a thread-safe way.

    Args:
        lock (threading.Lock): Lock to synchronize access to the connection.
        connection: Connection object to send results or errors.
        message (dict): Dictionary containing module path, function name, and optional args/kwargs.

    Expected message format:
        {
            "modulePath": "path/to/module.py",
            "function": "function_name",
            "args": [...],
            "kwargs": {...}
        }

    Sends:
        - On success: {'action': 'execution finished', 'message': 'process execution done', 'result': result}
        - On failure: {'action': 'error', 'exception': str(e), 'traceback': [...]}
    """
    try:
        modulePath = Path(message["modulePath"])
        sys.path.append(str(modulePath.parent))
        module = import_module(modulePath.stem)
        if not hasattr(module, message["function"]):
            raise Exception(f"Module {modulePath} has no function {message['function']}.")
        args = message.get("args", [])
        kwargs = message.get("kwargs", {})
        result = getattr(module, message["function"])(*args, **kwargs)
        logger.info(f"Executed")
        with lock:
            connection.send(
                dict(
                    action="execution finished",
                    message="process execution done",
                    result=result,
                )
            )
    except Exception as e:
        with lock:
            connection.send(
                dict(
                    action="error",
                    exception=str(e),
                    traceback=traceback.format_tb(e.__traceback__),
                )
            )


def launchListener():
    """
    Launches a listener on a random available port on localhost.
    Waits for client connections and handles incoming execution or exit messages.

    Workflow:
        - Prints the listening port once ready.
        - Accepts a single connection at a time.
        - Handles incoming messages in a loop.
        - For 'execute' messages, spawns a new thread to run the function.
        - For 'exit' messages, acknowledges and stops the listener.
        - On errors, sends error details back to the client.

    Note:
        The listener automatically closes after receiving an 'exit' command.
    """
    lock = threading.Lock()
    with Listener(("localhost", 0)) as listener:
        while True:
            # Print ready message for the environment manager (it can now open a client to send messages)
            print(f"Listening port {listener.address[1]}")
            with listener.accept() as connection:
                logger.debug(f"Connection accepted {listener.address}")
                message = ""
                try:
                    while message := getMessage(connection):
                        logger.debug(f"Got message: {message}")
                        if message["action"] == "execute":
                            logger.info(f"Execute {message['modulePath']}.{message['function']}({message['args']})")

                            thread = threading.Thread(
                                target=functionExecutor,
                                args=(lock, connection, message),
                            )
                            thread.start()

                        if message["action"] == "exit":
                            logger.info(f"exit")
                            with lock:
                                connection.send(dict(action="exited"))
                            listener.close()
                            return
                except Exception as e:
                    logger.error("Caught exception:")
                    logger.error(e)
                    logger.error(e.args)
                    for line in traceback.format_tb(e.__traceback__):
                        logger.error(line)
                    logger.error(message)
                    with lock:
                        connection.send(
                            dict(
                                action="error",
                                exception=str(e),
                                traceback=traceback.format_tb(e.__traceback__),
                            )
                        )


if __name__ == "__main__":
    launchListener()

logger.debug("Exit")
