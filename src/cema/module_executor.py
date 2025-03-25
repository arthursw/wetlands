import sys
import logging
import threading
import traceback
import argparse
from pathlib import Path
from importlib import import_module
from multiprocessing.connection import Listener

parser = argparse.ArgumentParser(
    "Cema module executor",
    "Module executor is executed in a conda environment. It listens to a port and wait for execution orders. When told, it can import a module and execute one of its function.",
)
parser.add_argument("environment", help="The name of the execution environment.")
args = parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("environments.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(args.environment)


def getMessage(connection):
    logger.debug(f"Waiting for message...")
    return connection.recv()


def functionExecutor(lock, connection, message):
    try:
        modulePath = Path(message["modulePath"])
        sys.path.append(str(modulePath.parent))
        module = import_module(modulePath.stem)
        if not hasattr(module, message["function"]):
            raise Exception(
                f"Module {modulePath} has no function {message['function']}."
            )
        result = getattr(module, message["function"])(*message["args"])
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
    lock = threading.Lock()
    with Listener(("localhost", 0)) as listener:
        while True:
            # Print ready message for the environment manager (it can now open a client to send messages)
            print(f"Listening port {listener.address[1]}")
            with listener.accept() as connection:
                logger.debug(f"Connection accepted {listener.address}")
                try:
                    while message := getMessage(connection):
                        logger.debug(f"Got message: {message}")
                        if message["action"] == "execute":
                            logger.info(
                                f"Execute {message['modulePath']}.{message['function']}({message['args']})"
                            )

                            thread = threading.Thread(
                                target=functionExecutor,
                                args=(lock, connection, message),
                            )
                            thread.start()

                        if message["action"] == "exit":
                            logger.info(f"exit")
                            with lock:
                                connection.send(dict(action="exited"))
                            connection.close()
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
