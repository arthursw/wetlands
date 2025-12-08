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
import importlib
import importlib.util
from multiprocessing.connection import Listener, Connection


def import_from_path(name: str, file_path: str | Path):
    file_path = Path(file_path)
    spec = importlib.util.spec_from_file_location(name, file_path)
    if spec is None:
        return None
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        return None
    spec.loader.exec_module(module)
    return module


try:
    ndarray_mod = import_from_path("wetlands_ndarray", Path(__file__).parent / "ndarray.py")
    if ndarray_mod is not None:
        ndarray_mod.register_ndarray_pickle()
except ImportError:
    # Do not support ndarray if numpy is not installed
    pass

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
        "-dp", "--debug_port", help="The debugpy port to listen to. Only provide in debug mode.", default=None, type=int
    )
    parser.add_argument(
        "-wip",
        "--wetlands_instance_path",
        help="Path to the folder containing the state of the wetlands instance to debug. Only provide in debug mode.",
        default=None,
        type=Path,
    )
    args = parser.parse_args()
    port = args.port
    logger = logging.getLogger(args.environment)
    if args.debug_port is not None:
        logger.setLevel(logging.DEBUG)
        try:
            import debugpy  # type: ignore[unused-import]

            logger.debug(f"Starting {args.environment} with python {sys.version}")
            _, debug_port = debugpy.listen(args.debug_port)
            print(f"Listening debug port {debug_port}")
        except ImportError as ie:
            logger.error("debugpy is not installed in this environment. Debugging is not available.")
            logger.error(str(ie))
else:
    logger = logging.getLogger("module_executor")


def send_message(lock: threading.Lock, connection: Connection, message: dict):
    """Thread-safe sending of messages."""
    with lock:
        connection.send(message)


def handle_execution_error(lock: threading.Lock, connection: Connection, e: Exception):
    """Common error handling for any execution type."""
    logger.error(str(e))
    logger.error("Traceback:")
    tbftb = traceback.format_tb(e.__traceback__)
    for line in tbftb:
        logger.error(line)
    sys.stderr.flush()
    send_message(
        lock,
        connection,
        dict(
            action="error",
            exception=str(e),
            traceback=tbftb,
        ),
    )
    logger.debug("Error sent")


def execute_function(message: dict):
    """Import a module and execute one of its functions."""
    module_path = Path(message["module_path"])
    logger.debug(f"Import module {module_path}")
    sys.path.append(str(module_path.parent))
    module = importlib.import_module(module_path.stem)
    if not hasattr(module, message["function"]):
        raise Exception(f"Module {module_path} has no function {message['function']}.")
    args = message.get("args", [])
    kwargs = message.get("kwargs", {})
    logger.info(f"Execute {message['module_path']}:{message['function']}({args})")
    try:
        result = getattr(module, message["function"])(*args, **kwargs)
    except SystemExit as se:
        raise Exception(f"Function raised SystemExit: {se}\n\n")
    logger.info("Executed")
    return result


def run_script(message: dict):
    """Run a Python script via runpy.run_path(), simulating 'python script.py args...'."""
    script_path = message["script_path"]
    args = message.get("args", [])
    run_name = message.get("run_name", "__main__")

    sys.argv = [script_path] + list(args)
    logger.info(f"Running script {script_path} with args {args} and run_name={run_name}")
    runpy.run_path(script_path, run_name=run_name)
    logger.info("Script executed")
    return None


def execution_worker(lock: threading.Lock, connection: Connection, message: dict):
    """
    Worker function handling both 'execute' and 'run' actions.
    """
    try:
        action = message["action"]
        if action == "execute":
            result = execute_function(message)
        elif action == "run":
            result = run_script(message)
        else:
            raise Exception(f"Unknown action: {action}")

        send_message(
            lock,
            connection,
            dict(
                action="execution finished",
                message=f"{action} completed",
                result=result,
            ),
        )
    except Exception as e:
        handle_execution_error(lock, connection, e)


def get_message(connection: Connection) -> dict:
    logger.debug("Waiting for message...")
    return connection.recv()


def launch_listener():
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
                    while message := get_message(connection):
                        logger.debug(f"Got message: {message}")

                        if message["action"] in ("execute", "run"):
                            logger.debug(f"Launch thread for action {message['action']}")
                            thread = threading.Thread(
                                target=execution_worker,
                                args=(lock, connection, message),
                            )
                            thread.start()

                        elif message["action"] == "exit":
                            logger.info("exit")
                            send_message(lock, connection, dict(action="exited"))
                            listener.close()
                            return
                except Exception as e:
                    handle_execution_error(lock, connection, e)


if __name__ == "__main__":
    launch_listener()

logger.debug("Exit")
