"""
This script launches a server inside a specified conda environment. It listens on a dynamically assigned
local port for incoming execution commands sent via a multiprocessing connection.

Clients can send instructions to:
- Dynamically import a Python module from a specified path and execute a function
- Run a Python script via runpy.run_path()
- Receive the result or any errors from the execution

Designed to be run within isolated environments for sandboxed execution of Python code modules.
"""

from __future__ import annotations

import ast
import sys
import logging
import threading
import traceback
import argparse
import runpy
import inspect
import os
from pathlib import Path
import importlib
import importlib.util
from multiprocessing.context import AuthenticationError
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


def _annotation_contains_pep604_union(annotation: ast.AST | None) -> bool:
    if annotation is None:
        return False
    return any(isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr) for node in ast.walk(annotation))


def _node_contains_line(node: ast.AST, line_number: int) -> bool:
    node_line = getattr(node, "lineno", None)
    if node_line is None:
        return False
    return node_line <= line_number <= getattr(node, "end_lineno", node_line)


def _module_uses_future_annotations(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )


def _type_error_is_from_pep604_annotation(e: TypeError, path: str | Path) -> bool:
    if sys.version_info >= (3, 10) or "unsupported operand type(s) for |" not in str(e):
        return False

    module_path = Path(path)
    try:
        source = module_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(module_path))
        target_path = module_path.resolve()
    except (OSError, SyntaxError, UnicodeDecodeError):
        return False
    if _module_uses_future_annotations(tree):
        return False

    failing_lines = set()
    for frame in traceback.extract_tb(e.__traceback__):
        try:
            frame_path = Path(frame.filename).resolve()
        except OSError:
            continue
        if frame_path == target_path:
            failing_lines.add(frame.lineno)

    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and any(_node_contains_line(node, line) for line in failing_lines):
            return _annotation_contains_pep604_union(node.annotation)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            _node_contains_line(node, line) for line in failing_lines
        ):
            annotations = [node.returns]
            args = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            if node.args.vararg is not None:
                args.append(node.args.vararg)
            if node.args.kwarg is not None:
                args.append(node.args.kwarg)
            annotations.extend(arg.annotation for arg in args)
            return any(_annotation_contains_pep604_union(annotation) for annotation in annotations)
    return False


def _raise_modern_annotation_error_if_needed(e: TypeError, path: str | Path) -> None:
    if not _type_error_is_from_pep604_annotation(e, path):
        return
    raise RuntimeError(
        f"{path} could not run with Python {sys.version_info.major}.{sys.version_info.minor}. "
        "It appears to use Python 3.10 union annotation syntax such as `A | B`. "
        "Add `from __future__ import annotations`, use `typing.Optional`/`typing.Union`, "
        "or run this code in a Python 3.10+ environment."
    ) from e


def _import_execution_module(module_path: Path):
    module_name = module_path.stem
    sys.path.append(str(module_path.parent))
    try:
        return importlib.import_module(module_name)
    except TypeError as e:
        _raise_modern_annotation_error_if_needed(e, module_path)
        raise


try:
    ndarray_mod = import_from_path("wetlands_ndarray", Path(__file__).parent / "ndarray.py")
    if ndarray_mod is not None:
        ndarray_mod.register_ndarray_pickle()
except ImportError:
    # Do not support ndarray if numpy is not installed
    pass

try:
    _task_file = Path(__file__).parent / "task.py"
    _task_spec = importlib.util.spec_from_file_location("wetlands_task", _task_file)
    if _task_spec is not None and _task_spec.loader is not None:
        _task_mod = importlib.util.module_from_spec(_task_spec)
        sys.modules["wetlands_task"] = _task_mod  # Required before exec for dataclass resolution
        _task_spec.loader.exec_module(_task_mod)
        RemoteTaskHandle = _task_mod.RemoteTaskHandle
    else:
        RemoteTaskHandle = None
except Exception:
    RemoteTaskHandle = None

# Active task handles for cancel support
_active_tasks: dict[str, object] = {}

port = 0
logger = logging.getLogger("module_executor")
_detached_stdio = False
args: argparse.Namespace | None = None


def configure_logging(wetlands_instance_path: Path, level: int = logging.INFO) -> Path:
    """Configure module executor logging under the Wetlands instance directory."""
    log_path = Path(wetlands_instance_path).resolve() / "environments.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s:%(process)d:%(name)s:%(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )
    return log_path


def _safe_print(message: str) -> None:
    try:
        print(message, flush=True)
    except (BrokenPipeError, OSError):
        pass


def _detach_standard_streams() -> None:
    """Stop persistent workers from depending on the launching process pipes."""
    global _detached_stdio
    if _detached_stdio:
        return

    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            root_logger.removeHandler(handler)
            handler.close()

    devnull = open(os.devnull, "w", encoding="utf-8")
    sys.stdout = devnull
    sys.stderr = devnull
    _detached_stdio = True


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
        default=Path("wetlands"),
        type=Path,
    )
    parser.add_argument(
        "--persistent",
        help="Keep the worker process alive after client disconnects so managers can reconnect.",
        action="store_true",
    )
    args = parser.parse_args()
    port = args.port
    configure_logging(args.wetlands_instance_path)
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

def send_message(lock: threading.Lock, connection: Connection, message: dict):
    """Thread-safe sending of messages."""
    with lock:
        connection.send(message)


def handle_execution_error(lock: threading.Lock, connection: Connection, e: Exception, task_id: str | None = None):
    """Common error handling for any execution type."""
    logger.error(str(e))
    logger.error("Traceback:")
    tbftb = traceback.format_tb(e.__traceback__)
    for line in tbftb:
        logger.error(line)
    sys.stderr.flush()
    msg = dict(
        action="error",
        exception=str(e),
        traceback=tbftb,
    )
    if task_id is not None:
        msg["task_id"] = task_id
    send_message(lock, connection, msg)
    logger.debug("Error sent")


def execute_function(message: dict, lock: threading.Lock | None = None, connection: Connection | None = None):
    """Import a module and execute one of its functions."""
    module_path = Path(message["module_path"])
    logger.debug(f"Import module {module_path}")
    module = _import_execution_module(module_path)
    if not hasattr(module, message["function"]):
        raise Exception(f"Module {module_path} has no function {message['function']}.")
    args = message.get("args", [])
    kwargs = message.get("kwargs", {})
    task_id = message.get("task_id")

    # Inject RemoteTaskHandle if the function accepts a 'task' parameter
    if task_id is not None and RemoteTaskHandle is not None and lock is not None and connection is not None:
        func = getattr(module, message["function"])
        try:
            sig = inspect.signature(func)
            if "task" in sig.parameters:
                handle = RemoteTaskHandle(task_id, lock, connection)
                _active_tasks[task_id] = handle
                kwargs = dict(kwargs)
                kwargs["task"] = handle
        except (ValueError, TypeError):
            pass

    logger.info(f"Execute {message['module_path']}:{message['function']}({args})")
    try:
        result = getattr(module, message["function"])(*args, **kwargs)
    except SystemExit as se:
        raise Exception(f"Function raised SystemExit: {se}\n\n")
    except TypeError as e:
        _raise_modern_annotation_error_if_needed(e, module_path)
        raise
    finally:
        if task_id is not None:
            _active_tasks.pop(task_id, None)
    logger.info("Executed")
    return result


def run_script(message: dict):
    """Run a Python script via runpy.run_path(), simulating 'python script.py args...'."""
    script_path = message["script_path"]
    args = message.get("args", [])
    run_name = message.get("run_name", "__main__")

    sys.argv = [script_path] + list(args)
    logger.info(f"Running script {script_path} with args {args} and run_name={run_name}")
    try:
        runpy.run_path(script_path, run_name=run_name)
    except TypeError as e:
        _raise_modern_annotation_error_if_needed(e, script_path)
        raise
    logger.info("Script executed")
    return None


def execution_worker(lock: threading.Lock, connection: Connection, message: dict):
    """
    Worker function handling both 'execute' and 'run' actions.
    """
    task_id = message.get("task_id")
    try:
        action = message["action"]
        if action == "execute":
            result = execute_function(message, lock, connection)
        elif action == "run":
            result = run_script(message)
        else:
            raise Exception(f"Unknown action: {action}")

        response = dict(
            action="execution finished",
            message=f"{action} completed",
            result=result,
        )
        if task_id is not None:
            response["task_id"] = task_id
        send_message(lock, connection, response)
    except Exception as e:
        handle_execution_error(lock, connection, e, task_id=task_id)


def get_message(connection: Connection) -> dict:
    logger.debug("Waiting for message...")
    return connection.recv()


def load_root_authkey(wetlands_instance_path: Path) -> bytes:
    """Read the root-local multiprocessing auth key."""
    return (Path(wetlands_instance_path).resolve() / "state" / "auth.key").read_bytes()


def launch_listener(authkey: bytes | None = None, persistent: bool = False):
    """
    Launches a listener on a random available port on localhost.
    Waits for client connections and handles 'execute', 'run', or 'exit' messages.
    """
    lock = threading.Lock()
    with Listener(("localhost", port), authkey=authkey) as listener:
        task_threads: list[threading.Thread] = []
        _safe_print(f"Listening port {listener.address[1]}")
        if persistent:
            _detach_standard_streams()
        while True:
            try:
                connection_context = listener.accept()
            except (AuthenticationError, EOFError):
                logger.warning("Rejected unauthenticated or abandoned client")
                if persistent:
                    continue
                return
            with connection_context as connection:
                logger.debug(f"Connection accepted {listener.address}")
                message = ""
                try:
                    while True:
                        try:
                            message = get_message(connection)
                        except (EOFError, OSError):
                            logger.debug("Client connection closed")
                            if persistent:
                                for thread in task_threads:
                                    thread.join()
                                task_threads.clear()
                                break
                            return
                        if not message:
                            if persistent:
                                for thread in task_threads:
                                    thread.join()
                                task_threads.clear()
                                break
                            return

                        logger.debug(f"Got message: {message}")

                        if message["action"] in ("execute", "run"):
                            logger.debug(f"Launch thread for action {message['action']}")
                            thread = threading.Thread(
                                target=execution_worker,
                                args=(lock, connection, message),
                            )
                            thread.start()
                            task_threads.append(thread)

                        elif message["action"] == "cancel":
                            cancel_task_id = message.get("task_id")
                            if cancel_task_id and cancel_task_id in _active_tasks:
                                handle = _active_tasks[cancel_task_id]
                                if hasattr(handle, "_set_cancel_requested"):
                                    handle._set_cancel_requested()  # type: ignore[attr-defined]
                                logger.debug(f"Cancel requested for task {cancel_task_id}")
                            else:
                                logger.debug(f"Cancel requested for unknown task {cancel_task_id}")

                        elif message["action"] == "exit":
                            logger.info("exit")
                            send_message(lock, connection, dict(action="exited"))
                            listener.close()
                            return

                        elif message["action"] == "detach":
                            logger.info("detach")
                            if persistent:
                                for thread in task_threads:
                                    thread.join()
                                task_threads.clear()
                                break
                            return
                except Exception as e:
                    handle_execution_error(lock, connection, e)


if __name__ == "__main__":
    assert args is not None
    launch_listener(authkey=load_root_authkey(args.wetlands_instance_path), persistent=args.persistent)

logger.debug("Exit")
