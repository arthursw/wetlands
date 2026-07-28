"""
This script launches a server inside a specified conda environment. It listens on a dynamically assigned
local port for incoming execution commands sent via a multiprocessing connection.

Clients send versioned execution envelopes for installed-module or explicit
development-path targets and receive structured progress, results, or failures.

Designed to run Python call targets with isolated dependencies.
This process boundary is not a security sandbox.
"""

from __future__ import annotations

import sys
import json
import contextlib
import logging
import threading
import traceback
import argparse
import inspect
import os
import platform
import socket
from pathlib import Path
import importlib
import importlib.util
import hashlib
import types
import uuid
import time
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
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_value_codec = import_from_path(
    "wetlands_worker_value_codec",
    Path(__file__).parent / "_internal" / "value_codec.py",
)
if _value_codec is None:
    raise RuntimeError("Could not load the Wetlands worker value codecs")
encode_value = _value_codec.encode_value
decode_value = _value_codec.decode_value
dispose_leases = _value_codec.dispose_leases
descriptor_codecs = _value_codec.descriptor_codecs
SUPPORTED_CODECS = _value_codec.SUPPORTED_CODECS

_protocol = import_from_path(
    "wetlands_worker_protocol",
    Path(__file__).parent / "protocol.py",
)
if _protocol is None:
    raise RuntimeError("Could not load the Wetlands execution protocol")
EXECUTION_PROTOCOL_VERSION = _protocol.EXECUTION_PROTOCOL_VERSION
WORKER_RUNTIME_VERSION = _protocol.WORKER_RUNTIME_VERSION
protocol_message = _protocol.protocol_message
validate_task_message = _protocol.validate_task_message
validate_target = _protocol.validate_target
worker_hello = _protocol.worker_hello

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
active_debug_port: int | None = None
STARTUP_EVENT = "wetlands.worker.ready"
STARTUP_SCHEMA_VERSION = 1
STARTUP_TOKEN_ENV = "WETLANDS_STARTUP_TOKEN"
_path_modules: dict[str, object] = {}
_path_module_keys: dict[str, str] = {}
_output_leases: dict[str, list[object]] = {}
_output_leases_lock = threading.RLock()
_active_tasks_lock = threading.RLock()
CONNECTION_LOSS_GRACE = 5.0
UNCOMMISSIONED_EXIT_CODE = 70


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def _create_split_stream_handlers(fmt: str) -> tuple[logging.StreamHandler, logging.StreamHandler]:
    formatter = logging.Formatter(fmt)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(_MaxLevelFilter(logging.INFO))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)

    return stdout_handler, stderr_handler


def configure_logging(wetlands_instance_path: Path, level: int = logging.INFO) -> Path:
    """Configure module executor logging under the Wetlands instance directory."""
    log_path = Path(wetlands_instance_path).resolve() / "environments.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s %(levelname)s:%(process)d:%(name)s:%(message)s"
    formatter = logging.Formatter(fmt)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    stdout_handler, stderr_handler = _create_split_stream_handlers(fmt)

    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        handler.close()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)
    return log_path


def _safe_print(message: str) -> None:
    try:
        print(message, flush=True)
    except (BrokenPipeError, OSError):
        pass


def _notify_startup(host: str, port: int, token: str, payload: dict) -> None:
    payload = payload.copy()
    payload["token"] = token
    data = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
    with socket.create_connection((host, port), timeout=5.0) as connection:
        connection.sendall(data)


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


def _watch_launcher_commission(
    commissioned: threading.Event,
    timeout: float,
    *,
    exit_process=None,
) -> None:
    """Exit an uncommissioned persistent worker after launcher loss/deadline."""
    if commissioned.wait(timeout=max(0.0, timeout)):
        return
    logger.error("Persistent worker was not commissioned before its startup deadline")
    (exit_process or os._exit)(UNCOMMISSIONED_EXIT_CODE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Wetlands module executor",
        "Module executor runs in an isolated Pixi environment and accepts versioned execution envelopes.",
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
    parser.add_argument(
        "--startup_host",
        help="Private Wetlands startup callback host used to report the dynamically assigned worker port.",
        default=None,
    )
    parser.add_argument(
        "--startup_port",
        help="Private Wetlands startup callback port used to report the dynamically assigned worker port.",
        default=None,
        type=int,
    )
    parser.add_argument("--environment_path", default=None)
    parser.add_argument("--generation_id", default=None)
    parser.add_argument("--recipe_hash", default=None)
    parser.add_argument("--pool_id", default=None)
    parser.add_argument("--worker_index", default=None, type=int)
    parser.add_argument("--commission_timeout", default=None, type=float)
    parser.add_argument("--commissioned", action="store_true")
    args = parser.parse_args()
    if (args.startup_host is None) != (args.startup_port is None):
        parser.error("--startup_host and --startup_port must be provided together")
    port = args.port
    configure_logging(args.wetlands_instance_path)
    logger = logging.getLogger(args.environment)
    if args.debug_port is not None:
        logger.setLevel(logging.DEBUG)
        try:
            import debugpy  # type: ignore[unused-import]

            logger.debug(f"Starting {args.environment} with python {sys.version}")
            _, active_debug_port = debugpy.listen(args.debug_port)
        except ImportError as ie:
            logger.error("debugpy is not installed in this environment. Debugging is not available.")
            logger.error(str(ie))


def send_message(lock: threading.Lock, connection: Connection, message: dict):
    """Thread-safe sending of messages."""
    with lock:
        connection.send(message)


def _remote_exception_payload(e: BaseException) -> dict:
    exc_type = type(e)
    return {
        "module": exc_type.__module__,
        "type_name": exc_type.__name__,
        "qualified_name": getattr(exc_type, "__qualname__", exc_type.__name__),
        "message": str(e),
        "traceback": "".join(traceback.format_exception(exc_type, e, e.__traceback__, chain=False)),
        "cause": _remote_exception_payload(e.__cause__) if e.__cause__ is not None else None,
        "context": _remote_exception_payload(e.__context__) if e.__context__ is not None else None,
        "suppress_context": bool(getattr(e, "__suppress_context__", False)),
    }


def _failure_payload(
    e: BaseException,
    *,
    task_id: str | None = None,
    call_target: str | None = None,
    category: str | None = None,
    serialization_context: str | None = None,
) -> dict:
    resolved_category = category or getattr(e, "category", None) or "remote_exception"
    resolved_context = serialization_context or getattr(e, "serialization_context", None)
    return {
        "category": resolved_category,
        "message": str(e),
        "task_id": task_id,
        "call_target": call_target,
        "traceback": "".join(traceback.format_exception(type(e), e, e.__traceback__, chain=True)),
        "traceback_frames": traceback.format_tb(e.__traceback__),
        "remote_exception": _remote_exception_payload(e),
        "worker": None,
        "exit_code": None,
        "signal": None,
        "timeout": None,
        "elapsed": None,
        "serialization_context": resolved_context,
    }


def handle_execution_error(
    lock: threading.Lock,
    connection: Connection,
    e: BaseException,
    task_id: str | None = None,
    *,
    call_target: str | None = None,
    category: str | None = None,
    serialization_context: str | None = None,
):
    """Common error handling for any execution type."""
    failure = _failure_payload(
        e,
        task_id=task_id,
        call_target=call_target,
        category=category,
        serialization_context=serialization_context,
    )
    _log_execution_failure(failure)
    sys.stderr.flush()
    msg = dict(
        action="error",
        failure=failure,
        exception=str(e),
        traceback=failure["traceback"],
    )
    if task_id is not None:
        msg["task_id"] = task_id
        msg["protocol_version"] = EXECUTION_PROTOCOL_VERSION
    send_message(lock, connection, msg)
    logger.debug("Error sent")


def _log_execution_failure(failure: dict) -> None:
    category = failure.get("category")
    level = logging.ERROR if category == "serialization" else logging.WARNING
    task_id = failure.get("task_id")
    call_target = failure.get("call_target")
    message = failure.get("message") or "Unknown task failure"

    if task_id is not None:
        prefix = f"Task {task_id} failed"
    else:
        prefix = "Remote execution failed"
    if call_target is not None:
        prefix += f" in {call_target}"
    logger.log(level, f"{prefix}: {message}")


def _resolve_qualified_attribute(value: object, qualname: str) -> object:
    current = value
    for component in qualname.split("."):
        current = getattr(current, component)
    if not callable(current):
        raise TypeError(f"Resolved target {qualname!r} is not callable")
    return current


def _resolve_protocol_target(target: dict) -> object:
    target = validate_target(target)
    kind = target.get("kind")
    qualname = target.get("qualname")
    if not isinstance(qualname, str) or not qualname:
        raise ValueError("Execution target has an invalid qualname")
    if kind == "import":
        module_name = target.get("module")
        if not isinstance(module_name, str) or not module_name:
            raise ValueError("Import target has an invalid module")
        module = importlib.import_module(module_name)
        return _resolve_qualified_attribute(module, qualname)
    if kind == "path":
        raw_path = target.get("path")
        if not isinstance(raw_path, str):
            raise ValueError("Path target has an invalid path")
        path = Path(raw_path).resolve(strict=True)
        if not path.is_file():
            raise FileNotFoundError(path)
        content = path.read_bytes()
        content_hash = hashlib.sha256(content).hexdigest()
        path_hash = hashlib.sha256(str(path).encode("utf-8")).hexdigest()
        cache = bool(target.get("cache", True))
        module_key = (
            f"_wetlands_path_{path_hash}_{content_hash}"
            if cache
            else f"_wetlands_path_{path_hash}_{content_hash}_{uuid.uuid4().hex}"
        )
        module = _path_modules.get(module_key) if cache else None
        if module is None:
            module = types.ModuleType(module_key)
            module.__file__ = str(path)
            module.__loader__ = None
            module.__package__ = ""
            sys.modules[module_key] = module
            try:
                exec(compile(content, str(path), "exec"), module.__dict__)
            except BaseException:
                sys.modules.pop(module_key, None)
                raise
            if cache:
                previous_key = _path_module_keys.get(str(path))
                if previous_key is not None and previous_key != module_key:
                    _path_modules.pop(previous_key, None)
                    sys.modules.pop(previous_key, None)
                _path_modules[module_key] = module
                _path_module_keys[str(path)] = module_key
            else:
                sys.modules.pop(module_key, None)
        return _resolve_qualified_attribute(module, qualname)
    raise ValueError(f"Unsupported execution target kind: {kind!r}")


def execute_protocol_envelope(message: dict, lock: threading.Lock, connection: Connection) -> None:
    action, task_id = validate_task_message(message)
    if action != "execute":
        raise ValueError(f"Expected an execute envelope, got {action!r}")
    raw_codecs = message.get("codecs")
    if not isinstance(raw_codecs, list) or any(
        not isinstance(item, dict)
        or set(item) != {"id", "version"}
        or not isinstance(item.get("id"), str)
        or type(item.get("version")) is not int
        for item in raw_codecs
    ):
        raise ValueError("Execution envelope has invalid codec capabilities")
    offered_codecs = {(item["id"], item["version"]) for item in raw_codecs}
    if len(offered_codecs) != len(raw_codecs):
        raise ValueError("Execution envelope contains duplicate codec capabilities")
    required_codecs = set(descriptor_codecs(message.get("args"), message.get("kwargs")))
    if offered_codecs != required_codecs:
        raise ValueError("Execution envelope codec capabilities do not match its value descriptors")
    unsupported_codecs = required_codecs - set(SUPPORTED_CODECS)
    if unsupported_codecs:
        formatted = ", ".join(f"{codec}@{version}" for codec, version in sorted(unsupported_codecs))
        raise ValueError(f"Worker does not support required task codecs: {formatted}")
    attachments: list[object] = []
    handle = RemoteTaskHandle(task_id, lock, connection)
    with _active_tasks_lock:
        if task_id in _active_tasks:
            raise ValueError(f"Task {task_id!r} is already active")
        _active_tasks[task_id] = handle
    canceled = False
    try:
        args = decode_value(message.get("args"), copy_arrays=True, path="args", attachments=attachments)
        kwargs = decode_value(message.get("kwargs"), copy_arrays=True, path="kwargs", attachments=attachments)
        if not isinstance(args, tuple) or not isinstance(kwargs, dict):
            raise ValueError("Execution arguments decoded to invalid container types")
        dispose_leases(attachments, unlink=False)
        attachments.clear()
        send_message(lock, connection, protocol_message("accepted", task_id))
        function = _resolve_protocol_target(message.get("target", {}))
        context_keyword = message.get("context_keyword")
        if context_keyword is not None:
            if not isinstance(context_keyword, str) or not context_keyword.isidentifier():
                raise ValueError("Execution context keyword is invalid")
            if context_keyword in kwargs:
                raise ValueError(f"Execution context keyword {context_keyword!r} conflicts with kwargs")
            signature = inspect.signature(function)
            parameter = signature.parameters.get(context_keyword)
            accepts_kwargs = any(
                candidate.kind is inspect.Parameter.VAR_KEYWORD for candidate in signature.parameters.values()
            )
            if parameter is not None and parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
                raise TypeError(f"Execution target accepts {context_keyword!r} only positionally")
            if parameter is None and not accepts_kwargs:
                raise TypeError(f"Execution target does not accept context keyword {context_keyword!r}")
            kwargs = dict(kwargs)
            kwargs[context_keyword] = handle
        result = function(*args, **kwargs)
        canceled = handle.cancel_requested
        if not canceled:
            encoded_result, output_leases = encode_value(
                result,
                path="result",
                lease_context=message.get("output_lease_context"),
            )
    finally:
        with _active_tasks_lock:
            _active_tasks.pop(task_id, None)
        dispose_leases(attachments, unlink=False)
        with contextlib.suppress(Exception):
            send_message(lock, connection, protocol_message("input_released", task_id))
    if canceled:
        send_message(lock, connection, protocol_message("canceled", task_id))
        return
    with _output_leases_lock:
        _output_leases[task_id] = output_leases
    try:
        send_message(lock, connection, protocol_message("result_offer", task_id, result=encoded_result))
    except BaseException:
        with _output_leases_lock:
            leases = _output_leases.pop(task_id, [])
        dispose_leases(leases, unlink=True)
        raise


def execution_worker(lock: threading.Lock, connection: Connection, message: dict):
    """Execute one versioned task envelope and report a structured failure."""
    task_id = message.get("task_id")
    target = message.get("target")
    call_target = None
    if isinstance(target, dict):
        if target.get("kind") == "import":
            call_target = f"{target.get('module')}:{target.get('qualname')}"
        elif target.get("kind") == "path":
            call_target = f"{target.get('path')}:{target.get('qualname')}"
    try:
        execute_protocol_envelope(message, lock, connection)
    except BaseException as e:
        handle_execution_error(lock, connection, e, task_id=task_id, call_target=call_target)


def get_message(connection: Connection) -> dict:
    logger.debug("Waiting for message...")
    return connection.recv()


def _quiesce_task_threads(task_threads: list[threading.Thread], timeout: float) -> bool:
    with _active_tasks_lock:
        handles = tuple(_active_tasks.values())
    for handle in handles:
        if hasattr(handle, "_set_cancel_requested"):
            handle._set_cancel_requested()
    deadline = time.monotonic() + timeout
    for thread in task_threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    quiesced = not any(thread.is_alive() for thread in task_threads)
    task_threads.clear()
    return quiesced


def load_root_authkey(wetlands_instance_path: Path) -> bytes:
    """Read the root-local multiprocessing auth key."""
    return (Path(wetlands_instance_path).resolve() / "state" / "auth.key").read_bytes()


def launch_listener(
    authkey: bytes | None = None,
    persistent: bool = False,
    *,
    startup_host: str | None = None,
    startup_port: int | None = None,
    startup_token: str | None = None,
    debug_port: int | None = None,
    environment_path: str | None = None,
    generation_id: str | None = None,
    recipe_hash: str | None = None,
    pool_id: str | None = None,
    worker_index: int | None = None,
    commission_timeout: float | None = None,
    commissioned: bool = False,
):
    """
    Launch an authenticated listener on a random ``127.0.0.1`` port.
    Handle versioned execution and lifecycle control messages.
    """
    for field, value in {
        "environment_path": environment_path,
        "generation_id": generation_id,
        "recipe_hash": recipe_hash,
    }.items():
        if not isinstance(value, str) or not value:
            raise ValueError(f"{field} is required for the worker handshake")
    if persistent:
        if not isinstance(pool_id, str) or not pool_id:
            raise ValueError("pool_id is required for a persistent worker")
        if type(worker_index) is not int or worker_index < 0:
            raise ValueError("worker_index is required for a persistent worker")
        if not commissioned and (not isinstance(commission_timeout, (int, float)) or commission_timeout <= 0):
            raise ValueError("commission_timeout must be positive for an uncommissioned persistent worker")
    hello = worker_hello(
        codecs=SUPPORTED_CODECS,
        python_version=platform.python_version(),
        pid=os.getpid(),
        environment_path=environment_path,
        generation_id=generation_id,
        recipe_hash=recipe_hash,
    )
    if persistent:
        hello.update({"pool_id": pool_id, "worker_index": worker_index})
    lock = threading.Lock()
    with Listener(("127.0.0.1", port), authkey=authkey) as listener:
        task_threads: list[threading.Thread] = []
        commission_event = threading.Event()
        if commissioned:
            commission_event.set()
        elif persistent:
            threading.Thread(
                target=_watch_launcher_commission,
                args=(commission_event, float(commission_timeout)),
                daemon=True,
                name="wetlands-launcher-loss-watchdog",
            ).start()
        if startup_host is not None or startup_port is not None:
            if startup_host is None or startup_port is None:
                raise ValueError("startup_host and startup_port must be provided together")
            if startup_token is None:
                raise ValueError(f"{STARTUP_TOKEN_ENV} must be set when startup callback is enabled")
            _notify_startup(
                startup_host,
                startup_port,
                startup_token,
                {
                    **hello,
                    "event": STARTUP_EVENT,
                    "schema_version": STARTUP_SCHEMA_VERSION,
                    "port": listener.address[1],
                    "debug_port": debug_port,
                },
            )
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
                send_message(lock, connection, hello)
                message = ""
                try:
                    while True:
                        try:
                            message = get_message(connection)
                        except (EOFError, OSError):
                            logger.debug("Client connection closed")
                            if _quiesce_task_threads(task_threads, CONNECTION_LOSS_GRACE) and persistent:
                                break
                            return
                        if not message:
                            if _quiesce_task_threads(task_threads, CONNECTION_LOSS_GRACE) and persistent:
                                break
                            return

                        target = message.get("target") if isinstance(message, dict) else None
                        target_kind = target.get("kind") if isinstance(target, dict) else None
                        target_name = None
                        if isinstance(target, dict):
                            target_name = target.get("module") or Path(str(target.get("path", ""))).name
                        logger.debug(
                            "Got action=%r task_id=%r target_kind=%r target=%r",
                            message.get("action") if isinstance(message, dict) else None,
                            message.get("task_id") if isinstance(message, dict) else None,
                            target_kind,
                            target_name,
                        )

                        if message["action"] == "execute":
                            task_threads[:] = [thread for thread in task_threads if thread.is_alive()]
                            if task_threads:
                                raise RuntimeError("Worker received a second task while one is still active")
                            with _output_leases_lock:
                                if _output_leases:
                                    raise RuntimeError(
                                        "Worker received a new task before the prior result was released"
                                    )
                            validate_task_message(message)
                            logger.debug(f"Launch thread for action {message['action']}")
                            thread = threading.Thread(
                                target=execution_worker,
                                args=(lock, connection, message),
                                daemon=True,
                            )
                            thread.start()
                            task_threads.append(thread)

                        elif message["action"] == "cancel":
                            _action, cancel_task_id = validate_task_message(message)
                            with _active_tasks_lock:
                                handle = _active_tasks.get(cancel_task_id)
                            if handle is not None:
                                if hasattr(handle, "_set_cancel_requested"):
                                    handle._set_cancel_requested()  # type: ignore[attr-defined]
                                logger.debug(f"Cancel requested for task {cancel_task_id}")
                            else:
                                logger.debug(f"Cancel requested for unknown task {cancel_task_id}")

                        elif message["action"] == "release":
                            _action, release_task_id = validate_task_message(message)
                            with _output_leases_lock:
                                leases = _output_leases.pop(release_task_id, [])
                            expected_names = {lease.name for lease in leases}
                            raw_names = message.get("names")
                            if not isinstance(raw_names, list) or not all(isinstance(name, str) for name in raw_names):
                                dispose_leases(leases, unlink=True)
                                raise ValueError("Result release contained invalid shared-memory names")
                            received_names = set(raw_names)
                            if received_names != expected_names:
                                dispose_leases(leases, unlink=True)
                                raise ValueError(
                                    f"Result release for task {release_task_id} did not match the worker lease table"
                                )
                            dispose_leases(leases, unlink=True)
                            send_message(
                                lock,
                                connection,
                                protocol_message(
                                    "released",
                                    release_task_id,
                                    names=sorted(expected_names),
                                ),
                            )

                        elif message["action"] == "exit":
                            if message.get("protocol_version") != EXECUTION_PROTOCOL_VERSION:
                                raise ValueError("Exit request used an incompatible protocol")
                            logger.info("exit")
                            send_message(
                                lock,
                                connection,
                                {
                                    "action": "exited",
                                    "protocol_version": EXECUTION_PROTOCOL_VERSION,
                                },
                            )
                            listener.close()
                            return

                        elif message["action"] == "detach":
                            if message.get("protocol_version") != EXECUTION_PROTOCOL_VERSION:
                                raise ValueError("Detach request used an incompatible protocol")
                            logger.info("detach")
                            if persistent:
                                if not _quiesce_task_threads(task_threads, 0.0):
                                    raise RuntimeError("Cannot detach a worker with an active task")
                                break
                            return
                        elif message["action"] == "commission":
                            if not persistent:
                                raise RuntimeError("Cannot commission a nonpersistent worker")
                            if message.get("protocol_version") != EXECUTION_PROTOCOL_VERSION:
                                raise ValueError("Commission request used an incompatible protocol")
                            if message.get("pool_id") != pool_id:
                                raise ValueError("Commission request used the wrong pool ID")
                            commission_event.set()
                            send_message(
                                lock,
                                connection,
                                {
                                    "action": "commissioned",
                                    "protocol_version": EXECUTION_PROTOCOL_VERSION,
                                    "pool_id": pool_id,
                                },
                            )
                        else:
                            raise ValueError(f"Unknown worker control action: {message['action']!r}")
                except Exception as e:
                    quiesced = _quiesce_task_threads(task_threads, CONNECTION_LOSS_GRACE)
                    handle_execution_error(lock, connection, e)
                    if not quiesced:
                        return
                finally:
                    with _output_leases_lock:
                        abandoned = list(_output_leases.values())
                        _output_leases.clear()
                    for leases in abandoned:
                        dispose_leases(leases, unlink=True)
                if not persistent:
                    return


if __name__ == "__main__":
    assert args is not None
    launch_listener(
        authkey=load_root_authkey(args.wetlands_instance_path),
        persistent=args.persistent,
        startup_host=args.startup_host,
        startup_port=args.startup_port,
        startup_token=os.environ.get(STARTUP_TOKEN_ENV),
        debug_port=active_debug_port,
        environment_path=args.environment_path,
        generation_id=args.generation_id,
        recipe_hash=args.recipe_hash,
        pool_id=args.pool_id,
        worker_index=args.worker_index,
        commission_timeout=args.commission_timeout,
        commissioned=args.commissioned,
    )

logger.debug("Exit")
