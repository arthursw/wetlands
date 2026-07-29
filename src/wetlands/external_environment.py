from __future__ import annotations

import enum
import json
import math
import subprocess
import time
import socket
import os
import secrets
import uuid
from pathlib import Path
from multiprocessing import connection as mp_connection
from multiprocessing.context import AuthenticationError
from multiprocessing.connection import Client, Connection
import functools
import hmac
import threading
import queue
from collections.abc import Callable, Iterable
from typing import Any, TYPE_CHECKING

from wetlands.logger import logger, LOG_SOURCE_EXECUTION
from wetlands.diagnostics import ExecutionFailure, WorkerInfo
from wetlands._internal.process_termination import (
    ProcessIdentityError,
    ProcessTerminationError,
    capture_process_identity,
    identity_matches,
    terminate_attached_process_tree,
    terminate_launched_process_tree,
)
from wetlands._internal.process_logger import ProcessLogger
from wetlands._internal import runtime_state
from wetlands._internal.provisioning import _read_ready, environment_lifecycle_gate
from wetlands._internal.value_codec import (
    REQUIRED_WORKER_CODECS,
    SharedMemoryLease,
    decode_value,
    descriptor_codecs,
    dispose_leases,
    encode_value,
    reconcile_shared_memory_leases,
    unlink_names,
)
from wetlands.protocol import (
    ACTION_ACCEPTED,
    ACTION_CANCELED,
    ACTION_FAILURE,
    ACTION_INPUT_RELEASED,
    ACTION_LOG,
    ACTION_RELEASED,
    ACTION_RESULT_OFFER,
    ACTION_UPDATE,
    EXECUTION_PROTOCOL_VERSION,
    ProtocolError,
    WORKER_RUNTIME_VERSION,
    execution_envelope,
    import_target,
    path_target,
    protocol_message,
    validate_worker_task_message,
    validate_worker_capabilities,
)
from wetlands.lifecycle import EnvironmentGenerationChangedError, WorkerStartError
from wetlands.task import ExecutionTask

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager

MODULE_EXECUTOR_FILE = "module_executor.py"
ATTACH_CONNECT_TIMEOUT = 5.0
STARTUP_EVENT = "wetlands.worker.ready"
STARTUP_SCHEMA_VERSION = 1
STARTUP_TOKEN_ENV = "WETLANDS_STARTUP_TOKEN"
STARTUP_CALLBACK_TIMEOUT = 30.0
STARTUP_CONNECTION_READ_TIMEOUT = 0.5
STARTUP_MAX_PAYLOAD_BYTES = 64 * 1024
POOL_COMMISSION_ACK_TIMEOUT = 5.0
LAUNCHER_LOSS_TIMEOUT_MARGIN = 30.0
WORKER_GRACEFUL_EXIT_TIMEOUT = 2.0
PROCESS_LOGGER_JOIN_TIMEOUT = 5.0
_NO_RESULT = object()
_WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000


class _DispatchOutcome(enum.Enum):
    DISPATCHED = "dispatched"
    SHUTTING_DOWN = "shutting_down"
    TASK_FAILED = "task_failed"
    WORKER_UNAVAILABLE = "worker_unavailable"


class _AttachTimeout(TimeoutError):
    """Raised when a live worker does not complete attach in time."""


def _assign_windows_kill_job(process: subprocess.Popen) -> None:
    """Put a worker in a kill-on-close Job Object when Windows permits it."""
    import ctypes
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        error = getattr(ctypes, "get_last_error")()
        raise OSError(error, getattr(ctypes, "FormatError")(error))
    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = _WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    configured = kernel32.SetInformationJobObject(
        job,
        9,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    assigned = configured and kernel32.AssignProcessToJobObject(
        job,
        wintypes.HANDLE(process._handle),  # type: ignore[attr-defined]
    )
    if not assigned:
        error = getattr(ctypes, "get_last_error")()
        kernel32.CloseHandle(job)
        raise OSError(error, getattr(ctypes, "FormatError")(error))
    process._wetlands_job_handle = job  # type: ignore[attr-defined]


def _close_windows_job(process: subprocess.Popen) -> None:
    handle = getattr(process, "_wetlands_job_handle", None)
    if handle is None:
        return
    process._wetlands_job_handle = None  # type: ignore[attr-defined]
    import ctypes

    getattr(ctypes, "WinDLL")("kernel32", use_last_error=True).CloseHandle(handle)


def _mp_connection_attr(*names: str) -> Any:
    """Return the first available multiprocessing.connection attribute."""
    for name in names:
        if hasattr(mp_connection, name):
            return getattr(mp_connection, name)
    raise AttributeError(f"multiprocessing.connection has none of: {', '.join(names)}")


def _open_startup_socket() -> socket.socket:
    startup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        startup_socket.bind(("127.0.0.1", 0))
        startup_socket.listen(1)
        startup_socket.settimeout(0.1)
    except Exception:
        startup_socket.close()
        raise
    return startup_socket


def _read_startup_payload(connection: socket.socket, timeout: float) -> dict[str, Any]:
    connection.settimeout(timeout)
    chunks: list[bytes] = []
    received = 0
    while True:
        chunk = connection.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        received += len(chunk)
        if received > STARTUP_MAX_PAYLOAD_BYTES:
            raise ValueError("startup payload exceeded size limit")
        if b"\n" in chunk:
            break

    raw_payload = b"".join(chunks).split(b"\n", 1)[0]
    if not raw_payload:
        raise ValueError("startup payload was empty")
    payload = json.loads(raw_payload.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("startup payload was not an object")
    return payload


def _validate_startup_payload(payload: dict[str, Any], token: str) -> dict[str, Any]:
    if payload.get("event") != STARTUP_EVENT:
        raise ValueError("startup payload had an unexpected event")
    if payload.get("schema_version") != STARTUP_SCHEMA_VERSION:
        raise ValueError("startup payload had an unexpected schema version")
    if not hmac.compare_digest(str(payload.get("token", "")), token):
        raise ValueError("startup payload token did not match")

    port = payload.get("port")
    if not isinstance(port, int) or not (0 < port <= 65535):
        raise ValueError("startup payload had an invalid worker port")

    management_port = payload.get("management_port")
    if not isinstance(management_port, int) or not (0 < management_port <= 65535):
        raise ValueError("startup payload had an invalid management port")

    validate_worker_capabilities(payload, required_codecs=REQUIRED_WORKER_CODECS)
    return payload


def _wait_for_startup_payload(
    startup_socket: socket.socket,
    token: str,
    process: subprocess.Popen,
    *,
    timeout: float = STARTUP_CALLBACK_TIMEOUT,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while True:
        if process.poll() is not None:
            raise RuntimeError(f"worker exited with return code {process.returncode} before startup callback")

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            detail = f": {last_error}" if last_error is not None else ""
            raise TimeoutError(f"timed out waiting for worker startup callback{detail}")

        startup_socket.settimeout(min(0.1, remaining))
        try:
            connection, _address = startup_socket.accept()
        except socket.timeout:
            continue

        with connection:
            try:
                payload = _read_startup_payload(connection, min(STARTUP_CONNECTION_READ_TIMEOUT, remaining))
                return _validate_startup_payload(payload, token)
            except Exception as exc:
                last_error = exc


def synchronized(method):
    """Decorator to wrap a method call with self._lock."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class _Worker:
    """Holds state for a single module_executor process."""

    __slots__ = (
        "index",
        "process",
        "port",
        "connection",
        "process_logger",
        "reader_thread",
        "pid",
        "persistent",
        "process_started_at",
        "process_group_id",
        "session_id",
        "_current_task",
        "_last_activity",
        "_finished_task_ids",
        "_retired",
        "_commissioned",
        "capabilities",
    )

    def __init__(
        self,
        index: int,
        process: subprocess.Popen | None,
        port: int,
        connection: Connection,
        process_logger: ProcessLogger | None,
        *,
        pid: int | None = None,
        persistent: bool = False,
        process_started_at: float | None = None,
        process_group_id: int | None = None,
        session_id: int | None = None,
        capabilities: Any = None,
    ) -> None:
        self.index = index
        self.process = process
        self.port = port
        self.connection = connection
        self.process_logger = process_logger
        self.pid = pid if pid is not None else (process.pid if process is not None else None)
        self.persistent = persistent
        launched_started_at = getattr(process, "_wetlands_started_at", None)
        launched_group_id = getattr(process, "_wetlands_process_group_id", None)
        launched_session_id = getattr(process, "_wetlands_session_id", None)
        self.process_started_at = (
            process_started_at
            if process_started_at is not None
            else (float(launched_started_at) if isinstance(launched_started_at, (int, float)) else None)
        )
        self.process_group_id = (
            process_group_id
            if process_group_id is not None
            else launched_group_id
            if isinstance(launched_group_id, int)
            else None
        )
        self.session_id = (
            session_id
            if session_id is not None
            else launched_session_id
            if isinstance(launched_session_id, int)
            else None
        )
        self.reader_thread: threading.Thread | None = None
        self._current_task: ExecutionTask[Any] | None = None
        self._last_activity: float = 0.0
        self._finished_task_ids: set[str] = set()
        self._retired = False
        self._commissioned = threading.Event()
        self.capabilities = capabilities

    def alive(self) -> bool:
        if self.process is not None:
            return self.process.poll() is None
        if self.pid is not None and self.process_started_at is not None:
            return identity_matches(self.pid, self.process_started_at)
        return False


class ExternalEnvironment:
    def __init__(
        self,
        name: str,
        path: Path | None,
        environment_manager: "EnvironmentManager",
        *,
        expected_generation_id: str | None = None,
        expected_recipe_hash: str | None = None,
    ) -> None:
        self.name = name
        self.path = path.resolve() if path is not None else None
        self.environment_manager = environment_manager
        self._expected_generation_id = expected_generation_id
        self._expected_recipe_hash = expected_recipe_hash
        self._pool_id: str | None = uuid.uuid4().hex
        self._fatal_error: BaseException | None = None
        self._lock = threading.RLock()
        # Worker pool state
        self._workers: list[_Worker] = []
        self._idle_workers: queue.Queue[_Worker] = queue.Queue()
        self._task_queue: queue.Queue[ExecutionTask[Any]] = queue.Queue()
        self._worker_env: Callable[[int], dict[str, str]] | None = None
        self._worker_timeout: float | None = None
        self._persistent: bool = False
        self._pool_commissioned = False
        self._launcher_loss_timeout = STARTUP_CALLBACK_TIMEOUT + LAUNCHER_LOSS_TIMEOUT_MARGIN
        self._authkey: bytes | None = None
        self._shutdown_event = threading.Event()
        self._controller_id: str | None = None

    def _ready_identity(self) -> dict[str, Any]:
        ready = _read_ready(Path(self.path).parent) if self.path is not None else None
        actual_generation_id = str(ready.get("generation_id")) if ready is not None else None
        actual_recipe_hash = str(ready.get("recipe_hash")) if ready is not None else None
        if (
            self._expected_generation_id is not None
            and self._expected_recipe_hash is not None
            and (
                actual_generation_id != self._expected_generation_id or actual_recipe_hash != self._expected_recipe_hash
            )
        ):
            raise EnvironmentGenerationChangedError(
                self.name,
                expected_generation_id=self._expected_generation_id,
                expected_recipe_hash=self._expected_recipe_hash,
                actual_generation_id=actual_generation_id,
                actual_recipe_hash=actual_recipe_hash,
            )
        if ready is None:
            raise RuntimeError(f"Environment {self.name!r} no longer has valid ready metadata")
        return ready

    def _project_path(self) -> Path:
        if self.path is None:
            raise RuntimeError(f"Environment {self.name!r} has no executable path")
        return self.path.parent.resolve()

    def _raise_if_failed(self) -> None:
        with self._lock:
            error = self._fatal_error
        if error is not None:
            raise error

    @synchronized
    def launch(
        self,
        *,
        max_workers: int = 1,
        worker_env: Callable[[int], dict[str, str]] | None = None,
        worker_timeout: float | None = None,
        persistent: bool = False,
    ) -> None:
        """Launches module executor process(es) in the environment.

        Args:
            max_workers: Number of worker processes to start.
                All workers share the same Pixi environment.
            worker_env: Optional callable receiving worker index (0-based),
                returning extra environment variables for that worker.
            worker_timeout: Optional inactivity timeout in seconds. If set and a
                worker sends no IPC message within this duration, it is treated as
                hung: the active task is failed, the worker is killed and replaced.
                Each IPC message resets the timer, so this is not a maximum task
                execution time.
            persistent: If True, workers are recorded in the root registry and can
                later be reconnected with ManagedEnvironment.attach_pool().
        """
        self._raise_if_failed()
        if worker_timeout is not None and (
            type(worker_timeout) not in {int, float} or not math.isfinite(worker_timeout) or worker_timeout <= 0
        ):
            raise ValueError("worker_timeout must be None or a positive finite number")
        if self.launched():
            return
        if max_workers < 1:
            raise ValueError("max_workers must be at least one")
        reconcile_shared_memory_leases(self.environment_manager.root)

        self._worker_env = worker_env
        self._worker_timeout = worker_timeout
        self._persistent = persistent
        self._pool_commissioned = False
        self._launcher_loss_timeout = max_workers * STARTUP_CALLBACK_TIMEOUT + LAUNCHER_LOSS_TIMEOUT_MARGIN
        self._authkey = runtime_state.load_or_create_root_authkey(self.environment_manager.root)
        self._shutdown_event.clear()
        if self._persistent:
            self._controller_id = uuid.uuid4().hex
            try:
                runtime_state.claim_controller(
                    self.environment_manager.root,
                    self.name,
                    self._controller_id,
                )
            except BaseException:
                self._controller_id = None
                raise
            try:
                runtime_state.reconcile_persistent_pool(
                    self.environment_manager.root,
                    self.name,
                    grace=float(self.environment_manager.termination_grace),
                )
                live_workers = runtime_state.live_workers_for_env(
                    self.environment_manager.root,
                    self.name,
                )
                if live_workers:
                    raise WorkerStartError(
                        self.name,
                        f"Live persistent workers already exist for environment '{self.name}'. "
                        "Attach to or exit the existing workers before launching again.",
                    )
                assert self._pool_id is not None
                runtime_state.begin_persistent_pool_attempt(
                    self.environment_manager.root,
                    env_name=self.name,
                    pool_id=self._pool_id,
                    expected_worker_count=max_workers,
                )
            except BaseException:
                runtime_state.release_controller(
                    self.environment_manager.root,
                    self.name,
                    self._controller_id,
                )
                self._controller_id = None
                raise

        started: list[_Worker] = []
        try:
            for i in range(max_workers):
                worker = self._launch_worker(i, worker_env)
                started.append(worker)
            if self._persistent:
                self._commission_workers(started)
                assert self._pool_id is not None
                runtime_state.commission_persistent_pool(
                    self.environment_manager.root,
                    env_name=self.name,
                    pool_id=self._pool_id,
                )
                self._pool_commissioned = True
        except BaseException as error:
            self._shutdown_event.set()
            cleanup_errors: list[str] = []
            cleanup_complete = True
            for worker in started:
                try:
                    if not self._remove_dead_worker(worker):
                        cleanup_complete = False
                        cleanup_errors.append(f"worker {worker.index} process tree could not be verified as terminated")
                except Exception as cleanup_error:
                    cleanup_complete = False
                    cleanup_errors.append(str(cleanup_error))
            self._drain_idle_workers()
            if self._persistent and self._pool_id is not None and cleanup_complete:
                try:
                    discarded = runtime_state.discard_persistent_pool(
                        self.environment_manager.root,
                        env_name=self.name,
                        pool_id=self._pool_id,
                    )
                    if not discarded:
                        cleanup_errors.append(
                            "persistent pool journal retained because worker records survived cleanup"
                        )
                except Exception as cleanup_error:
                    cleanup_errors.append(str(cleanup_error))
            if self._controller_id is not None:
                try:
                    runtime_state.release_controller(
                        self.environment_manager.root,
                        self.name,
                        self._controller_id,
                    )
                except Exception as cleanup_error:
                    cleanup_errors.append(str(cleanup_error))
                self._controller_id = None
            if isinstance(
                error,
                (EnvironmentGenerationChangedError, WorkerStartError),
            ) or not isinstance(error, Exception):
                raise
            raise WorkerStartError(
                self.name,
                str(error),
                cleanup_errors=tuple(cleanup_errors),
            ) from error

        self._workers.extend(started)
        for worker in started:
            self._idle_workers.put(worker)

        # Start health monitor thread
        self._health_thread = threading.Thread(
            target=self._health_monitor_loop,
            daemon=True,
            name=f"wetlands-health-{self.name}",
        )
        self._health_thread.start()

    def _drain_idle_workers(self) -> None:
        while True:
            try:
                self._idle_workers.get_nowait()
            except queue.Empty:
                return

    def _launch_worker(
        self,
        index: int,
        worker_env: Callable[[int], dict[str, str]] | None,
    ) -> _Worker:
        """Launch a single module_executor process and return a _Worker."""
        module_executor_path = Path(__file__).parent.resolve() / MODULE_EXECUTOR_FILE
        ready = self._ready_identity()
        startup_socket = _open_startup_socket()
        startup_host, startup_port = startup_socket.getsockname()
        startup_token = secrets.token_urlsafe(32)
        worker_id = uuid.uuid4().hex
        root = self.environment_manager.root.resolve()
        argv = [
            str(self._environment_python()),
            "-u",
            str(module_executor_path),
            self.name,
            "--root",
            str(root),
            "--environment_path",
            str(self._project_path()),
            "--generation_id",
            str(ready["generation_id"]),
            "--recipe_hash",
            str(ready["recipe_hash"]),
            "--worker_index",
            str(index),
            "--worker_id",
            worker_id,
            "--pool_id",
            str(self._pool_id),
            "--startup_host",
            str(startup_host),
            "--startup_port",
            str(startup_port),
        ]
        if self._persistent:
            argv.append("--persistent")
            if self._pool_commissioned:
                argv.append("--commissioned")
            else:
                argv.extend(["--commission_timeout", str(self._launcher_loss_timeout)])

        log_context = {"log_source": LOG_SOURCE_EXECUTION, "env_name": self.name, "call_target": MODULE_EXECUTOR_FILE}
        if len(self._workers) > 0 or index > 0:
            log_context["worker_index"] = str(index)

        env = os.environ.copy()
        if worker_env is not None:
            env.update(worker_env(index))
        env[STARTUP_TOKEN_ENV] = startup_token
        for variable in ("PYTHONEXECUTABLE", "PYTHONHOME", "PYTHONPATH"):
            env.pop(variable, None)

        process: subprocess.Popen | None = None
        connection: Connection | None = None
        process_logger: ProcessLogger | None = None
        recorded = False
        worker: _Worker | None = None
        try:
            process = self._spawn_worker_process(argv, env, log_context)
            process_logger = ProcessLogger(process, log_context, logger)
            process_logger.start_reading()

            try:
                startup_payload = _wait_for_startup_payload(startup_socket, startup_token, process)
            except Exception as e:
                raise Exception(
                    f"Could not receive startup information for worker {index}: {e}."
                    f"{self._worker_startup_failure_details(process, process_logger)}"
                ) from e

            expected_identity = {
                "pid": process.pid,
                "environment_path": str(self._project_path()),
                "generation_id": str(ready["generation_id"]),
                "recipe_hash": str(ready["recipe_hash"]),
            }
            if self._persistent:
                expected_identity.update(
                    {
                        "pool_id": self._pool_id,
                        "worker_index": index,
                    }
                )
            startup_capabilities = validate_worker_capabilities(
                startup_payload,
                required_codecs=REQUIRED_WORKER_CODECS,
                expected_identity=expected_identity,
            )
            if process.poll() is not None:
                raise Exception(
                    f"Worker {index} exited with return code {process.returncode} before accepting connections."
                    f"{self._worker_startup_failure_details(process, process_logger)}"
                )
            port = int(startup_payload["port"])
            management_port = int(startup_payload["management_port"])
            if port == 0:
                raise Exception(
                    f"Could not find the server port for worker {index}."
                    f"{self._worker_startup_failure_details(process, process_logger)}"
                )
            authkey = self._authkey or runtime_state.load_or_create_root_authkey(self.environment_manager.root)
            connection, capabilities = self._connect_worker(
                port,
                authkey,
                expected_identity=expected_identity,
            )
            if capabilities != startup_capabilities:
                raise RuntimeError("Worker connection handshake did not match its startup callback")
            worker = _Worker(
                index,
                process,
                port,
                connection,
                process_logger,
                persistent=self._persistent,
                capabilities=capabilities,
            )

            runtime_state.record_worker(
                self.environment_manager.root,
                env_name=self.name,
                env_path=Path(self.path).parent if self.path is not None else None,
                worker_index=index,
                pid=process.pid,
                port=port,
                persistent=self._persistent,
                generation_id=str(ready["generation_id"]),
                recipe_hash=str(ready["recipe_hash"]),
                worker_runtime_version=WORKER_RUNTIME_VERSION,
                protocol_version=EXECUTION_PROTOCOL_VERSION,
                pool_id=self._pool_id,
                worker_id=worker_id,
                management_port=management_port,
            )
            recorded = True

            self._start_reader_thread(worker)
            return worker
        except BaseException as error:
            if process is None:
                raise
            terminated = self._cleanup_failed_worker_launch(process, connection)
            if recorded and terminated:
                try:
                    runtime_state.remove_worker(
                        self.environment_manager.root,
                        self.name,
                        index,
                        self._pool_id,
                    )
                except Exception as cleanup_error:
                    cleanup_failure = WorkerStartError(
                        self.name,
                        str(error),
                        worker_index=index,
                        cleanup_errors=(
                            f"worker terminated but its durable ownership record could not be removed: {cleanup_error}",
                        ),
                    )
                    if worker is not None:
                        worker._retired = True
                        with self._lock:
                            if worker not in self._workers:
                                self._workers.append(worker)
                            self._fatal_error = cleanup_failure
                    raise cleanup_failure from error
            if not terminated:
                ownership = (
                    "durable worker ownership record retained"
                    if recorded
                    else "worker ownership could not be durably recorded"
                )
                cleanup_failure = WorkerStartError(
                    self.name,
                    str(error),
                    worker_index=index,
                    cleanup_errors=("worker process-tree termination could not be verified; " + ownership,),
                )
                if recorded and worker is not None:
                    worker._retired = True
                    with self._lock:
                        if worker not in self._workers:
                            self._workers.append(worker)
                        self._fatal_error = cleanup_failure
                raise cleanup_failure from error
            raise
        finally:
            startup_socket.close()

    def _commission_workers(self, workers: Iterable[_Worker]) -> None:
        if self._pool_id is None:
            raise RuntimeError("Persistent worker pool has no pool ID")
        workers = tuple(workers)
        for worker in workers:
            worker.connection.send(
                {
                    "action": "commission",
                    "protocol_version": EXECUTION_PROTOCOL_VERSION,
                    "pool_id": self._pool_id,
                }
            )
        deadline = time.monotonic() + POOL_COMMISSION_ACK_TIMEOUT
        for worker in workers:
            if not worker._commissioned.wait(max(0.0, deadline - time.monotonic())):
                raise RuntimeError(f"Worker {worker.index} did not acknowledge pool commission")

    def _environment_python(self) -> Path:
        if self.path is None:
            raise RuntimeError("Managed environment path is unavailable")
        project = Path(self.path).parent.resolve()
        prefix = project / ".pixi" / "envs" / "default"
        candidates = (prefix / "python.exe",) if os.name == "nt" else (prefix / "bin" / "python", prefix / "python")
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        pixi = self.environment_manager.pixi_executable
        if pixi is None and self.environment_manager._prepared is not None:
            pixi = self.environment_manager._prepared.executable
        if pixi is not None:
            probe = subprocess.run(
                [
                    str(pixi),
                    "run",
                    "--manifest-path",
                    str(self.path),
                    "python",
                    "-c",
                    "import sys; print(sys.executable)",
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if probe.returncode == 0 and probe.stdout.strip():
                discovered = Path(probe.stdout.splitlines()[-1]).resolve()
                if discovered.is_file():
                    return discovered
        raise RuntimeError(f"Managed Pixi environment has no discoverable Python executable under {prefix}")

    def _spawn_worker_process(
        self,
        argv: list[str],
        env: dict[str, str],
        log_context: dict[str, str],
    ) -> subprocess.Popen:
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "encoding": "utf-8",
            "errors": "replace",
            "bufsize": 1,
            "env": env,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP")
        else:
            kwargs["start_new_session"] = True
        process = subprocess.Popen(argv, **kwargs)
        identity_captured = False
        try:
            identity = capture_process_identity(process.pid)
            identity_captured = True
            process._wetlands_started_at = identity.started_at  # type: ignore[attr-defined]
            process._wetlands_process_group_id = identity.process_group_id  # type: ignore[attr-defined]
            process._wetlands_session_id = identity.session_id  # type: ignore[attr-defined]
            if os.name == "nt":
                if not self._persistent:
                    _assign_windows_kill_job(process)
            elif identity.process_group_id != process.pid or identity.session_id != process.pid:
                raise ProcessIdentityError(f"Worker PID {process.pid} did not start in its own POSIX session")
        except BaseException as error:
            if identity_captured:
                try:
                    terminate_launched_process_tree(
                        process,
                        grace=WORKER_GRACEFUL_EXIT_TIMEOUT,
                        close_windows_job=_close_windows_job,
                    )
                except ProcessTerminationError as termination_error:
                    raise termination_error from error
            else:
                # Without a captured start identity there is no safe target for
                # the normal tree terminator. This immediate child is still the
                # Popen object we just created, so direct termination is the
                # bounded last resort.
                if process.poll() is None:
                    process.kill()
                process.wait()
            raise
        return process

    def _cleanup_failed_worker_launch(self, process: subprocess.Popen, connection: Connection | None = None) -> bool:
        if connection is not None:
            try:
                connection.send({"action": "exit", "protocol_version": EXECUTION_PROTOCOL_VERSION})
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass

        return self._terminate_launched_worker(process)

    def _terminate_launched_worker(self, process: subprocess.Popen) -> bool:
        try:
            terminate_launched_process_tree(
                process,
                grace=WORKER_GRACEFUL_EXIT_TIMEOUT,
                close_windows_job=_close_windows_job,
            )
        except ProcessTerminationError as error:
            logger.error("Worker process-tree termination failed: %s", error)
            return False
        return True

    def _terminate_attached_worker(self, worker: _Worker) -> bool:
        if worker.pid is None or worker.process_started_at is None:
            logger.error(
                "Refusing to terminate attached worker %s: recorded process identity is incomplete",
                worker.index,
            )
            return False
        try:
            terminate_attached_process_tree(
                worker.pid,
                expected_started_at=worker.process_started_at,
                expected_process_group_id=worker.process_group_id,
                expected_session_id=worker.session_id,
                grace=WORKER_GRACEFUL_EXIT_TIMEOUT,
            )
        except ProcessTerminationError as error:
            logger.error("Attached worker %s termination failed: %s", worker.index, error)
            return False
        return True

    def _worker_startup_failure_details(self, process: subprocess.Popen, process_logger: ProcessLogger) -> str:
        details: list[str] = []
        script_path = getattr(process, "_wetlands_script_path", None)
        if script_path:
            details.append(f"Startup script: {script_path}")

        try:
            output = process_logger.get_output()
        except Exception:
            output = []
        if output:
            tail = [str(line) for line in output[-20:]]
            details.append("Recent worker output:\n" + "\n".join(tail))

        if not details:
            return ""
        return "\n" + "\n".join(details)

    def _task_call_target(self, task: ExecutionTask[Any] | None) -> str | None:
        if task is None:
            return None
        payload = getattr(task, "_payload", {})
        return payload.get("_call_target") if isinstance(payload, dict) else None

    def _worker_info(self, worker: _Worker) -> WorkerInfo:
        return WorkerInfo(
            environment=self.name,
            index=worker.index,
            pid=worker.pid,
            port=worker.port,
            persistent=worker.persistent,
        )

    def _worker_returncode(self, worker: _Worker) -> int | None:
        if worker.process is None:
            return None
        worker.process.poll()
        return worker.process.returncode if isinstance(worker.process.returncode, int) else None

    def _worker_connection_failure(
        self,
        worker: _Worker,
        task: ExecutionTask[Any],
        message: str,
    ) -> ExecutionFailure:
        return ExecutionFailure.worker_connection(
            message,
            task_id=task.id,
            call_target=self._task_call_target(task),
            worker=self._worker_info(worker),
        )

    def _start_reader_thread(self, worker: _Worker) -> None:
        """Start the IPC reader thread for one worker."""
        reader = threading.Thread(
            target=self._worker_reader_loop,
            args=(worker,),
            daemon=True,
            name=f"wetlands-reader-{self.name}-{worker.index}",
        )
        worker.reader_thread = reader
        reader.start()

    def _worker_reader_loop(self, worker: _Worker) -> None:
        """Daemon thread that reads IPC messages from a worker and dispatches to the current Task."""
        conn = worker.connection
        while True:
            try:
                message = conn.recv()
                worker._last_activity = time.time()
            except (EOFError, OSError):
                if self._shutdown_event.is_set():
                    break
                task = worker._current_task
                if task is not None and not task.state.terminal:
                    if getattr(task, "_terminal_cleanup_in_progress", False):
                        break
                    pending_result = getattr(task, "_pending_result", _NO_RESULT)
                    offered_names = list(getattr(task, "_offered_names", ()))
                    returncode = self._worker_returncode(worker)
                    retired = self._remove_dead_worker(worker)
                    unlink_names(
                        offered_names,
                        ledger_root=self.environment_manager.root,
                    )
                    self._cleanup_task_inputs(task)
                    if pending_result is not _NO_RESULT:
                        task._set_completed(pending_result)
                    else:
                        if returncode is not None:
                            task._set_failed(
                                ExecutionFailure.worker_died(
                                    task_id=task.id,
                                    call_target=self._task_call_target(task),
                                    worker=self._worker_info(worker),
                                    returncode=returncode,
                                )
                            )
                        else:
                            task._set_failed(
                                self._worker_connection_failure(worker, task, "Worker connection closed unexpectedly")
                            )
                    worker._current_task = None
                    if retired:
                        self._try_replace_worker(worker.index)
                    break
                if worker.persistent and worker.alive():
                    with self._lock:
                        if worker in self._workers:
                            self._workers.remove(worker)
                    try:
                        if worker.connection and not worker.connection.closed:
                            worker.connection.close()
                    except OSError:
                        pass
                    break
                worker._current_task = None
                self._remove_dead_worker(worker)
                break

            task = worker._current_task
            if task is None:
                if self._shutdown_event.is_set():
                    break
                # launch() holds the runtime lock while commissioning the
                # complete pool, so this startup acknowledgement must remain
                # lock-free. The event provides the required synchronization.
                if self._is_valid_commissioned_message(worker, message):
                    worker._commissioned.set()
                    continue
                with self._lock:
                    if worker._retired:
                        break
                self._fail_worker_protocol(
                    worker,
                    None,
                    "Worker sent an execution message with no active task",
                )
                break
            if not self._worker_owns_task(worker, task):
                break

            try:
                action = validate_worker_task_message(message, expected_task_id=task.id)
                self._validate_worker_message_order(task, action)
            except ProtocolError as error:
                self._fail_worker_protocol(worker, task, str(error))
                break
            except Exception as error:
                self._fail_worker_protocol(
                    worker,
                    task,
                    f"Worker protocol validation failed unexpectedly ({type(error).__name__})",
                )
                break
            if not self._worker_owns_task(worker, task):
                break

            if action == ACTION_RESULT_OFFER:
                attachments: list[SharedMemoryLease] = []
                try:
                    result = decode_value(
                        message.get("result"),
                        copy_arrays=True,
                        path="result",
                        attachments=attachments,
                    )
                    offered_names = [lease.name for lease in attachments]
                    dispose_leases(attachments, unlink=False)
                    task._pending_result = result  # type: ignore[attr-defined]
                    task._offered_names = offered_names  # type: ignore[attr-defined]
                    worker.connection.send(protocol_message("release", task.id, names=offered_names))
                    task._result_release_sent = True  # type: ignore[attr-defined]
                except Exception as error:
                    offered_names = [lease.name for lease in attachments]
                    dispose_leases(attachments, unlink=False)
                    failure = ExecutionFailure.serialization(
                        f"Failed to decode worker result: {error}",
                        task_id=task.id,
                        call_target=self._task_call_target(task),
                        context="result",
                        worker=self._worker_info(worker),
                    )
                    retired = self._cleanup_failed_task_worker(
                        worker,
                        task,
                        offered_names=(set(offered_names) | set(getattr(task, "_offered_names", ()))),
                    )
                    task._set_failed(failure)
                    if retired:
                        self._try_replace_worker(worker.index)
                    break
            elif action == ACTION_RELEASED:
                result = getattr(task, "_pending_result", _NO_RESULT)
                offered_names = list(getattr(task, "_offered_names", ()))
                released_names = message.get("names")
                if set(released_names) != set(offered_names):
                    failure = self._worker_connection_failure(
                        worker,
                        task,
                        "Worker result-release acknowledgement was invalid",
                    )
                    retired = self._cleanup_failed_task_worker(
                        worker,
                        task,
                        offered_names=offered_names,
                    )
                    task._set_failed(failure)
                    if retired:
                        self._try_replace_worker(worker.index)
                    break
                self._cleanup_task_inputs(task)
                if task.cancellation_requested:
                    task._set_canceled()
                else:
                    task._set_completed(result)
                worker._finished_task_ids.add(task.id)
                with self._lock:
                    if worker._current_task is task:
                        worker._current_task = None
                self._dispatch_or_idle(worker)
            elif action in (ACTION_FAILURE, ACTION_CANCELED):
                if action == ACTION_FAILURE:
                    failure = ExecutionFailure.from_payload(message, call_target=self._task_call_target(task))
                    self._log_task_failure(failure)
                self._cleanup_task_inputs(task)
                task._on_message(message)
                worker._finished_task_ids.add(task.id)
                with self._lock:
                    if worker._current_task is task:
                        worker._current_task = None
                # Return worker to idle pool and dispatch next queued task
                self._dispatch_or_idle(worker)
            elif action == ACTION_UPDATE:
                task._on_message(message)
            elif action == ACTION_ACCEPTED:
                task._accepted = True  # type: ignore[attr-defined]
                logger.debug("Worker %s: task %s accepted", worker.index, task.id)
            elif action == ACTION_INPUT_RELEASED:
                task._inputs_released = True  # type: ignore[attr-defined]
                logger.debug("Worker %s: task %s %s", worker.index, task.id, action)
            elif action == ACTION_LOG:
                logger.log(message["level"], message["message"])

    def _is_valid_commissioned_message(self, worker: _Worker, message: Any) -> bool:
        return (
            self._persistent
            and not worker._commissioned.is_set()
            and isinstance(message, dict)
            and set(message) == {"action", "protocol_version", "pool_id"}
            and message["action"] == "commissioned"
            and message["protocol_version"] == EXECUTION_PROTOCOL_VERSION
            and isinstance(message["pool_id"], str)
            and message["pool_id"] == self._pool_id
        )

    def _worker_owns_task(self, worker: _Worker, task: ExecutionTask[Any]) -> bool:
        with self._lock:
            return (
                worker in self._workers
                and not worker._retired
                and worker._current_task is task
                and not getattr(task, "_terminal_cleanup_in_progress", False)
            )

    def _validate_worker_message_order(self, task: ExecutionTask[Any], action: str) -> None:
        accepted = bool(getattr(task, "_accepted", False))
        inputs_released = bool(getattr(task, "_inputs_released", False))
        result_offered = getattr(task, "_pending_result", _NO_RESULT) is not _NO_RESULT
        release_sent = bool(getattr(task, "_result_release_sent", False))

        if task.state.terminal:
            raise ProtocolError("Worker sent a message after the task became terminal")
        if action == ACTION_ACCEPTED:
            if accepted or inputs_released or result_offered:
                raise ProtocolError("Worker accepted the task out of order or more than once")
            return
        if action == ACTION_INPUT_RELEASED:
            if inputs_released or result_offered:
                raise ProtocolError("Worker released task inputs out of order or more than once")
            return
        if action in {ACTION_UPDATE, ACTION_LOG}:
            if not accepted or inputs_released or result_offered:
                raise ProtocolError(f"Worker sent {action} outside active task execution")
            return
        if action == ACTION_RESULT_OFFER:
            if not accepted or not inputs_released or result_offered:
                raise ProtocolError("Worker offered a result out of order or more than once")
            return
        if action == ACTION_RELEASED:
            if not result_offered or not release_sent:
                raise ProtocolError("Worker released an unknown result or acknowledged it out of order")
            return
        if action == ACTION_CANCELED:
            if not accepted or not inputs_released or result_offered:
                raise ProtocolError("Worker reported cancellation out of order")
            return
        if action == ACTION_FAILURE and result_offered:
            raise ProtocolError("Worker reported failure after offering a result")

    def _fail_worker_protocol(
        self,
        worker: _Worker,
        task: ExecutionTask[Any] | None,
        detail: str,
    ) -> None:
        logger.error("Worker %s protocol failure: %s", worker.index, detail)
        with self._lock:
            pool_was_usable = worker in self._workers
        if task is None:
            retired = self._remove_dead_worker(worker)
        else:
            failure = self._worker_connection_failure(
                worker,
                task,
                f"Worker protocol failure: {detail}",
            )
            offered_names = tuple(getattr(task, "_offered_names", ()))
            retired = self._cleanup_failed_task_worker(
                worker,
                task,
                offered_names=offered_names,
            )
            task._set_failed(failure)
        if retired and pool_was_usable:
            self._try_replace_worker(worker.index)

    _HEALTH_CHECK_INTERVAL = 5  # seconds

    def _health_monitor_loop(self) -> None:
        """Daemon thread that detects dead or hung workers."""
        while not self._shutdown_event.wait(timeout=self._HEALTH_CHECK_INTERVAL):
            with self._lock:
                workers = list(self._workers)

            for worker in workers:
                task = worker._current_task
                if task is None or task.state.terminal or getattr(task, "_terminal_cleanup_in_progress", False):
                    continue

                # Check 1: Is the process dead?
                if not worker.alive():
                    rc = self._worker_returncode(worker)
                    logger.error(f"Worker {worker.index} died (exit code {rc}) while running task {task.id}")
                    failure = ExecutionFailure.worker_died(
                        task_id=task.id,
                        call_target=self._task_call_target(task),
                        worker=self._worker_info(worker),
                        returncode=rc,
                    )
                    retired = self._cleanup_failed_task_worker(worker, task)
                    task._set_failed(failure)
                    if retired:
                        self._try_replace_worker(worker.index)
                    continue

                # Check 2: Has the worker timed out? (hung but alive)
                if self._worker_timeout is not None:
                    elapsed = time.time() - worker._last_activity
                    if elapsed > self._worker_timeout:
                        logger.error(
                            f"Worker {worker.index} timed out (no response for {elapsed:.0f}s) "
                            f"while running task {task.id}"
                        )
                        failure = ExecutionFailure.timeout_failure(
                            task_id=task.id,
                            call_target=self._task_call_target(task),
                            worker=self._worker_info(worker),
                            timeout=self._worker_timeout,
                            elapsed=elapsed,
                        )
                        retired = self._cleanup_failed_task_worker(worker, task)
                        task._set_failed(failure)
                        if retired:
                            self._try_replace_worker(worker.index)

    def _try_replace_worker(self, index: int) -> None:
        """Attempt to launch a replacement worker at the given index."""
        with self._lock:
            if self._shutdown_event.is_set() or any(worker.index == index for worker in self._workers):
                return
        try:
            with environment_lifecycle_gate(self.environment_manager, self.name):
                with self._lock:
                    if self._shutdown_event.is_set() or any(worker.index == index for worker in self._workers):
                        return
                worker = self._launch_worker(index, self._worker_env)
                with self._lock:
                    if self._shutdown_event.is_set() or any(existing.index == index for existing in self._workers):
                        if worker.process is None:
                            raise RuntimeError("A replacement worker has no launched process")
                        terminated = self._cleanup_failed_worker_launch(
                            worker.process,
                            worker.connection,
                        )  # type: ignore[arg-type]
                        if terminated:
                            try:
                                runtime_state.remove_worker(
                                    self.environment_manager.root,
                                    self.name,
                                    worker.index,
                                    self._pool_id,
                                )
                            except Exception as cleanup_error:
                                worker._retired = True
                                self._workers.append(worker)
                                raise WorkerStartError(
                                    self.name,
                                    "replacement became unnecessary and terminated, "
                                    "but its durable ownership record could not be removed",
                                    worker_index=index,
                                    phase="replace",
                                    cleanup_errors=(str(cleanup_error),),
                                ) from cleanup_error
                        else:
                            worker._retired = True
                            self._workers.append(worker)
                            raise WorkerStartError(
                                self.name,
                                "replacement became unnecessary but its process tree "
                                "could not be verified as terminated",
                                worker_index=index,
                                phase="replace",
                                cleanup_errors=("durable worker ownership record retained for retry",),
                            )
                        return
                    self._workers.append(worker)
                self._dispatch_or_idle(worker)
                logger.info(f"Replacement worker {index} launched successfully.")
        except EnvironmentGenerationChangedError as error:
            logger.error("Worker pool generation changed during replacement: %s", error)
            with self._lock:
                self._fatal_error = error
            self._exit()
        except WorkerStartError as error:
            logger.error("Failed to replace worker %s: %s", index, error)
            with self._lock:
                self._fatal_error = error
        except Exception as error:
            logger.error(f"Failed to launch replacement worker {index}: {error}")
            replacement_error = WorkerStartError(
                self.name,
                str(error),
                worker_index=index,
                phase="replace",
            )
            with self._lock:
                self._fatal_error = replacement_error

    def _remove_dead_worker(
        self,
        worker: _Worker,
        *,
        retirement_claimed: bool = False,
    ) -> bool:
        """Remove a dead worker from all pools and clean up its resources."""
        with self._lock:
            if retirement_claimed:
                if not worker._retired:
                    raise RuntimeError("Worker retirement was not claimed before cleanup")
            else:
                if worker._retired:
                    return False
                worker._retired = True

        try:
            if worker.connection and not worker.connection.closed:
                worker.connection.close()
        except OSError:
            pass

        terminated = True
        cleanup_failure: WorkerStartError | None = None
        if worker.process is not None:
            terminated = self._terminate_launched_worker(worker.process)
        elif worker.pid is not None:
            terminated = self._terminate_attached_worker(worker)

        if worker.process_logger is not None:
            worker.process_logger.join(timeout=PROCESS_LOGGER_JOIN_TIMEOUT)

        if worker.process and worker.process.stdout:
            try:
                worker.process.stdout.close()
            except OSError:
                pass
        if worker.process and worker.process.stderr:
            try:
                worker.process.stderr.close()
            except OSError:
                pass

        if terminated:
            try:
                runtime_state.remove_worker(
                    self.environment_manager.root,
                    self.name,
                    worker.index,
                    self._pool_id,
                )
            except Exception as error:
                cleanup_failure = WorkerStartError(
                    self.name,
                    "worker terminated but its durable ownership record could not be removed",
                    worker_index=worker.index,
                    phase="cleanup",
                    cleanup_errors=(str(error),),
                )
                with self._lock:
                    if worker not in self._workers:
                        self._workers.append(worker)
                    self._fatal_error = cleanup_failure
                terminated = False
            else:
                with self._lock:
                    if worker in self._workers:
                        self._workers.remove(worker)
        if terminated:
            try:
                reconcile_shared_memory_leases(self.environment_manager.root)
            except Exception as error:
                logger.error(
                    "Shared-memory lease reconciliation after worker retirement failed: %s",
                    error,
                )
        else:
            if cleanup_failure is None:
                cleanup_failure = WorkerStartError(
                    self.name,
                    "worker process tree could not be verified as terminated",
                    worker_index=worker.index,
                    phase="cleanup",
                    cleanup_errors=("durable worker ownership record retained for retry",),
                )
            with self._lock:
                if worker not in self._workers:
                    self._workers.append(worker)
                self._fatal_error = cleanup_failure

        logger.warning(
            "Worker %s cleanup %s. %s worker(s) remaining.",
            worker.index,
            "completed" if terminated else "is unverified",
            len(self._workers),
        )
        return terminated

    def _dispatch_or_idle(self, worker: _Worker) -> None:
        """Try to dispatch the next queued task to this worker, or return it to idle pool."""
        while True:
            with self._lock:
                if (
                    self._shutdown_event.is_set()
                    or worker not in self._workers
                    or worker._retired
                    or worker._current_task is not None
                ):
                    return
            try:
                task = self._task_queue.get_nowait()
            except queue.Empty:
                with self._lock:
                    if (
                        not self._shutdown_event.is_set()
                        and worker in self._workers
                        and not worker._retired
                        and worker._current_task is None
                    ):
                        self._idle_workers.put(worker)
                return
            if task.state.terminal:
                self._cleanup_task_inputs(task)
                continue
            outcome = self._dispatch_to_worker(worker, task)
            if outcome is _DispatchOutcome.DISPATCHED:
                return
            if outcome is _DispatchOutcome.WORKER_UNAVAILABLE:
                self._queue_task_or_fail_shutdown(task)
                return

    def _dispatch_to_worker(
        self,
        worker: _Worker,
        task: ExecutionTask[Any],
    ) -> _DispatchOutcome:
        """Send a task's payload to a worker for execution."""
        payload = task._payload  # type: ignore[attr-defined]
        payload["task_id"] = task.id
        if worker.pid is None or worker.process_started_at is None:
            failure = ExecutionFailure.environment(
                "Worker process identity is unavailable for durable result transport",
                task_id=task.id,
                call_target=self._task_call_target(task),
            )
            self._cleanup_task_inputs(task)
            task._set_failed(failure)
            return _DispatchOutcome.TASK_FAILED
        payload["output_lease_context"] = self._lease_context(
            task.id,
            direction="output",
            creator_pid=worker.pid,
            creator_started_at=worker.process_started_at,
        )
        raw_required = payload.get("codecs")
        if worker.capabilities is not None and isinstance(raw_required, list):
            required = {(item.get("id"), item.get("version")) for item in raw_required if isinstance(item, dict)}
            available = {(codec.id, codec.version) for codec in worker.capabilities.codecs}
            missing = required - available
            if missing:
                formatted = ", ".join(f"{codec}@{version}" for codec, version in sorted(missing))
                failure = ExecutionFailure.environment(
                    f"Worker environment is missing required task codecs: {formatted}",
                    task_id=task.id,
                    call_target=self._task_call_target(task),
                )
                self._cleanup_task_inputs(task)
                task._set_failed(failure)
                return _DispatchOutcome.TASK_FAILED
        with self._lock:
            shutting_down = self._shutdown_event.is_set()
            if shutting_down:
                unavailable = False
            else:
                unavailable = worker not in self._workers or worker._retired or worker._current_task is not None
            if unavailable:
                return _DispatchOutcome.WORKER_UNAVAILABLE
            if not shutting_down:
                worker._current_task = task
                worker._last_activity = time.time()
        if shutting_down:
            self._fail_task_for_shutdown(task)
            return _DispatchOutcome.SHUTTING_DOWN
        task._set_running()

        if worker.process_logger:
            call_target = payload.get("_call_target", MODULE_EXECUTOR_FILE)
            worker.process_logger.update_log_context({"call_target": call_target})

        try:
            worker.connection.send(payload)
        except (OSError, BrokenPipeError) as e:
            failure = self._worker_connection_failure(
                worker,
                task,
                f"Failed to send to worker {worker.index}: {e}",
            )
            retired = self._cleanup_failed_task_worker(worker, task)
            task._set_failed(failure)
            if retired:
                self._try_replace_worker(worker.index)
            return _DispatchOutcome.TASK_FAILED
        except Exception as e:
            failure = ExecutionFailure.serialization(
                f"Failed to serialize task payload for worker {worker.index}: {e}",
                task_id=task.id,
                call_target=self._task_call_target(task),
                context="payload",
                worker=self._worker_info(worker),
            )
            retired = self._cleanup_failed_task_worker(worker, task)
            task._set_failed(failure)
            if retired:
                self._try_replace_worker(worker.index)
            return _DispatchOutcome.TASK_FAILED
        return _DispatchOutcome.DISPATCHED

    def _submit_task(
        self,
        task: ExecutionTask[Any],
        start: bool,
    ) -> ExecutionTask[Any]:
        """Wire up a task's start/cancel functions and optionally start it."""

        def _start() -> None:
            while True:
                if self._shutdown_event.is_set():
                    self._fail_task_for_shutdown(task)
                    return
                try:
                    worker = self._idle_workers.get_nowait()
                except queue.Empty:
                    self._queue_task_or_fail_shutdown(task)
                    return
                # Skip dead workers that are still in the idle queue
                if worker not in self._workers or worker._retired:
                    continue
                outcome = self._dispatch_to_worker(worker, task)
                if outcome is _DispatchOutcome.WORKER_UNAVAILABLE:
                    self._queue_task_or_fail_shutdown(task)
                elif outcome is _DispatchOutcome.TASK_FAILED:
                    if worker in self._workers and not worker._retired:
                        self._idle_workers.put(worker)
                return

        def _cancel() -> None:
            # Find which worker has this task and send cancel
            for w in self._workers:
                if w._current_task is task:
                    try:
                        w.connection.send(protocol_message("cancel", task.id))
                    except (OSError, BrokenPipeError):
                        retired = self._cleanup_failed_task_worker(w, task)
                        task._set_canceled()
                        if retired:
                            self._try_replace_worker(w.index)
                        return
                    threading.Thread(
                        target=self._force_cancel_after_grace,
                        args=(w, task),
                        daemon=True,
                        name=f"wetlands-cancel-{task.id[:8]}",
                    ).start()
                    return
            self._cleanup_task_inputs(task)
            task._set_canceled()

        task._set_start_fn(_start)
        task._set_cancel_fn(_cancel)
        if start:
            task._start()
        return task

    def _queue_task_or_fail_shutdown(self, task: ExecutionTask[Any]) -> bool:
        with self._lock:
            if self._shutdown_event.is_set():
                queued = False
            else:
                self._task_queue.put(task)
                queued = True
        if not queued:
            self._fail_task_for_shutdown(task)
        return queued

    def _fail_task_for_shutdown(self, task: ExecutionTask[Any]) -> None:
        failure = ExecutionFailure.environment(
            "Environment is shutting down",
            task_id=task.id,
            call_target=self._task_call_target(task),
        )
        self._cleanup_task_inputs(task)
        task._set_failed(failure)

    def _force_cancel_after_grace(self, worker: _Worker, task: ExecutionTask[Any]) -> None:
        grace = float(self.environment_manager.termination_grace)
        if task._done_event.wait(timeout=grace):  # type: ignore[attr-defined]
            return
        with self._lock:
            if worker._current_task is not task or worker._retired:
                return
        retired = self._cleanup_failed_task_worker(worker, task)
        task._set_canceled()
        if retired:
            self._try_replace_worker(worker.index)

    def _cleanup_failed_task_worker(
        self,
        worker: _Worker,
        task: ExecutionTask[Any],
        *,
        offered_names: Iterable[str] = (),
    ) -> bool:
        """Finish transport and worker cleanup before publishing task failure."""
        with self._lock:
            task._terminal_cleanup_in_progress = True  # type: ignore[attr-defined]
            if worker._current_task is task:
                worker._current_task = None
            retirement_claimed = not worker._retired
            worker._retired = True
        names = list(offered_names)
        if names:
            unlink_names(
                names,
                ledger_root=self.environment_manager.root,
            )
        self._cleanup_task_inputs(task)
        if not retirement_claimed:
            return False
        return self._remove_dead_worker(worker, retirement_claimed=True)

    def _cleanup_task_inputs(self, task: ExecutionTask[Any]) -> None:
        with task._lock:  # type: ignore[attr-defined]
            leases = getattr(task, "_input_leases", None)
            if leases is None:
                return
            task._input_leases = None  # type: ignore[attr-defined]
        dispose_leases(leases, unlink=True)

    def _lease_context(
        self,
        task_id: str,
        *,
        direction: str,
        creator_pid: int,
        creator_started_at: float,
    ) -> dict[str, Any]:
        generation_id = self._expected_generation_id
        if generation_id is None:
            generation_id = str(self._ready_identity()["generation_id"])
        if self._pool_id is None:
            raise RuntimeError("Worker pool has no transport identity")
        return {
            "root": str(self.environment_manager.root),
            "creator_pid": creator_pid,
            "creator_started_at": creator_started_at,
            "environment_name": self.name,
            "generation_id": generation_id,
            "pool_id": self._pool_id,
            "task_id": task_id,
            "direction": direction,
        }

    def submit_import(
        self,
        target: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        context_keyword: str | None = None,
    ) -> ExecutionTask[Any]:
        descriptor = import_target(target)
        return self._submit_encoded(
            descriptor,
            target,
            args,
            kwargs or {},
            context_keyword,
        )

    def submit_path(
        self,
        path: str | Path,
        qualname: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        cache: bool = True,
        context_keyword: str | None = None,
    ) -> ExecutionTask[Any]:
        descriptor = path_target(path, qualname, cache=cache)
        canonical = Path(descriptor["path"])
        return self._submit_encoded(
            descriptor,
            f"{canonical}:{qualname}",
            args,
            kwargs or {},
            context_keyword,
        )

    def _submit_encoded(
        self,
        target: dict[str, Any],
        call_target: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        context_keyword: str | None,
    ) -> ExecutionTask[Any]:
        self._raise_if_failed()
        task: ExecutionTask[Any] = ExecutionTask()
        host_identity = capture_process_identity(os.getpid())
        lease_context = self._lease_context(
            task.id,
            direction="input",
            creator_pid=host_identity.pid,
            creator_started_at=host_identity.started_at,
        )
        argument_leases: list[SharedMemoryLease] = []
        keyword_leases: list[SharedMemoryLease] = []
        try:
            encoded_args, argument_leases = encode_value(
                tuple(args),
                path="args",
                lease_context=lease_context,
            )
            encoded_kwargs, keyword_leases = encode_value(
                dict(kwargs),
                path="kwargs",
                lease_context=lease_context,
            )
        except BaseException:
            dispose_leases(argument_leases, unlink=True)
            dispose_leases(keyword_leases, unlink=True)
            raise
        task._input_leases = [*argument_leases, *keyword_leases]  # type: ignore[attr-defined]
        task._payload = execution_envelope(  # type: ignore[attr-defined]
            task_id=task.id,
            target=target,
            args=encoded_args,
            kwargs=encoded_kwargs,
            codecs=descriptor_codecs(encoded_args, encoded_kwargs),
            context_keyword=context_keyword,
        )
        task._payload["_call_target"] = call_target  # type: ignore[attr-defined]
        return self._submit_task(task, True)

    def attach_workers(
        self,
        worker_entries: Iterable[dict[str, Any]],
        authkey: bytes,
        timeout: float = ATTACH_CONNECT_TIMEOUT,
    ) -> None:
        """Attach this environment to existing persistent worker processes."""
        self._persistent = True
        self._authkey = authkey
        self._shutdown_event.clear()
        reconcile_shared_memory_leases(self.environment_manager.root)
        entries = tuple(worker_entries)
        pool_ids = {entry.get("pool_id") for entry in entries}
        if len(pool_ids) > 1:
            raise WorkerStartError(
                self.name,
                "registered workers do not belong to one worker pool",
                phase="attach",
            )
        if pool_ids:
            pool_id = next(iter(pool_ids))
            if not isinstance(pool_id, str) or not pool_id:
                raise WorkerStartError(
                    self.name,
                    "registered workers have an invalid pool ID",
                    phase="attach",
                )
            self._pool_id = pool_id
        self._controller_id = uuid.uuid4().hex
        try:
            runtime_state.claim_controller(
                self.environment_manager.root,
                self.name,
                self._controller_id,
            )
        except BaseException:
            self._controller_id = None
            raise
        attached: list[_Worker] = []
        try:
            if not entries:
                raise WorkerStartError(
                    self.name,
                    "no persistent workers are registered",
                    phase="attach",
                )
            published = runtime_state.live_workers_for_env(
                self.environment_manager.root,
                self.name,
            )
            entry_identities = {
                (
                    entry.get("pool_id"),
                    entry.get("worker_index"),
                    entry.get("pid"),
                    entry.get("port"),
                )
                for entry in entries
            }
            published_identities = {
                (
                    entry.get("pool_id"),
                    entry.get("worker_index"),
                    entry.get("pid"),
                    entry.get("port"),
                )
                for entry in published
            }
            if (
                not published
                or len(entry_identities) != len(entries)
                or len(published_identities) != len(published)
                or entry_identities != published_identities
            ):
                raise WorkerStartError(
                    self.name,
                    "registered workers are not one complete commissioned pool",
                    phase="attach",
                )
            for entry in entries:
                attached.append(self._attach_worker(entry, authkey, timeout=timeout))
        except BaseException as error:
            cleanup_errors: list[str] = []
            for worker in attached:
                try:
                    if not worker.connection.closed:
                        worker.connection.send({"action": "detach", "protocol_version": EXECUTION_PROTOCOL_VERSION})
                        worker.connection.close()
                except Exception as cleanup_error:
                    cleanup_errors.append(str(cleanup_error))
            try:
                runtime_state.release_controller(
                    self.environment_manager.root,
                    self.name,
                    self._controller_id,
                )
            except Exception as cleanup_error:
                cleanup_errors.append(str(cleanup_error))
            self._controller_id = None
            if isinstance(
                error,
                (EnvironmentGenerationChangedError, WorkerStartError),
            ) or not isinstance(error, Exception):
                raise
            raise WorkerStartError(
                self.name,
                str(error),
                phase="attach",
                cleanup_errors=tuple(cleanup_errors),
            ) from error

        try:
            self._workers.extend(attached)
            for worker in attached:
                self._start_reader_thread(worker)
                self._idle_workers.put(worker)
            if not self._workers:
                raise WorkerStartError(
                    self.name,
                    "no live authenticated persistent workers were found",
                    phase="attach",
                )
        except BaseException:
            self._workers.clear()
            self._drain_idle_workers()
            for worker in attached:
                try:
                    if not worker.connection.closed:
                        worker.connection.send({"action": "detach", "protocol_version": EXECUTION_PROTOCOL_VERSION})
                        worker.connection.close()
                except Exception:
                    pass
            if self._controller_id is not None:
                runtime_state.release_controller(
                    self.environment_manager.root,
                    self.name,
                    self._controller_id,
                )
                self._controller_id = None
            raise

        self._pool_commissioned = True

        self._health_thread = threading.Thread(
            target=self._health_monitor_loop,
            daemon=True,
            name=f"wetlands-health-{self.name}",
        )
        self._health_thread.start()

    def _attach_worker(
        self,
        entry: dict[str, Any],
        authkey: bytes,
        timeout: float = ATTACH_CONNECT_TIMEOUT,
    ) -> _Worker:
        ready = self._ready_identity()
        expected_identity = {
            "pid": int(entry["pid"]),
            "environment_path": str(self._project_path()),
            "generation_id": str(ready["generation_id"]),
            "recipe_hash": str(ready["recipe_hash"]),
            "pool_id": entry.get("pool_id"),
            "worker_index": int(entry["worker_index"]),
        }
        connection, capabilities = self._connect_worker(
            int(entry["port"]),
            authkey,
            timeout=timeout,
            expected_identity=expected_identity,
        )
        worker = _Worker(
            int(entry["worker_index"]),
            None,
            int(entry["port"]),
            connection,
            None,
            pid=int(entry["pid"]),
            persistent=True,
            process_started_at=float(entry["process_started_at"]),
            process_group_id=(int(entry["process_group_id"]) if entry.get("process_group_id") is not None else None),
            session_id=int(entry["session_id"]) if entry.get("session_id") is not None else None,
            capabilities=capabilities,
        )
        return worker

    def _connect_worker(
        self,
        port: int,
        authkey: bytes,
        timeout: float | None = None,
        *,
        expected_identity: dict[str, object],
    ) -> tuple[Connection, Any]:
        if timeout is None:
            connection = Client(("127.0.0.1", port), authkey=authkey)
            try:
                capabilities = self._receive_worker_hello(
                    connection,
                    timeout=ATTACH_CONNECT_TIMEOUT,
                    expected_identity=expected_identity,
                )
            except BaseException:
                connection.close()
                raise
            return connection, capabilities

        # Client() has no timeout parameter and can block during both socket
        # connect and the multiprocessing auth handshake. Persistent attach uses
        # this bounded equivalent so a busy or stale worker can produce an
        # actionable error instead of hanging the caller.
        address = ("127.0.0.1", port)
        sock = socket.socket(socket.AF_INET)
        try:
            sock.settimeout(timeout)
            sock.connect(address)
            sock.setblocking(True)
            connection = Connection(sock.detach())
        except TimeoutError as e:
            sock.close()
            raise _AttachTimeout(f"Timed out connecting to worker on port {port}.") from e
        except Exception:
            sock.close()
            raise

        try:
            self._answer_challenge_with_timeout(connection, authkey, timeout)
            self._deliver_challenge_with_timeout(connection, authkey, timeout)
        except Exception:
            connection.close()
            raise
        try:
            capabilities = self._receive_worker_hello(
                connection,
                timeout=timeout,
                expected_identity=expected_identity,
            )
        except BaseException:
            connection.close()
            raise
        return connection, capabilities

    def _receive_worker_hello(
        self,
        connection: Connection,
        *,
        timeout: float,
        expected_identity: dict[str, object],
    ) -> Any:
        if not mp_connection.wait([connection], timeout):
            raise _AttachTimeout("Timed out waiting for worker capability handshake.")
        try:
            payload = connection.recv()
        except (EOFError, OSError) as error:
            raise _AttachTimeout("Worker connection closed before capability handshake.") from error
        if not isinstance(payload, dict) or payload.get("action") != "hello":
            raise RuntimeError("Worker did not send the required capability handshake")
        return validate_worker_capabilities(
            payload,
            required_codecs=REQUIRED_WORKER_CODECS,
            expected_identity=expected_identity,
        )

    def _recv_bytes_with_timeout(self, connection: Connection, timeout: float, maxlength: int) -> bytes:
        if not mp_connection.wait([connection], timeout):
            raise _AttachTimeout("Timed out waiting for worker authentication.")
        return connection.recv_bytes(maxlength)

    def _answer_challenge_with_timeout(self, connection: Connection, authkey: bytes, timeout: float) -> None:
        if not isinstance(authkey, bytes):
            raise TypeError("authkey should be a byte string")
        challenge = _mp_connection_attr("_CHALLENGE", "CHALLENGE")
        welcome = _mp_connection_attr("_WELCOME", "WELCOME")
        message = self._recv_bytes_with_timeout(connection, timeout, 256)
        if not message.startswith(challenge):
            raise AuthenticationError(f"Protocol error, expected challenge: {message=}")
        message = message[len(challenge) :]
        md5only_message_length = getattr(mp_connection, "_MD5ONLY_MESSAGE_LENGTH", None)
        if md5only_message_length is not None and len(message) < md5only_message_length:
            raise AuthenticationError(f"challenge too short: {len(message)} bytes")
        if hasattr(mp_connection, "_create_response"):
            create_response = getattr(mp_connection, "_create_response")
            digest = create_response(authkey, message)
        else:
            digest = hmac.new(authkey, message, "md5").digest()
        connection.send_bytes(digest)
        response = self._recv_bytes_with_timeout(connection, timeout, 256)
        if response != welcome:
            raise AuthenticationError("digest sent was rejected")

    def _deliver_challenge_with_timeout(
        self,
        connection: Connection,
        authkey: bytes,
        timeout: float,
        digest_name: str = "sha256",
    ) -> None:
        if not isinstance(authkey, bytes):
            raise TypeError("authkey should be a byte string")
        challenge = _mp_connection_attr("_CHALLENGE", "CHALLENGE")
        welcome = _mp_connection_attr("_WELCOME", "WELCOME")
        failure = _mp_connection_attr("_FAILURE", "FAILURE")
        message_length = _mp_connection_attr("MESSAGE_LENGTH")
        if hasattr(mp_connection, "_verify_challenge"):
            message = os.urandom(message_length)
            message = b"{%s}%s" % (digest_name.encode("ascii"), message)
        else:
            message = os.urandom(message_length)
        connection.send_bytes(challenge + message)
        response = self._recv_bytes_with_timeout(connection, timeout, 256)
        if hasattr(mp_connection, "_verify_challenge"):
            try:
                verify_challenge = getattr(mp_connection, "_verify_challenge")
                verify_challenge(authkey, message, response)
            except AuthenticationError:
                connection.send_bytes(failure)
                raise
        elif response != hmac.new(authkey, message, "md5").digest():
            connection.send_bytes(failure)
            raise AuthenticationError("digest received was wrong")
        connection.send_bytes(welcome)

    def _log_task_failure(self, failure: ExecutionFailure) -> None:
        level = 30 if failure.category.value == "remote_exception" else 40
        logger.log(level, failure.summary())

    def _gracefully_stop_process(
        self,
        process: subprocess.Popen | None,
        process_logger: ProcessLogger | None,
    ) -> bool:
        if process is None:
            return True

        try:
            if process.poll() is None:
                process.wait(timeout=WORKER_GRACEFUL_EXIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass

        terminated = self._terminate_launched_worker(process)

        if process_logger is not None:
            process_logger.join(timeout=PROCESS_LOGGER_JOIN_TIMEOUT)

        if process.stdout:
            try:
                process.stdout.close()
            except OSError:
                pass
        if process.stderr:
            try:
                process.stderr.close()
            except OSError:
                pass
        return terminated

    @synchronized
    def launched(self) -> bool:
        """Return whether this runtime currently controls at least one live worker."""
        return any(worker.alive() and not worker.connection.closed for worker in self._workers)

    @property
    def worker_count(self) -> int:
        """Number of currently active workers."""
        with self._lock:
            return len(self._workers)

    @synchronized
    def _exit(self) -> None:
        """Close connections and kill all worker processes."""
        # Stop health monitor
        self._shutdown_event.set()

        all_terminated = True
        survivors: list[_Worker] = []
        cleanup_errors: list[str] = []
        if self._workers:
            for worker in list(self._workers):
                active = worker._current_task is not None and not worker._current_task.state.terminal
                active_task = worker._current_task if active else None
                active_failure = None
                if active:
                    assert active_task is not None
                    active_task._terminal_cleanup_in_progress = True  # type: ignore[attr-defined]
                    active_failure = ExecutionFailure.environment(
                        "Environment is shutting down",
                        task_id=active_task.id,
                        call_target=self._task_call_target(active_task),
                    )
                if active:
                    try:
                        worker.connection.close()
                    except OSError as error:
                        cleanup_errors.append(f"worker {worker.index} connection close failed: {error}")
                    if worker.process is not None:
                        terminated = self._terminate_launched_worker(worker.process)
                        if worker.process_logger is not None:
                            worker.process_logger.join(timeout=PROCESS_LOGGER_JOIN_TIMEOUT)
                    elif worker.pid is not None:
                        terminated = self._terminate_attached_worker(worker)
                    else:
                        terminated = True
                else:
                    try:
                        worker.connection.send({"action": "exit", "protocol_version": EXECUTION_PROTOCOL_VERSION})
                    except OSError:
                        pass
                    try:
                        worker.connection.close()
                    except OSError as error:
                        cleanup_errors.append(f"worker {worker.index} connection close failed: {error}")
                    if worker.process is not None:
                        terminated = self._gracefully_stop_process(
                            worker.process,
                            worker.process_logger,
                        )
                    elif worker.pid is not None:
                        terminated = self._terminate_attached_worker(worker)
                    else:
                        terminated = True
                if active_task is not None:
                    self._cleanup_task_inputs(active_task)
                    worker._current_task = None
                    assert active_failure is not None
                    active_task._set_failed(active_failure)
                all_terminated = all_terminated and terminated
                if terminated:
                    try:
                        runtime_state.remove_worker(
                            self.environment_manager.root,
                            self.name,
                            worker.index,
                            self._pool_id,
                        )
                    except Exception as error:
                        survivors.append(worker)
                        cleanup_errors.append(
                            f"worker {worker.index} terminated but its durable "
                            f"ownership record could not be removed: {error}"
                        )
                else:
                    survivors.append(worker)
                    cleanup_errors.append(
                        f"worker {worker.index} process-tree termination "
                        "could not be verified; durable ownership record retained"
                    )
            self._workers[:] = survivors
            self._drain_idle_workers()

        try:
            reconcile_shared_memory_leases(self.environment_manager.root)
        except Exception as error:
            cleanup_errors.append(f"shared-memory lease reconciliation failed: {error}")

        if self._persistent and self._pool_id is not None and all_terminated:
            try:
                discarded = runtime_state.discard_persistent_pool(
                    self.environment_manager.root,
                    env_name=self.name,
                    pool_id=self._pool_id,
                )
            except Exception as error:
                cleanup_errors.append(f"persistent pool journal cleanup failed: {error}")
            else:
                if not discarded:
                    cleanup_errors.append("persistent pool journal retained because worker records remain")

        while True:
            try:
                task = self._task_queue.get_nowait()
                failure = ExecutionFailure.environment(
                    "Environment is shutting down",
                    task_id=task.id,
                )
                self._cleanup_task_inputs(task)
                task._set_failed(failure)
            except queue.Empty:
                break

        if self._controller_id is not None and not survivors:
            try:
                runtime_state.release_controller(
                    self.environment_manager.root,
                    self.name,
                    self._controller_id,
                )
            except Exception as error:
                cleanup_errors.append(f"controller release failed: {error}")
            else:
                self._controller_id = None

        if cleanup_errors:
            close_error = WorkerStartError(
                self.name,
                "worker pool cleanup did not complete",
                phase="close",
                cleanup_errors=tuple(cleanup_errors),
            )
            with self._lock:
                self._fatal_error = close_error
            raise close_error

    @synchronized
    def detach(self) -> None:
        """Close local connections without stopping persistent worker processes."""
        if not self._persistent:
            raise RuntimeError("Only persistent worker pools can be detached")
        if any(worker._current_task is not None for worker in self._workers) or not self._task_queue.empty():
            raise RuntimeError("Cannot detach a worker pool with running or queued tasks")
        self._shutdown_event.set()
        for worker in list(self._workers):
            try:
                if worker.connection and not worker.connection.closed:
                    if worker.persistent:
                        worker.connection.send({"action": "detach", "protocol_version": EXECUTION_PROTOCOL_VERSION})
                    worker.connection.close()
            except OSError:
                pass
            if worker._current_task is not None and not worker._current_task.state.terminal:
                task = worker._current_task
                failure = ExecutionFailure.environment(
                    "Environment is detaching",
                    task_id=task.id,
                    call_target=self._task_call_target(task),
                )
                self._cleanup_task_inputs(task)
                task._set_failed(failure)
            worker._current_task = None
        self._workers.clear()
        self._drain_idle_workers()
        while True:
            try:
                task = self._task_queue.get_nowait()
                failure = ExecutionFailure.environment(
                    "Environment is detaching",
                    task_id=task.id,
                )
                self._cleanup_task_inputs(task)
                task._set_failed(failure)
            except queue.Empty:
                break
        if self._controller_id is not None:
            runtime_state.release_controller(
                self.environment_manager.root,
                self.name,
                self._controller_id,
            )
            self._controller_id = None
