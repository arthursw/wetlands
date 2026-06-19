from __future__ import annotations

import json
import logging
import subprocess
import time
import socket
import os
import secrets
from pathlib import Path
from multiprocessing import connection as mp_connection
from multiprocessing.context import AuthenticationError
from multiprocessing.connection import Client, Connection
import functools
import hmac
import threading
import queue
from collections.abc import Callable, Iterable, Iterator
from typing import Any, TYPE_CHECKING, Union
from send2trash import send2trash

from wetlands.logger import logger, LOG_SOURCE_EXECUTION
from wetlands._internal.command_generator import Commands
from wetlands._internal.dependency_manager import Dependencies
from wetlands.environment import Environment
from wetlands._internal.exceptions import ExecutionException
from wetlands._internal.diagnostics import TaskFailure, TaskFailureCategory, WorkerInfo
from wetlands._internal.command_executor import CommandExecutor
from wetlands._internal.process_logger import ProcessLogger
from wetlands._internal.shell import shell_quote
from wetlands._internal import runtime_state
from wetlands.task import Task, TaskStatus

try:
    from wetlands.ndarray import register_ndarray_pickle

    register_ndarray_pickle()
except ImportError:
    # Do not support ndarray if numpy is not installed
    pass

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
WORKER_GRACEFUL_EXIT_TIMEOUT = 2.0
PROCESS_LOGGER_JOIN_TIMEOUT = 5.0


class _AttachTimeout(TimeoutError):
    """Raised when a live worker does not complete attach in time."""


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

    debug_port = payload.get("debug_port")
    if debug_port is not None and (not isinstance(debug_port, int) or not (0 < debug_port <= 65535)):
        raise ValueError("startup payload had an invalid debug port")

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
        "_current_task",
        "_last_activity",
        "_finished_task_ids",
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
    ) -> None:
        self.index = index
        self.process = process
        self.port = port
        self.connection = connection
        self.process_logger = process_logger
        self.pid = pid if pid is not None else (process.pid if process is not None else None)
        self.persistent = persistent
        self.reader_thread: threading.Thread | None = None
        self._current_task: Task[Any] | None = None
        self._last_activity: float = 0.0
        self._finished_task_ids: set[str] = set()

    def alive(self) -> bool:
        if self.process is not None:
            return self.process.poll() is None
        if self.pid is not None:
            return runtime_state.pid_exists(self.pid)
        return False


class ExternalEnvironment(Environment):
    port: int | None = None
    process: subprocess.Popen | None = None
    connection: Connection | None = None

    def __init__(self, name: str, path: Path | None, environment_manager: "EnvironmentManager") -> None:
        super().__init__(name, path, environment_manager)
        self._lock = threading.RLock()
        self._process_logger: ProcessLogger | None = None
        # Worker pool state
        self._workers: list[_Worker] = []
        self._idle_workers: queue.Queue[_Worker] = queue.Queue()
        self._task_queue: queue.Queue[Task[Any]] = queue.Queue()
        self._additional_activate_commands: Commands = {}
        self._worker_env: Callable[[int], dict[str, str]] | None = None
        self._worker_timeout: float | None = None
        self._persistent: bool = False
        self._authkey: bytes | None = None
        self._shutdown_event = threading.Event()

    @synchronized
    def launch(
        self,
        additional_activate_commands: Commands = {},
        *,
        max_workers: int = 1,
        worker_env: Callable[[int], dict[str, str]] | None = None,
        worker_timeout: float | None = None,
        persistent: bool = False,
    ) -> None:
        """Launches module executor process(es) in the environment.

        Args:
            additional_activate_commands: Platform-specific activation commands.
            max_workers: Number of worker processes to start.
                All workers share the same conda environment (no duplication).
            worker_env: Optional callable receiving worker index (0-based),
                returning extra environment variables for that worker.
            worker_timeout: Optional inactivity timeout in seconds. If set and a
                worker sends no IPC message within this duration, it is treated as
                hung: the active task is failed, the worker is killed and replaced.
            persistent: If True, workers are recorded in the root registry and can
                later be reconnected with EnvironmentManager.attach().
        """
        if self.launched():
            return

        self._additional_activate_commands = additional_activate_commands
        self._worker_env = worker_env
        self._worker_timeout = worker_timeout
        self._persistent = persistent
        self._authkey = runtime_state.load_or_create_root_authkey(self.environment_manager.wetlands_instance_path)
        self._shutdown_event.clear()
        if self._persistent:
            live_workers = runtime_state.live_workers_for_env(self.environment_manager.wetlands_instance_path, self.name)
            if live_workers:
                raise Exception(
                    f"Live persistent workers already exist for environment '{self.name}'. "
                    "Use EnvironmentManager.attach() or exit the existing workers before launching again."
                )

        # Ensure debugpy is installed if in debug mode
        if self.environment_manager.debug:
            self._ensure_debugpy_installed()

        for i in range(max_workers):
            worker = self._launch_worker(i, additional_activate_commands, worker_env)
            self._workers.append(worker)
            self._idle_workers.put(worker)

        # For backward compat, expose first worker's port/process/connection
        if self._workers:
            first = self._workers[0]
            self.port = first.port
            self.process = first.process
            self.connection = first.connection
            self._process_logger = first.process_logger

        # Start health monitor thread
        self._health_thread = threading.Thread(
            target=self._health_monitor_loop,
            daemon=True,
            name=f"wetlands-health-{self.name}",
        )
        self._health_thread.start()

    def _launch_worker(
        self,
        index: int,
        additional_activate_commands: Commands,
        worker_env: Callable[[int], dict[str, str]] | None,
    ) -> _Worker:
        """Launch a single module_executor process and return a _Worker."""
        module_executor_path = Path(__file__).parent.resolve() / MODULE_EXECUTOR_FILE

        debug_args = " --debug_port 0" if self.environment_manager.debug else ""
        persistent_args = " --persistent" if self._persistent else ""
        startup_socket = _open_startup_socket()
        startup_host, startup_port = startup_socket.getsockname()
        startup_token = secrets.token_urlsafe(32)
        startup_args = " ".join(
            [
                " --startup_host",
                shell_quote(str(startup_host)),
                "--startup_port",
                str(startup_port),
            ]
        )
        wetlands_instance_path = self.environment_manager.wetlands_instance_path.resolve()
        commands = [
            " ".join(
                [
                    "python",
                    "-u",
                    shell_quote(module_executor_path),
                    shell_quote(self.name),
                    "--wetlands_instance_path",
                    shell_quote(wetlands_instance_path),
                ]
            )
            + debug_args
            + persistent_args
            + startup_args
        ]

        log_context = {"log_source": LOG_SOURCE_EXECUTION, "env_name": self.name, "call_target": MODULE_EXECUTOR_FILE}
        if len(self._workers) > 0 or index > 0:
            log_context["worker_index"] = str(index)

        # Build popen_kwargs with worker-specific env vars
        popen_kwargs: dict[str, Any] = {}
        env = os.environ.copy()
        if worker_env is not None:
            env.update(worker_env(index))
        env[STARTUP_TOKEN_ENV] = startup_token
        popen_kwargs["env"] = env

        try:
            process = self.execute_commands(
                commands, additional_activate_commands, log_context=log_context, popen_kwargs=popen_kwargs
            )

            process_logger = self.environment_manager.get_process_logger(process)
            if process_logger is None:
                raise Exception(f"Failed to retrieve ProcessLogger for worker {index}")

            try:
                startup_payload = _wait_for_startup_payload(startup_socket, startup_token, process)
            except Exception as e:
                raise Exception(
                    f"Could not receive startup information for worker {index}: {e}."
                    f"{self._worker_startup_failure_details(process, process_logger)}"
                ) from e
        finally:
            startup_socket.close()

        if process.poll() is not None:
            raise Exception(
                f"Worker {index} exited with return code {process.returncode} before accepting connections."
                f"{self._worker_startup_failure_details(process, process_logger)}"
            )
        port = int(startup_payload["port"])
        if port == 0:
            raise Exception(
                f"Could not find the server port for worker {index}."
                f"{self._worker_startup_failure_details(process, process_logger)}"
            )
        debug_port = startup_payload.get("debug_port")
        if self.environment_manager.debug and debug_port is not None:
            previous_process = self.process
            self.process = process
            try:
                self.environment_manager.register_environment(self, int(debug_port), module_executor_path)
            finally:
                self.process = previous_process

        authkey = self._authkey or runtime_state.load_or_create_root_authkey(self.environment_manager.wetlands_instance_path)
        connection: Connection | None = None
        try:
            connection = self._connect_worker(port, authkey)
            worker = _Worker(index, process, port, connection, process_logger, persistent=self._persistent)

            if self._persistent:
                runtime_state.record_worker(
                    self.environment_manager.wetlands_instance_path,
                    env_name=self.name,
                    env_path=self.path,
                    worker_index=index,
                    pid=process.pid,
                    port=port,
                    persistent=True,
                )

            self._start_reader_thread(worker)
            return worker
        except Exception:
            self._cleanup_failed_worker_launch(process, connection)
            raise

    def _cleanup_failed_worker_launch(
        self, process: subprocess.Popen, connection: Connection | None = None
    ) -> None:
        if connection is not None:
            try:
                connection.send(dict(action="exit"))
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass

        if process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass

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

    def _task_call_target(self, task: Task[Any] | None) -> str | None:
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

    def _worker_connection_failure(self, worker: _Worker, task: Task[Any], message: str) -> TaskFailure:
        return TaskFailure.worker_connection(
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
                task = worker._current_task
                if task is not None and not task.status.is_finished():
                    returncode = self._worker_returncode(worker)
                    if returncode is not None:
                        task._set_failed(
                            TaskFailure.worker_died(
                                task_id=task.id,
                                call_target=self._task_call_target(task),
                                worker=self._worker_info(worker),
                                returncode=returncode,
                            )
                        )
                    else:
                        task._set_failed(self._worker_connection_failure(worker, task, "Worker connection closed unexpectedly"))
                    worker._current_task = None
                    self._remove_dead_worker(worker)
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
            message_task_id = message.get("task_id")
            if task is None:
                if message_task_id is not None and message_task_id in worker._finished_task_ids:
                    logger.debug(f"Worker {worker.index}: ignoring late message for finished task {message_task_id}: {message}")
                    continue
                # No active task — this is a legacy message or unexpected
                logger.warning(f"Worker {worker.index}: received message with no active task: {message}")
                continue
            if message_task_id is not None and message_task_id != task.id:
                if message_task_id in worker._finished_task_ids:
                    logger.debug(f"Worker {worker.index}: ignoring stale message for finished task {message_task_id}: {message}")
                    continue
                logger.warning(
                    f"Worker {worker.index}: ignoring stale message for task {message_task_id}; "
                    f"current task is {task.id}: {message}"
                )
                continue

            action = message.get("action")
            if action in ("execution finished", "error", "canceled"):
                if action == "error":
                    failure = TaskFailure.from_payload(message, call_target=self._task_call_target(task))
                    self._log_task_failure(failure)
                task._on_message(message)
                worker._finished_task_ids.add(task.id)
                worker._current_task = None
                # Return worker to idle pool and dispatch next queued task
                self._dispatch_or_idle(worker)
            elif action == "update":
                task._on_message(message)
            elif action == "log":
                level = message.get("level", 20)
                logger.log(level, message.get("message", ""), extra=message.get("extra"))
            else:
                logger.warning(f"Worker {worker.index}: unexpected message: {message}")

    _HEALTH_CHECK_INTERVAL = 5  # seconds

    def _health_monitor_loop(self) -> None:
        """Daemon thread that detects dead or hung workers."""
        while not self._shutdown_event.wait(timeout=self._HEALTH_CHECK_INTERVAL):
            with self._lock:
                workers = list(self._workers)

            for worker in workers:
                task = worker._current_task
                if task is None or task.status.is_finished():
                    continue

                # Check 1: Is the process dead?
                if not worker.alive():
                    rc = self._worker_returncode(worker)
                    logger.error(f"Worker {worker.index} died (exit code {rc}) while running task {task.id}")
                    task._set_failed(
                        TaskFailure.worker_died(
                            task_id=task.id,
                            call_target=self._task_call_target(task),
                            worker=self._worker_info(worker),
                            returncode=rc,
                        )
                    )
                    worker._current_task = None
                    self._remove_dead_worker(worker)
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
                        task._set_failed(
                            TaskFailure.timeout_failure(
                                task_id=task.id,
                                call_target=self._task_call_target(task),
                                worker=self._worker_info(worker),
                                timeout=self._worker_timeout,
                                elapsed=elapsed,
                            )
                        )
                        worker._current_task = None
                        self._remove_dead_worker(worker)
                        self._try_replace_worker(worker.index)

    def _try_replace_worker(self, index: int) -> None:
        """Attempt to launch a replacement worker at the given index."""
        try:
            worker = self._launch_worker(index, self._additional_activate_commands, self._worker_env)
            with self._lock:
                self._workers.append(worker)
            self._dispatch_or_idle(worker)
            logger.info(f"Replacement worker {index} launched successfully.")
        except Exception as e:
            logger.error(f"Failed to launch replacement worker {index}: {e}")

    def _remove_dead_worker(self, worker: _Worker) -> None:
        """Remove a dead worker from all pools and clean up its resources."""
        with self._lock:
            if worker in self._workers:
                self._workers.remove(worker)

        try:
            if worker.connection and not worker.connection.closed:
                worker.connection.close()
        except OSError:
            pass

        if worker.process and worker.process.poll() is None:
            CommandExecutor.kill_process(worker.process)
        elif worker.process is None and worker.pid is not None and runtime_state.pid_exists(worker.pid):
            CommandExecutor.kill_pid(worker.pid)

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

        if worker.persistent:
            runtime_state.remove_worker(self.environment_manager.wetlands_instance_path, self.name, worker.index)

        logger.warning(f"Worker {worker.index} removed (dead). {len(self._workers)} worker(s) remaining.")

    def _dispatch_or_idle(self, worker: _Worker) -> None:
        """Try to dispatch the next queued task to this worker, or return it to idle pool."""
        if worker not in self._workers:
            return
        try:
            task = self._task_queue.get_nowait()
            self._dispatch_to_worker(worker, task)
        except queue.Empty:
            self._idle_workers.put(worker)

    def _dispatch_to_worker(self, worker: _Worker, task: Task[Any]) -> None:
        """Send a task's payload to a worker for execution."""
        payload = task._payload  # type: ignore[attr-defined]
        payload["task_id"] = task.id
        worker._current_task = task
        worker._last_activity = time.time()
        task._set_running()

        if worker.process_logger:
            call_target = payload.get("_call_target", MODULE_EXECUTOR_FILE)
            worker.process_logger.update_log_context({"call_target": call_target})

        try:
            worker.connection.send(payload)
        except (OSError, BrokenPipeError) as e:
            task._set_failed(
                self._worker_connection_failure(worker, task, f"Failed to send to worker {worker.index}: {e}")
            )
            worker._current_task = None
            self._remove_dead_worker(worker)
            self._try_replace_worker(worker.index)
        except Exception as e:
            task._set_failed(
                TaskFailure.serialization(
                    f"Failed to serialize task payload for worker {worker.index}: {e}",
                    task_id=task.id,
                    call_target=self._task_call_target(task),
                    context="payload",
                    worker=self._worker_info(worker),
                )
            )
            worker._current_task = None
            self._dispatch_or_idle(worker)

    def _submit_task(self, task: Task[Any], start: bool) -> Task[Any]:
        """Wire up a task's start/cancel functions and optionally start it."""

        def _start() -> None:
            while True:
                try:
                    worker = self._idle_workers.get_nowait()
                except queue.Empty:
                    self._task_queue.put(task)
                    return
                # Skip dead workers that are still in the idle queue
                if worker not in self._workers:
                    continue
                self._dispatch_to_worker(worker, task)
                return

        def _cancel() -> None:
            # Find which worker has this task and send cancel
            for w in self._workers:
                if w._current_task is task:
                    try:
                        w.connection.send({"action": "cancel", "task_id": task.id})
                    except (OSError, BrokenPipeError):
                        pass
                    return

        task._set_start_fn(_start)
        task._set_cancel_fn(_cancel)
        if start:
            task.start()
        return task

    # --- Public Task API ---

    def submit(
        self,
        module_path: str | Path,
        function: str,
        args: tuple = (),
        kwargs: dict[str, Any] | None = None,
        *,
        start: bool = True,
    ) -> Task[Any]:
        """Submit a function for non-blocking execution in the remote environment.

        Args:
            module_path: Path to the module to import.
            function: Name of the function to execute.
            args: Positional arguments (must be picklable).
            kwargs: Keyword arguments (must be picklable).
            start: If True (default), dispatch immediately. If False, stays PENDING.

        Returns:
            A Task object.
        """
        kwargs = kwargs or {}
        task: Task[Any] = Task()
        module_name = Path(module_path).stem
        task._payload = dict(  # type: ignore[attr-defined]
            action="execute",
            module_path=str(module_path),
            function=function,
            args=args,
            kwargs=kwargs,
            _call_target=f"{module_name}:{function}",
        )
        return self._submit_task(task, start)

    def submit_script(
        self,
        script_path: str | Path,
        args: tuple = (),
        run_name: str = "__main__",
        *,
        start: bool = True,
    ) -> Task[None]:
        """Submit a script for non-blocking execution.

        Args:
            script_path: Path to the Python script.
            args: Command-line arguments.
            run_name: Value for runpy.run_path(run_name=...).
            start: If True (default), dispatch immediately.

        Returns:
            A Task[None].
        """
        task: Task[None] = Task()
        script_name = Path(script_path).name
        task._payload = dict(  # type: ignore[attr-defined]
            action="run",
            script_path=str(script_path),
            args=args,
            run_name=run_name,
            _call_target=script_name,
        )
        return self._submit_task(task, start)

    def map(
        self,
        module_path: str | Path,
        function: str,
        iterable: Iterable[Any],
        *,
        timeout: float | None = None,
        ordered: bool = True,
    ) -> Iterator[Any]:
        """Execute function once for each item, distributing across workers.

        Args:
            module_path: Module containing the function.
            function: Function name.
            iterable: Items to process (one task per item).
            timeout: Max seconds to wait for each result.
            ordered: If True, yield in submission order. If False, yield as completed.

        Returns:
            Iterator of results.
        """
        tasks = self.map_tasks(module_path, function, iterable)
        if ordered:
            for task in tasks:
                task.wait_for(timeout=timeout)
                if task.status == TaskStatus.FAILED:
                    raise task.exception  # type: ignore[misc]
                yield task.result
        else:
            # Yield results as they complete
            remaining = set(range(len(tasks)))
            while remaining:
                for i in list(remaining):
                    t = tasks[i]
                    try:
                        t.wait_for(timeout=0.01)
                    except TimeoutError:
                        continue
                    remaining.discard(i)
                    if t.status == TaskStatus.FAILED:
                        raise t.exception  # type: ignore[misc]
                    yield t.result

    def map_tasks(
        self,
        module_path: str | Path,
        function: str,
        iterable: Iterable[Any],
    ) -> list[Task[Any]]:
        """Submit one task per item, distributing across workers.

        All tasks are started immediately.

        Args:
            module_path: Module containing the function.
            function: Function name.
            iterable: Items to process.

        Returns:
            List of Task objects.
        """
        return [self.submit(module_path, function, args=(item,)) for item in iterable]

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
        for entry in worker_entries:
            worker_index = entry.get("worker_index", "<unknown>")
            port = entry.get("port", "<unknown>")
            try:
                worker = self._attach_worker(entry, authkey, timeout=timeout)
            except _AttachTimeout as e:
                logger.warning(
                    "Timed out attaching to persistent worker %s for environment '%s' on port %s: %s",
                    worker_index,
                    self.name,
                    port,
                    e,
                    exc_info=True,
                )
                continue
            except Exception as e:
                logger.warning(
                    "Discarding persistent worker %s for environment '%s' on port %s after attach failure: %s",
                    worker_index,
                    self.name,
                    port,
                    e,
                    exc_info=True,
                )
                runtime_state.remove_worker(
                    self.environment_manager.wetlands_instance_path,
                    self.name,
                    int(entry["worker_index"]),
                )
                continue
            self._workers.append(worker)
            self._idle_workers.put(worker)

        if not self._workers:
            raise Exception(f"No live authenticated persistent workers found for environment '{self.name}'.")

        first = self._workers[0]
        self.port = first.port
        self.process = first.process
        self.connection = first.connection
        self._process_logger = first.process_logger

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
        connection = self._connect_worker(int(entry["port"]), authkey, timeout=timeout)
        worker = _Worker(
            int(entry["worker_index"]),
            None,
            int(entry["port"]),
            connection,
            None,
            pid=int(entry["pid"]),
            persistent=True,
        )
        self._start_reader_thread(worker)
        return worker

    def _connect_worker(self, port: int, authkey: bytes, timeout: float | None = None) -> Connection:
        if timeout is None:
            return Client(("localhost", port), authkey=authkey)

        # Client() has no timeout parameter and can block during both socket
        # connect and the multiprocessing auth handshake. Persistent attach uses
        # this bounded equivalent so a busy or stale worker can produce an
        # actionable error instead of hanging the caller.
        address = ("localhost", port)
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
        return connection

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

    def _ensure_debugpy_installed(self) -> None:
        """Install debugpy in the environment if it is not already installed."""
        installed_packages = self.environment_manager.get_installed_packages(self)
        if any(pkg["name"] == "debugpy" for pkg in installed_packages):
            return
        logger.info(f"Installing debugpy in environment '{self.name}' for debug mode.")
        self.environment_manager.install(self, {"conda": ["debugpy"]}, _mark_unmanaged=False)

    def _send_and_wait(self, payload: dict) -> Any:
        """Send a payload to the remote environment and wait for its response.
        Used by the legacy blocking execute()/run_script() methods.
        """
        connection = self.connection
        if connection is None or connection.closed:
            raise ExecutionException(TaskFailure.environment("Connection not ready.", call_target=payload.get("_call_target")))

        try:
            connection.send(payload)
        except BrokenPipeError as e:
            logger.error(f"Broken pipe. The peer process might have terminated. Exception: {e}.")
            raise ExecutionException(
                TaskFailure.worker_connection(
                    f"Broken pipe. The peer process might have terminated. Exception: {e}.",
                    call_target=payload.get("_call_target"),
                )
            ) from e
        except OSError as e:
            if e.errno == 9:
                logger.error("Connection closed abruptly by the peer.")
                raise ExecutionException(
                    TaskFailure.worker_connection(
                        "Connection closed abruptly by the peer.",
                        call_target=payload.get("_call_target"),
                    )
                ) from e
            raise
        except Exception as e:
            raise ExecutionException(
                TaskFailure.serialization(
                    f"Failed to serialize task payload: {e}",
                    call_target=payload.get("_call_target"),
                    context="payload",
                )
            ) from e

        try:
            while message := connection.recv():
                action = message.get("action")
                if action == "execution finished":
                    logger.info(f"{payload.get('action')} finished")
                    return message.get("result")
                elif action == "error":
                    failure = TaskFailure.from_payload(message, call_target=payload.get("_call_target"))
                    self._log_task_failure(failure)
                    raise ExecutionException(failure)
                else:
                    logger.warning(f"Got an unexpected message: {message}")

        except EOFError:
            logger.info("Connection closed gracefully by the peer.")
            raise ExecutionException(
                TaskFailure.worker_connection(
                    "Connection closed gracefully by the peer.",
                    call_target=payload.get("_call_target"),
                )
            )
        except BrokenPipeError as e:
            logger.error(f"Broken pipe. The peer process might have terminated. Exception: {e}.")
            raise ExecutionException(
                TaskFailure.worker_connection(
                    f"Broken pipe. The peer process might have terminated. Exception: {e}.",
                    call_target=payload.get("_call_target"),
                )
            ) from e
        except OSError as e:
            if e.errno == 9:  # Bad file descriptor
                logger.error("Connection closed abruptly by the peer.")
                raise ExecutionException(
                    TaskFailure.worker_connection(
                        "Connection closed abruptly by the peer.",
                        call_target=payload.get("_call_target"),
                    )
                ) from e
            else:
                logger.error(f"Unexpected OSError: {e}")
                raise e
        return None

    def _log_task_failure(self, failure: TaskFailure) -> None:
        level = logging.WARNING if failure.category == TaskFailureCategory.REMOTE_EXCEPTION else logging.ERROR
        logger.log(level, failure.summary())

    def _gracefully_stop_process(
        self,
        process: subprocess.Popen | None,
        process_logger: ProcessLogger | None,
    ) -> None:
        if process is None:
            return

        timed_out = False
        try:
            if process.poll() is None:
                process.wait(timeout=WORKER_GRACEFUL_EXIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            timed_out = True

        if timed_out:
            CommandExecutor.kill_process(process)

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

    @synchronized
    def execute(self, module_path: str | Path, function: str, args: tuple = (), kwargs: dict[str, Any] = {}) -> Any:
        """Executes a function in the given module and return the result.
        Warning: all arguments (args and kwargs) must be picklable!

        When workers are available, uses submit() internally for dispatch.
        Falls back to legacy _send_and_wait when no worker pool is set up.

        Args:
            module_path: the path to the module to import
            function: the name of the function to execute
            args: the argument list for the function
            kwargs: the keyword arguments for the function

        Returns:
            The result of the function.
        Raises:
            ExecutionException: on remote errors.
        """
        if self._workers:
            # Use task-based dispatch through worker pool
            task = self.submit(module_path, function, args=args, kwargs=kwargs)
            task.wait_for()
            if task.status == TaskStatus.FAILED:
                raise task.exception  # type: ignore[misc]
            return task.result

        # Legacy path (no worker pool — direct connection)
        module_name = Path(module_path).stem
        call_target = f"{module_name}:{function}"
        if self._process_logger:
            self._process_logger.update_log_context({"call_target": call_target})

        try:
            payload = dict(
                action="execute",
                module_path=str(module_path),
                function=function,
                args=args,
                kwargs=kwargs,
                _call_target=call_target,
            )
            return self._send_and_wait(payload)
        finally:
            if self._process_logger:
                self._process_logger.update_log_context({"call_target": MODULE_EXECUTOR_FILE})

    @synchronized
    def run_script(self, script_path: str | Path, args: tuple = (), run_name: str = "__main__") -> Any:
        """Runs a Python script remotely using runpy.run_path().

        Args:
            script_path: Path to the script to execute.
            args: List of arguments to pass.
            run_name: Value for runpy.run_path(run_name=...).

        Returns:
            The resulting globals dict, or None on failure.
        """
        if self._workers:
            task = self.submit_script(script_path, args=args, run_name=run_name)
            task.wait_for()
            if task.status == TaskStatus.FAILED:
                raise task.exception  # type: ignore[misc]
            return task.result

        script_name = Path(script_path).name
        if self._process_logger:
            self._process_logger.update_log_context({"call_target": script_name})

        try:
            payload = dict(
                action="run",
                script_path=str(script_path),
                args=args,
                run_name=run_name,
                _call_target=script_name,
            )
            return self._send_and_wait(payload)
        finally:
            if self._process_logger:
                self._process_logger.update_log_context({"call_target": MODULE_EXECUTOR_FILE})

    @synchronized
    def launched(self) -> bool:
        """Return true if the environment server process is launched and the connection is open."""
        if self._workers:
            return any(
                w.alive() and w.connection is not None and not w.connection.closed for w in self._workers
            )
        return (
            self.process is not None
            and self.process.poll() is None
            and self.connection is not None
            and not self.connection.closed
            and self.connection.writable
            and self.connection.readable
        )

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

        if self._workers:
            for worker in self._workers:
                if worker._current_task is not None and not worker._current_task.status.is_finished():
                    worker._current_task._set_failed(
                        TaskFailure.environment(
                            "Environment is shutting down",
                            task_id=worker._current_task.id,
                            call_target=self._task_call_target(worker._current_task),
                        )
                    )
                    worker._current_task = None
                try:
                    worker.connection.send(dict(action="exit"))
                except OSError:
                    pass
                worker.connection.close()
                if worker.process is not None:
                    self._gracefully_stop_process(worker.process, worker.process_logger)
                elif worker.pid is not None:
                    CommandExecutor.kill_pid(worker.pid)
                if worker.persistent:
                    runtime_state.remove_worker(self.environment_manager.wetlands_instance_path, self.name, worker.index)
            self._workers.clear()
            while not self._idle_workers.empty():
                try:
                    self._idle_workers.get_nowait()
                except queue.Empty:
                    break

            # Fail any tasks still in the queue
            while True:
                try:
                    task = self._task_queue.get_nowait()
                    task._set_failed(TaskFailure.environment("Environment is shutting down", task_id=task.id))
                except queue.Empty:
                    break

            self._process_logger = None
            return

        # Legacy single-process path
        if self.connection is not None:
            try:
                self.connection.send(dict(action="exit"))
            except OSError as e:
                if e.args[0] == "handle is closed":
                    pass
            self.connection.close()

        process_logger = self._process_logger
        self._gracefully_stop_process(self.process, process_logger)
        self._process_logger = None

    @synchronized
    def detach(self) -> None:
        """Close local connections without stopping persistent worker processes."""
        self._shutdown_event.set()
        for worker in list(self._workers):
            try:
                if worker.connection and not worker.connection.closed:
                    if worker.persistent:
                        worker.connection.send(dict(action="detach"))
                    worker.connection.close()
            except OSError:
                pass
            if worker._current_task is not None and not worker._current_task.status.is_finished():
                worker._current_task._set_failed(
                    TaskFailure.environment(
                        "Environment is detaching",
                        task_id=worker._current_task.id,
                        call_target=self._task_call_target(worker._current_task),
                    )
                )
            worker._current_task = None
        self._workers.clear()
        while not self._idle_workers.empty():
            try:
                self._idle_workers.get_nowait()
            except queue.Empty:
                break
        while True:
            try:
                task = self._task_queue.get_nowait()
                task._set_failed(TaskFailure.environment("Environment is detaching", task_id=task.id))
            except queue.Empty:
                break
        self.connection = None
        self.process = None
        self.port = None
        self._process_logger = None

    @synchronized
    def delete(self) -> None:
        """Deletes this external environment and cleans up associated resources."""
        if self.path is None:
            raise Exception("Cannot delete an environment with no path.")

        if not self.environment_manager.environment_exists(self.path):
            raise Exception(f"The environment {self.name} does not exist.")

        if self.launched():
            self._exit()

        if self.environment_manager.settings_manager.use_pixi:
            send2trash(self.path.parent)
        else:
            send2trash(self.path)

        if self.name in self.environment_manager.environments:
            del self.environment_manager.environments[self.name]

    @synchronized
    def update(
        self,
        dependencies: Union[Dependencies, None] = None,
        additional_install_commands: Commands | None = None,
    ) -> "Environment":
        """Updates this external environment by deleting it and recreating it."""
        if not self.path:
            raise Exception("Cannot update an environment with no path.")

        if not self.environment_manager.environment_exists(self.path):
            raise Exception(f"The environment {self.name} does not exist.")

        self.delete()

        return self.environment_manager.create(
            str(self.name),
            dependencies=dependencies,
            additional_install_commands=additional_install_commands,
        )
