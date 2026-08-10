"""Managed external commands launched inside a provisioned environment."""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import functools
import math
import os
import signal
import subprocess
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from numbers import Real
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import psutil

from wetlands._internal.process_termination import (
    ProcessIdentity,
    ProcessIdentityError,
    ProcessTerminationError,
    _posix_group_members,
    _terminate_posix_group,
    _wait_for_posix_group_exit,
    capture_process_identity,
    identity_matches,
    terminate_launched_process_tree,
)
from wetlands._internal.provisioning import _windows_git_long_paths_overrides
from wetlands.external_environment import _assign_windows_kill_job, _close_windows_job

if TYPE_CHECKING:
    from wetlands.managed_environment import ManagedEnvironment

_EVENT_CAPACITY = 1024
_READ_SIZE = 8192


async def _run_blocking(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(function, *args, **kwargs))


class OutputStream(str, Enum):
    """The output pipe that produced an event."""

    STDOUT = "stdout"
    STDERR = "stderr"


@dataclass(frozen=True)
class OutputEvent:
    """One decoded line or trailing partial line from a managed command."""

    sequence: int
    timestamp: float
    stream: OutputStream
    text: str


@dataclass(frozen=True)
class ManagedProcessResult:
    """The immutable terminal result of a managed command."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    started_at: float
    ended_at: float


class ProcessError(RuntimeError):
    """Base class for errors reported by a managed command."""

    def __init__(self, message: str, *, argv: tuple[str, ...], environment: str, generation_id: str) -> None:
        super().__init__(message)
        self.argv = argv
        self.environment = environment
        self.generation_id = generation_id


class ProcessExitError(ProcessError):
    """A checked command exited with a non-zero status."""

    def __init__(self, result: ManagedProcessResult, *, environment: str, generation_id: str) -> None:
        super().__init__(
            f"Command {result.argv!r} exited with status {result.returncode}",
            argv=result.argv,
            environment=environment,
            generation_id=generation_id,
        )
        self.result = result


class ProcessTimeoutError(ProcessError, TimeoutError):
    """A command exceeded the timeout of a wait operation."""

    def __init__(
        self,
        timeout: float,
        result: ManagedProcessResult,
        *,
        environment: str,
        generation_id: str,
    ) -> None:
        super().__init__(
            f"Command {result.argv!r} did not finish within {timeout} seconds",
            argv=result.argv,
            environment=environment,
            generation_id=generation_id,
        )
        self.timeout = timeout
        self.result = result


class ProcessOutputLimitError(ProcessError):
    """A command emitted more output than its configured capture limit."""

    def __init__(
        self,
        limit: int,
        result: ManagedProcessResult,
        truncated_streams: frozenset[OutputStream],
        *,
        environment: str,
        generation_id: str,
    ) -> None:
        streams = ", ".join(sorted(stream.value for stream in truncated_streams))
        super().__init__(
            f"Command {result.argv!r} exceeded its {limit}-byte output limit on {streams}",
            argv=result.argv,
            environment=environment,
            generation_id=generation_id,
        )
        self.limit = limit
        self.result = result
        self.truncated_streams = truncated_streams


class ProcessEventLagError(ProcessError):
    """An output observer fell behind the bounded event history."""

    def __init__(
        self,
        first_unavailable_sequence: int,
        oldest_retained_sequence: int,
        *,
        argv: tuple[str, ...],
        environment: str,
        generation_id: str,
    ) -> None:
        super().__init__(
            "Output observer fell behind: "
            f"sequence {first_unavailable_sequence} is unavailable; oldest retained is {oldest_retained_sequence}",
            argv=argv,
            environment=environment,
            generation_id=generation_id,
        )
        self.first_unavailable_sequence = first_unavailable_sequence
        self.oldest_retained_sequence = oldest_retained_sequence


class ProcessLineTimeoutError(ProcessError, TimeoutError):
    """No matching output event arrived before a readiness deadline."""

    def __init__(
        self,
        timeout: float,
        *,
        argv: tuple[str, ...],
        environment: str,
        generation_id: str,
    ) -> None:
        super().__init__(
            f"No matching output from command {argv!r} arrived within {timeout} seconds",
            argv=argv,
            environment=environment,
            generation_id=generation_id,
        )
        self.timeout = timeout


class ProcessCleanupError(ProcessError):
    """The complete owned process tree could not be proven terminated."""

    def __init__(
        self,
        failures: Sequence[BaseException],
        result: ManagedProcessResult | None,
        *,
        argv: tuple[str, ...],
        environment: str,
        generation_id: str,
        initiating_error: ProcessError | None = None,
    ) -> None:
        frozen_failures = tuple(failures)
        details = "; ".join(str(failure) for failure in frozen_failures)
        super().__init__(
            f"Could not completely clean up command {argv!r}: {details}",
            argv=argv,
            environment=environment,
            generation_id=generation_id,
        )
        self.failures = frozen_failures
        self.result = result
        self.initiating_error = initiating_error
        if initiating_error is not None:
            self.__cause__ = initiating_error


@dataclass(frozen=True)
class _LaunchOptions:
    argv: tuple[str, ...]
    cwd: Path
    env_overlay: dict[str, str | None]
    output_limit: int


def _validate_timeout(timeout: float | None, *, allow_none: bool = True) -> float | None:
    if timeout is None:
        if allow_none:
            return None
        raise TypeError("timeout must be a finite non-negative number")
    if isinstance(timeout, bool) or not isinstance(timeout, Real):
        raise TypeError("timeout must be a finite non-negative number or None")
    normalized = float(timeout)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError("timeout must be a finite non-negative number")
    return normalized


def _validate_check(check: bool) -> bool:
    if not isinstance(check, bool):
        raise TypeError("check must be a bool")
    return check


def _validate_launch_options(
    *,
    argv: Sequence[str],
    cwd: str | Path | None,
    env: Mapping[str, str | None] | None,
    output_limit: int,
    default_cwd: Path,
) -> _LaunchOptions:
    if isinstance(argv, (str, bytes)) or not isinstance(argv, Sequence):
        raise TypeError("argv must be a non-empty sequence of strings")
    normalized_argv = tuple(argv)
    if not normalized_argv:
        raise ValueError("argv must not be empty")
    if any(not isinstance(argument, str) for argument in normalized_argv):
        raise TypeError("every argv element must be a string")
    if any("\0" in argument for argument in normalized_argv):
        raise ValueError("argv elements must not contain null bytes")

    if isinstance(output_limit, bool) or not isinstance(output_limit, int):
        raise TypeError("output_limit must be a non-negative integer")
    if output_limit < 0:
        raise ValueError("output_limit must be a non-negative integer")

    raw_cwd = default_cwd if cwd is None else Path(cwd).expanduser()
    try:
        normalized_cwd = raw_cwd.resolve(strict=True)
    except (FileNotFoundError, RuntimeError) as error:
        raise ValueError(f"cwd does not exist: {raw_cwd}") from error
    if not normalized_cwd.is_dir():
        raise ValueError(f"cwd is not a directory: {normalized_cwd}")

    normalized_overlay: dict[str, str | None] = {}
    if env is not None:
        if not isinstance(env, Mapping):
            raise TypeError("env must be a mapping or None")
        for key, value in env.items():
            if not isinstance(key, str):
                raise TypeError("environment variable names must be strings")
            if not key or "=" in key or "\0" in key:
                raise ValueError("environment variable names must be non-empty and contain neither '=' nor null bytes")
            if value is not None and not isinstance(value, str):
                raise TypeError("environment variable values must be strings or None")
            if value is not None and "\0" in value:
                raise ValueError("environment variable values must not contain null bytes")
            normalized_overlay[key] = value

    return _LaunchOptions(normalized_argv, normalized_cwd, normalized_overlay, output_limit)


class ManagedProcess:
    """A supervised command and its owned process tree.

    Instances are returned by :meth:`ManagedEnvironment.spawn`; applications do
    not construct them directly.
    """

    def __init__(
        self,
        *,
        environment: ManagedEnvironment,
        argv: tuple[str, ...],
        process: subprocess.Popen[bytes],
        output_limit: int,
        started_at: float,
    ) -> None:
        self._environment_handle = environment
        self._environment = environment.name
        self._generation_id = environment.generation_id
        self._argv = argv
        self._process = process
        self._output_limit = output_limit
        self._started_at = started_at
        self._identity: ProcessIdentity | None = None

        self._condition = threading.Condition(threading.RLock())
        self._done = threading.Event()
        self._cleanup_lock = threading.Lock()
        self._cause: tuple[str, float | None] | None = None
        self._result: ManagedProcessResult | None = None
        self._terminal_error: ProcessError | None = None
        self._ownership_clean = False
        self._registered = False
        self._released = False

        self._output_lock = threading.Lock()
        self._captured = 0
        self._stdout = bytearray()
        self._stderr = bytearray()
        self._truncated_streams: set[OutputStream] = set()

        self._event_condition = threading.Condition(threading.RLock())
        self._events: deque[OutputEvent] = deque(maxlen=_EVENT_CAPACITY)
        self._next_sequence = 0
        self._streams_closed = False
        self._readers: list[threading.Thread] = []
        self._reader_errors: list[BaseException] = []
        self._supervisor: threading.Thread | None = None

    _validate_launch_options = staticmethod(_validate_launch_options)

    @classmethod
    def _launch(
        cls,
        *,
        environment: ManagedEnvironment,
        argv: Sequence[str],
        cwd: str | Path | None = None,
        env: Mapping[str, str | None] | None = None,
        output_limit: int = 1_048_576,
    ) -> ManagedProcess:
        options = _validate_launch_options(
            argv=argv,
            cwd=cwd,
            env=env,
            output_limit=output_limit,
            default_cwd=environment.path,
        )
        return cls._launch_validated(environment=environment, options=options)

    @classmethod
    def _launch_validated(cls, *, environment: ManagedEnvironment, options: _LaunchOptions) -> ManagedProcess:
        command = [
            str(environment.pixi_executable_path),
            "run",
            "--manifest-path",
            str(environment.pixi_manifest_path),
            "--locked",
            "--",
            *options.argv,
        ]
        launch_env = dict(os.environ)
        launch_env.update(
            {
                "PIXI_HOME": str(environment._manager.state_root / "pixi-home"),
                "PIXI_CACHE_DIR": str(environment._manager.state_root / "pixi-cache"),
                **{
                    ("NO_PROXY" if scheme == "no_proxy" else f"{scheme.upper()}_PROXY"): value
                    for scheme, value in (environment._manager.network or {}).items()
                },
            }
        )
        if os.name == "nt":
            launch_env.update(_windows_git_long_paths_overrides(os.environ))
        cls._apply_environment_overlay(launch_env, options.env_overlay)

        popen_options: dict[str, Any] = {
            "cwd": options.cwd,
            "env": launch_env,
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": False,
        }
        if os.name == "nt":
            popen_options["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP") | getattr(
                subprocess, "CREATE_SUSPENDED", 0x00000004
            )
        else:
            popen_options["start_new_session"] = True

        started_at = time.time()
        try:
            process = cast(subprocess.Popen[bytes], subprocess.Popen(command, **popen_options))
        except OSError as error:
            message = (
                f"Could not launch command {options.argv!r} in environment {environment.name!r} "
                f"generation {environment.generation_id!r} through Pixi: {error}"
            )
            try:
                contextual_error = type(error)(error.errno, message, error.filename)
            except (TypeError, ValueError):
                contextual_error = OSError(error.errno, message, error.filename)
            raise contextual_error from error
        handle = cls(
            environment=environment,
            argv=options.argv,
            process=process,
            output_limit=options.output_limit,
            started_at=started_at,
        )
        if os.name == "nt":
            process._wetlands_suspended = True  # type: ignore[attr-defined]
        try:
            handle._registered = True
            environment._register_process(handle)
            identity = capture_process_identity(process.pid)
            handle._identity = identity
            if os.name != "nt" and (identity.process_group_id != process.pid or identity.session_id != process.pid):
                raise ProcessIdentityError(
                    f"Managed command PID {process.pid} has no proven isolated process-session ownership"
                )
            process._wetlands_started_at = identity.started_at  # type: ignore[attr-defined]
            process._wetlands_process_group_id = identity.process_group_id  # type: ignore[attr-defined]
            process._wetlands_session_id = identity.session_id  # type: ignore[attr-defined]
            if os.name == "nt":
                _assign_windows_kill_job(process)
            handle._start_readers()
            if os.name == "nt":
                handle._resume_windows_process()
                process._wetlands_suspended = False  # type: ignore[attr-defined]
            handle._start_supervisor()
        except BaseException as launch_error:
            cleanup_errors = handle._cleanup_failed_launch()
            if cleanup_errors:
                cleanup_error = handle._freeze_cleanup_error(cleanup_errors)
                raise cleanup_error from launch_error
            raise
        return handle

    @staticmethod
    def _apply_environment_overlay(target: dict[str, str], overlay: Mapping[str, str | None]) -> None:
        for key, value in overlay.items():
            actual_key = key
            if os.name == "nt":
                inherited = next((candidate for candidate in target if candidate.casefold() == key.casefold()), None)
                if inherited is not None:
                    actual_key = inherited
            if value is None:
                target.pop(actual_key, None)
            else:
                if actual_key != key:
                    target.pop(actual_key, None)
                target[key] = value

    @property
    def argv(self) -> tuple[str, ...]:
        return self._argv

    @property
    def environment(self) -> str:
        return self._environment

    @property
    def generation_id(self) -> str:
        return self._generation_id

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def returncode(self) -> int | None:
        return self._process.poll()

    @property
    def running(self) -> bool:
        return self.returncode is None

    def _start_readers(self) -> None:
        assert self._process.stdout is not None
        assert self._process.stderr is not None
        stdout_reader = threading.Thread(
            target=self._drain,
            args=(self._process.stdout, OutputStream.STDOUT),
            name=f"wetlands-process-{self.pid}-stdout",
            daemon=True,
        )
        stderr_reader = threading.Thread(
            target=self._drain,
            args=(self._process.stderr, OutputStream.STDERR),
            name=f"wetlands-process-{self.pid}-stderr",
            daemon=True,
        )
        for reader, pipe in (
            (stdout_reader, self._process.stdout),
            (stderr_reader, self._process.stderr),
        ):
            try:
                reader.start()
            except BaseException:
                pipe.close()
                raise
            self._readers.append(reader)

    def _resume_windows_process(self) -> None:
        """Resume a Windows child created suspended after mandatory Job assignment."""
        import ctypes
        from ctypes import wintypes

        thread_ids = [thread.id for thread in psutil.Process(self.pid).threads()]
        if not thread_ids:
            raise OSError(f"Could not find the suspended primary thread for PID {self.pid}")
        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.OpenThread.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenThread.restype = wintypes.HANDLE
        kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
        kernel32.ResumeThread.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        for thread_id in thread_ids:
            handle = kernel32.OpenThread(0x0002, False, wintypes.DWORD(thread_id))
            if not handle:
                error = getattr(ctypes, "get_last_error")()
                raise OSError(error, getattr(ctypes, "FormatError")(error))
            try:
                previous_count = kernel32.ResumeThread(handle)
                if previous_count == 0xFFFFFFFF:
                    error = getattr(ctypes, "get_last_error")()
                    raise OSError(error, getattr(ctypes, "FormatError")(error))
                if previous_count == 0:
                    raise ProcessIdentityError(
                        f"Suspended process thread {thread_id} for PID {self.pid} was already running"
                    )
            finally:
                kernel32.CloseHandle(handle)

    def _start_supervisor(self) -> None:
        supervisor = threading.Thread(
            target=self._supervise,
            name=f"wetlands-process-{self.pid}-supervisor",
            daemon=True,
        )
        self._supervisor = supervisor
        supervisor.start()

    def _drain(self, pipe: Any, stream: OutputStream) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        pending = ""
        try:
            while True:
                chunk = pipe.read1(_READ_SIZE) if hasattr(pipe, "read1") else pipe.read(_READ_SIZE)
                if not chunk:
                    break
                accepted = b""
                breach = False
                with self._output_lock:
                    if not self._truncated_streams:
                        available = self._output_limit - self._captured
                        accepted = chunk[:available]
                        self._captured += len(accepted)
                        target = self._stdout if stream is OutputStream.STDOUT else self._stderr
                        target.extend(accepted)
                        breach = len(chunk) > len(accepted)
                        if breach:
                            self._truncated_streams.add(stream)
                    elif chunk:
                        self._truncated_streams.add(stream)
                if accepted:
                    pending += decoder.decode(accepted, final=False)
                    pending = self._publish_complete_lines(stream, pending)
                if breach:
                    self._request("output_limit", None)
            pending += decoder.decode(b"", final=True)
            if pending:
                self._publish_event(stream, pending)
        except BaseException as error:
            with self._condition:
                self._reader_errors.append(error)
            self._request("cleanup", None)
        finally:
            try:
                pipe.close()
            except OSError:
                pass

    def _publish_complete_lines(self, stream: OutputStream, pending: str) -> str:
        while True:
            newline = pending.find("\n")
            if newline < 0:
                return pending
            end = newline + 1
            self._publish_event(stream, pending[:end])
            pending = pending[end:]

    def _publish_event(self, stream: OutputStream, text: str) -> None:
        with self._event_condition:
            event = OutputEvent(self._next_sequence, time.time(), stream, text)
            self._next_sequence += 1
            self._events.append(event)
            self._event_condition.notify_all()

    def _request(self, cause: str, value: float | None) -> None:
        with self._condition:
            if self._cause is None:
                self._cause = (cause, value)
            self._condition.notify_all()

    def _supervise(self) -> None:
        cleanup_errors: list[BaseException] = []
        cause: tuple[str, float | None] | None
        try:
            while True:
                with self._condition:
                    cause = self._cause
                if cause is not None or self._process.poll() is not None:
                    break
                with self._condition:
                    self._condition.wait(0.02)

            grace = self._environment_handle._manager.termination_grace
            if cause is not None and cause[0] == "kill":
                try:
                    self._kill_tree()
                except BaseException as error:
                    cleanup_errors.append(error)
            elif cause is not None and cause[0] == "terminate" and cause[1] is not None:
                grace = cause[1]
            if cause is None or cause[0] != "kill":
                try:
                    self._terminate_tree(grace)
                except BaseException as error:
                    cleanup_errors.append(error)
            self._join_readers(cleanup_errors)
            with self._condition:
                cleanup_errors.extend(self._reader_errors)
        except BaseException as error:
            cleanup_errors.append(error)

        result = self._make_result()
        if not cleanup_errors:
            try:
                self._release_once()
            except BaseException as error:
                cleanup_errors.append(error)

        with self._condition:
            cause = self._cause
            if cleanup_errors:
                self._terminal_error = self._new_cleanup_error(cleanup_errors, result)
            elif self._terminal_error is None and cause is not None and cause[0] == "output_limit":
                self._terminal_error = ProcessOutputLimitError(
                    self._output_limit,
                    result,
                    frozenset(self._truncated_streams),
                    environment=self.environment,
                    generation_id=self.generation_id,
                )
            elif self._terminal_error is None and cause is not None and cause[0] == "timeout":
                assert cause[1] is not None
                self._terminal_error = ProcessTimeoutError(
                    cause[1],
                    result,
                    environment=self.environment,
                    generation_id=self.generation_id,
                )
            self._result = result
            self._ownership_clean = not cleanup_errors
            self._done.set()
            self._condition.notify_all()
        with self._event_condition:
            self._streams_closed = True
            self._event_condition.notify_all()

    def _terminate_tree(self, grace: float) -> None:
        if os.name == "nt":
            self._terminate_windows_job(grace)
            return
        terminate_launched_process_tree(
            self._process,
            grace=grace,
            close_windows_job=_close_windows_job,
        )

    def _kill_tree(self) -> None:
        if os.name == "nt":
            self._terminate_windows_job(0.0, force=True)
            return
        if self._identity is None:
            raise ProcessIdentityError(f"Managed command PID {self.pid} has no recorded process identity")
        process_group_id = self._identity.process_group_id
        session_id = self._identity.session_id
        if process_group_id != self.pid or session_id != self.pid:
            raise ProcessIdentityError(
                f"Managed command PID {self.pid} has no proven isolated process-session ownership"
            )
        assert process_group_id is not None
        if self._process.poll() is None:
            if not identity_matches(self.pid, self._identity.started_at):
                raise ProcessIdentityError(f"Refusing to signal PID {self.pid}: its process start identity changed")
            if os.getpgid(self.pid) != process_group_id or os.getsid(self.pid) != session_id:
                raise ProcessIdentityError(f"Refusing to signal PID {self.pid}: its process-session identity changed")
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except ProcessLookupError:
            with contextlib.suppress(subprocess.TimeoutExpired, OSError):
                self._process.wait(timeout=0)
            return
        verification_timeout = max(1.0, self._environment_handle._manager.termination_grace)
        if _wait_for_posix_group_exit(
            process_group_id,
            process=self._process,
            timeout=verification_timeout,
        ):
            return
        survivors = _posix_group_members(process_group_id)
        details = f"; surviving PIDs: {survivors}" if survivors else ""
        raise ProcessTerminationError(f"Managed command process group {process_group_id} survived SIGKILL{details}")

    def _terminate_windows_job(self, grace: float, *, force: bool = False) -> None:
        """Gracefully signal, then terminate and verify the mandatory Windows Job."""
        handle = getattr(self._process, "_wetlands_job_handle", None)
        if handle is None:
            raise ProcessIdentityError(f"Managed command PID {self.pid} has no assigned Windows Job Object")
        if self._identity is None:
            raise ProcessIdentityError(f"Managed command PID {self.pid} has no recorded process identity")
        leader_running = self._process.poll() is None
        suspended = bool(getattr(self._process, "_wetlands_suspended", False))
        if leader_running and not identity_matches(self.pid, self._identity.started_at):
            raise ProcessIdentityError(f"Refusing to signal PID {self.pid}: its process start identity changed")

        deadline = time.monotonic() + max(0.0, grace)
        if leader_running and not suspended and not force:
            try:
                self._process.send_signal(getattr(signal, "CTRL_BREAK_EVENT"))
            except OSError:
                pass
            while self._windows_job_active_processes(handle) and time.monotonic() < deadline:
                time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))

        if self._windows_job_active_processes(handle):
            self._terminate_windows_job_object(handle)
            force_deadline = time.monotonic() + max(1.0, grace)
            while self._windows_job_active_processes(handle) and time.monotonic() < force_deadline:
                time.sleep(min(0.02, max(0.0, force_deadline - time.monotonic())))
        survivors = self._windows_job_active_processes(handle)
        if survivors:
            raise ProcessTerminationError(
                f"Managed command Windows Job for PID {self.pid} retained {survivors} active processes after termination"
            )
        try:
            self._process.wait(timeout=max(1.0, grace))
        except (subprocess.TimeoutExpired, OSError) as error:
            raise ProcessTerminationError(f"Could not reap managed command PID {self.pid}") from error
        _close_windows_job(self._process)

    @staticmethod
    def _windows_job_active_processes(handle: Any) -> int:
        import ctypes
        from ctypes import wintypes

        class JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("TotalUserTime", ctypes.c_longlong),
                ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD),
                ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD),
                ("TotalTerminatedProcesses", wintypes.DWORD),
            ]

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.QueryInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
        )
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        information = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION()
        returned = wintypes.DWORD()
        queried = kernel32.QueryInformationJobObject(
            handle,
            1,
            ctypes.byref(information),
            ctypes.sizeof(information),
            ctypes.byref(returned),
        )
        if not queried:
            error = getattr(ctypes, "get_last_error")()
            raise OSError(error, getattr(ctypes, "FormatError")(error))
        return int(information.ActiveProcesses)

    @staticmethod
    def _terminate_windows_job_object(handle: Any) -> None:
        import ctypes

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = (ctypes.c_void_p, ctypes.c_uint)
        kernel32.TerminateJobObject.restype = ctypes.c_int
        if not kernel32.TerminateJobObject(handle, 1):
            error = getattr(ctypes, "get_last_error")()
            raise OSError(error, getattr(ctypes, "FormatError")(error))

    def _join_readers(self, errors: list[BaseException]) -> None:
        timeout = max(1.0, self._environment_handle._manager.termination_grace * 2)
        for reader in self._readers:
            reader.join(timeout)
            if reader.is_alive():
                errors.append(RuntimeError(f"Output reader {reader.name} did not finish"))

    def _make_result(self) -> ManagedProcessResult:
        returncode = self._process.poll()
        if returncode is None:
            returncode = -1
        with self._output_lock:
            stdout = bytes(self._stdout).decode("utf-8", errors="replace")
            stderr = bytes(self._stderr).decode("utf-8", errors="replace")
        return ManagedProcessResult(
            argv=self.argv,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            started_at=self._started_at,
            ended_at=max(self._started_at, time.time()),
        )

    def _new_cleanup_error(
        self,
        failures: Sequence[BaseException],
        result: ManagedProcessResult | None,
    ) -> ProcessCleanupError:
        initiating_error: ProcessError | None = None
        with self._condition:
            cause = self._cause
        if result is not None and cause is not None and cause[0] == "timeout" and cause[1] is not None:
            initiating_error = ProcessTimeoutError(
                cause[1],
                result,
                environment=self.environment,
                generation_id=self.generation_id,
            )
        return ProcessCleanupError(
            failures,
            result,
            argv=self.argv,
            environment=self.environment,
            generation_id=self.generation_id,
            initiating_error=initiating_error,
        )

    def _freeze_cleanup_error(self, failures: Sequence[BaseException]) -> ProcessCleanupError:
        result = self._make_result()
        error = self._new_cleanup_error(failures, result)
        with self._condition:
            self._terminal_error = error
            self._result = result
            self._done.set()
            self._condition.notify_all()
        with self._event_condition:
            self._streams_closed = True
            self._event_condition.notify_all()
        return error

    def _cleanup_failed_launch(self) -> tuple[BaseException, ...]:
        errors: list[BaseException] = []
        try:
            self._terminate_tree(self._environment_handle._manager.termination_grace)
        except BaseException as primary_error:
            try:
                self._terminate_uninitialized_process()
            except BaseException as fallback_error:
                errors.extend((primary_error, fallback_error))
        self._join_readers(errors)
        with self._condition:
            errors.extend(self._reader_errors)
        if not errors:
            try:
                self._release_once()
            except BaseException as error:
                errors.append(error)
        self._ownership_clean = not errors
        return tuple(errors)

    def _terminate_uninitialized_process(self) -> None:
        """Best-effort containment cleanup when post-Popen identity setup failed."""
        grace = self._environment_handle._manager.termination_grace
        if os.name != "nt":
            _terminate_posix_group(self.pid, grace=grace, process=self._process)
            return
        try:
            _close_windows_job(self._process)
        finally:
            if self._process.poll() is None:
                self._process.kill()
            self._process.wait(timeout=max(1.0, grace))

    def _release_once(self) -> None:
        with self._condition:
            if self._released or not self._registered:
                return
            self._environment_handle._release_process(self)
            self._released = True

    def _retry_cleanup(self) -> None:
        with self._cleanup_lock:
            if self._ownership_clean:
                return
            failures: list[BaseException] = []
            try:
                self._terminate_tree(self._environment_handle._manager.termination_grace)
            except BaseException as error:
                failures.append(error)
            self._join_readers(failures)
            if not failures:
                try:
                    self._release_once()
                except BaseException as error:
                    failures.append(error)
            if failures:
                raise self._terminal_error or self._new_cleanup_error(failures, self._make_result())
            self._ownership_clean = True

    def wait(self, timeout: float | None = None, *, check: bool = True) -> ManagedProcessResult:
        normalized_timeout = _validate_timeout(timeout)
        normalized_check = _validate_check(check)
        if normalized_timeout is None:
            self._done.wait()
        elif not self._done.wait(normalized_timeout):
            with self._condition:
                if self._process.poll() is None and not self._done.is_set():
                    self._request("timeout", normalized_timeout)
            self._done.wait()
        return self._outcome(normalized_check)

    async def wait_async(self, timeout: float | None = None, *, check: bool = True) -> ManagedProcessResult:
        normalized_timeout = _validate_timeout(timeout)
        normalized_check = _validate_check(check)
        try:
            return await _run_blocking(self.wait, normalized_timeout, check=normalized_check)
        except asyncio.CancelledError:
            cleanup = asyncio.ensure_future(_run_blocking(self.close))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            try:
                cleanup.result()
            except BaseException:
                pass
            raise

    def __await__(self) -> Any:
        return self.wait_async().__await__()

    def _outcome(self, check: bool) -> ManagedProcessResult:
        assert self._result is not None
        if self._terminal_error is not None:
            raise self._terminal_error
        if check and self._result.returncode != 0:
            raise ProcessExitError(
                self._result,
                environment=self.environment,
                generation_id=self.generation_id,
            )
        return self._result

    async def events(self, *, replay: bool = True) -> AsyncIterator[OutputEvent]:
        if not isinstance(replay, bool):
            raise TypeError("replay must be a bool")
        with self._event_condition:
            cursor = self._events[0].sequence if replay and self._events else self._next_sequence
        while True:
            item = await _run_blocking(self._next_event, cursor, 0.1)
            if item is None:
                with self._event_condition:
                    if self._streams_closed:
                        return
                continue
            event, cursor = item
            yield event

    def wait_for_line(
        self,
        predicate: Callable[[OutputEvent], bool],
        timeout: float | None = None,
        *,
        replay: bool = True,
    ) -> OutputEvent:
        if not callable(predicate):
            raise TypeError("predicate must be callable")
        normalized_timeout = _validate_timeout(timeout)
        if not isinstance(replay, bool):
            raise TypeError("replay must be a bool")
        deadline = None if normalized_timeout is None else time.monotonic() + normalized_timeout
        with self._event_condition:
            cursor = self._events[0].sequence if replay and self._events else self._next_sequence
        while True:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            item = self._next_event(cursor, remaining)
            if item is None:
                if self._streams_closed:
                    raise EOFError(f"Command {self.argv!r} closed its output without a matching event")
                assert normalized_timeout is not None
                raise ProcessLineTimeoutError(
                    normalized_timeout,
                    argv=self.argv,
                    environment=self.environment,
                    generation_id=self.generation_id,
                )
            event, cursor = item
            if predicate(event):
                return event

    def _next_event(self, cursor: int, timeout: float | None) -> tuple[OutputEvent, int] | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._event_condition:
            while True:
                oldest = self._events[0].sequence if self._events else self._next_sequence
                if cursor < oldest:
                    raise ProcessEventLagError(
                        cursor,
                        oldest,
                        argv=self.argv,
                        environment=self.environment,
                        generation_id=self.generation_id,
                    )
                if cursor < self._next_sequence:
                    event = self._events[cursor - oldest]
                    return event, cursor + 1
                if self._streams_closed:
                    return None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                else:
                    remaining = None
                self._event_condition.wait(remaining)

    def terminate(self, timeout: float | None = None) -> None:
        grace = _validate_timeout(timeout)
        self._request("terminate", grace)
        self._done.wait()
        if isinstance(self._terminal_error, ProcessCleanupError):
            raise self._terminal_error

    def kill(self) -> None:
        self._request("kill", 0.0)
        self._done.wait()
        if isinstance(self._terminal_error, ProcessCleanupError):
            raise self._terminal_error

    def close(self) -> None:
        if not self._done.is_set():
            self._request("close", None)
            self._done.wait()
        if not self._ownership_clean:
            self._retry_cleanup()

    def __enter__(self) -> ManagedProcess:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> Literal[False]:
        self.close()
        return False


__all__ = [
    "ManagedProcess",
    "ManagedProcessResult",
    "OutputEvent",
    "OutputStream",
    "ProcessCleanupError",
    "ProcessError",
    "ProcessEventLagError",
    "ProcessExitError",
    "ProcessLineTimeoutError",
    "ProcessOutputLimitError",
    "ProcessTimeoutError",
]
