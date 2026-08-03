"""Pixi preparation and environment provisioning for Wetlands 2.0."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import platform
import re
import shlex
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import unicodedata
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, TYPE_CHECKING

from packaging.requirements import InvalidRequirement, Requirement
import psutil

from wetlands._internal import runtime_state
from wetlands._internal.artifact_registry import PIXI_SHA256, PIXI_VERSION
from wetlands._internal.process_termination import (
    ProcessIdentityError,
    ProcessTerminationError,
    capture_process_identity,
    terminate_launched_process_tree,
)
from wetlands.lifecycle import (
    EnvironmentInUseError,
    EnvironmentRecipeConflictError,
    UnmanagedTargetError,
)
from wetlands.operation import (
    Operation,
    OperationCanceled,
    OperationEventKind,
    OperationError,
    OperationFailure,
    PreparationError,
    ProvisioningError,
)
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION
from wetlands.specs import (
    MANAGED_DEBUGPY_VERSION,
    MANAGED_RUNTIME_PYPI,
    EnvironmentSpec,
    PixiInfo,
    ProvisioningStage,
    _parse_pinned_git_url,
    environment_name_key,
    validate_environment_name,
)

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager
    from wetlands.managed_environment import ManagedEnvironment
    from wetlands.specs import LocalPackage

READY_SCHEMA_VERSION = 2
OWNER_MARKER = ".wetlands-owned"
READY_DIRECTORY = ".wetlands"
READY_FILENAME = "ready.json"


@dataclass(frozen=True)
class ProvisioningStep:
    """An internal command step with a safe display representation."""

    id: str
    stage: ProvisioningStage
    argv: tuple[str, ...]
    cwd: Path | None = None
    environment: Mapping[str, str] | None = field(default=None, repr=False)
    display: str | None = None
    shell: bool = False


def _safe_command(argv: tuple[str, ...], display: str | None = None) -> str:
    if display is not None:
        return display
    rendered = subprocess.list2cmdline(argv) if os.name == "nt" else shlex.join(argv)
    rendered = re.sub(r"(?i)(https?://)([^/@\s]+)@", r"\1<redacted>@", rendered)
    rendered = re.sub(
        r"(?i)\b(authorization)(\s*:\s*)(?:basic|bearer)?\s*[^\s'\"]+",
        r"\1\2<redacted>",
        rendered,
    )
    rendered = re.sub(
        (
            r"(?i)(token|password|passwd|proxy|client_secret|api_key|access_key)"
            r"(\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s]+)"
        ),
        r"\1\2<redacted>",
        rendered,
    )
    rendered = re.sub(
        r"(?i)(--?(?:token|password|passwd|proxy|client-secret|api-key|access-key))(\s+)([^\s]+)",
        r"\1\2<redacted>",
        rendered,
    )
    return rendered


class CancelableFileLock:
    def __init__(
        self,
        path: Path,
        operation: Operation[Any] | None,
        stage: ProvisioningStage,
        *,
        environment_name: str | None = None,
        error_type: type[OperationError] | None = ProvisioningError,
    ):
        self.path = path
        self.operation = operation
        self.stage = stage
        self.environment_name = environment_name
        self.error_type = error_type
        self._file: Any = None

    def __enter__(self) -> CancelableFileLock:
        try:
            return self._acquire()
        except (OperationCanceled, OperationError):
            raise
        except OSError as error:
            if self._file is not None:
                with contextlib.suppress(OSError):
                    self._file.close()
            if self.operation is None or self.error_type is None:
                raise
            raise self.error_type(
                OperationFailure(
                    operation_id=self.operation.id,
                    stage=self.stage.value,
                    environment=self.environment_name,
                    message=f"Could not acquire lifecycle lock: {error}",
                )
            ) from error

    def _acquire(self) -> CancelableFileLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+b")
        if os.name == "nt":
            import msvcrt

            self._file.seek(0, os.SEEK_END)
            if self._file.tell() == 0:
                self._file.write(b"\0")
                self._file.flush()
            while True:
                if self.operation is not None and self.operation.cancellation_requested:
                    self._file.close()
                    raise OperationCanceled(self.operation.id)
                try:
                    self._file.seek(0)
                    msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
                    break
                except Exception:
                    if self.operation is not None:
                        self.operation._emit(
                            OperationEventKind.STEP,
                            "Waiting for lifecycle lock",
                            stage=self.stage.value,
                            environment=self.environment_name,
                        )
                    time.sleep(0.1)
        else:
            import fcntl

            while True:
                if self.operation is not None and self.operation.cancellation_requested:
                    self._file.close()
                    raise OperationCanceled(self.operation.id)
                try:
                    fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if self.operation is not None:
                        self.operation._emit(
                            OperationEventKind.STEP,
                            "Waiting for lifecycle lock",
                            stage=self.stage.value,
                            environment=self.environment_name,
                        )
                    time.sleep(0.1)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._file is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                self._file.close()


def environment_lifecycle_gate(
    manager: EnvironmentManager,
    name: str,
    *,
    operation: Operation[Any] | None = None,
    error_type: type[OperationError] | None = None,
) -> CancelableFileLock:
    """Return the cross-process gate for one canonical managed-environment path."""
    normalized_name = validate_environment_name(name)
    canonical_root = manager.environments_root.resolve(strict=False)
    canonical_target = canonical_root / environment_name_key(normalized_name)
    canonical_key = unicodedata.normalize(
        "NFC",
        os.path.normcase(str(canonical_target)),
    ).casefold()
    lock_name = hashlib.sha256(canonical_key.encode()).hexdigest()
    return CancelableFileLock(
        manager.state_root / "locks" / "environments" / f"{lock_name}.lock",
        operation,
        ProvisioningStage.LOCK_WAIT,
        environment_name=normalized_name,
        error_type=(error_type or ProvisioningError) if operation is not None else None,
    )


class _WindowsJob:
    """Minimal kill-on-close Job Object wrapper used without optional extensions."""

    _KILL_ON_CLOSE = 0x00002000
    _EXTENDED_LIMIT_INFORMATION = 9

    def __init__(self, process: subprocess.Popen[str]) -> None:
        import ctypes
        from ctypes import wintypes

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_uint64),
                ("WriteOperationCount", ctypes.c_uint64),
                ("OtherOperationCount", ctypes.c_uint64),
                ("ReadTransferCount", ctypes.c_uint64),
                ("WriteTransferCount", ctypes.c_uint64),
                ("OtherTransferCount", ctypes.c_uint64),
            ]

        class BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            error = getattr(ctypes, "get_last_error")()
            raise OSError(error, getattr(ctypes, "FormatError")(error))
        try:
            information = EXTENDED_LIMIT_INFORMATION()
            information.BasicLimitInformation.LimitFlags = self._KILL_ON_CLOSE
            if not kernel32.SetInformationJobObject(
                handle,
                self._EXTENDED_LIMIT_INFORMATION,
                ctypes.byref(information),
                ctypes.sizeof(information),
            ):
                error = getattr(ctypes, "get_last_error")()
                raise OSError(error, getattr(ctypes, "FormatError")(error))
            process_handle = wintypes.HANDLE(int(getattr(process, "_handle")))
            if not kernel32.AssignProcessToJobObject(handle, process_handle):
                error = getattr(ctypes, "get_last_error")()
                raise OSError(error, getattr(ctypes, "FormatError")(error))
        except BaseException:
            kernel32.CloseHandle(handle)
            raise
        self._kernel32 = kernel32
        self._handle = handle
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            if self._handle is None:
                return
            self._kernel32.CloseHandle(self._handle)
            self._handle = None


class ProcessTreeRunner:
    def __init__(
        self,
        operation: Operation[Any],
        *,
        grace: float,
        environment_name: str | None = None,
        error_type: type[OperationError] = ProvisioningError,
    ):
        self.operation = operation
        self.grace = grace
        self.environment_name = environment_name
        self.error_type = error_type
        self._lock = threading.RLock()
        self._active: subprocess.Popen[str] | None = None
        self._active_job: _WindowsJob | None = None
        self._active_closer: Callable[[], None] | None = None
        self._termination_error: str | None = None
        self._termination_finished = threading.Event()
        operation._set_cancel_callback(self.terminate_active)

    def run(self, step: ProvisioningStep) -> tuple[str, ...]:
        if self.operation.cancellation_requested:
            raise OperationCanceled(self.operation.id)
        command = _safe_command(step.argv, step.display)
        self.operation._emit(
            OperationEventKind.STEP,
            command,
            stage=step.stage.value,
            step_id=step.id,
            environment=self.environment_name,
        )
        child_environment = os.environ.copy()
        if step.environment:
            child_environment.update(step.environment)
        popen_command: Any = step.argv
        if step.shell:
            popen_command = subprocess.list2cmdline(step.argv) if os.name == "nt" else shlex.join(step.argv)
        kwargs: dict[str, Any] = {
            "cwd": str(step.cwd) if step.cwd is not None else None,
            "env": child_environment,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "errors": "replace",
            "bufsize": 1,
            "shell": step.shell,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP")
        else:
            kwargs["start_new_session"] = True
        with self._lock:
            if self.operation.cancellation_requested:
                raise OperationCanceled(self.operation.id)
            try:
                process = subprocess.Popen(popen_command, **kwargs)
            except OSError as error:
                failure = OperationFailure(
                    operation_id=self.operation.id,
                    stage=step.stage.value,
                    step_id=step.id,
                    message=f"Could not start provisioning step {step.id!r}: {error}",
                    command=command,
                    environment=self.environment_name,
                )
                raise self.error_type(failure) from error
            try:
                identity = capture_process_identity(process.pid)
                process._wetlands_started_at = identity.started_at  # type: ignore[attr-defined]
                process._wetlands_process_group_id = identity.process_group_id  # type: ignore[attr-defined]
                process._wetlands_session_id = identity.session_id  # type: ignore[attr-defined]
                if os.name != "nt" and (identity.process_group_id != process.pid or identity.session_id != process.pid):
                    raise ProcessIdentityError(
                        f"Provisioning process {process.pid} did not start in its own POSIX session"
                    )
            except BaseException as error:
                with contextlib.suppress(OSError):
                    process.kill()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=self.grace)
                failure = OperationFailure(
                    operation_id=self.operation.id,
                    stage=step.stage.value,
                    step_id=step.id,
                    message=f"Could not establish process ownership for provisioning step {step.id!r}",
                    command=command,
                    environment=self.environment_name,
                    cleanup_error=str(error),
                )
                raise self.error_type(failure) from error
            self._active = process
            self._termination_error = None
            self._termination_finished.clear()
            if os.name == "nt":
                try:
                    self._active_job = _WindowsJob(process)
                except Exception:
                    self.operation._emit(
                        OperationEventKind.STEP,
                        "Windows Job Object unavailable; using recursive process termination",
                        stage=step.stage.value,
                        step_id=step.id,
                        environment=self.environment_name,
                    )
        stdout_tail: deque[str] = deque(maxlen=100)
        stderr_tail: deque[str] = deque(maxlen=100)
        reader_errors: list[str] = []
        reader_error_lock = threading.Lock()

        def drain(stream: Any, name: str, tail: deque[str]) -> None:
            if stream is None:
                return
            try:
                for raw_line in iter(stream.readline, ""):
                    line = raw_line.rstrip("\r\n")
                    safe_line = _safe_command((line,))
                    tail.append(safe_line)
                    self.operation._emit(
                        OperationEventKind.OUTPUT,
                        safe_line,
                        stage=step.stage.value,
                        step_id=step.id,
                        stream=name,
                        line=safe_line,
                        environment=self.environment_name,
                    )
            except BaseException as error:
                with reader_error_lock:
                    reader_errors.append(f"{name} reader failed: {error}")

        readers = [
            threading.Thread(target=drain, args=(process.stdout, "stdout", stdout_tail), daemon=True),
            threading.Thread(target=drain, args=(process.stderr, "stderr", stderr_tail), daemon=True),
        ]
        for reader in readers:
            reader.start()
        returncode: int | None = None
        while returncode is None:
            try:
                returncode = process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                if self.operation.cancellation_requested and self._termination_finished.is_set():
                    with self._lock:
                        termination_error = self._termination_error
                    if termination_error is not None:
                        break

        cleanup_errors: list[str] = []
        if returncode is None:
            try:
                returncode = process.wait(timeout=max(1.0, self.grace))
            except subprocess.TimeoutExpired:
                pass
        if self.operation.cancellation_requested:
            wait_timeout = max(1.0, (self.grace * 2) + 0.1)
            if not self._termination_finished.wait(timeout=wait_timeout):
                cleanup_errors.append("process-tree termination callback did not finish")
        if returncode is not None:
            try:
                self._verify_finished_tree(process, self._active_job)
            except ProcessTerminationError as error:
                cleanup_errors.append(str(error))
        else:
            cleanup_errors.append("provisioning process did not terminate and could not be reaped")

        for reader in readers:
            reader.join(timeout=max(1.0, self.grace))
        live_readers = [reader.name for reader in readers if reader.is_alive()]
        if live_readers:
            cleanup_errors.append(f"output readers did not terminate: {', '.join(live_readers)}")
        with reader_error_lock:
            cleanup_errors.extend(reader_errors)
        with self._lock:
            if self._active is process:
                self._active = None
            termination_error = self._termination_error
            job = self._active_job
            self._active_job = None
        if termination_error is not None:
            cleanup_errors.append(termination_error)
        if job is not None:
            try:
                job.close()
            except BaseException as error:
                cleanup_errors.append(f"Windows Job Object cleanup failed: {error}")
        if self.operation.cancellation_requested:
            if cleanup_errors:
                failure = OperationFailure(
                    operation_id=self.operation.id,
                    stage=step.stage.value,
                    step_id=step.id,
                    message=f"Cancellation cleanup failed for provisioning step {step.id!r}",
                    command=command,
                    returncode=returncode,
                    stdout_tail=tuple(stdout_tail),
                    stderr_tail=tuple(stderr_tail),
                    environment=self.environment_name,
                    cleanup_error="; ".join(dict.fromkeys(cleanup_errors)),
                )
                raise self.error_type(failure)
            raise OperationCanceled(self.operation.id)
        if cleanup_errors:
            failure = OperationFailure(
                operation_id=self.operation.id,
                stage=step.stage.value,
                step_id=step.id,
                message=f"Process cleanup failed for provisioning step {step.id!r}",
                command=command,
                returncode=returncode,
                stdout_tail=tuple(stdout_tail),
                stderr_tail=tuple(stderr_tail),
                environment=self.environment_name,
                cleanup_error="; ".join(dict.fromkeys(cleanup_errors)),
            )
            raise self.error_type(failure)
        assert returncode is not None
        if returncode != 0:
            failure = OperationFailure(
                operation_id=self.operation.id,
                stage=step.stage.value,
                step_id=step.id,
                message=f"Provisioning step {step.id!r} failed with exit code {returncode}",
                command=command,
                returncode=returncode,
                stdout_tail=tuple(stdout_tail),
                stderr_tail=tuple(stderr_tail),
                environment=self.environment_name,
            )
            raise self.error_type(failure)
        return tuple(stdout_tail)

    def set_active_closer(self, closer: Callable[[], None]) -> None:
        with self._lock:
            self._active_closer = closer
            requested = self.operation.cancellation_requested
        if requested:
            try:
                closer()
            except BaseException as error:
                self._record_termination_error(f"active resource cleanup failed: {error}")

    def clear_active_closer(self, closer: Callable[[], None]) -> None:
        with self._lock:
            if self._active_closer == closer:
                self._active_closer = None

    def terminate_active(self) -> None:
        with self._lock:
            process = self._active
            job = self._active_job
            closer = self._active_closer
        try:
            if closer is not None:
                try:
                    closer()
                except BaseException as error:
                    self._record_termination_error(f"active resource cleanup failed: {error}")
            if process is None:
                return
            try:
                if os.name == "nt":
                    self._terminate_windows_process_tree(process, job)
                else:
                    terminate_launched_process_tree(
                        process,
                        grace=self.grace,
                        close_windows_job=lambda _process: None,
                    )
            except BaseException as error:
                self._record_termination_error(str(error))
        finally:
            self._termination_finished.set()

    def _record_termination_error(self, message: str) -> None:
        with self._lock:
            if self._termination_error is None:
                self._termination_error = message
            elif message not in self._termination_error:
                self._termination_error = f"{self._termination_error}; {message}"

    def _verify_finished_tree(
        self,
        process: subprocess.Popen[str],
        job: _WindowsJob | None,
    ) -> None:
        if os.name == "nt":
            if process.poll() is None:
                self._terminate_windows_process_tree(process, job)
            elif job is not None:
                job.close()
            return
        terminate_launched_process_tree(
            process,
            grace=self.grace,
            close_windows_job=lambda _process: None,
        )

    def _terminate_windows_process_tree(
        self,
        process: subprocess.Popen[str],
        job: _WindowsJob | None,
    ) -> None:
        expected_started_at = getattr(process, "_wetlands_started_at", None)
        try:
            parent = psutil.Process(process.pid)
            if not isinstance(expected_started_at, (int, float)) or parent.create_time() != expected_started_at:
                raise ProcessIdentityError(
                    f"Refusing to terminate provisioning PID {process.pid}: its process start identity changed"
                )
            descendants = parent.children(recursive=True)
        except psutil.NoSuchProcess:
            if job is not None:
                job.close()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=self.grace)
            return
        except psutil.AccessDenied as error:
            raise ProcessTerminationError(
                f"Could not inspect provisioning process tree rooted at PID {process.pid}"
            ) from error

        targets = [*descendants, parent]
        for target in targets:
            with contextlib.suppress(psutil.NoSuchProcess):
                target.terminate()
        _, survivors = psutil.wait_procs(targets, timeout=self.grace)
        if survivors and job is not None:
            job.close()
        for survivor in survivors:
            with contextlib.suppress(psutil.NoSuchProcess):
                survivor.kill()
        _, survivors = psutil.wait_procs(survivors, timeout=self.grace)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=self.grace)
        if process.poll() is None or survivors:
            survivor_pids = sorted(target.pid for target in survivors)
            raise ProcessTerminationError(
                f"Provisioning process tree rooted at PID {process.pid} survived forced "
                f"termination; surviving PIDs: {survivor_pids}"
            )


def _pixi_target() -> str:
    architecture = "aarch64" if platform.machine().lower() in {"aarch64", "arm64"} else "x86_64"
    if platform.system() == "Windows":
        return f"pixi-{architecture}-pc-windows-msvc.zip"
    if platform.system() == "Darwin":
        return f"pixi-{architecture}-apple-darwin.tar.gz"
    return f"pixi-{architecture}-unknown-linux-musl.tar.gz"


def _pixi_executable(root: Path) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return root / "bin" / f"pixi{suffix}"


def _copy_cancelable(source: Any, destination: Any, operation: Operation[Any]) -> None:
    while True:
        if operation.cancellation_requested:
            raise OperationCanceled(operation.id)
        block = source.read(1024 * 256)
        if not block:
            return
        destination.write(block)


def _detect_pixi_version(executable: Path, runner: ProcessTreeRunner) -> str:
    lines = runner.run(
        ProvisioningStep(
            "pixi-version",
            ProvisioningStage.PIXI_DISCOVERY,
            (str(executable), "--version"),
        )
    )
    match = re.search(r"\b([0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)\b", "\n".join(lines))
    if match is None:
        raise PreparationError(
            OperationFailure(
                operation_id=runner.operation.id,
                stage=ProvisioningStage.PIXI_DISCOVERY.value,
                message=f"Could not determine Pixi version from {executable}",
                command=_safe_command((str(executable), "--version")),
            )
        )
    return match.group(1)


def _prepare_pixi_impl(manager: EnvironmentManager, operation: Operation[Any]) -> PixiInfo:
    runner = ProcessTreeRunner(
        operation,
        grace=manager.termination_grace,
        error_type=PreparationError,
    )
    lock = manager.state_root / "locks" / "pixi.lock"
    with CancelableFileLock(
        lock,
        operation,
        ProvisioningStage.LOCK_WAIT,
        error_type=PreparationError,
    ):
        if manager.pixi_executable is not None:
            executable = manager.pixi_executable
            if not executable.is_file():
                raise PreparationError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.PIXI_DISCOVERY.value,
                        message=f"Configured Pixi executable does not exist: {executable}",
                    )
                )
            return PixiInfo(executable, _detect_pixi_version(executable, runner), False)

        install_root = manager.root / "pixi"
        executable = _pixi_executable(install_root)
        marker = install_root / "bin" / ".wetlands-pixi-version"
        expected_version = PIXI_VERSION.removeprefix("v")
        if executable.is_file() and marker.is_file():
            if marker.read_text(encoding="utf-8").strip() == PIXI_VERSION:
                version = _detect_pixi_version(executable, runner)
                if version == expected_version:
                    return PixiInfo(executable, version, True)

        artifact = _pixi_target()
        expected_checksum = PIXI_SHA256.get(artifact)
        if expected_checksum is None:
            raise PreparationError(
                OperationFailure(
                    operation_id=operation.id,
                    stage=ProvisioningStage.PIXI_DISCOVERY.value,
                    message=f"No trusted Pixi artifact is registered for this platform: {artifact}",
                )
            )
        url = f"https://github.com/prefix-dev/pixi/releases/download/{PIXI_VERSION}/{artifact}"
        install_root.mkdir(parents=True, exist_ok=True)
        bin_root = install_root / "bin"
        bin_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".pixi-install-", dir=bin_root) as temporary:
            archive_path = Path(temporary) / artifact
            staged_path = Path(temporary) / executable.name
            operation._emit(
                OperationEventKind.STEP,
                f"Downloading Pixi {PIXI_VERSION}",
                stage=ProvisioningStage.PIXI_DOWNLOAD.value,
                step_id="pixi-download",
            )
            proxy_handler = urllib.request.ProxyHandler(
                {scheme: value for scheme, value in (manager.network or {}).items() if scheme in {"http", "https"}}
            )
            opener = urllib.request.build_opener(proxy_handler)
            digest = hashlib.sha256()
            response: Any = None
            close_response: Callable[[], None] | None = None
            try:
                response = opener.open(url, timeout=10)
                close_response = response.close
                assert close_response is not None
                runner.set_active_closer(close_response)
                with response, archive_path.open("wb") as destination:
                    total = int(response.headers.get("Content-Length", "0")) or None
                    current = 0
                    while True:
                        if operation.cancellation_requested:
                            raise OperationCanceled(operation.id)
                        block = response.read(1024 * 256)
                        if not block:
                            break
                        destination.write(block)
                        digest.update(block)
                        current += len(block)
                        operation._emit(
                            OperationEventKind.PROGRESS,
                            "Downloading Pixi",
                            stage=ProvisioningStage.PIXI_DOWNLOAD.value,
                            step_id="pixi-download",
                            current=current,
                            maximum=total,
                        )
            except OperationCanceled:
                archive_path.unlink(missing_ok=True)
                raise
            except BaseException as error:
                archive_path.unlink(missing_ok=True)
                if operation.cancellation_requested:
                    raise OperationCanceled(operation.id) from error
                raise PreparationError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.PIXI_DOWNLOAD.value,
                        step_id="pixi-download",
                        message=f"Could not download Pixi {PIXI_VERSION}: {error}",
                    )
                ) from error
            finally:
                if close_response is not None:
                    runner.clear_active_closer(close_response)
            if digest.hexdigest() != expected_checksum:
                archive_path.unlink(missing_ok=True)
                raise PreparationError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.PIXI_VERIFY.value,
                        message="Downloaded Pixi archive failed checksum verification",
                    )
                )
            try:
                if artifact.endswith(".zip"):
                    with zipfile.ZipFile(archive_path) as archive:
                        zip_members = [
                            member
                            for member in archive.infolist()
                            if Path(member.filename).name in {"pixi", "pixi.exe"}
                        ]
                        if len(zip_members) != 1:
                            raise ValueError("Pixi archive did not contain exactly one executable")
                        with archive.open(zip_members[0]) as source, staged_path.open("wb") as destination:
                            _copy_cancelable(source, destination, operation)
                else:
                    with tarfile.open(archive_path, "r:gz") as archive:
                        tar_members = [
                            member for member in archive.getmembers() if Path(member.name).name in {"pixi", "pixi.exe"}
                        ]
                        if len(tar_members) != 1:
                            raise ValueError("Pixi archive did not contain exactly one executable")
                        tar_source = archive.extractfile(tar_members[0])
                        if tar_source is None:
                            raise ValueError("Could not extract Pixi executable")
                        with tar_source, staged_path.open("wb") as destination:
                            _copy_cancelable(tar_source, destination, operation)
            except OperationCanceled:
                raise
            except BaseException as error:
                if operation.cancellation_requested:
                    raise OperationCanceled(operation.id) from error
                raise PreparationError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.PIXI_INSTALL.value,
                        message=f"Could not extract the verified Pixi archive: {error}",
                    )
                ) from error
            if os.name != "nt":
                staged_path.chmod(0o755)
            version = _detect_pixi_version(staged_path, runner)
            if version != expected_version:
                raise PreparationError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.PIXI_VERIFY.value,
                        message=f"Expected Pixi {expected_version}, found {version}",
                    )
                )
            os.replace(staged_path, executable)
            _atomic_write(marker, f"{PIXI_VERSION}\n".encode())
        return PixiInfo(executable, expected_version, True)


def prepare_pixi(manager: EnvironmentManager, operation: Operation[Any]) -> PixiInfo:
    try:
        return _prepare_pixi_impl(manager, operation)
    except (OperationCanceled, PreparationError):
        raise
    except BaseException as error:
        raise PreparationError(
            OperationFailure(
                operation_id=operation.id,
                stage=ProvisioningStage.PIXI_INSTALL.value,
                message=f"Pixi preparation failed: {error}",
            )
        ) from error


def _toml_quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _split_conda_dependency(dependency: str) -> tuple[str, str]:
    value = dependency.split("::", 1)[-1].strip()
    match = re.match(r"^([A-Za-z0-9_.-]+)\s*(.*)$", value)
    if match is None:
        raise ValueError(f"Invalid conda dependency: {dependency!r}")
    return match.group(1), match.group(2).strip() or "*"


def _render_pypi_dependency(dependency: str) -> tuple[str, str]:
    try:
        requirement = Requirement(dependency)
    except InvalidRequirement as error:
        raise ValueError(f"Invalid PyPI dependency: {dependency!r}") from error
    if requirement.marker is not None:
        raise ValueError(f"PyPI environment markers are not supported in EnvironmentSpec: {dependency!r}")
    if requirement.url:
        parsed_url = urllib.parse.urlsplit(requirement.url)
        if parsed_url.scheme == "git+https":
            repository_url, revision = _parse_pinned_git_url(requirement.url)
            fields = [
                f"git = {_toml_quote(repository_url)}",
                f"rev = {_toml_quote(revision)}",
            ]
            if requirement.extras:
                extras = ", ".join(_toml_quote(extra) for extra in sorted(requirement.extras))
                fields.append(f"extras = [{extras}]")
            return requirement.name, f"{{ {', '.join(fields)} }}"
        if requirement.extras:
            extras = ", ".join(_toml_quote(extra) for extra in sorted(requirement.extras))
            return requirement.name, f"{{ url = {_toml_quote(requirement.url)}, extras = [{extras}] }}"
        return requirement.name, f"{{ url = {_toml_quote(requirement.url)} }}"
    version = str(requirement.specifier) or "*"
    if requirement.extras:
        extras = ", ".join(_toml_quote(extra) for extra in sorted(requirement.extras))
        return requirement.name, f"{{ version = {_toml_quote(version)}, extras = [{extras}] }}"
    return requirement.name, _toml_quote(version)


def _render_local_dependency(package: LocalPackage) -> str:
    fields = [f"path = {_toml_quote(str(package.source))}"]
    if package.editable:
        fields.append("editable = true")
    if package.extras:
        extras = ", ".join(_toml_quote(extra) for extra in sorted(package.extras))
        fields.append(f"extras = [{extras}]")
    return f"{{ {', '.join(fields)} }}"


def render_pixi_manifest(name: str, spec: EnvironmentSpec) -> bytes:
    channels = list(spec.channels)
    for dependency in spec.conda:
        if "::" in dependency:
            channel = dependency.split("::", 1)[0]
            if channel not in channels:
                channels.append(channel)
    machine = platform.machine().lower()
    if platform.system() == "Windows":
        target_platform = "win-64"
    elif platform.system() == "Darwin":
        target_platform = "osx-arm64" if machine in {"arm64", "aarch64"} else "osx-64"
    else:
        target_platform = "linux-aarch64" if machine in {"arm64", "aarch64"} else "linux-64"
    lines = [
        "[workspace]",
        f"name = {_toml_quote(name)}",
        f"channels = [{', '.join(_toml_quote(channel) for channel in channels)}]",
        f"platforms = [{_toml_quote(target_platform)}]",
        "",
        "[dependencies]",
        f"python = {_toml_quote(spec.python)}",
    ]
    for dependency in sorted(spec.conda):
        conda_package, constraint = _split_conda_dependency(dependency)
        lines.append(f"{_toml_quote(conda_package)} = {_toml_quote(constraint)}")
    pypi_dependencies = (*spec.pypi, *MANAGED_RUNTIME_PYPI)
    if pypi_dependencies or spec.local:
        lines.extend(("", "[pypi-dependencies]"))
        for dependency in sorted(pypi_dependencies):
            pypi_package, rendered = _render_pypi_dependency(dependency)
            lines.append(f"{_toml_quote(pypi_package)} = {rendered}")
        for local_package in sorted(
            spec.local,
            key=lambda item: item.distribution_name,
        ):
            lines.append(f"{_toml_quote(local_package.distribution_name)} = {_render_local_dependency(local_package)}")
    lines.append("")
    return "\n".join(lines).encode()


def _metadata_recipe(spec: EnvironmentSpec) -> dict[str, Any]:
    recipe = spec.normalized()
    commands: list[dict[str, Any]] = []
    for command in spec.post_install:
        argv_payload = json.dumps(command.argv, separators=(",", ":")).encode()
        commands.append(
            {
                "display": _safe_command(command.argv, command.display),
                "shell": command.shell,
                "argv_sha256": hashlib.sha256(argv_payload).hexdigest(),
            }
        )
    recipe["post_install"] = commands
    return recipe


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary_name)
        raise


def _ready_path(target: Path) -> Path:
    return target / READY_DIRECTORY / READY_FILENAME


def _is_link_or_reparse(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    if stat.S_ISLNK(metadata.st_mode):
        return True
    attributes = getattr(metadata, "st_file_attributes", None)
    if os.name == "nt" and attributes is None:
        return True
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool((attributes or 0) & reparse_flag)


def _regular_file(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(metadata.st_mode) and not _is_link_or_reparse(path)


_FileIdentity = tuple[int, int, int]
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
_POSIX_FILE_IDENTITY = 0
_WINDOWS_FILE_IDENTITY = 1
_WINDOWS_LEGACY_FILE_IDENTITY = 2


def _file_identity(metadata: os.stat_result) -> _FileIdentity:
    return _POSIX_FILE_IDENTITY, metadata.st_dev, metadata.st_ino


def _windows_handle_identity(handle: int) -> _FileIdentity:
    """Return the volume and file ID for an already-open Windows handle."""
    import ctypes
    from ctypes import wintypes

    class _FileId128(ctypes.Structure):
        _fields_ = (("identifier", ctypes.c_ubyte * 16),)

    class _FileIdInfo(ctypes.Structure):
        _fields_ = (
            ("volume_serial_number", ctypes.c_ulonglong),
            ("file_id", _FileId128),
        )

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = (
            ("file_attributes", wintypes.DWORD),
            ("creation_time", wintypes.FILETIME),
            ("last_access_time", wintypes.FILETIME),
            ("last_write_time", wintypes.FILETIME),
            ("volume_serial_number", wintypes.DWORD),
            ("file_size_high", wintypes.DWORD),
            ("file_size_low", wintypes.DWORD),
            ("number_of_links", wintypes.DWORD),
            ("file_index_high", wintypes.DWORD),
            ("file_index_low", wintypes.DWORD),
        )

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    native_handle = wintypes.HANDLE(handle)
    get_information_ex = kernel32.GetFileInformationByHandleEx
    get_information_ex.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    get_information_ex.restype = wintypes.BOOL
    information = _FileIdInfo()
    # FileIdInfo is the 128-bit identifier required for ReFS and other modern
    # Windows filesystems. The older 64-bit file index can legitimately be zero.
    if get_information_ex(
        native_handle,
        18,
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        file_id = int.from_bytes(bytes(information.file_id.identifier), "little")
        if file_id:
            return (
                _WINDOWS_FILE_IDENTITY,
                int(information.volume_serial_number),
                file_id,
            )

    # Retain compatibility with filesystems or old Windows releases that do not
    # implement FileIdInfo but do provide the traditional 64-bit file index.
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    )
    get_information.restype = wintypes.BOOL
    legacy = _ByHandleFileInformation()
    if get_information(native_handle, ctypes.byref(legacy)):
        file_index = (int(legacy.file_index_high) << 32) | int(legacy.file_index_low)
        if file_index:
            return (
                _WINDOWS_LEGACY_FILE_IDENTITY,
                int(legacy.volume_serial_number),
                file_index,
            )

    error = getattr(ctypes, "get_last_error")()
    detail = getattr(ctypes, "FormatError")(error) if error else "no stable file ID was reported"
    raise RuntimeError(f"Windows file identity is unavailable: {detail}")


def _windows_path_identity(path: Path) -> _FileIdentity:
    """Open *path* without following a reparse point and read its file ID."""
    import ctypes
    from ctypes import wintypes

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    handle = create_file(
        os.path.abspath(path),
        0,
        0x00000001 | 0x00000002 | 0x00000004,
        None,
        3,
        0x00200000 | 0x02000000,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle in (None, invalid_handle):
        error = getattr(ctypes, "get_last_error")()
        raise OSError(error, getattr(ctypes, "FormatError")(error), path)
    try:
        return _windows_handle_identity(int(handle))
    finally:
        close_handle(handle)


def _path_identity(path: Path) -> _FileIdentity:
    if os.name == "nt":
        return _windows_path_identity(path)
    return _file_identity(path.lstat())


def _descriptor_identity(descriptor: int) -> _FileIdentity:
    if os.name == "nt":
        import msvcrt

        return _windows_handle_identity(msvcrt.get_osfhandle(descriptor))  # type: ignore[attr-defined]
    return _file_identity(os.fstat(descriptor))


def _require_identity(
    metadata: os.stat_result,
    expected: _FileIdentity,
    *,
    description: str,
) -> None:
    if _file_identity(metadata) != expected:
        raise RuntimeError(f"{description} changed identity")


def _require_direct_target(root: Path, target: Path) -> None:
    absolute_root = Path(os.path.abspath(root))
    absolute_target = Path(os.path.abspath(target))
    if absolute_target.parent != absolute_root or absolute_target == absolute_root:
        raise RuntimeError(f"Refusing to operate outside managed environment root: {target}")


def _revalidate_path(path: Path, expected: _FileIdentity, *, description: str) -> None:
    try:
        identity = _path_identity(path)
    except OSError as error:
        raise RuntimeError(f"{description} is no longer reachable at {path}") from error
    if _is_link_or_reparse(path):
        raise RuntimeError(f"{description} became a link or reparse point: {path}")
    if identity != expected:
        raise RuntimeError(f"{description} changed identity")


def _revalidate_entry(
    directory_fd: int,
    name: str,
    expected: _FileIdentity,
    *,
    description: str,
) -> None:
    try:
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as error:
        raise RuntimeError(f"{description} is no longer present") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise RuntimeError(f"{description} became a symbolic link")
    _require_identity(metadata, expected, description=description)


@contextlib.contextmanager
def _open_posix_directory(
    path: Path,
    *,
    expected_identity: _FileIdentity | None = None,
    description: str,
) -> Iterator[tuple[int, _FileIdentity]]:
    if _is_link_or_reparse(path):
        raise RuntimeError(f"Refusing to open linked {description}: {path}")
    before = os.stat(path, follow_symlinks=False)
    if not stat.S_ISDIR(before.st_mode):
        raise RuntimeError(f"{description} is not a directory: {path}")
    descriptor = os.open(path, _DIRECTORY_FLAGS)
    try:
        identity = _file_identity(os.fstat(descriptor))
        _require_identity(before, identity, description=description)
        if expected_identity is not None and identity != expected_identity:
            raise RuntimeError(f"{description} changed identity")
        _revalidate_path(path, identity, description=description)
        yield descriptor, identity
        _revalidate_path(path, identity, description=description)
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def _open_posix_target(
    root: Path,
    target: Path,
    *,
    expected_identity: _FileIdentity | None = None,
    require_marker: bool,
    revalidate_on_exit: bool = True,
) -> Iterator[tuple[int, int, _FileIdentity, _FileIdentity]]:
    _require_direct_target(root, target)
    with _open_posix_directory(
        root,
        description="managed environment root",
    ) as (root_fd, root_identity):
        metadata = os.stat(target.name, dir_fd=root_fd, follow_symlinks=False)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise RuntimeError(f"Managed environment target is not a directory: {target}")
        target_fd = os.open(target.name, _DIRECTORY_FLAGS, dir_fd=root_fd)
        try:
            target_identity = _file_identity(os.fstat(target_fd))
            _require_identity(metadata, target_identity, description="managed environment target")
            if expected_identity is not None and target_identity != expected_identity:
                raise RuntimeError(f"Managed environment target changed identity: {target}")
            _revalidate_entry(
                root_fd,
                target.name,
                target_identity,
                description="managed environment target",
            )
            if require_marker and not _valid_owner_marker_at(target_fd):
                raise RuntimeError(f"Refusing to remove an unmanaged target: {target}")
            yield root_fd, target_fd, root_identity, target_identity
            if revalidate_on_exit:
                _revalidate_entry(
                    root_fd,
                    target.name,
                    target_identity,
                    description="managed environment target",
                )
        finally:
            os.close(target_fd)


def _read_regular_file_at(
    directory_fd: int,
    name: str,
    *,
    maximum_bytes: int | None = None,
) -> bytes:
    before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise RuntimeError(f"Refusing to read non-regular file {name!r}")
    descriptor = os.open(name, _READ_FLAGS, dir_fd=directory_fd)
    try:
        opened = os.fstat(descriptor)
        _require_identity(before, _file_identity(opened), description=f"file {name!r}")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if maximum_bytes is not None and size > maximum_bytes:
                raise RuntimeError(f"File {name!r} exceeds its safe size limit")
            chunks.append(chunk)
        _revalidate_entry(
            directory_fd,
            name,
            _file_identity(opened),
            description=f"file {name!r}",
        )
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _valid_owner_marker_at(target_fd: int) -> bool:
    try:
        marker = _read_regular_file_at(target_fd, OWNER_MARKER, maximum_bytes=4096)
    except (OSError, RuntimeError):
        return False
    return bool(marker.strip())


def _target_guard(
    root: Path,
    target: Path,
    root_fd: int,
    root_identity: _FileIdentity,
    target_identity: _FileIdentity,
) -> None:
    _revalidate_path(root, root_identity, description="managed environment root")
    _revalidate_entry(
        root_fd,
        target.name,
        target_identity,
        description="managed environment target",
    )


def _atomic_write_at(
    directory_fd: int,
    name: str,
    content: bytes,
    *,
    guard: Callable[[], None],
) -> None:
    temporary_name = f".{name}.{uuid.uuid4().hex}.tmp"
    guard()
    descriptor = os.open(temporary_name, _WRITE_FLAGS, 0o600, dir_fd=directory_fd)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    except BaseException:
        os.close(descriptor)
        with contextlib.suppress(OSError):
            os.unlink(temporary_name, dir_fd=directory_fd)
        raise
    else:
        os.close(descriptor)
    try:
        guard()
        os.replace(
            temporary_name,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
        guard()
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary_name, dir_fd=directory_fd)
        raise


def _ensure_managed_root(root: Path) -> None:
    if os.path.lexists(root):
        if _is_link_or_reparse(root) or not root.is_dir():
            raise RuntimeError(f"Refusing to use linked or non-directory environment root: {root}")
        return
    parent = root.parent
    if _is_link_or_reparse(parent):
        raise RuntimeError(f"Refusing to create an environment root below a linked parent: {parent}")
    if os.name == "nt":
        parent_identity = _path_identity(parent)
        root.mkdir()
        _revalidate_path(parent, parent_identity, description="environment root parent")
        if _is_link_or_reparse(root):
            raise RuntimeError(f"New environment root is a reparse point: {root}")
        return
    with _open_posix_directory(parent, description="environment root parent") as (
        parent_fd,
        parent_identity,
    ):
        os.mkdir(root.name, dir_fd=parent_fd)
        root_metadata = os.stat(root.name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
            raise RuntimeError(f"New environment root is not a safe directory: {root}")
        _revalidate_path(parent, parent_identity, description="environment root parent")
        _revalidate_entry(
            parent_fd,
            root.name,
            _file_identity(root_metadata),
            description="managed environment root",
        )


def _create_managed_target(root: Path, target: Path) -> _FileIdentity:
    _require_direct_target(root, target)
    _ensure_managed_root(root)
    if os.name == "nt":
        root_identity = _path_identity(root)
        if _is_link_or_reparse(root):
            raise RuntimeError(f"Refusing to use linked managed environment root: {root}")
        target.mkdir()
        if _is_link_or_reparse(target):
            raise RuntimeError(f"New environment target is a reparse point: {target}")
        identity = _path_identity(target)
        _revalidate_path(root, root_identity, description="managed environment root")
        _revalidate_path(target, identity, description="managed environment target")
        return identity
    with _open_posix_directory(root, description="managed environment root") as (
        root_fd,
        root_identity,
    ):
        os.mkdir(target.name, dir_fd=root_fd)
        metadata = os.stat(target.name, dir_fd=root_fd, follow_symlinks=False)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise RuntimeError(f"New environment target is not a safe directory: {target}")
        identity = _file_identity(metadata)
        _target_guard(root, target, root_fd, root_identity, identity)
        return identity


def _write_target_file(
    root: Path,
    target: Path,
    name: str,
    content: bytes,
    *,
    expected_identity: _FileIdentity,
    require_marker: bool,
) -> None:
    if os.name == "nt":
        _assert_managed_target(
            root,
            target,
            require_marker=require_marker,
            expected_identity=expected_identity,
        )
        _atomic_write(target / name, content)
        _assert_managed_target(
            root,
            target,
            require_marker=require_marker or name == OWNER_MARKER,
            expected_identity=expected_identity,
        )
        return
    with _open_posix_target(
        root,
        target,
        expected_identity=expected_identity,
        require_marker=require_marker,
    ) as (root_fd, target_fd, root_identity, target_identity):

        def guard() -> None:
            _target_guard(
                root,
                target,
                root_fd,
                root_identity,
                target_identity,
            )

        _atomic_write_at(target_fd, name, content, guard=guard)


def _read_target_file(
    root: Path,
    target: Path,
    name: str,
    *,
    expected_identity: _FileIdentity,
) -> bytes:
    if os.name == "nt":
        _assert_managed_target(
            root,
            target,
            require_marker=True,
            expected_identity=expected_identity,
        )
        value = _read_regular_file_windows(target / name)
        _assert_managed_target(
            root,
            target,
            require_marker=True,
            expected_identity=expected_identity,
        )
        return value
    with _open_posix_target(
        root,
        target,
        expected_identity=expected_identity,
        require_marker=True,
    ) as (_, target_fd, _, _):
        return _read_regular_file_at(target_fd, name)


def _read_regular_file_windows(path: Path, *, maximum_bytes: int | None = None) -> bytes:
    if not _regular_file(path):
        raise RuntimeError(f"Refusing to read linked or non-regular file: {path}")
    before = _path_identity(path)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    try:
        opened = _descriptor_identity(descriptor)
        if before != opened:
            raise RuntimeError(f"file {path} changed identity")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if maximum_bytes is not None and size > maximum_bytes:
                raise RuntimeError(f"File {path} exceeds its safe size limit")
            chunks.append(chunk)
        _revalidate_path(path, opened, description=f"file {path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _valid_ready_payload(
    value: Any,
    *,
    target: Path,
    canonical_target: str,
    manifest: bytes,
    lock: bytes,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    required_strings = (
        "name",
        "canonical_path",
        "recipe_hash",
        "manifest_sha256",
        "lock_sha256",
        "generation_id",
        "operation_id",
        "pixi_version",
        "pixi_executable",
    )
    if (
        value.get("schema_version") != READY_SCHEMA_VERSION
        or value.get("state") != "ready"
        or any(not isinstance(value.get(key), str) or not value[key] for key in required_strings)
        or value.get("canonical_path") != canonical_target
        or value.get("name") != target.name
        or hashlib.sha256(manifest).hexdigest() != value["manifest_sha256"]
        or hashlib.sha256(lock).hexdigest() != value["lock_sha256"]
    ):
        return None
    return value


def _read_ready(target: Path) -> dict[str, Any] | None:
    root = target.parent
    try:
        if os.name == "nt":
            _assert_managed_target(root, target, require_marker=True)
            metadata_directory = target / READY_DIRECTORY
            if _is_link_or_reparse(metadata_directory) or not metadata_directory.is_dir():
                return None
            target_identity = _path_identity(target)
            metadata_identity = _path_identity(metadata_directory)
            ready_bytes = _read_regular_file_windows(
                metadata_directory / READY_FILENAME,
                maximum_bytes=1024 * 1024,
            )
            manifest = _read_regular_file_windows(target / "pixi.toml")
            lock = _read_regular_file_windows(target / "pixi.lock")
            _revalidate_path(target, target_identity, description="managed environment target")
            _revalidate_path(
                metadata_directory,
                metadata_identity,
                description="ready metadata directory",
            )
            canonical_target = str(target.resolve(strict=True))
        else:
            with _open_posix_target(
                root,
                target,
                require_marker=True,
            ) as (root_fd, target_fd, root_identity, target_identity):
                metadata = os.stat(
                    READY_DIRECTORY,
                    dir_fd=target_fd,
                    follow_symlinks=False,
                )
                if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                    return None
                metadata_fd = os.open(READY_DIRECTORY, _DIRECTORY_FLAGS, dir_fd=target_fd)
                try:
                    metadata_identity = _file_identity(os.fstat(metadata_fd))
                    _require_identity(
                        metadata,
                        metadata_identity,
                        description="ready metadata directory",
                    )
                    ready_bytes = _read_regular_file_at(
                        metadata_fd,
                        READY_FILENAME,
                        maximum_bytes=1024 * 1024,
                    )
                    manifest = _read_regular_file_at(target_fd, "pixi.toml")
                    lock = _read_regular_file_at(target_fd, "pixi.lock")
                    _revalidate_entry(
                        target_fd,
                        READY_DIRECTORY,
                        metadata_identity,
                        description="ready metadata directory",
                    )
                    _target_guard(
                        root,
                        target,
                        root_fd,
                        root_identity,
                        target_identity,
                    )
                    canonical_target = str(root.resolve(strict=True) / target.name)
                finally:
                    os.close(metadata_fd)
        value = json.loads(ready_bytes.decode("utf-8"))
    except (OSError, RuntimeError, UnicodeDecodeError, ValueError):
        return None
    return _valid_ready_payload(
        value,
        target=target,
        canonical_target=canonical_target,
        manifest=manifest,
        lock=lock,
    )


def _target_has_valid_owner_marker(root: Path, target: Path) -> bool:
    try:
        if os.name == "nt":
            _assert_managed_target(root, target, require_marker=False)
            marker = _read_regular_file_windows(
                target / OWNER_MARKER,
                maximum_bytes=4096,
            )
            return bool(marker.strip())
        with _open_posix_target(
            root,
            target,
            require_marker=False,
        ) as (_, target_fd, _, _):
            return _valid_owner_marker_at(target_fd)
    except (OSError, RuntimeError):
        return False


def _assert_managed_target(
    root: Path,
    target: Path,
    *,
    require_marker: bool,
    expected_identity: _FileIdentity | None = None,
) -> None:
    _require_direct_target(root, target)
    if os.name != "nt":
        with _open_posix_target(
            root,
            target,
            expected_identity=expected_identity,
            require_marker=require_marker,
        ):
            return
    if _is_link_or_reparse(root):
        raise RuntimeError(f"Refusing to use a linked managed environment root: {root}")
    if not root.is_dir():
        raise RuntimeError(f"Managed environment root is not a directory: {root}")
    if _is_link_or_reparse(target) or not target.is_dir():
        raise RuntimeError(f"Refusing to traverse a link, reparse point, or non-directory: {target}")
    root_identity = _path_identity(root)
    target_identity = _path_identity(target)
    if expected_identity is not None and target_identity != expected_identity:
        raise RuntimeError(f"Managed environment target changed identity: {target}")
    _revalidate_path(root, root_identity, description="managed environment root")
    _revalidate_path(target, target_identity, description="managed environment target")
    if require_marker:
        try:
            marker = _read_regular_file_windows(
                target / OWNER_MARKER,
                maximum_bytes=4096,
            )
        except (OSError, RuntimeError) as error:
            raise RuntimeError(f"Refusing to remove an unmanaged target: {target}") from error
        if not marker.strip():
            raise RuntimeError(f"Refusing to remove an unmanaged target: {target}")
    _revalidate_path(root, root_identity, description="managed environment root")
    _revalidate_path(target, target_identity, description="managed environment target")


def _safe_remove_tree(target: Path, *, expected_identity: _FileIdentity | None = None) -> None:
    if _is_link_or_reparse(target) or not target.is_dir():
        raise RuntimeError(f"Refusing to recurse into linked or non-directory target: {target}")
    identity = _path_identity(target)
    if expected_identity is not None and identity != expected_identity:
        raise RuntimeError(f"Directory changed identity before removal: {target}")
    with os.scandir(target) as entries:
        for entry in entries:
            path = Path(entry.path)
            if _is_link_or_reparse(path):
                try:
                    path.unlink()
                except (IsADirectoryError, PermissionError):
                    if os.name != "nt":
                        raise
                    os.rmdir(path)
            elif entry.is_dir(follow_symlinks=False):
                child_identity = _path_identity(path)
                _safe_remove_tree(path, expected_identity=child_identity)
            else:
                before = _path_identity(path)
                _revalidate_path(path, before, description="managed environment file")
                path.unlink()
            _revalidate_path(target, identity, description="managed environment directory")
    _revalidate_path(target, identity, description="managed environment directory")
    target.rmdir()


def _remove_directory_contents_fd(
    directory_fd: int,
    root_device: int,
    *,
    guard: Callable[[], None],
) -> None:
    for name in os.listdir(directory_fd):
        guard()
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISDIR(metadata.st_mode):
            if metadata.st_dev != root_device:
                raise RuntimeError(f"Refusing to cross a filesystem boundary while removing {name!r}")
            child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                _require_identity(
                    metadata,
                    _file_identity(os.fstat(child_fd)),
                    description=f"directory {name!r}",
                )
                _remove_directory_contents_fd(child_fd, root_device, guard=guard)
            finally:
                os.close(child_fd)
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            _require_identity(
                current,
                _file_identity(metadata),
                description=f"directory {name!r}",
            )
            guard()
            os.rmdir(name, dir_fd=directory_fd)
        else:
            guard()
            os.unlink(name, dir_fd=directory_fd)


def _remove_target_posix(
    root: Path,
    target: Path,
    *,
    expected_identity: _FileIdentity | None = None,
) -> None:
    with _open_posix_target(
        root,
        target,
        expected_identity=expected_identity,
        require_marker=True,
        revalidate_on_exit=False,
    ) as (root_fd, target_fd, root_identity, target_identity):

        def guard() -> None:
            _target_guard(
                root,
                target,
                root_fd,
                root_identity,
                target_identity,
            )

        guard()
        _remove_directory_contents_fd(
            target_fd,
            os.fstat(target_fd).st_dev,
            guard=guard,
        )
        guard()
        os.rmdir(target.name, dir_fd=root_fd)


def _remove_target(
    root: Path,
    target: Path,
    *,
    expected_identity: _FileIdentity | None = None,
) -> None:
    if os.name == "nt":
        _assert_managed_target(
            root,
            target,
            require_marker=True,
            expected_identity=expected_identity,
        )
        identity = _path_identity(target)
        _safe_remove_tree(target, expected_identity=identity)
    else:
        _remove_target_posix(root, target, expected_identity=expected_identity)


def _remove_empty_target(
    root: Path,
    target: Path,
    *,
    expected_identity: _FileIdentity,
) -> None:
    if os.name == "nt":
        _assert_managed_target(
            root,
            target,
            require_marker=False,
            expected_identity=expected_identity,
        )
        target.rmdir()
        return
    with _open_posix_target(
        root,
        target,
        expected_identity=expected_identity,
        require_marker=False,
        revalidate_on_exit=False,
    ) as (root_fd, _, root_identity, target_identity):
        _target_guard(root, target, root_fd, root_identity, target_identity)
        os.rmdir(target.name, dir_fd=root_fd)


def _publish_ready(
    root: Path,
    target: Path,
    content: bytes,
    *,
    expected_identity: _FileIdentity,
) -> None:
    if os.name == "nt":
        _assert_managed_target(
            root,
            target,
            require_marker=True,
            expected_identity=expected_identity,
        )
        metadata_directory = target / READY_DIRECTORY
        if os.path.lexists(metadata_directory):
            if _is_link_or_reparse(metadata_directory) or not metadata_directory.is_dir():
                raise RuntimeError(f"Ready metadata parent is linked or unsafe: {metadata_directory}")
        else:
            metadata_directory.mkdir()
        metadata_identity = _path_identity(metadata_directory)
        _revalidate_path(
            metadata_directory,
            metadata_identity,
            description="ready metadata directory",
        )
        _atomic_write(metadata_directory / READY_FILENAME, content)
        _revalidate_path(
            metadata_directory,
            metadata_identity,
            description="ready metadata directory",
        )
        _assert_managed_target(
            root,
            target,
            require_marker=True,
            expected_identity=expected_identity,
        )
        return
    with _open_posix_target(
        root,
        target,
        expected_identity=expected_identity,
        require_marker=True,
    ) as (root_fd, target_fd, root_identity, target_identity):
        try:
            metadata = os.stat(
                READY_DIRECTORY,
                dir_fd=target_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            os.mkdir(READY_DIRECTORY, dir_fd=target_fd)
            metadata = os.stat(
                READY_DIRECTORY,
                dir_fd=target_fd,
                follow_symlinks=False,
            )
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise RuntimeError("Ready metadata parent is linked or not a directory")
        metadata_fd = os.open(READY_DIRECTORY, _DIRECTORY_FLAGS, dir_fd=target_fd)
        try:
            metadata_identity = _file_identity(os.fstat(metadata_fd))
            _require_identity(
                metadata,
                metadata_identity,
                description="ready metadata directory",
            )

            def guard() -> None:
                _target_guard(
                    root,
                    target,
                    root_fd,
                    root_identity,
                    target_identity,
                )
                _revalidate_entry(
                    target_fd,
                    READY_DIRECTORY,
                    metadata_identity,
                    description="ready metadata directory",
                )

            _atomic_write_at(
                metadata_fd,
                READY_FILENAME,
                content,
                guard=guard,
            )
        finally:
            os.close(metadata_fd)


def _matching_journals(manager: EnvironmentManager, target: Path) -> Iterator[Path]:
    journal_root = manager.state_root / "operations"
    if not journal_root.is_dir() or _is_link_or_reparse(journal_root):
        return
    for path in journal_root.glob("*.json"):
        if not _regular_file(path):
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("target") == str(target):
            yield path


def _discard_matching_journals(manager: EnvironmentManager, target: Path) -> None:
    for path in _matching_journals(manager, target):
        path.unlink(missing_ok=True)


def _find_name_alias(root: Path, name: str) -> str | None:
    if not root.is_dir() or _is_link_or_reparse(root):
        return None
    key = environment_name_key(name)
    for path in root.iterdir():
        try:
            candidate = validate_environment_name(path.name)
        except ValueError:
            continue
        if environment_name_key(candidate) == key and candidate != name:
            return candidate
    return None


def _journal(manager: EnvironmentManager, operation: Operation[Any], payload: Mapping[str, Any]) -> Path:
    path = manager.state_root / "operations" / f"{operation.id}.json"
    _atomic_write(path, json.dumps(dict(payload), sort_keys=True, indent=2).encode())
    return path


def provision_environment(
    manager: EnvironmentManager,
    operation: Operation[Any],
    name: str,
    spec: EnvironmentSpec,
    replace_existing: bool,
) -> ManagedEnvironment:
    from wetlands.managed_environment import ManagedEnvironment

    try:
        pixi = manager._prepare_sync(operation)
    except PreparationError as error:
        raise ProvisioningError(error.failure) from error
    if operation.cancellation_requested:
        raise OperationCanceled(operation.id)
    target = manager.environments_root / name
    manifest_path = target / "pixi.toml"
    lock_path = target / "pixi.lock"
    runner = ProcessTreeRunner(operation, grace=manager.termination_grace, environment_name=name)
    journal_path: Path | None = None
    created_target = False
    created_target_identity: _FileIdentity | None = None
    ownership_published = False
    current_stage = ProvisioningStage.LOCK_WAIT
    with environment_lifecycle_gate(manager, name, operation=operation):
        try:
            if operation.cancellation_requested:
                raise OperationCanceled(operation.id)
            current_stage = ProvisioningStage.TARGET_INSPECTION
            operation._emit(
                OperationEventKind.STEP,
                f"Inspecting environment {name!r}",
                stage=ProvisioningStage.TARGET_INSPECTION.value,
                environment=name,
            )
            alias = _find_name_alias(manager.environments_root, name)
            if alias is not None:
                raise ProvisioningError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=current_stage.value,
                        environment=name,
                        message=f"Environment name {name!r} aliases existing managed name {alias!r}",
                    )
                )
            target_exists = os.path.lexists(target)
            ready = _read_ready(target) if target_exists else None
            manifest = render_pixi_manifest(name, spec)
            generated_manifest_hash = hashlib.sha256(manifest).hexdigest()
            supplied_lock_hash = hashlib.sha256(spec.lock_bytes).hexdigest() if spec.lock_bytes is not None else None
            if ready is not None:
                matches = (
                    ready.get("schema_version") == READY_SCHEMA_VERSION
                    and ready.get("recipe_hash") == spec.recipe_hash
                    and ready.get("manifest_sha256") == generated_manifest_hash
                    and ready.get("pixi_version") == pixi.version
                    and ready.get("pixi_executable") == str(pixi.executable)
                    and (supplied_lock_hash is None or ready.get("lock_sha256") == supplied_lock_hash)
                )
                if matches:
                    _discard_matching_journals(manager, target)
                    return ManagedEnvironment._from_ready(manager, name, target, ready)
                if not replace_existing:
                    existing_recipe_hash = ready.get("recipe_hash")
                    if isinstance(existing_recipe_hash, str) and existing_recipe_hash != spec.recipe_hash:
                        raise EnvironmentRecipeConflictError(
                            name,
                            existing_recipe_hash=existing_recipe_hash,
                            requested_recipe_hash=spec.recipe_hash,
                        )
                    raise ProvisioningError(
                        OperationFailure(
                            operation_id=operation.id,
                            stage=ProvisioningStage.TARGET_INSPECTION.value,
                            environment=name,
                            message=(
                                f"Environment {name!r} exists with a different recipe or Pixi identity; "
                                "pass replace_existing=True to rebuild it"
                            ),
                        )
                    )
                runtime_state.reconcile_persistent_pool(
                    manager.root,
                    name,
                    grace=manager.termination_grace,
                )
                live_workers = runtime_state.live_workers_for_env(
                    manager.root,
                    name,
                    include_nonpersistent=True,
                )
                if live_workers:
                    generation_id = ready.get("generation_id")
                    raise EnvironmentInUseError(
                        name,
                        str(generation_id) if generation_id is not None else None,
                    )
                current_stage = ProvisioningStage.INCOMPLETE_REMOVAL
                _remove_target(manager.environments_root, target)
            elif target_exists:
                if not _target_has_valid_owner_marker(manager.environments_root, target):
                    raise UnmanagedTargetError(name, target)
                runtime_state.reconcile_persistent_pool(
                    manager.root,
                    name,
                    grace=manager.termination_grace,
                )
                live_workers = runtime_state.live_workers_for_env(
                    manager.root,
                    name,
                    include_nonpersistent=True,
                )
                if live_workers:
                    generation_id = live_workers[0].get("generation_id")
                    raise EnvironmentInUseError(
                        name,
                        str(generation_id) if generation_id is not None else None,
                    )
                current_stage = ProvisioningStage.INCOMPLETE_REMOVAL
                operation._emit(
                    OperationEventKind.CLEANUP,
                    f"Removing incomplete environment {name!r}",
                    stage=ProvisioningStage.INCOMPLETE_REMOVAL.value,
                    environment=name,
                )
                _remove_target(manager.environments_root, target)
            _discard_matching_journals(manager, target)

            created_target_identity = _create_managed_target(
                manager.environments_root,
                target,
            )
            created_target = True
            _write_target_file(
                manager.environments_root,
                target,
                OWNER_MARKER,
                f"{operation.id}\n".encode(),
                expected_identity=created_target_identity,
                require_marker=False,
            )
            ownership_published = True
            journal_path = _journal(
                manager,
                operation,
                {
                    "operation_id": operation.id,
                    "environment": name,
                    "target": str(target),
                    "state": "building",
                    "recipe_hash": spec.recipe_hash,
                },
            )
            current_stage = ProvisioningStage.PROJECT_MATERIALIZATION
            operation._emit(
                OperationEventKind.STEP,
                "Writing deterministic Pixi project",
                stage=ProvisioningStage.PROJECT_MATERIALIZATION.value,
                environment=name,
            )
            _write_target_file(
                manager.environments_root,
                target,
                manifest_path.name,
                manifest,
                expected_identity=created_target_identity,
                require_marker=True,
            )
            if spec.lock_bytes is not None:
                _write_target_file(
                    manager.environments_root,
                    target,
                    lock_path.name,
                    spec.lock_bytes,
                    expected_identity=created_target_identity,
                    require_marker=True,
                )

            pixi_environment = {
                "PIXI_HOME": str(manager.state_root / "pixi-home"),
                "PIXI_CACHE_DIR": str(manager.state_root / "pixi-cache"),
                **{
                    ("NO_PROXY" if scheme == "no_proxy" else f"{scheme.upper()}_PROXY"): value
                    for scheme, value in (manager.network or {}).items()
                },
            }
            install_argv = [
                str(pixi.executable),
                "install",
                "--manifest-path",
                str(manifest_path),
            ]
            if spec.lock_bytes is not None:
                install_argv.append("--locked")
            current_stage = (
                ProvisioningStage.LOCK_RESOLUTION if spec.lock_bytes is None else ProvisioningStage.CONDA_INSTALL
            )
            operation._emit(
                OperationEventKind.STEP,
                ("Resolving pixi.lock" if spec.lock_bytes is None else "Verifying supplied pixi.lock"),
                stage=ProvisioningStage.LOCK_RESOLUTION.value,
                step_id="pixi-install",
                environment=name,
            )
            current_stage = ProvisioningStage.CONDA_INSTALL
            _assert_managed_target(
                manager.environments_root,
                target,
                require_marker=True,
                expected_identity=created_target_identity,
            )
            runner.run(
                ProvisioningStep(
                    "pixi-install",
                    ProvisioningStage.CONDA_INSTALL,
                    tuple(install_argv),
                    cwd=target,
                    environment=pixi_environment,
                )
            )
            operation._emit(
                OperationEventKind.STEP,
                (
                    "Pixi installed the declared Python dependencies, local packages, and managed worker runtime"
                    if spec.local
                    else (
                        "Pixi installed the declared PyPI dependencies and managed worker runtime"
                        if spec.pypi
                        else "Pixi installed the managed worker runtime"
                    )
                ),
                stage=ProvisioningStage.PYPI_INSTALL.value,
                step_id="pixi-install",
                environment=name,
            )
            try:
                lock_bytes = _read_target_file(
                    manager.environments_root,
                    target,
                    lock_path.name,
                    expected_identity=created_target_identity,
                )
            except (OSError, RuntimeError) as error:
                raise ProvisioningError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.LOCK_RESOLUTION.value,
                        environment=name,
                        message="Pixi completed without producing pixi.lock",
                    )
                ) from error
            actual_lock_hash = hashlib.sha256(lock_bytes).hexdigest()
            if supplied_lock_hash is not None and actual_lock_hash != supplied_lock_hash:
                raise ProvisioningError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.LOCK_RESOLUTION.value,
                        step_id="pixi-install",
                        environment=name,
                        message="Pixi modified the supplied pixi.lock during locked installation",
                    )
                )
            for index, command in enumerate(spec.post_install):
                current_stage = ProvisioningStage.POST_INSTALL
                command_argv: tuple[str, ...]
                if command.shell:
                    shell_command = command.argv[0] if len(command.argv) == 1 else shlex.join(command.argv)
                    if os.name == "nt":
                        command_argv = ("cmd.exe", "/d", "/s", "/c", shell_command)
                    else:
                        command_argv = ("/bin/sh", "-c", shell_command)
                else:
                    command_argv = command.argv
                post_install_argv = (
                    str(pixi.executable),
                    "run",
                    "--manifest-path",
                    str(manifest_path),
                    *command_argv,
                )
                _assert_managed_target(
                    manager.environments_root,
                    target,
                    require_marker=True,
                    expected_identity=created_target_identity,
                )
                runner.run(
                    ProvisioningStep(
                        f"post-install-{index}",
                        ProvisioningStage.POST_INSTALL,
                        post_install_argv,
                        cwd=target,
                        environment=pixi_environment,
                        display=command.display,
                    )
                )
            current_stage = ProvisioningStage.VALIDATION
            _assert_managed_target(
                manager.environments_root,
                target,
                require_marker=True,
                expected_identity=created_target_identity,
            )
            runner.run(
                ProvisioningStep(
                    "validate-runtime",
                    ProvisioningStage.VALIDATION,
                    (
                        str(pixi.executable),
                        "run",
                        "--manifest-path",
                        str(manifest_path),
                        "python",
                        "-c",
                        (
                            "import debugpy, importlib.metadata, sys; "
                            f"expected = {MANAGED_DEBUGPY_VERSION!r}; "
                            "actual = importlib.metadata.version('debugpy'); "
                            "sys.exit("
                            "f'Wetlands managed runtime requires debugpy {expected}, found {actual}'"
                            ") if actual != expected else print(sys.executable)"
                        ),
                    ),
                    cwd=target,
                    environment=pixi_environment,
                )
            )
            if operation.cancellation_requested:
                raise OperationCanceled(operation.id)
            manifest_bytes = _read_target_file(
                manager.environments_root,
                target,
                manifest_path.name,
                expected_identity=created_target_identity,
            )
            lock_bytes = _read_target_file(
                manager.environments_root,
                target,
                lock_path.name,
                expected_identity=created_target_identity,
            )
            manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
            lock_hash = hashlib.sha256(lock_bytes).hexdigest()
            generation = operation.id
            canonical_target = str(manager.environments_root.resolve(strict=True) / target.name)
            metadata = {
                "schema_version": READY_SCHEMA_VERSION,
                "state": "ready",
                "name": name,
                "canonical_path": canonical_target,
                "recipe": _metadata_recipe(spec),
                "recipe_hash": spec.recipe_hash,
                "manifest_sha256": manifest_hash,
                "lock_sha256": lock_hash,
                "generation_id": generation,
                "operation_id": operation.id,
                "pixi_version": pixi.version,
                "pixi_executable": str(pixi.executable),
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "completed_at": time.time(),
            }
            operation._emit(
                OperationEventKind.STEP,
                "Publishing ready metadata",
                stage=ProvisioningStage.METADATA_PUBLICATION.value,
                environment=name,
            )
            current_stage = ProvisioningStage.METADATA_PUBLICATION
            if not operation._seal_cancellation():
                raise OperationCanceled(operation.id)
            _publish_ready(
                manager.environments_root,
                target,
                json.dumps(metadata, sort_keys=True, indent=2).encode(),
                expected_identity=created_target_identity,
            )
            if journal_path is not None:
                journal_path.unlink(missing_ok=True)
            return ManagedEnvironment._from_ready(manager, name, target, metadata)
        except BaseException as error:
            if created_target and created_target_identity is not None:
                current_stage = ProvisioningStage.CLEANUP
                operation._emit(
                    OperationEventKind.CLEANUP,
                    f"Removing incomplete environment {name!r}",
                    stage=ProvisioningStage.CLEANUP.value,
                    environment=name,
                )
                try:
                    if ownership_published:
                        _remove_target(
                            manager.environments_root,
                            target,
                            expected_identity=created_target_identity,
                        )
                    else:
                        _remove_empty_target(
                            manager.environments_root,
                            target,
                            expected_identity=created_target_identity,
                        )
                except BaseException as cleanup_error:
                    failure = OperationFailure(
                        operation_id=operation.id,
                        stage=ProvisioningStage.CLEANUP.value,
                        environment=name,
                        message=f"Could not clean incomplete environment {name!r}",
                        cleanup_error=str(cleanup_error),
                    )
                    raise ProvisioningError(failure) from error
            if journal_path is not None:
                journal_path.unlink(missing_ok=True)
            if isinstance(
                error,
                (
                    EnvironmentInUseError,
                    EnvironmentRecipeConflictError,
                    OperationCanceled,
                    ProvisioningError,
                    UnmanagedTargetError,
                ),
            ):
                raise
            failure = OperationFailure(
                operation_id=operation.id,
                stage=current_stage.value,
                environment=name,
                message=f"Provisioning environment {name!r} failed: {error}",
            )
            raise ProvisioningError(failure) from error
