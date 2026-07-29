from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass

import psutil


class ProcessTerminationError(RuntimeError):
    """A worker process tree could not be terminated safely or completely."""


class ProcessIdentityError(ProcessTerminationError):
    """Recorded process ownership no longer matches the live operating-system process."""


@dataclass(frozen=True)
class ProcessIdentity:
    """Stable-enough identity and process-group ownership recorded for one worker."""

    pid: int
    started_at: float
    process_group_id: int | None
    session_id: int | None


def capture_process_identity(pid: int) -> ProcessIdentity:
    """Capture the identity needed to validate a worker before later signaling it."""
    try:
        started_at = psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied) as error:
        raise ProcessIdentityError(f"Cannot inspect worker process {pid}") from error

    if os.name == "nt":
        return ProcessIdentity(pid, started_at, None, None)
    try:
        process_group_id = os.getpgid(pid)
        session_id = os.getsid(pid)
    except OSError as error:
        raise ProcessIdentityError(f"Cannot inspect worker process group for PID {pid}") from error
    return ProcessIdentity(pid, started_at, process_group_id, session_id)


def identity_matches(pid: int, expected_started_at: float) -> bool:
    """Return whether PID still names the process whose start time was recorded."""
    try:
        return psutil.Process(pid).create_time() == expected_started_at
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def terminate_launched_process_tree(
    process: subprocess.Popen,
    *,
    grace: float,
    close_windows_job: Callable[[subprocess.Popen], None],
) -> None:
    """Terminate a worker launched by this process and verify that its tree is gone."""
    if os.name == "nt":
        _terminate_windows_tree(
            process.pid,
            expected_started_at=getattr(process, "_wetlands_started_at", None),
            grace=grace,
            process=process,
            close_windows_job=close_windows_job,
        )
        return

    process_group_id = getattr(process, "_wetlands_process_group_id", None)
    session_id = getattr(process, "_wetlands_session_id", None)
    started_at = getattr(process, "_wetlands_started_at", None)
    if not isinstance(started_at, (int, float)) or process_group_id != process.pid or session_id != process.pid:
        raise ProcessIdentityError(f"Worker PID {process.pid} has no proven isolated process-session ownership")

    if process.poll() is None:
        _validate_posix_identity(
            process.pid,
            expected_started_at=float(started_at),
            expected_process_group_id=process_group_id,
            expected_session_id=session_id,
        )
    _terminate_posix_group(process_group_id, grace=grace, process=process)


def terminate_attached_process_tree(
    pid: int,
    *,
    expected_started_at: float,
    expected_process_group_id: int | None,
    expected_session_id: int | None,
    grace: float,
) -> None:
    """Terminate an attached worker only after proving its recorded live identity."""
    try:
        actual_started_at = psutil.Process(pid).create_time()
    except psutil.NoSuchProcess:
        if os.name != "nt" and recorded_posix_group_exists(
            pid,
            expected_process_group_id=expected_process_group_id,
            expected_session_id=expected_session_id,
        ):
            assert expected_process_group_id is not None
            _terminate_posix_group(expected_process_group_id, grace=grace)
        return
    except psutil.AccessDenied as error:
        raise ProcessIdentityError(f"Cannot validate attached worker PID {pid}") from error
    if actual_started_at != expected_started_at:
        raise ProcessIdentityError(f"Refusing to signal PID {pid}: its process start identity changed")

    if os.name == "nt":
        _terminate_windows_tree(
            pid,
            expected_started_at=expected_started_at,
            grace=grace,
        )
        return

    _validate_posix_identity(
        pid,
        expected_started_at=expected_started_at,
        expected_process_group_id=expected_process_group_id,
        expected_session_id=expected_session_id,
    )
    assert expected_process_group_id is not None
    _terminate_posix_group(expected_process_group_id, grace=grace)


def recorded_posix_group_exists(
    leader_pid: int,
    *,
    expected_process_group_id: int | None,
    expected_session_id: int | None,
) -> bool:
    """Prove whether an exited leader's isolated POSIX process group still exists."""
    if os.name == "nt":
        return False
    if expected_process_group_id != leader_pid or expected_session_id != leader_pid:
        raise ProcessIdentityError(
            f"Cannot inspect descendants of worker PID {leader_pid}: "
            "isolated process-session ownership was not recorded"
        )
    if not _posix_group_exists(expected_process_group_id):
        return False

    members: list[int] = []
    for process in psutil.process_iter(["pid"]):
        try:
            if os.getpgid(process.pid) != expected_process_group_id:
                continue
            if os.getsid(process.pid) != expected_session_id:
                raise ProcessIdentityError(
                    f"Recorded process group {expected_process_group_id} changed session ownership"
                )
            members.append(process.pid)
        except ProcessLookupError:
            continue
        except PermissionError as error:
            raise ProcessIdentityError(
                f"Cannot verify ownership of recorded process group {expected_process_group_id}"
            ) from error
        except OSError as error:
            raise ProcessIdentityError(f"Cannot inspect recorded process group {expected_process_group_id}") from error
        except psutil.Error as error:
            raise ProcessIdentityError(
                f"Cannot enumerate recorded process group {expected_process_group_id}"
            ) from error
    if not members:
        if _posix_group_exists(expected_process_group_id):
            raise ProcessIdentityError(
                f"Recorded process group {expected_process_group_id} exists but its members cannot be verified"
            )
        return False
    try:
        psutil.Process(leader_pid).create_time()
    except psutil.NoSuchProcess:
        pass
    except psutil.AccessDenied as error:
        raise ProcessIdentityError(f"Cannot revalidate exited worker leader PID {leader_pid}") from error
    else:
        raise ProcessIdentityError(f"Worker leader PID {leader_pid} was reused while validating its process group")
    return True


def _validate_posix_identity(
    pid: int,
    *,
    expected_started_at: float,
    expected_process_group_id: int | None,
    expected_session_id: int | None,
) -> None:
    if expected_process_group_id != pid or expected_session_id != pid:
        raise ProcessIdentityError(f"Refusing to signal PID {pid}: isolated process-session ownership is not proven")
    if not identity_matches(pid, expected_started_at):
        raise ProcessIdentityError(f"Refusing to signal PID {pid}: its process start identity changed")
    try:
        live_process_group_id = os.getpgid(pid)
        live_session_id = os.getsid(pid)
    except ProcessLookupError as error:
        raise ProcessIdentityError(f"Worker PID {pid} disappeared during identity validation") from error
    except OSError as error:
        raise ProcessIdentityError(f"Cannot validate process-session ownership for PID {pid}") from error
    if live_process_group_id != expected_process_group_id or live_session_id != expected_session_id:
        raise ProcessIdentityError(f"Refusing to signal PID {pid}: its process-session identity changed")


def _terminate_posix_group(
    process_group_id: int,
    *,
    grace: float,
    process: subprocess.Popen | None = None,
) -> None:
    if not _posix_group_exists(process_group_id):
        _reap(process)
        return

    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        _reap(process)
        return
    except OSError as error:
        raise ProcessTerminationError(f"Could not terminate worker process group {process_group_id}") from error

    if _wait_for_posix_group_exit(process_group_id, process=process, timeout=grace):
        return

    try:
        os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        _reap(process)
        return
    except OSError as error:
        raise ProcessTerminationError(f"Could not force-kill worker process group {process_group_id}") from error

    if _wait_for_posix_group_exit(process_group_id, process=process, timeout=grace):
        return
    survivors = _posix_group_members(process_group_id)
    details = f"; surviving PIDs: {survivors}" if survivors else ""
    raise ProcessTerminationError(f"Worker process group {process_group_id} survived SIGKILL{details}")


def _wait_for_posix_group_exit(
    process_group_id: int,
    *,
    process: subprocess.Popen | None,
    timeout: float,
) -> bool:
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        _reap(process)
        if not _posix_group_exists(process_group_id):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))


def _posix_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _posix_group_members(process_group_id: int) -> list[int]:
    members: list[int] = []
    for process in psutil.process_iter(["pid"]):
        try:
            if os.getpgid(process.pid) == process_group_id:
                members.append(process.pid)
        except (OSError, psutil.Error):
            continue
    return sorted(members)


def _reap(process: subprocess.Popen | None) -> None:
    if process is None:
        return
    with contextlib.suppress(subprocess.TimeoutExpired, OSError):
        process.wait(timeout=0)


def _terminate_windows_tree(
    pid: int,
    *,
    expected_started_at: float | None,
    grace: float,
    process: subprocess.Popen | None = None,
    close_windows_job: Callable[[subprocess.Popen], None] | None = None,
) -> None:
    job_handle = getattr(process, "_wetlands_job_handle", None) if process is not None else None
    try:
        parent = psutil.Process(pid)
        started_at = parent.create_time()
    except psutil.NoSuchProcess:
        if process is not None and job_handle is not None and close_windows_job is not None:
            close_windows_job(process)
        _reap(process)
        return
    except psutil.AccessDenied as error:
        raise ProcessIdentityError(f"Cannot validate worker PID {pid}") from error
    if expected_started_at is None or started_at != expected_started_at:
        raise ProcessIdentityError(f"Refusing to signal PID {pid}: its process start identity changed")

    try:
        descendants = parent.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied) as error:
        raise ProcessTerminationError(f"Cannot enumerate worker process tree for PID {pid}") from error
    targets = [*descendants, parent]

    if process is not None and job_handle is not None and close_windows_job is not None:
        close_windows_job(process)
    else:
        for target in targets:
            with contextlib.suppress(psutil.NoSuchProcess):
                target.terminate()

    _, survivors = psutil.wait_procs(targets, timeout=max(0.0, grace))
    for survivor in survivors:
        with contextlib.suppress(psutil.NoSuchProcess):
            survivor.kill()
    _, survivors = psutil.wait_procs(survivors, timeout=max(0.0, grace))
    _reap(process)
    if survivors:
        survivor_pids = sorted(target.pid for target in survivors)
        raise ProcessTerminationError(
            f"Worker process tree rooted at PID {pid} survived forced termination; surviving PIDs: {survivor_pids}"
        )
