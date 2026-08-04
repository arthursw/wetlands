"""Crash-recoverable background reclamation for removed environments."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import secrets
import stat
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from wetlands._internal.provisioning import (
    CancelableFileLock,
    _DIRECTORY_FLAGS,
    _FileIdentity,
    _assert_managed_target,
    _atomic_write,
    _file_identity,
    _is_link_or_reparse,
    _open_posix_directory,
    _path_identity,
    _read_regular_file_windows,
    _regular_file,
    _remove_target,
    _require_identity,
    _revalidate_entry,
    _revalidate_path,
    _target_has_valid_owner_marker,
    _valid_owner_marker_at,
    _write_target_file,
)
from wetlands.specs import ProvisioningStage, validate_environment_name
from wetlands.operation import Operation

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager

logger = logging.getLogger(__name__)

QUARANTINE_DIRECTORY = ".wetlands-gc"
_QUARANTINE_MARKER = ".wetlands-gc.json"
_QUARANTINE_TOKEN = ".wetlands-gc-token"
_QUARANTINE_SCHEMA_VERSION = 1
_QUARANTINE_KIND = "wetlands-environment-quarantine"
_TOMB_PATTERN = re.compile(r"^tomb-([0-9a-f]{64})\.tree$")
_RECORD_PATTERN = re.compile(r"^tomb-([0-9a-f]{64})\.json$")
_VALID_STATES = frozenset({"prepared", "detached", "purging"})


@dataclass(frozen=True)
class QuarantinedEnvironment:
    token: str
    original_name: str
    operation_id: str
    tomb_name: str
    identity: _FileIdentity
    state: str


def _metadata_lock(
    manager: EnvironmentManager,
    operation: Operation[Any] | None = None,
) -> CancelableFileLock:
    return CancelableFileLock(
        manager.state_root / "locks" / "environment-gc-metadata.lock",
        operation,
        ProvisioningStage.CLEANUP,
        error_type=None,
    )


def _purge_lock(manager: EnvironmentManager) -> CancelableFileLock:
    return CancelableFileLock(
        manager.state_root / "locks" / "environment-gc-purge.lock",
        None,
        ProvisioningStage.CLEANUP,
        error_type=None,
    )


def _quarantine_root(manager: EnvironmentManager) -> Path:
    return manager.root / QUARANTINE_DIRECTORY


def _marker_payload() -> bytes:
    return json.dumps(
        {
            "kind": _QUARANTINE_KIND,
            "schema_version": _QUARANTINE_SCHEMA_VERSION,
        },
        sort_keys=True,
    ).encode()


def _validate_quarantine_root(
    manager: EnvironmentManager,
    root: Path,
    *,
    require_environments_root: bool = True,
) -> _FileIdentity:
    environments_root = manager.environments_root
    manager_root = manager.root
    if _is_link_or_reparse(manager_root) or not manager_root.is_dir():
        raise RuntimeError(f"Manager root is linked or not a directory: {manager_root}")
    if _is_link_or_reparse(root) or not root.is_dir():
        raise RuntimeError(f"Environment quarantine is linked or not a directory: {root}")
    manager_identity = _path_identity(manager_root)
    root_identity = _path_identity(root)
    if manager_identity[1] != root_identity[1]:
        raise RuntimeError("Environment quarantine crosses a filesystem boundary")
    environments_identity: _FileIdentity | None = None
    if require_environments_root:
        if _is_link_or_reparse(environments_root) or not environments_root.is_dir():
            raise RuntimeError(f"Managed environment root is linked or not a directory: {environments_root}")
        environments_identity = _path_identity(environments_root)
        if environments_identity[1] != root_identity[1]:
            raise RuntimeError("Environment quarantine crosses a filesystem boundary")
    marker = root / _QUARANTINE_MARKER
    if not _regular_file(marker):
        raise RuntimeError(f"Environment quarantine marker is missing or invalid: {marker}")
    if _read_regular_file_windows(marker, maximum_bytes=4096) != _marker_payload():
        raise RuntimeError(f"Environment quarantine marker has invalid contents: {marker}")
    _revalidate_path(manager_root, manager_identity, description="manager root")
    if environments_identity is not None:
        _revalidate_path(environments_root, environments_identity, description="managed environment root")
    _revalidate_path(root, root_identity, description="environment quarantine")
    return root_identity


def _rename_no_replace(
    source_name: str,
    destination_name: str,
    *,
    source_directory_fd: int,
    destination_directory_fd: int,
) -> None:
    """Rename a POSIX directory entry while refusing an existing destination."""

    import ctypes
    import platform
    import sys

    libc = ctypes.CDLL(None, use_errno=True)
    source = os.fsencode(source_name)
    destination = os.fsencode(destination_name)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is not None:
            rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
            rename.restype = ctypes.c_int
            result = rename(source_directory_fd, source, destination_directory_fd, destination, 1)
        else:
            syscall_numbers = {
                "aarch64": 276,
                "arm64": 276,
                "armv7l": 382,
                "i386": 353,
                "i686": 353,
                "ppc64": 357,
                "ppc64le": 357,
                "riscv64": 276,
                "s390x": 347,
                "x86_64": 316,
            }
            machine = platform.machine().casefold()
            syscall_number = syscall_numbers.get(machine)
            if syscall_number is None:
                raise RuntimeError(f"Atomic no-replace rename is unavailable on Linux architecture {machine!r}")
            syscall = libc.syscall
            syscall.restype = ctypes.c_long
            result = syscall(
                ctypes.c_long(syscall_number),
                ctypes.c_int(source_directory_fd),
                ctypes.c_char_p(source),
                ctypes.c_int(destination_directory_fd),
                ctypes.c_char_p(destination),
                ctypes.c_uint(1),
            )
    elif sys.platform == "darwin":
        rename = getattr(libc, "renameatx_np", None)
        if rename is None:
            raise RuntimeError("This macOS runtime does not provide renameatx_np")
        rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(source_directory_fd, source, destination_directory_fd, destination, 4)
    else:  # pragma: no cover - Wetlands supports POSIX lifecycle operations on Linux and macOS
        raise RuntimeError("Atomic no-replace rename is unavailable on this POSIX platform")
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), source_name, destination_name)


def _sync_detach_directories(source_directory_fd: int, destination_directory_fd: int) -> None:
    os.fsync(source_directory_fd)
    os.fsync(destination_directory_fd)


def _ensure_quarantine_root(manager: EnvironmentManager) -> tuple[Path, _FileIdentity]:
    environments_root = manager.environments_root
    root = _quarantine_root(manager)
    if not environments_root.is_dir() or _is_link_or_reparse(environments_root):
        raise RuntimeError(f"Managed environment root is linked or not a directory: {environments_root}")
    environments_identity = _path_identity(environments_root)
    if not os.path.lexists(root):
        staging = manager.root / f"{QUARANTINE_DIRECTORY}.init-{secrets.token_hex(16)}"
        try:
            staging.mkdir(mode=0o700)
            _atomic_write(staging / _QUARANTINE_MARKER, _marker_payload())
            _revalidate_path(environments_root, environments_identity, description="managed environment root")
            try:
                if os.name == "nt":
                    os.rename(staging, root)
                else:
                    with _open_posix_directory(manager.root, description="manager root") as (manager_fd, _):
                        with _open_posix_directory(staging, description="staged environment quarantine") as (
                            staging_fd,
                            _,
                        ):
                            os.fsync(staging_fd)
                        _rename_no_replace(
                            staging.name,
                            root.name,
                            source_directory_fd=manager_fd,
                            destination_directory_fd=manager_fd,
                        )
                        os.fsync(manager_fd)
            except FileExistsError:
                (staging / _QUARANTINE_MARKER).unlink(missing_ok=True)
                staging.rmdir()
        except BaseException:
            with contextlib.suppress(OSError):
                (staging / _QUARANTINE_MARKER).unlink()
            with contextlib.suppress(OSError):
                staging.rmdir()
            raise
    return root, _validate_quarantine_root(manager, root)


def _record_path(root: Path, tomb_name: str) -> Path:
    return root / f"{tomb_name[:-5]}.json"


def _record_payload(record: QuarantinedEnvironment) -> bytes:
    return json.dumps(
        {
            "identity": list(record.identity),
            "kind": _QUARANTINE_KIND,
            "original_name": record.original_name,
            "operation_id": record.operation_id,
            "schema_version": _QUARANTINE_SCHEMA_VERSION,
            "state": record.state,
            "token": record.token,
            "tomb_name": record.tomb_name,
        },
        sort_keys=True,
        indent=2,
    ).encode()


def _write_record(root: Path, record: QuarantinedEnvironment) -> None:
    _atomic_write(_record_path(root, record.tomb_name), _record_payload(record))


def _updated_record(record: QuarantinedEnvironment, state: str) -> QuarantinedEnvironment:
    return QuarantinedEnvironment(
        token=record.token,
        original_name=record.original_name,
        operation_id=record.operation_id,
        tomb_name=record.tomb_name,
        identity=record.identity,
        state=state,
    )


def _move_target_into_quarantine(
    manager: EnvironmentManager,
    target: Path,
    tomb: Path,
    *,
    expected_identity: _FileIdentity,
) -> None:
    root = manager.environments_root
    quarantine = tomb.parent
    if os.name == "nt":
        _assert_managed_target(root, target, require_marker=True, expected_identity=expected_identity)
        root_identity = _path_identity(root)
        quarantine_identity = _validate_quarantine_root(manager, quarantine)
        if os.path.lexists(tomb):
            raise FileExistsError(tomb)
        _revalidate_path(target, expected_identity, description="managed environment target")
        os.rename(target, tomb)
        _revalidate_path(root, root_identity, description="managed environment root")
        _revalidate_path(quarantine, quarantine_identity, description="environment quarantine")
        _revalidate_path(tomb, expected_identity, description="quarantined environment")
        if os.path.lexists(target):
            raise RuntimeError("Managed environment source survived quarantine rename")
        return

    with _open_posix_directory(root, description="managed environment root") as (root_fd, _):
        with _open_posix_directory(quarantine, description="environment quarantine") as (quarantine_fd, _):
            source_metadata = os.stat(target.name, dir_fd=root_fd, follow_symlinks=False)
            if not stat.S_ISDIR(source_metadata.st_mode) or stat.S_ISLNK(source_metadata.st_mode):
                raise RuntimeError(f"Managed environment target is not a directory: {target}")
            source_fd = os.open(target.name, _DIRECTORY_FLAGS, dir_fd=root_fd)
            try:
                source_identity = _file_identity(os.fstat(source_fd))
                _require_identity(source_metadata, source_identity, description="managed environment target")
                if source_identity != expected_identity:
                    raise RuntimeError(f"Managed environment target changed identity: {target}")
                if not _valid_owner_marker_at(source_fd):
                    raise RuntimeError(f"Refusing to quarantine an unmanaged target: {target}")
                if os.fstat(root_fd).st_dev != os.fstat(quarantine_fd).st_dev:
                    raise RuntimeError("Environment quarantine crosses a filesystem boundary")
                try:
                    os.stat(tomb.name, dir_fd=quarantine_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise FileExistsError(tomb)
                _revalidate_entry(
                    root_fd,
                    target.name,
                    source_identity,
                    description="managed environment target",
                )
                _rename_no_replace(
                    target.name,
                    tomb.name,
                    source_directory_fd=root_fd,
                    destination_directory_fd=quarantine_fd,
                )
                _revalidate_entry(
                    quarantine_fd,
                    tomb.name,
                    source_identity,
                    description="quarantined environment",
                )
                try:
                    os.stat(target.name, dir_fd=root_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise RuntimeError("Managed environment source survived quarantine rename")
                _sync_detach_directories(root_fd, quarantine_fd)
            finally:
                os.close(source_fd)


def quarantine_environment(
    manager: EnvironmentManager,
    target: Path,
    *,
    operation_id: str,
    expected_identity: _FileIdentity | None = None,
    before_commit: Callable[[], None] | None = None,
    lock_operation: Operation[Any] | None = None,
) -> QuarantinedEnvironment:
    """Atomically detach one proven environment into durable quarantine."""

    with _metadata_lock(manager, lock_operation):
        root, root_identity = _ensure_quarantine_root(manager)
        if not _target_has_valid_owner_marker(manager.environments_root, target):
            raise RuntimeError(f"Refusing to quarantine an unmanaged target: {target}")
        identity = _path_identity(target)
        if expected_identity is not None and identity != expected_identity:
            raise RuntimeError(f"Managed environment target changed identity: {target}")
        token = secrets.token_hex(32)
        tomb_name = f"tomb-{token}.tree"
        record = QuarantinedEnvironment(
            token=token,
            original_name=target.name,
            operation_id=operation_id,
            tomb_name=tomb_name,
            identity=identity,
            state="prepared",
        )
        _write_record(root, record)
        try:
            _write_target_file(
                manager.environments_root,
                target,
                _QUARANTINE_TOKEN,
                f"{token}\n".encode(),
                expected_identity=identity,
                require_marker=True,
            )
            _revalidate_path(root, root_identity, description="environment quarantine")
            if before_commit is not None:
                before_commit()
            tomb = root / tomb_name
            attempts = 3 if os.name == "nt" else 1
            for attempt in range(attempts):
                try:
                    _move_target_into_quarantine(
                        manager,
                        target,
                        tomb,
                        expected_identity=identity,
                    )
                    break
                except PermissionError:
                    if attempt + 1 == attempts:
                        raise
                    time.sleep(0.05 * (attempt + 1))
        except BaseException:
            tomb = root / tomb_name
            committed = False
            try:
                committed = (
                    not os.path.lexists(target)
                    and os.path.lexists(tomb)
                    and not _is_link_or_reparse(tomb)
                    and tomb.is_dir()
                    and _path_identity(tomb) == identity
                )
            except OSError:
                pass
            if committed:
                logger.warning(
                    "Environment %s was detached as %s, but post-rename validation did not complete",
                    target.name,
                    tomb_name,
                )
            else:
                if os.path.lexists(target) and not os.path.lexists(tomb):
                    with contextlib.suppress(OSError):
                        _record_path(root, tomb_name).unlink()
                raise
        detached = _updated_record(record, "detached")
        try:
            _write_record(root, detached)
        except OSError:
            logger.warning("Environment %s was detached but its quarantine record could not be finalized", target.name)
        return detached


def _parse_identity(value: Any) -> _FileIdentity | None:
    if not isinstance(value, list) or len(value) != 3 or any(type(item) is not int for item in value):
        return None
    if value[0] not in {0, 1, 2} or value[1] < 0 or value[2] < 0:
        return None
    return value[0], value[1], value[2]


def _read_record(path: Path) -> QuarantinedEnvironment | None:
    match = _RECORD_PATTERN.fullmatch(path.name)
    if match is None or not _regular_file(path):
        return None
    try:
        if path.stat().st_size > 16 * 1024:
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    identity = _parse_identity(payload.get("identity")) if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != _QUARANTINE_SCHEMA_VERSION
        or payload.get("kind") != _QUARANTINE_KIND
        or payload.get("token") != match.group(1)
        or payload.get("tomb_name") != f"tomb-{match.group(1)}.tree"
        or not isinstance(payload.get("original_name"), str)
        or not isinstance(payload.get("operation_id"), str)
        or not payload["operation_id"]
        or payload.get("state") not in _VALID_STATES
        or identity is None
    ):
        return None
    try:
        original_name = validate_environment_name(payload["original_name"])
    except ValueError:
        return None
    return QuarantinedEnvironment(
        token=match.group(1),
        original_name=original_name,
        operation_id=payload["operation_id"],
        tomb_name=payload["tomb_name"],
        identity=identity,
        state=payload["state"],
    )


def _stale_prepared_intent_matches(original: Path, record: QuarantinedEnvironment) -> bool:
    try:
        if _is_link_or_reparse(original) or not original.is_dir() or _path_identity(original) != record.identity:
            return False
        token_path = original / _QUARANTINE_TOKEN
        if not os.path.lexists(token_path):
            return True
        token = _read_regular_file_windows(token_path, maximum_bytes=4096)
        return token == f"{record.token}\n".encode()
    except (OSError, RuntimeError):
        return False


def discover_quarantined_environments(manager: EnvironmentManager) -> tuple[QuarantinedEnvironment, ...]:
    """Return securely recoverable tombstones, leaving ambiguous entries untouched."""

    with _metadata_lock(manager):
        root = _quarantine_root(manager)
        if not os.path.lexists(root):
            return ()
        _validate_quarantine_root(manager, root, require_environments_root=False)
        recovered: list[QuarantinedEnvironment] = []
        for path in root.iterdir():
            record = _read_record(path)
            if record is None:
                continue
            tomb = root / record.tomb_name
            original = manager.environments_root / record.original_name
            if not os.path.lexists(tomb):
                if record.state == "prepared" and os.path.lexists(original):
                    if _stale_prepared_intent_matches(original, record):
                        with contextlib.suppress(OSError):
                            path.unlink()
                elif record.state in {"detached", "purging"} or not os.path.lexists(original):
                    with contextlib.suppress(OSError):
                        path.unlink()
                continue
            if _is_link_or_reparse(tomb) or not tomb.is_dir():
                continue
            try:
                tomb_identity = _path_identity(tomb)
            except OSError:
                continue
            if tomb_identity != record.identity:
                continue
            if record.state == "prepared":
                try:
                    token = _read_regular_file_windows(tomb / _QUARANTINE_TOKEN, maximum_bytes=4096)
                except (OSError, RuntimeError):
                    continue
                if token != f"{record.token}\n".encode():
                    continue
                record = _updated_record(record, "detached")
                _write_record(root, record)
            recovered.append(record)
        return tuple(recovered)


def purge_quarantined_environment(
    manager: EnvironmentManager,
    record: QuarantinedEnvironment,
    *,
    stop_requested: Callable[[], bool] | None = None,
) -> None:
    """Resume physical reclamation of one durably authorized tombstone."""

    with _purge_lock(manager):
        root = _quarantine_root(manager)
        _validate_quarantine_root(manager, root, require_environments_root=False)
        record_path = _record_path(root, record.tomb_name)
        current = _read_record(record_path)
        if current is None or current.token != record.token or current.identity != record.identity:
            return
        tomb = root / current.tomb_name
        if not os.path.lexists(tomb):
            if current.state in {"detached", "purging"}:
                record_path.unlink(missing_ok=True)
            return
        if _is_link_or_reparse(tomb) or not tomb.is_dir() or _path_identity(tomb) != current.identity:
            raise RuntimeError(f"Quarantined environment changed identity: {tomb}")
        if current.state != "purging":
            token = _read_regular_file_windows(tomb / _QUARANTINE_TOKEN, maximum_bytes=4096)
            if token != f"{current.token}\n".encode():
                raise RuntimeError(f"Quarantined environment token is missing or invalid: {tomb}")
        purging = _updated_record(current, "purging")
        if current.state != "purging":
            _write_record(root, purging)
        _remove_target(
            root,
            tomb,
            expected_identity=current.identity,
            require_marker=False,
            stop_requested=stop_requested,
        )
        record_path.unlink(missing_ok=True)


class EnvironmentReclaimer:
    """One lazy serialized background reclaimer for an environment manager."""

    def __init__(self, manager: EnvironmentManager) -> None:
        self._manager = manager
        self._condition = threading.Condition()
        self._pending: deque[QuarantinedEnvironment] = deque()
        self._tokens: set[str] = set()
        self._thread: threading.Thread | None = None
        self._stop = False
        self._scan_requested = False
        self._scanning = False
        self._active = False

    def wake(self) -> None:
        with self._condition:
            if self._stop:
                return
            self._scan_requested = True
            if not self._ensure_thread_locked():
                return
            self._condition.notify_all()

    def enqueue(self, record: QuarantinedEnvironment) -> None:
        with self._condition:
            if self._stop:
                return
            if record.token not in self._tokens:
                self._tokens.add(record.token)
                self._pending.append(record)
            if not self._ensure_thread_locked():
                return
            self._condition.notify_all()

    def _ensure_thread_locked(self) -> bool:
        if self._thread is None:
            thread = threading.Thread(
                target=self._run,
                name="wetlands-environment-reclaimer",
                daemon=True,
            )
            try:
                thread.start()
            except RuntimeError as error:
                logger.warning("Could not start environment reclaimer: %s", error)
                return False
            self._thread = thread
        return True

    def _queue_recovered(self) -> None:
        for record in discover_quarantined_environments(self._manager):
            with self._condition:
                if record.token not in self._tokens:
                    self._tokens.add(record.token)
                    self._pending.append(record)

    def _run(self) -> None:
        while True:
            with self._condition:
                while not self._stop and not self._scan_requested and not self._pending:
                    self._condition.wait()
                if self._stop:
                    self._condition.notify_all()
                    return
                scan = self._scan_requested
                self._scan_requested = False
                self._scanning = scan
                record = self._pending.popleft() if self._pending else None
                if record is not None:
                    self._active = True
            if scan:
                try:
                    self._queue_recovered()
                except BaseException as error:
                    logger.warning("Could not scan removed environments for reclamation: %s", error)
                finally:
                    with self._condition:
                        self._scanning = False
                        self._condition.notify_all()
            if record is None:
                with self._condition:
                    self._condition.notify_all()
                continue
            try:
                self._purge_with_retries(record)
            except InterruptedError:
                pass
            except BaseException as error:
                logger.warning("Could not reclaim removed environment %s: %s", record.original_name, error)
            finally:
                with self._condition:
                    self._tokens.discard(record.token)
                    self._active = False
                    self._condition.notify_all()

    def _purge_with_retries(self, record: QuarantinedEnvironment) -> None:
        attempts = 3 if os.name == "nt" else 1
        for attempt in range(attempts):
            try:
                purge_quarantined_environment(
                    self._manager,
                    record,
                    stop_requested=lambda: self._stop,
                )
                return
            except PermissionError:
                if attempt + 1 == attempts or self._stop:
                    raise
                time.sleep(0.05 * (attempt + 1))

    def close(self) -> None:
        with self._condition:
            self._stop = True
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=0.25)

    def wait_idle(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._active or self._pending or self._scan_requested or self._scanning:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True


__all__ = [
    "EnvironmentReclaimer",
    "QUARANTINE_DIRECTORY",
    "QuarantinedEnvironment",
    "discover_quarantined_environments",
    "purge_quarantined_environment",
    "quarantine_environment",
]
