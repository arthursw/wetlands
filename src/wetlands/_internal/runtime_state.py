from __future__ import annotations

import contextlib
import enum
import json
import math
import os
import secrets
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psutil

from wetlands._internal.process_termination import (
    ProcessTerminationError,
    capture_process_identity,
    recorded_posix_group_exists,
    terminate_attached_process_tree,
)


SCHEMA_VERSION = 4
STATE_DIR_NAME = "state"
AUTH_KEY_FILE = "auth.key"
WORKERS_FILE = "workers.json"
LOCK_FILE = "workers.lock"


class RuntimeStateError(RuntimeError):
    """Runtime coordination state is unavailable or cannot be trusted."""


class RuntimeRegistryError(RuntimeStateError):
    """The durable worker registry cannot be read or validated safely."""


class WorkerIdentityUnavailableError(RuntimeStateError):
    """A recorded worker's live process identity cannot be inspected safely."""


class _RecordedTreeState(enum.Enum):
    LEADER_ALIVE = "leader_alive"
    GROUP_ALIVE = "group_alive"
    DEAD = "dead"


def state_dir(root: str | Path) -> Path:
    """Return the Wetlands runtime state directory, creating it if needed."""
    path = Path(root).resolve() / STATE_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return path


def load_or_create_root_authkey(root: str | Path) -> bytes:
    """Load or create the root-local multiprocessing auth key."""
    path = state_dir(root) / AUTH_KEY_FILE
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path.read_bytes()

    key = secrets.token_bytes(32)
    with os.fdopen(fd, "wb") as f:
        f.write(key)
        f.flush()
        os.fsync(f.fileno())
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return key


@contextlib.contextmanager
def root_lock(root: str | Path) -> Iterator[None]:
    """Serialize registry read-modify-write operations for one Wetlands root."""
    lock_path = state_dir(root) / LOCK_FILE
    with open(lock_path, "a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def atomic_write_json(path: str | Path, payload: dict[str, Any]) -> None:
    """Atomically write JSON by replacing the destination after fsync."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, destination)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(temp_name)
        raise


def _empty_registry() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "workers": {},
        "controllers": {},
        "persistent_pools": {},
    }


def _is_positive_number(value: object) -> bool:
    return type(value) in (int, float) and math.isfinite(float(value)) and float(value) > 0


def _valid_worker_entry(key: str, entry: dict[str, Any]) -> bool:
    env_name = entry.get("env_name")
    worker_index = entry.get("worker_index")
    pool_id = entry.get("pool_id")
    pid = entry.get("pid")
    port = entry.get("port")
    persistent = entry.get("persistent")
    process_group_id = entry.get("process_group_id")
    session_id = entry.get("session_id")
    return (
        isinstance(env_name, str)
        and bool(env_name)
        and type(worker_index) is int
        and worker_index >= 0
        and (pool_id is None or (isinstance(pool_id, str) and bool(pool_id)))
        and (not persistent or isinstance(pool_id, str))
        and key == worker_key(env_name, worker_index, pool_id)
        and type(pid) is int
        and pid > 0
        and type(port) is int
        and 0 < port <= 65535
        and type(persistent) is bool
        and (entry.get("env_path") is None or isinstance(entry.get("env_path"), str))
        and isinstance(entry.get("generation_id"), str)
        and bool(entry.get("generation_id"))
        and isinstance(entry.get("recipe_hash"), str)
        and bool(entry.get("recipe_hash"))
        and isinstance(entry.get("worker_runtime_version"), str)
        and bool(entry.get("worker_runtime_version"))
        and type(entry.get("protocol_version")) is int
        and type(entry.get("commissioned")) is bool
        and _is_positive_number(entry.get("process_started_at"))
        and (process_group_id is None or (type(process_group_id) is int and process_group_id > 0))
        and (session_id is None or (type(session_id) is int and session_id > 0))
        and _is_positive_number(entry.get("started_at"))
    )


def _valid_controller_entry(entry: dict[str, Any]) -> bool:
    return (
        isinstance(entry.get("controller_id"), str)
        and bool(entry.get("controller_id"))
        and type(entry.get("pid")) is int
        and entry["pid"] > 0
        and _is_positive_number(entry.get("process_started_at"))
        and _is_positive_number(entry.get("claimed_at"))
    )


def _valid_pool_entry(env_name: str, entry: dict[str, Any]) -> bool:
    return (
        entry.get("env_name") == env_name
        and isinstance(entry.get("pool_id"), str)
        and bool(entry.get("pool_id"))
        and type(entry.get("expected_worker_count")) is int
        and entry["expected_worker_count"] >= 1
        and type(entry.get("commissioned")) is bool
        and _is_positive_number(entry.get("started_at"))
        and ("commissioned_at" not in entry or _is_positive_number(entry.get("commissioned_at")))
    )


def load_workers(root: str | Path) -> dict[str, Any]:
    try:
        path = state_dir(root) / WORKERS_FILE
        exists = path.exists()
    except OSError as error:
        raise RuntimeRegistryError(f"Cannot inspect worker registry under {Path(root).resolve()}") from error
    if not exists:
        return _empty_registry()
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise RuntimeRegistryError(f"Cannot read worker registry {path}") from error
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RuntimeRegistryError(f"Worker registry {path} contains invalid JSON") from error
    if not isinstance(data, dict):
        raise RuntimeRegistryError(f"Worker registry {path} must contain a JSON object")
    schema_version = data.get("schema_version")
    if type(schema_version) is not int or schema_version != SCHEMA_VERSION:
        raise RuntimeRegistryError(
            f"Worker registry {path} has unsupported schema version {schema_version!r}; expected {SCHEMA_VERSION}"
        )
    for field in ("workers", "controllers", "persistent_pools"):
        entries = data.get(field)
        if not isinstance(entries, dict):
            raise RuntimeRegistryError(f"Worker registry {path} field {field!r} must be an object")
        if any(not isinstance(key, str) or not isinstance(value, dict) for key, value in entries.items()):
            raise RuntimeRegistryError(f"Worker registry {path} field {field!r} contains an invalid entry")
    workers = data["workers"]
    controllers = data["controllers"]
    persistent_pools = data["persistent_pools"]
    if any(not _valid_worker_entry(key, entry) for key, entry in workers.items()):
        raise RuntimeRegistryError(f"Worker registry {path} field 'workers' contains an invalid entry")
    if any(not key or not _valid_controller_entry(entry) for key, entry in controllers.items()):
        raise RuntimeRegistryError(f"Worker registry {path} field 'controllers' contains an invalid entry")
    if any(not key or not _valid_pool_entry(key, entry) for key, entry in persistent_pools.items()):
        raise RuntimeRegistryError(f"Worker registry {path} field 'persistent_pools' contains an invalid entry")
    return data


def _recorded_process_tree_state(entry: dict[str, Any]) -> _RecordedTreeState:
    """Inspect a recorded leader and any safely attributable POSIX descendants."""
    pid = int(entry["pid"])
    expected_started_at = float(entry["process_started_at"])
    try:
        actual_started_at = psutil.Process(pid).create_time()
    except psutil.NoSuchProcess:
        if os.name == "nt":
            return _RecordedTreeState.DEAD
        try:
            group_alive = recorded_posix_group_exists(
                pid,
                expected_process_group_id=(
                    int(entry["process_group_id"]) if entry.get("process_group_id") is not None else None
                ),
                expected_session_id=(int(entry["session_id"]) if entry.get("session_id") is not None else None),
            )
        except ProcessTerminationError as error:
            raise WorkerIdentityUnavailableError(
                f"Cannot inspect recorded worker process tree for PID {pid}"
            ) from error
        return _RecordedTreeState.GROUP_ALIVE if group_alive else _RecordedTreeState.DEAD
    except psutil.AccessDenied as error:
        raise WorkerIdentityUnavailableError(f"Cannot inspect recorded worker PID {pid}") from error
    if actual_started_at == expected_started_at:
        return _RecordedTreeState.LEADER_ALIVE
    raise WorkerIdentityUnavailableError(
        f"Recorded worker PID {pid} was reused; process-tree death cannot be proven safely"
    )


def worker_key(env_name: str, worker_index: int, pool_id: str | None = None) -> str:
    if pool_id is None:
        return f"{env_name}:{worker_index}"
    return f"{env_name}:{pool_id}:{worker_index}"


def record_worker(
    root: str | Path,
    *,
    env_name: str,
    env_path: str | Path | None,
    worker_index: int,
    pid: int,
    port: int,
    persistent: bool,
    generation_id: str,
    recipe_hash: str,
    worker_runtime_version: str,
    protocol_version: int,
    pool_id: str | None = None,
) -> None:
    if persistent and not pool_id:
        raise ValueError("persistent workers require a pool_id")
    key = worker_key(env_name, worker_index, pool_id)
    identity = capture_process_identity(pid)
    with root_lock(root):
        registry = load_workers(root)
        pool = registry["persistent_pools"].get(env_name) if persistent else None
        if persistent and (
            pool is None or pool.get("pool_id") != pool_id or type(pool.get("expected_worker_count")) is not int
        ):
            raise RuntimeError(
                f"Persistent worker {env_name!r}:{worker_index} does not belong to an active pool attempt"
            )
        registry["workers"][key] = {
            "env_name": env_name,
            "env_path": str(Path(env_path).resolve()) if env_path is not None else None,
            "worker_index": worker_index,
            "pool_id": pool_id,
            "pid": pid,
            "port": port,
            "persistent": persistent,
            "generation_id": generation_id,
            "recipe_hash": recipe_hash,
            "worker_runtime_version": worker_runtime_version,
            "protocol_version": protocol_version,
            "commissioned": bool(pool and pool.get("commissioned")),
            "process_started_at": identity.started_at,
            "process_group_id": identity.process_group_id,
            "session_id": identity.session_id,
            "started_at": time.time(),
        }
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def begin_persistent_pool_attempt(
    root: str | Path,
    *,
    env_name: str,
    pool_id: str,
    expected_worker_count: int,
) -> None:
    """Durably publish an uncommissioned persistent-pool launch attempt."""
    if not pool_id:
        raise ValueError("pool_id must be nonempty")
    if expected_worker_count < 1:
        raise ValueError("expected_worker_count must be at least one")
    with root_lock(root):
        registry = load_workers(root)
        current = registry["persistent_pools"].get(env_name)
        if current is not None:
            raise RuntimeError(f"Persistent environment {env_name!r} already has a pool journal")
        registry["persistent_pools"][env_name] = {
            "env_name": env_name,
            "pool_id": pool_id,
            "expected_worker_count": expected_worker_count,
            "commissioned": False,
            "started_at": time.time(),
        }
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def commission_persistent_pool(
    root: str | Path,
    *,
    env_name: str,
    pool_id: str,
) -> None:
    """Atomically publish a complete authenticated worker set as attachable."""
    with root_lock(root):
        registry = load_workers(root)
        pool = registry["persistent_pools"].get(env_name)
        if pool is None or pool.get("pool_id") != pool_id:
            raise RuntimeError(f"Persistent pool attempt {env_name!r}/{pool_id} no longer exists")
        expected = pool.get("expected_worker_count")
        if type(expected) is not int or expected < 1:
            raise RuntimeError("Persistent pool journal has an invalid expected worker count")
        workers = [
            entry
            for entry in registry["workers"].values()
            if entry.get("persistent") and entry.get("env_name") == env_name and entry.get("pool_id") == pool_id
        ]
        indices = {entry.get("worker_index") for entry in workers}
        if len(workers) != expected or indices != set(range(expected)):
            raise RuntimeError(
                f"Persistent pool {env_name!r}/{pool_id} is incomplete: "
                f"expected {expected} workers, found indices {sorted(indices)}"
            )
        for entry in workers:
            entry["commissioned"] = True
        pool["commissioned"] = True
        pool["commissioned_at"] = time.time()
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def discard_persistent_pool(
    root: str | Path,
    *,
    env_name: str,
    pool_id: str,
) -> bool:
    """Forget one pool journal and its worker records after process cleanup."""
    with root_lock(root):
        registry = load_workers(root)
        survivors = [
            entry
            for entry in registry["workers"].values()
            if entry.get("env_name") == env_name and entry.get("pool_id") == pool_id
        ]
        if survivors:
            return False
        pool = registry["persistent_pools"].get(env_name)
        if pool is not None and pool.get("pool_id") == pool_id:
            registry["persistent_pools"].pop(env_name, None)
            atomic_write_json(state_dir(root) / WORKERS_FILE, registry)
        return True


def reconcile_persistent_pool(
    root: str | Path,
    env_name: str,
    *,
    grace: float,
) -> bool:
    """Remove an abandoned or incomplete pool after identity-safe tree termination.

    Returns ``True`` when an incomplete journal was reconciled. A complete,
    commissioned, live pool is left untouched and returns ``False``.
    """
    with root_lock(root):
        registry = load_workers(root)
        pool = registry["persistent_pools"].get(env_name)
        if pool is None:
            return False
        pool_id = pool.get("pool_id")
        expected = pool.get("expected_worker_count")
        if not isinstance(pool_id, str) or not pool_id or type(expected) is not int:
            raise RuntimeError(f"Persistent pool journal for {env_name!r} is malformed")
        workers = [
            entry
            for entry in registry["workers"].values()
            if entry.get("persistent") and entry.get("env_name") == env_name and entry.get("pool_id") == pool_id
        ]
        indices = {entry.get("worker_index") for entry in workers}
        tree_states: list[_RecordedTreeState] = []
        for entry in workers:
            pid = entry.get("pid")
            started_at = entry.get("process_started_at")
            if not isinstance(pid, int) or not isinstance(started_at, (int, float)):
                raise RuntimeRegistryError(f"Persistent worker record for {env_name!r} has incomplete process identity")
            tree_states.append(_recorded_process_tree_state(entry))
        complete = (
            bool(pool.get("commissioned"))
            and len(workers) == expected
            and indices == set(range(expected))
            and all(bool(entry.get("commissioned")) for entry in workers)
            and all(state is _RecordedTreeState.LEADER_ALIVE for state in tree_states)
        )
        if complete:
            return False

        for entry, tree_state in zip(workers, tree_states):
            pid = entry.get("pid")
            started_at = entry.get("process_started_at")
            if not isinstance(pid, int) or not isinstance(started_at, (int, float)):
                continue
            if tree_state is _RecordedTreeState.DEAD:
                continue
            try:
                terminate_attached_process_tree(
                    pid,
                    expected_started_at=float(started_at),
                    expected_process_group_id=(
                        int(entry["process_group_id"]) if entry.get("process_group_id") is not None else None
                    ),
                    expected_session_id=(int(entry["session_id"]) if entry.get("session_id") is not None else None),
                    grace=grace,
                )
            except ProcessTerminationError:
                # Preserve the journal and every record so the next operation
                # cannot mistake an unverified survivor for cleaned state.
                raise

        for key, entry in list(registry["workers"].items()):
            if entry.get("env_name") == env_name and entry.get("pool_id") == pool_id:
                registry["workers"].pop(key, None)
        registry["persistent_pools"].pop(env_name, None)
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)
        return True


def remove_worker(
    root: str | Path,
    env_name: str,
    worker_index: int,
    pool_id: str | None = None,
) -> None:
    key = worker_key(env_name, worker_index, pool_id)
    with root_lock(root):
        registry = load_workers(root)
        registry["workers"].pop(key, None)
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def remove_workers_for_env(root: str | Path, env_name: str) -> None:
    with root_lock(root):
        registry = load_workers(root)
        workers = registry["workers"]
        for key, entry in list(workers.items()):
            if entry.get("env_name") == env_name:
                workers.pop(key, None)
        registry["controllers"].pop(env_name, None)
        registry["persistent_pools"].pop(env_name, None)
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def claim_controller(root: str | Path, env_name: str, controller_id: str) -> None:
    """Atomically claim exclusive control of one persistent environment."""
    if not controller_id:
        raise ValueError("controller_id must be nonempty")
    pid = os.getpid()
    process_started_at = psutil.Process(pid).create_time()
    with root_lock(root):
        registry = load_workers(root)
        current = registry["controllers"].get(env_name)
        if current is not None and current.get("controller_id") != controller_id:
            current_pid = current.get("pid")
            current_started_at = current.get("process_started_at")
            live = False
            if isinstance(current_pid, int) and psutil.pid_exists(current_pid):
                try:
                    live = psutil.Process(current_pid).create_time() == current_started_at
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    live = True
            if live:
                raise RuntimeError(f"Persistent environment {env_name!r} is controlled by another live process")
        registry["controllers"][env_name] = {
            "controller_id": controller_id,
            "pid": pid,
            "process_started_at": process_started_at,
            "claimed_at": time.time(),
        }
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def release_controller(root: str | Path, env_name: str, controller_id: str) -> None:
    """Release a controller claim only when it is still owned by the caller."""
    with root_lock(root):
        registry = load_workers(root)
        current = registry["controllers"].get(env_name)
        if current is not None and current.get("controller_id") == controller_id:
            registry["controllers"].pop(env_name, None)
            atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def pid_exists(pid: int) -> bool:
    return psutil.pid_exists(pid)


def live_workers_for_env(
    root: str | Path,
    env_name: str,
    *,
    expected_identity: dict[str, object] | None = None,
    include_nonpersistent: bool = False,
) -> list[dict[str, Any]]:
    registry = load_workers(root)
    persistent_pool = registry["persistent_pools"].get(env_name)
    workers = []
    for key, entry in list(registry["workers"].items()):
        if entry.get("env_name") != env_name:
            continue
        if not include_nonpersistent and not entry.get("persistent", False):
            continue
        pid = entry.get("pid")
        process_started_at = entry.get("process_started_at")
        if not isinstance(pid, int) or not isinstance(process_started_at, (int, float)):
            raise RuntimeRegistryError(f"Worker registry entry {key!r} has incomplete process identity")
        tree_state = _recorded_process_tree_state(entry)
        if tree_state is _RecordedTreeState.DEAD:
            pool_id = entry.get("pool_id")
            remove_worker(
                root,
                env_name,
                int(entry.get("worker_index", -1)),
                str(pool_id) if pool_id is not None else None,
            )
            continue
        if tree_state is _RecordedTreeState.GROUP_ALIVE and not include_nonpersistent:
            raise WorkerIdentityUnavailableError(
                f"Recorded worker PID {pid} exited while its process group still has descendants"
            )
        if expected_identity is not None:
            mismatches = {
                field: (expected, entry.get(field))
                for field, expected in expected_identity.items()
                if entry.get(field) != expected
            }
            if mismatches:
                details = ", ".join(
                    f"{field}: expected {expected!r}, got {actual!r}"
                    for field, (expected, actual) in sorted(mismatches.items())
                )
                raise RuntimeError(f"Persistent worker identity mismatch ({details})")
        entry = dict(entry)
        entry["_key"] = key
        workers.append(entry)
    if not include_nonpersistent:
        if not isinstance(persistent_pool, dict) or not persistent_pool.get("commissioned"):
            return []
        pool_id = persistent_pool.get("pool_id")
        expected = persistent_pool.get("expected_worker_count")
        matching = [entry for entry in workers if entry.get("pool_id") == pool_id and entry.get("commissioned")]
        indices = {entry.get("worker_index") for entry in matching}
        if type(expected) is not int or len(matching) != expected or indices != set(range(expected)):
            return []
        workers = matching
    workers.sort(
        key=lambda item: (
            str(item.get("pool_id") or ""),
            int(item["worker_index"]),
        )
    )
    return workers
