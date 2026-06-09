from __future__ import annotations

import contextlib
import json
import os
import secrets
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psutil


SCHEMA_VERSION = 1
STATE_DIR_NAME = "state"
AUTH_KEY_FILE = "auth.key"
WORKERS_FILE = "workers.json"
LOCK_FILE = "workers.lock"


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
    return {"schema_version": SCHEMA_VERSION, "workers": {}}


def load_workers(root: str | Path) -> dict[str, Any]:
    path = state_dir(root) / WORKERS_FILE
    if not path.exists():
        return _empty_registry()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _empty_registry()
    if data.get("schema_version") != SCHEMA_VERSION or not isinstance(data.get("workers"), dict):
        return _empty_registry()
    return data


def worker_key(env_name: str, worker_index: int) -> str:
    return f"{env_name}:{worker_index}"


def record_worker(
    root: str | Path,
    *,
    env_name: str,
    env_path: str | Path | None,
    worker_index: int,
    pid: int,
    port: int,
    persistent: bool,
) -> None:
    key = worker_key(env_name, worker_index)
    with root_lock(root):
        registry = load_workers(root)
        registry["workers"][key] = {
            "env_name": env_name,
            "env_path": str(Path(env_path).resolve()) if env_path is not None else None,
            "worker_index": worker_index,
            "pid": pid,
            "port": port,
            "persistent": persistent,
            "started_at": time.time(),
        }
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def remove_worker(root: str | Path, env_name: str, worker_index: int) -> None:
    key = worker_key(env_name, worker_index)
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
        atomic_write_json(state_dir(root) / WORKERS_FILE, registry)


def pid_exists(pid: int) -> bool:
    return psutil.pid_exists(pid)


def live_workers_for_env(root: str | Path, env_name: str) -> list[dict[str, Any]]:
    registry = load_workers(root)
    workers = []
    for key, entry in list(registry["workers"].items()):
        if entry.get("env_name") != env_name or not entry.get("persistent", False):
            continue
        pid = entry.get("pid")
        if not isinstance(pid, int) or not pid_exists(pid):
            remove_worker(root, env_name, int(entry.get("worker_index", -1)))
            continue
        entry = dict(entry)
        entry["_key"] = key
        workers.append(entry)
    workers.sort(key=lambda item: int(item["worker_index"]))
    return workers
