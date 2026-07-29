"""Self-contained protocol value codecs used by the host and worker runtime."""

from __future__ import annotations

import contextlib
import importlib
import inspect
import json
import os
import secrets
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from multiprocessing import shared_memory
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from numpy.typing import NDArray

CORE_CODEC_ID = "wetlands.core"
CORE_CODEC_VERSION = 1
NUMPY_CODEC_ID = "wetlands.numpy-shm"
NUMPY_CODEC_VERSION = 1
CODEC_MARKER = "__wetlands_codec__"
MAX_ARRAY_DIMENSIONS = 64
MAX_ARRAY_NBYTES = (1 << 63) - 1
LEASE_LEDGER_SCHEMA_VERSION = 1
LEASE_LEDGER_FILE = "shared_memory_leases.json"
LEASE_LEDGER_LOCK_FILE = "shared_memory_leases.lock"

_created_names: set[str] = set()
_created_names_lock = threading.Lock()


class ValueCodecRegistry:
    """Private capability registry for the self-contained transport runtime.

    Wetlands 2 intentionally keeps codec registration internal until a third-party
    codec can be specified without exposing shared-memory lease implementation
    details as public API.
    """

    def __init__(self) -> None:
        self._codecs: dict[tuple[str, int], tuple[Any, Any, Any]] = {}

    def register(
        self,
        codec_id: str,
        version: int,
        *,
        matches: Any,
        encode: Any,
        decode: Any,
    ) -> None:
        capability = (codec_id, version)
        if not isinstance(codec_id, str) or not codec_id or type(version) is not int or version < 1:
            raise ValueError("Codec capabilities require a nonempty ID and positive integer version")
        if capability in self._codecs:
            raise ValueError(f"Codec {codec_id!r} version {version} is already registered")
        if not callable(matches) or not callable(encode) or not callable(decode):
            raise TypeError("Codec registration hooks must be callable")
        self._codecs[capability] = (matches, encode, decode)

    @property
    def capabilities(self) -> tuple[tuple[str, int], ...]:
        return tuple(self._codecs)

    def require(self, codec_id: str, version: int, *, path: str) -> None:
        if (codec_id, version) not in self._codecs:
            raise ValueDecodingError(f"{path}: unsupported codec {codec_id!r} version {version!r}")

    def encode(
        self,
        value: Any,
        *,
        path: str,
        seen: set[int],
        lease_context: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], list[SharedMemoryLease]]:
        for matches, encoder, _decoder in self._codecs.values():
            if matches(value):
                return encoder(
                    value,
                    path=path,
                    seen=seen,
                    lease_context=lease_context,
                )
        raise ValueEncodingError(
            f"{path}: unsupported value of type {type(value).__module__}.{type(value).__qualname__}"
        )

    def decode(
        self,
        payload: Any,
        *,
        copy_arrays: bool,
        path: str,
        attachments: list[SharedMemoryLease] | None,
        seen: set[int],
    ) -> Any:
        if not isinstance(payload, dict):
            raise ValueDecodingError(f"{path}: malformed codec payload")
        codec_id = payload.get("id")
        version = payload.get("version")
        if not isinstance(codec_id, str) or type(version) is not int:
            raise ValueDecodingError(f"{path}: malformed codec identity")
        self.require(codec_id, version, path=path)
        decoder = self._codecs[(codec_id, version)][2]
        return decoder(
            payload,
            copy_arrays=copy_arrays,
            path=path,
            attachments=attachments,
            seen=seen,
        )


REQUIRED_WORKER_CODECS = ((CORE_CODEC_ID, CORE_CODEC_VERSION),)


class ValueEncodingError(TypeError):
    pass


class ValueDecodingError(ValueError):
    pass


@dataclass
class SharedMemoryLease:
    name: str
    memory: shared_memory.SharedMemory
    creator: bool
    ledger_root: str | None = None
    released: bool = False
    _closed: bool = False
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self.memory.close()
            except (BufferError, FileNotFoundError):
                pass
            self._closed = True

    def unlink(self) -> None:
        with self._lock:
            if self.released:
                return
            try:
                self.memory.unlink()
            except FileNotFoundError:
                pass
            finally:
                if self.creator:
                    with _created_names_lock:
                        _created_names.discard(self.name)
                    if self.ledger_root is not None:
                        _remove_lease_records(self.ledger_root, [self.name])
                self.released = True

    def dispose(self) -> None:
        with self._lock:
            self.close()
            if self.creator:
                self.unlink()


def _ledger_state_dir(root: str | Path) -> Path:
    state = Path(root).expanduser().resolve(strict=False) / "state"
    state.mkdir(parents=True, exist_ok=True)
    try:
        state.chmod(0o700)
    except OSError:
        pass
    return state


@contextlib.contextmanager
def _lease_ledger_lock(root: str | Path) -> Iterator[None]:
    state = _ledger_state_dir(root)
    state.mkdir(parents=True, exist_ok=True)
    lock_path = state / LEASE_LEDGER_LOCK_FILE
    with open(lock_path, "a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            locking = getattr(msvcrt, "locking")
            locking(lock_file.fileno(), getattr(msvcrt, "LK_LOCK"), 1)
            try:
                yield
            finally:
                lock_file.seek(0)
                locking(lock_file.fileno(), getattr(msvcrt, "LK_UNLCK"), 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _empty_lease_ledger() -> dict[str, Any]:
    return {"schema_version": LEASE_LEDGER_SCHEMA_VERSION, "leases": {}}


def _load_lease_ledger(root: str | Path) -> dict[str, Any]:
    path = _ledger_state_dir(root) / LEASE_LEDGER_FILE
    if not path.exists():
        return _empty_lease_ledger()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Shared-memory lease ledger is unreadable: {path}") from error
    if payload.get("schema_version") != LEASE_LEDGER_SCHEMA_VERSION or not isinstance(payload.get("leases"), dict):
        raise RuntimeError(f"Shared-memory lease ledger has an incompatible schema: {path}")
    return payload


def _write_lease_ledger(root: str | Path, payload: dict[str, Any]) -> None:
    destination = _ledger_state_dir(root) / LEASE_LEDGER_FILE
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        dir=destination.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


def _validate_lease_context(context: Any) -> dict[str, Any]:
    required = {
        "root",
        "creator_pid",
        "creator_started_at",
        "environment_name",
        "generation_id",
        "pool_id",
        "task_id",
        "direction",
    }
    if not isinstance(context, dict) or set(context) != required:
        raise ValueEncodingError("Shared-memory lease context is malformed")
    if not isinstance(context["root"], str) or not context["root"]:
        raise ValueEncodingError("Shared-memory lease context has an invalid root")
    if type(context["creator_pid"]) is not int or context["creator_pid"] <= 0:
        raise ValueEncodingError("Shared-memory lease context has an invalid creator PID")
    if not isinstance(context["creator_started_at"], (int, float)):
        raise ValueEncodingError("Shared-memory lease context has an invalid creator identity")
    for name in ("environment_name", "generation_id", "pool_id", "task_id"):
        if not isinstance(context[name], str) or not context[name]:
            raise ValueEncodingError(f"Shared-memory lease context has an invalid {name}")
    if context["direction"] not in {"input", "output"}:
        raise ValueEncodingError("Shared-memory lease context has an invalid direction")
    return dict(context)


def _record_lease(name: str, context: dict[str, Any]) -> None:
    metadata = _validate_lease_context(context)
    root = metadata.pop("root")
    with _lease_ledger_lock(root):
        ledger = _load_lease_ledger(root)
        if name in ledger["leases"]:
            raise RuntimeError(f"Shared-memory segment {name!r} already has a lease record")
        ledger["leases"][name] = {
            "name": name,
            **metadata,
            "created_at": time.time(),
        }
        _write_lease_ledger(root, ledger)


def _new_shared_memory_name() -> str:
    """Return an explicit, unpredictable name suitable for every supported OS."""
    # Darwin limits POSIX shared-memory names to 31 bytes including the slash
    # prepended by multiprocessing.shared_memory. A 96-bit token leaves ample
    # collision resistance while remaining portable.
    return f"wls_{secrets.token_hex(12)}"


def _remove_lease_records(root: str | Path, names: list[str]) -> None:
    if not names:
        return
    with _lease_ledger_lock(root):
        ledger = _load_lease_ledger(root)
        changed = False
        for name in names:
            changed = ledger["leases"].pop(name, None) is not None or changed
        if changed:
            _write_lease_ledger(root, ledger)


def load_shared_memory_lease_ledger(root: str | Path) -> dict[str, Any]:
    """Return a snapshot of the manager-scoped lease ledger for diagnostics/tests."""
    with _lease_ledger_lock(root):
        return _load_lease_ledger(root)


def _open_for_unlink(name: str) -> shared_memory.SharedMemory:
    parameters: Mapping[str, inspect.Parameter]
    try:
        parameters = inspect.signature(shared_memory.SharedMemory).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "track" in parameters:
        return shared_memory.SharedMemory(name=name, create=False, track=False)
    return shared_memory.SharedMemory(name=name, create=False)


def reconcile_shared_memory_leases(root: str | Path) -> tuple[str, ...]:
    """Unlink leases only after proving that their recorded creator is dead."""
    try:
        import psutil
    except ImportError as error:
        raise RuntimeError("psutil is required to reconcile shared-memory leases") from error

    recovered: list[str] = []
    with _lease_ledger_lock(root):
        ledger = _load_lease_ledger(root)
        for name, entry in list(ledger["leases"].items()):
            pid = entry.get("creator_pid")
            started_at = entry.get("creator_started_at")
            if type(pid) is not int or not isinstance(started_at, (int, float)):
                raise RuntimeError(f"Shared-memory lease {name!r} has an invalid creator identity")
            try:
                actual_started_at = psutil.Process(pid).create_time()
            except psutil.NoSuchProcess:
                creator_dead = True
            except psutil.AccessDenied:
                creator_dead = False
            else:
                creator_dead = actual_started_at != float(started_at)
            if not creator_dead:
                continue
            try:
                memory = _open_for_unlink(name)
            except FileNotFoundError:
                pass
            else:
                try:
                    memory.unlink()
                except FileNotFoundError:
                    pass
                finally:
                    memory.close()
            ledger["leases"].pop(name, None)
            with _created_names_lock:
                _created_names.discard(name)
            recovered.append(name)
        if recovered:
            _write_lease_ledger(root, ledger)
    return tuple(recovered)


def _open_non_owner(name: str) -> shared_memory.SharedMemory:
    parameters: Mapping[str, inspect.Parameter]
    try:
        parameters = inspect.signature(shared_memory.SharedMemory).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "track" in parameters:
        return shared_memory.SharedMemory(name=name, create=False, track=False)
    memory = shared_memory.SharedMemory(name=name, create=False)
    with _created_names_lock:
        locally_owned = name in _created_names
    if os.name != "nt" and not locally_owned:
        try:
            from multiprocessing import resource_tracker

            resource_tracker.unregister(memory._name, "shared_memory")  # type: ignore[attr-defined]
        except Exception:
            pass
    return memory


def _descriptor(codec: str, version: int, kind: str, **payload: Any) -> dict[str, Any]:
    return {
        CODEC_MARKER: {
            "id": codec,
            "version": version,
            "kind": kind,
            **payload,
        }
    }


def descriptor_codecs(*descriptors: Any) -> tuple[tuple[str, int], ...]:
    """Return the codec capabilities actually referenced by descriptors."""
    found: set[tuple[str, int]] = set()
    seen: set[int] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            identity = id(value)
            if identity in seen:
                raise ValueDecodingError("Cyclic codec descriptors are unsupported")
            seen.add(identity)
            try:
                if set(value) == {CODEC_MARKER} and isinstance(value[CODEC_MARKER], dict):
                    payload = value[CODEC_MARKER]
                    codec_id = payload.get("id")
                    version = payload.get("version")
                    if not isinstance(codec_id, str) or type(version) is not int:
                        raise ValueDecodingError("Malformed codec identity in value descriptor")
                    found.add((codec_id, version))
                for item in value.values():
                    visit(item)
            finally:
                seen.remove(identity)
        elif isinstance(value, (list, tuple)):
            identity = id(value)
            if identity in seen:
                raise ValueDecodingError("Cyclic codec descriptors are unsupported")
            seen.add(identity)
            try:
                for item in value:
                    visit(item)
            finally:
                seen.remove(identity)

    for descriptor in descriptors:
        visit(descriptor)
    order = {capability: index for index, capability in enumerate(SUPPORTED_CODECS)}
    return tuple(sorted(found, key=lambda capability: order.get(capability, len(order))))


def _encode_dtype(dtype: Any) -> dict[str, Any]:
    if dtype.metadata:
        raise ValueEncodingError("NumPy dtype metadata is unsupported")
    if dtype.fields is None:
        if dtype.subdtype is not None:
            base, shape = dtype.subdtype
            return {
                "kind": "subarray",
                "base": _encode_dtype(base),
                "shape": tuple(int(item) for item in shape),
            }
        return {"kind": "scalar", "value": dtype.str}

    fields: list[dict[str, Any]] = []
    for name in dtype.names or ():
        field = dtype.fields[name]
        title = field[2] if len(field) > 2 else None
        if title is not None and not isinstance(title, str):
            raise ValueEncodingError("NumPy field titles must be strings")
        fields.append(
            {
                "name": name,
                "dtype": _encode_dtype(field[0]),
                "offset": int(field[1]),
                "title": title,
            }
        )
    return {
        "kind": "struct",
        "fields": fields,
        "itemsize": int(dtype.itemsize),
        "aligned": bool(dtype.isalignedstruct),
    }


def _decode_dtype(descriptor: Any, *, path: str, np: Any) -> Any:
    if not isinstance(descriptor, dict):
        raise ValueDecodingError(f"{path}: invalid array dtype descriptor")
    kind = descriptor.get("kind")
    try:
        if kind == "scalar":
            if set(descriptor) != {"kind", "value"} or not isinstance(descriptor["value"], str):
                raise ValueDecodingError(f"{path}: invalid scalar dtype descriptor")
            dtype = np.dtype(descriptor["value"])
        elif kind == "subarray":
            if set(descriptor) != {"kind", "base", "shape"}:
                raise ValueDecodingError(f"{path}: invalid subarray dtype descriptor")
            shape = descriptor["shape"]
            if (
                not isinstance(shape, tuple)
                or not shape
                or len(shape) > MAX_ARRAY_DIMENSIONS
                or not all(type(item) is int and 0 <= item <= MAX_ARRAY_NBYTES for item in shape)
            ):
                raise ValueDecodingError(f"{path}: invalid subarray dtype shape")
            dtype = np.dtype((_decode_dtype(descriptor["base"], path=f"{path}.base", np=np), shape))
        elif kind == "struct":
            if set(descriptor) != {"kind", "fields", "itemsize", "aligned"}:
                raise ValueDecodingError(f"{path}: invalid structured dtype descriptor")
            raw_fields = descriptor["fields"]
            itemsize = descriptor["itemsize"]
            aligned = descriptor["aligned"]
            if (
                not isinstance(raw_fields, list)
                or not raw_fields
                or type(itemsize) is not int
                or not (0 <= itemsize <= MAX_ARRAY_NBYTES)
                or type(aligned) is not bool
            ):
                raise ValueDecodingError(f"{path}: invalid structured dtype descriptor")
            names: list[str] = []
            formats: list[Any] = []
            offsets: list[int] = []
            titles: list[str | None] = []
            for index, field in enumerate(raw_fields):
                field_path = f"{path}.fields[{index}]"
                if not isinstance(field, dict) or set(field) != {"name", "dtype", "offset", "title"}:
                    raise ValueDecodingError(f"{field_path}: invalid dtype field descriptor")
                name = field["name"]
                offset = field["offset"]
                title = field["title"]
                if (
                    not isinstance(name, str)
                    or not name
                    or name in names
                    or type(offset) is not int
                    or not (0 <= offset <= itemsize)
                    or (title is not None and not isinstance(title, str))
                ):
                    raise ValueDecodingError(f"{field_path}: invalid dtype field descriptor")
                names.append(name)
                formats.append(_decode_dtype(field["dtype"], path=f"{field_path}.dtype", np=np))
                offsets.append(offset)
                titles.append(title)
            specification: dict[str, Any] = {
                "names": names,
                "formats": formats,
                "offsets": offsets,
                "itemsize": itemsize,
            }
            if any(title is not None for title in titles):
                specification["titles"] = titles
            dtype = np.dtype(specification, align=aligned)
            if dtype.itemsize != itemsize or bool(dtype.isalignedstruct) != aligned:
                raise ValueDecodingError(f"{path}: structured dtype layout is inconsistent")
        else:
            raise ValueDecodingError(f"{path}: unsupported dtype descriptor kind {kind!r}")
    except ValueDecodingError:
        raise
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueDecodingError(f"{path}: invalid array dtype") from error
    if dtype.hasobject:
        raise ValueDecodingError(f"{path}: object-dtype arrays are unsupported")
    if dtype.metadata:
        raise ValueDecodingError(f"{path}: NumPy dtype metadata is unsupported")
    return dtype


def _encode_registered_value(
    value: Any,
    *,
    path: str,
    seen: set[int],
    lease_context: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[SharedMemoryLease]]:
    if value is None:
        return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "none"), []
    if isinstance(value, bool):
        return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "bool", value=bool(value)), []
    if isinstance(value, int):
        return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "int", value=int(value)), []
    if isinstance(value, float):
        return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "float", value=float(value)), []
    if isinstance(value, str):
        return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "str", value=str(value)), []
    if isinstance(value, bytes):
        return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "bytes", value=bytes(value)), []

    np: Any
    try:
        np = importlib.import_module("numpy")
    except ImportError:
        np = None
    if np is not None and isinstance(value, np.ndarray):
        if value.dtype.hasobject:
            raise ValueEncodingError(f"{path}: object-dtype NumPy arrays are unsupported")
        if value.dtype.metadata:
            raise ValueEncodingError(f"{path}: NumPy dtype metadata is unsupported")
        contiguous = np.array(value, copy=True, order="C", subok=False)
        nbytes = int(contiguous.nbytes)
        if contiguous.ndim > MAX_ARRAY_DIMENSIONS or nbytes > MAX_ARRAY_NBYTES:
            raise ValueEncodingError(f"{path}: NumPy array exceeds transport limits")
        try:
            dtype = _encode_dtype(contiguous.dtype)
        except ValueEncodingError as error:
            raise ValueEncodingError(f"{path}: {error}") from error
        if nbytes == 0:
            return (
                _descriptor(
                    NUMPY_CODEC_ID,
                    NUMPY_CODEC_VERSION,
                    "ndarray",
                    name=None,
                    shape=tuple(int(item) for item in contiguous.shape),
                    dtype=dtype,
                    nbytes=0,
                    segment_size=0,
                ),
                [],
            )
        ledger_root = None
        memory_name = _new_shared_memory_name()
        if lease_context is not None:
            ledger_root = str(_validate_lease_context(lease_context)["root"])
            _record_lease(memory_name, lease_context)
        try:
            memory = shared_memory.SharedMemory(
                name=memory_name,
                create=True,
                size=nbytes,
            )
        except BaseException:
            if ledger_root is not None:
                _remove_lease_records(ledger_root, [memory_name])
            raise
        lease = SharedMemoryLease(
            memory.name,
            memory,
            creator=True,
            ledger_root=ledger_root,
        )
        with _created_names_lock:
            _created_names.add(memory.name)
        try:
            target: NDArray[Any] = np.ndarray(contiguous.shape, dtype=contiguous.dtype, buffer=memory.buf)
            target[...] = contiguous
            del target
        except BaseException:
            lease.dispose()
            raise
        return (
            _descriptor(
                NUMPY_CODEC_ID,
                NUMPY_CODEC_VERSION,
                "ndarray",
                name=memory.name,
                shape=tuple(int(item) for item in contiguous.shape),
                dtype=dtype,
                nbytes=nbytes,
                segment_size=memory.size,
            ),
            [lease],
        )

    if isinstance(value, (list, tuple, dict)):
        identity = id(value)
        if identity in seen:
            raise ValueEncodingError(f"{path}: cyclic values are unsupported")
        seen.add(identity)
        try:
            leases: list[SharedMemoryLease] = []
            if isinstance(value, list):
                items = []
                for index, item in enumerate(value):
                    encoded, item_leases = encode_value(
                        item,
                        path=f"{path}[{index}]",
                        _seen=seen,
                        lease_context=lease_context,
                    )
                    items.append(encoded)
                    leases.extend(item_leases)
                return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "list", items=items), leases
            if isinstance(value, tuple):
                items = []
                for index, item in enumerate(value):
                    encoded, item_leases = encode_value(
                        item,
                        path=f"{path}[{index}]",
                        _seen=seen,
                        lease_context=lease_context,
                    )
                    items.append(encoded)
                    leases.extend(item_leases)
                return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "tuple", items=items), leases
            pairs = []
            for key, item in value.items():
                if not isinstance(key, (type(None), bool, int, float, str, bytes)):
                    raise ValueEncodingError(f"{path}: unsupported dictionary key {key!r}")
                encoded_key, key_leases = encode_value(
                    key,
                    path=f"{path}.<key>",
                    _seen=seen,
                    lease_context=lease_context,
                )
                encoded_item, item_leases = encode_value(
                    item,
                    path=f"{path}[{key!r}]",
                    _seen=seen,
                    lease_context=lease_context,
                )
                pairs.append((encoded_key, encoded_item))
                leases.extend(key_leases)
                leases.extend(item_leases)
            return _descriptor(CORE_CODEC_ID, CORE_CODEC_VERSION, "dict", items=pairs), leases
        except BaseException:
            dispose_leases(leases, unlink=True)
            raise
        finally:
            seen.remove(identity)
    raise ValueEncodingError(f"{path}: unsupported value of type {type(value).__module__}.{type(value).__qualname__}")


def encode_value(
    value: Any,
    *,
    path: str = "$",
    _seen: set[int] | None = None,
    lease_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[SharedMemoryLease]]:
    seen = _seen if _seen is not None else set()
    return _registry.encode(
        value,
        path=path,
        seen=seen,
        lease_context=lease_context,
    )


def decode_value(
    descriptor: Any,
    *,
    copy_arrays: bool,
    path: str = "$",
    attachments: list[SharedMemoryLease] | None = None,
) -> Any:
    attachment_start = len(attachments) if attachments is not None else 0
    try:
        return _decode_value(
            descriptor,
            copy_arrays=copy_arrays,
            path=path,
            attachments=attachments,
            seen=set(),
        )
    except BaseException:
        if attachments is not None:
            added = attachments[attachment_start:]
            del attachments[attachment_start:]
            dispose_leases(added, unlink=False)
        raise


def _decode_value(
    descriptor: Any,
    *,
    copy_arrays: bool,
    path: str,
    attachments: list[SharedMemoryLease] | None,
    seen: set[int],
) -> Any:
    if not isinstance(descriptor, dict) or set(descriptor) != {CODEC_MARKER}:
        raise ValueDecodingError(f"{path}: malformed value descriptor")
    identity = id(descriptor)
    if identity in seen:
        raise ValueDecodingError(f"{path}: cyclic value descriptors are unsupported")
    seen.add(identity)
    try:
        return _registry.decode(
            descriptor[CODEC_MARKER],
            copy_arrays=copy_arrays,
            path=path,
            attachments=attachments,
            seen=seen,
        )
    finally:
        seen.remove(identity)


def _decode_payload(
    payload: Any,
    *,
    copy_arrays: bool,
    path: str,
    attachments: list[SharedMemoryLease] | None,
    seen: set[int],
) -> Any:
    if not isinstance(payload, dict):
        raise ValueDecodingError(f"{path}: malformed codec payload")
    codec_id = payload.get("id")
    version = payload.get("version")
    kind = payload.get("kind")
    if not isinstance(codec_id, str) or type(version) is not int or not isinstance(kind, str):
        raise ValueDecodingError(f"{path}: malformed codec identity")
    _registry.require(codec_id, version, path=path)
    if codec_id == CORE_CODEC_ID:
        if kind == "none":
            if set(payload) != {"id", "version", "kind"}:
                raise ValueDecodingError(f"{path}: invalid none payload")
            return None
        if kind in {"bool", "int", "float", "str", "bytes"}:
            if set(payload) != {"id", "version", "kind", "value"}:
                raise ValueDecodingError(f"{path}: invalid {kind} payload")
            value = payload.get("value")
            expected = {
                "bool": bool,
                "int": int,
                "float": float,
                "str": str,
                "bytes": bytes,
            }[kind]
            if type(value) is not expected:
                raise ValueDecodingError(f"{path}: invalid {kind} payload")
            return value
        if set(payload) != {"id", "version", "kind", "items"}:
            raise ValueDecodingError(f"{path}: invalid {kind} payload")
        items = payload.get("items")
        if not isinstance(items, list):
            raise ValueDecodingError(f"{path}: invalid {kind} items")
        if kind == "list":
            return [
                _decode_value(
                    item,
                    copy_arrays=copy_arrays,
                    path=f"{path}[{index}]",
                    attachments=attachments,
                    seen=seen,
                )
                for index, item in enumerate(items)
            ]
        if kind == "tuple":
            return tuple(
                _decode_value(
                    item,
                    copy_arrays=copy_arrays,
                    path=f"{path}[{index}]",
                    attachments=attachments,
                    seen=seen,
                )
                for index, item in enumerate(items)
            )
        if kind == "dict":
            result = {}
            for index, pair in enumerate(items):
                if type(pair) is not tuple or len(pair) != 2:
                    raise ValueDecodingError(f"{path}: invalid dictionary pair at index {index}")
                key = _decode_value(
                    pair[0],
                    copy_arrays=copy_arrays,
                    path=f"{path}.<key>",
                    attachments=attachments,
                    seen=seen,
                )
                if type(key) not in {type(None), bool, int, float, str, bytes}:
                    raise ValueDecodingError(f"{path}: unsupported decoded dictionary key")
                if key in result:
                    raise ValueDecodingError(f"{path}: duplicate decoded dictionary key {key!r}")
                result[key] = _decode_value(
                    pair[1],
                    copy_arrays=copy_arrays,
                    path=f"{path}[{key!r}]",
                    attachments=attachments,
                    seen=seen,
                )
            return result
        raise ValueDecodingError(f"{path}: unsupported core kind {kind!r}")

    if kind != "ndarray":
        raise ValueDecodingError(f"{path}: unsupported NumPy kind {kind!r}")
    if set(payload) != {
        "id",
        "version",
        "kind",
        "name",
        "shape",
        "dtype",
        "nbytes",
        "segment_size",
    }:
        raise ValueDecodingError(f"{path}: invalid NumPy payload")
    try:
        import numpy as np
    except ImportError as error:
        raise ValueDecodingError(f"{path}: NumPy is required to decode an array: {error}") from error
    shape_payload = payload.get("shape")
    if (
        not isinstance(shape_payload, tuple)
        or len(shape_payload) > MAX_ARRAY_DIMENSIONS
        or not all(type(item) is int and 0 <= item <= MAX_ARRAY_NBYTES for item in shape_payload)
    ):
        raise ValueDecodingError(f"{path}: invalid array shape")
    dtype = _decode_dtype(payload.get("dtype"), path=f"{path}.dtype", np=np)
    count = 1
    for dimension in shape_payload:
        if dimension and count > MAX_ARRAY_NBYTES // dimension:
            raise ValueDecodingError(f"{path}: array shape overflows")
        count *= dimension
    if dtype.itemsize and count > MAX_ARRAY_NBYTES // int(dtype.itemsize):
        raise ValueDecodingError(f"{path}: array byte count overflows")
    expected_nbytes = count * int(dtype.itemsize)
    nbytes = payload.get("nbytes")
    if type(nbytes) is not int or nbytes != expected_nbytes:
        raise ValueDecodingError(f"{path}: array byte count does not match shape and dtype")
    segment_size = payload.get("segment_size")
    if type(segment_size) is not int or segment_size < nbytes:
        raise ValueDecodingError(f"{path}: invalid shared-memory segment size")
    if nbytes == 0:
        if payload.get("name") is not None:
            raise ValueDecodingError(f"{path}: empty array cannot reference shared memory")
        try:
            return np.empty(shape_payload, dtype=dtype)
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueDecodingError(f"{path}: invalid empty array layout") from error
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        raise ValueDecodingError(f"{path}: invalid shared-memory name")
    memory = _open_non_owner(name)
    if len(memory.buf) < nbytes or (os.name != "nt" and memory.size != segment_size):
        memory.close()
        raise ValueDecodingError(f"{path}: shared-memory size does not match its descriptor")
    lease = SharedMemoryLease(name, memory, creator=False)
    try:
        array: NDArray[Any] = np.ndarray(shape_payload, dtype=dtype, buffer=memory.buf)
        if not copy_arrays:
            if attachments is None:
                raise ValueDecodingError(f"{path}: zero-copy array decoding requires an attachment lease list")
            attachments.append(lease)
            return array
        array_copy = array.copy(order="C")
        del array
        if attachments is not None:
            attachments.append(lease)
        else:
            lease.close()
        return array_copy
    except BaseException:
        lease.close()
        raise


def dispose_leases(leases: list[SharedMemoryLease], *, unlink: bool) -> None:
    for lease in leases:
        if unlink:
            lease.dispose()
        else:
            lease.close()


def unlink_names(names: list[str], *, ledger_root: str | Path | None = None) -> None:
    for name in names:
        try:
            memory = _open_for_unlink(name)
        except FileNotFoundError:
            continue
        try:
            memory.unlink()
        except FileNotFoundError:
            pass
        finally:
            memory.close()
            with _created_names_lock:
                _created_names.discard(name)
    if ledger_root is not None:
        _remove_lease_records(ledger_root, names)


def _matches_core_value(value: Any) -> bool:
    return isinstance(value, (type(None), bool, int, float, str, bytes, list, tuple, dict))


_registry = ValueCodecRegistry()
_registry.register(
    CORE_CODEC_ID,
    CORE_CODEC_VERSION,
    matches=_matches_core_value,
    encode=_encode_registered_value,
    decode=_decode_payload,
)
try:
    import numpy as _numpy_runtime
except ImportError:
    pass
else:
    _registry.register(
        NUMPY_CODEC_ID,
        NUMPY_CODEC_VERSION,
        matches=lambda value: isinstance(value, _numpy_runtime.ndarray),
        encode=_encode_registered_value,
        decode=_decode_payload,
    )
SUPPORTED_CODECS = _registry.capabilities
