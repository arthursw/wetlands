"""Self-contained contracts for the Wetlands execution protocol.

This module deliberately has no imports from the ``wetlands`` package so the
worker bootstrap can load the same source file inside an isolated environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, cast

EXECUTION_PROTOCOL_VERSION = 1
MANAGEMENT_PROTOCOL_VERSION = 1
WORKER_RUNTIME_VERSION = "2.3.3"

ACTION_HELLO = "hello"
ACTION_EXECUTE = "execute"
ACTION_ACCEPTED = "accepted"
ACTION_INPUT_RELEASED = "input_released"
ACTION_UPDATE = "update"
ACTION_LOG = "log"
ACTION_RESULT_OFFER = "result_offer"
ACTION_RELEASE = "release"
ACTION_RELEASED = "released"
ACTION_FAILURE = "error"
ACTION_CANCEL = "cancel"
ACTION_CANCELED = "canceled"

TASK_ACTIONS = frozenset(
    {
        ACTION_EXECUTE,
        ACTION_ACCEPTED,
        ACTION_INPUT_RELEASED,
        ACTION_UPDATE,
        ACTION_LOG,
        ACTION_RESULT_OFFER,
        ACTION_RELEASE,
        ACTION_RELEASED,
        ACTION_FAILURE,
        ACTION_CANCEL,
        ACTION_CANCELED,
    }
)

WORKER_TASK_ACTIONS = frozenset(
    {
        ACTION_ACCEPTED,
        ACTION_INPUT_RELEASED,
        ACTION_UPDATE,
        ACTION_LOG,
        ACTION_RESULT_OFFER,
        ACTION_RELEASED,
        ACTION_FAILURE,
        ACTION_CANCELED,
    }
)

_TASK_MESSAGE_FIELDS = frozenset({"action", "protocol_version", "task_id"})
_MAX_REMOTE_EXCEPTION_DEPTH = 32


class ProtocolError(RuntimeError):
    """A malformed execution message was observed."""


class ProtocolCompatibilityError(ProtocolError):
    """The host and worker cannot safely communicate."""


@dataclass(frozen=True)
class CodecCapability:
    id: str
    version: int


@dataclass(frozen=True)
class WorkerCapabilities:
    protocol_version: int
    codecs: tuple[CodecCapability, ...]
    runtime_version: str
    python_version: str
    pid: int
    environment_path: str
    generation_id: str
    recipe_hash: str


def _codec_payload(codecs: Iterable[tuple[str, int]]) -> list[dict[str, object]]:
    return [{"id": codec_id, "version": version} for codec_id, version in codecs]


def protocol_message(action: str, task_id: str, **payload: Any) -> dict[str, Any]:
    if action not in TASK_ACTIONS:
        raise ValueError(f"Unknown task protocol action: {action!r}")
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("task_id must be a nonempty string")
    return {
        "action": action,
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": task_id,
        **payload,
    }


def execution_envelope(
    *,
    task_id: str,
    target: dict[str, Any],
    args: dict[str, Any],
    kwargs: dict[str, Any],
    codecs: Iterable[tuple[str, int]],
    context_keyword: str | None = None,
) -> dict[str, Any]:
    if context_keyword is not None and not context_keyword.isidentifier():
        raise ValueError("context_keyword must be a Python identifier or None")
    validated_target = validate_target(target)
    return protocol_message(
        ACTION_EXECUTE,
        task_id,
        target=validated_target,
        args=args,
        kwargs=kwargs,
        codecs=_codec_payload(codecs),
        context_keyword=context_keyword,
    )


def import_target(target: str) -> dict[str, Any]:
    if target.count(":") != 1:
        raise ValueError("Import target must be 'package.module:qualified.callable'")
    module_name, qualname = target.split(":", 1)
    descriptor = {"kind": "import", "module": module_name, "qualname": qualname}
    return validate_target(descriptor)


def path_target(path: str | Path, qualname: str, *, cache: bool) -> dict[str, Any]:
    canonical = Path(path).expanduser().resolve(strict=True)
    if not canonical.is_file():
        raise FileNotFoundError(canonical)
    descriptor = {
        "kind": "path",
        "path": str(canonical),
        "qualname": qualname,
        "cache": bool(cache),
    }
    return validate_target(descriptor)


def validate_target(target: Any) -> dict[str, Any]:
    if not isinstance(target, dict):
        raise ProtocolError("Execution target must be an object")
    kind = target.get("kind")
    qualname = target.get("qualname")
    if not _is_dotted_identifier(qualname):
        raise ProtocolError("Execution target has an invalid qualified name")
    if kind == "import":
        module_name = target.get("module")
        if not _is_dotted_identifier(module_name):
            raise ProtocolError("Import target has an invalid module name")
        if set(target) != {"kind", "module", "qualname"}:
            raise ProtocolError("Import target contains unexpected fields")
        return {"kind": "import", "module": module_name, "qualname": qualname}
    if kind == "path":
        raw_path = target.get("path")
        cache = target.get("cache")
        if not isinstance(raw_path, str) or not raw_path:
            raise ProtocolError("Path target has an invalid path")
        if type(cache) is not bool:
            raise ProtocolError("Path target cache flag must be a boolean")
        if set(target) != {"kind", "path", "qualname", "cache"}:
            raise ProtocolError("Path target contains unexpected fields")
        return {"kind": "path", "path": raw_path, "qualname": qualname, "cache": cache}
    raise ProtocolError(f"Unsupported execution target kind: {kind!r}")


def validate_task_message(message: Any, *, expected_task_id: str | None = None) -> tuple[str, str]:
    if not isinstance(message, dict):
        raise ProtocolError("Execution message must be an object")
    action = message.get("action")
    if action not in TASK_ACTIONS:
        raise ProtocolError(f"Unknown execution action: {action!r}")
    protocol_version = message.get("protocol_version")
    if protocol_version != EXECUTION_PROTOCOL_VERSION:
        raise ProtocolCompatibilityError(
            f"Execution protocol mismatch: expected {EXECUTION_PROTOCOL_VERSION}, got {protocol_version!r}"
        )
    task_id = message.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise ProtocolError("Execution message has an invalid task ID")
    if expected_task_id is not None and task_id != expected_task_id:
        raise ProtocolError(f"Execution message task ID {task_id!r} does not match {expected_task_id!r}")
    return action, task_id


def validate_worker_task_message(message: Any, *, expected_task_id: str) -> str:
    """Validate one worker-to-host task message and return its action.

    This validates the complete protocol-v1 payload shape. Lifecycle ordering
    remains the responsibility of the host, which owns the task state.
    """

    action, _task_id = validate_task_message(message, expected_task_id=expected_task_id)
    if action not in WORKER_TASK_ACTIONS:
        raise ProtocolError(f"Action {action!r} is not valid from worker to host")

    fields = set(message)
    if action in {ACTION_ACCEPTED, ACTION_INPUT_RELEASED, ACTION_CANCELED}:
        _require_fields(fields, _TASK_MESSAGE_FIELDS, action)
    elif action == ACTION_RESULT_OFFER:
        _require_fields(fields, _TASK_MESSAGE_FIELDS | {"result"}, action)
    elif action == ACTION_RELEASED:
        _require_fields(fields, _TASK_MESSAGE_FIELDS | {"names"}, action)
        names = message["names"]
        if (
            not isinstance(names, list)
            or any(not isinstance(name, str) or not name for name in names)
            or len(names) != len(set(names))
        ):
            raise ProtocolError("Worker released message has invalid shared-memory names")
    elif action == ACTION_LOG:
        _require_fields(fields, _TASK_MESSAGE_FIELDS | {"level", "message"}, action)
        if type(message["level"]) is not int:
            raise ProtocolError("Worker log level must be an integer")
        if not isinstance(message["message"], str):
            raise ProtocolError("Worker log message must be a string")
    elif action == ACTION_UPDATE:
        optional = {"message", "current", "maximum", "outputs"}
        unexpected = fields - (_TASK_MESSAGE_FIELDS | optional)
        if unexpected:
            raise ProtocolError(f"Worker update contains unexpected fields: {sorted(unexpected)!r}")
        if "message" in message and not isinstance(message["message"], str):
            raise ProtocolError("Worker progress message must be a string")
        for field in ("current", "maximum"):
            if field in message and (type(message[field]) is not int or message[field] < 0):
                raise ProtocolError(f"Worker progress {field} must be a nonnegative integer")
        if "outputs" in message:
            outputs = message["outputs"]
            if not isinstance(outputs, dict) or any(not isinstance(key, str) or not key for key in outputs):
                raise ProtocolError("Worker intermediate outputs must be an object with nonempty string keys")
            _validate_intermediate_value(outputs, path="outputs")
    elif action == ACTION_FAILURE:
        _require_fields(
            fields,
            _TASK_MESSAGE_FIELDS | {"failure", "exception", "traceback"},
            action,
        )
        if not isinstance(message["exception"], str) or not isinstance(message["traceback"], str):
            raise ProtocolError("Worker failure exception and traceback must be strings")
        _validate_failure_payload(message["failure"], expected_task_id=expected_task_id)
    return action


def worker_hello(
    *,
    codecs: Iterable[tuple[str, int]],
    python_version: str,
    pid: int,
    environment_path: str,
    generation_id: str,
    recipe_hash: str,
) -> dict[str, Any]:
    return {
        "action": ACTION_HELLO,
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "codecs": _codec_payload(codecs),
        "worker_runtime_version": WORKER_RUNTIME_VERSION,
        "python_version": python_version,
        "pid": pid,
        "environment_path": environment_path,
        "generation_id": generation_id,
        "recipe_hash": recipe_hash,
    }


def validate_worker_capabilities(
    payload: Any,
    *,
    required_codecs: Iterable[tuple[str, int]],
    expected_identity: dict[str, object] | None = None,
) -> WorkerCapabilities:
    if not isinstance(payload, dict):
        raise ProtocolCompatibilityError("Worker handshake was not an object")
    if payload.get("action") != ACTION_HELLO:
        raise ProtocolCompatibilityError("Worker handshake has an invalid action")
    base_fields = {
        "action",
        "protocol_version",
        "codecs",
        "worker_runtime_version",
        "python_version",
        "pid",
        "environment_path",
        "generation_id",
        "recipe_hash",
    }
    persistent_fields = {"pool_id", "worker_index"}
    startup_fields = {"event", "schema_version", "port", "management_port", "token"}
    extra_fields = set(payload) - base_fields
    if extra_fields & persistent_fields and not persistent_fields.issubset(extra_fields):
        raise ProtocolCompatibilityError("Worker handshake has an incomplete persistent identity")
    if extra_fields & startup_fields and not startup_fields.issubset(extra_fields):
        raise ProtocolCompatibilityError("Worker startup handshake has incomplete callback metadata")
    if extra_fields not in (set(), persistent_fields, startup_fields, persistent_fields | startup_fields):
        raise ProtocolCompatibilityError(f"Worker handshake contains unexpected fields: {sorted(extra_fields)!r}")
    protocol_version = payload.get("protocol_version")
    if protocol_version != EXECUTION_PROTOCOL_VERSION:
        raise ProtocolCompatibilityError(
            f"Execution protocol mismatch: host={EXECUTION_PROTOCOL_VERSION}, worker={protocol_version!r}"
        )
    runtime_version = payload.get("worker_runtime_version")
    if runtime_version != WORKER_RUNTIME_VERSION:
        raise ProtocolCompatibilityError(
            f"Worker runtime mismatch: host={WORKER_RUNTIME_VERSION}, worker={runtime_version!r}"
        )
    raw_codecs = payload.get("codecs")
    if not isinstance(raw_codecs, list):
        raise ProtocolCompatibilityError("Worker handshake did not report codec capabilities")
    codecs: list[CodecCapability] = []
    for item in raw_codecs:
        if (
            not isinstance(item, dict)
            or set(item) != {"id", "version"}
            or not isinstance(item.get("id"), str)
            or not item["id"]
            or type(item.get("version")) is not int
            or item["version"] < 1
        ):
            raise ProtocolCompatibilityError("Worker handshake contains an invalid codec capability")
        codecs.append(CodecCapability(item["id"], item["version"]))
    available = {(codec.id, codec.version) for codec in codecs}
    if len(available) != len(codecs):
        raise ProtocolCompatibilityError("Worker handshake contains duplicate codec capabilities")
    missing = set(required_codecs) - available
    if missing:
        formatted = ", ".join(f"{codec}@{version}" for codec, version in sorted(missing))
        raise ProtocolCompatibilityError(f"Worker is missing required codecs: {formatted}")

    pid = payload.get("pid")
    environment_path = payload.get("environment_path")
    generation_id = payload.get("generation_id")
    recipe_hash = payload.get("recipe_hash")
    python_version = payload.get("python_version")
    if type(pid) is not int or pid <= 0:
        raise ProtocolCompatibilityError("Worker handshake has an invalid PID")
    for field, value in {
        "python_version": python_version,
        "environment_path": environment_path,
        "generation_id": generation_id,
        "recipe_hash": recipe_hash,
    }.items():
        if not isinstance(value, str) or not value:
            raise ProtocolCompatibilityError(f"Worker handshake has an invalid {field}")
    if persistent_fields.issubset(payload):
        if not isinstance(payload["pool_id"], str) or not payload["pool_id"]:
            raise ProtocolCompatibilityError("Worker handshake has an invalid pool_id")
        if type(payload["worker_index"]) is not int or payload["worker_index"] < 0:
            raise ProtocolCompatibilityError("Worker handshake has an invalid worker_index")

    if expected_identity is not None:
        mismatches = {
            key: (expected, payload.get(key))
            for key, expected in expected_identity.items()
            if payload.get(key) != expected
        }
        if mismatches:
            details = ", ".join(
                f"{key}: expected {expected!r}, got {actual!r}"
                for key, (expected, actual) in sorted(mismatches.items())
            )
            raise ProtocolCompatibilityError(f"Worker identity mismatch ({details})")

    return WorkerCapabilities(
        protocol_version=protocol_version,
        codecs=tuple(codecs),
        runtime_version=runtime_version,
        python_version=cast(str, python_version),
        pid=pid,
        environment_path=cast(str, environment_path),
        generation_id=cast(str, generation_id),
        recipe_hash=cast(str, recipe_hash),
    )


def _is_dotted_identifier(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and all(part.isidentifier() for part in value.split("."))


def _require_fields(actual: set[str], expected: set[str] | frozenset[str], action: str) -> None:
    if actual != set(expected):
        missing = sorted(set(expected) - actual)
        unexpected = sorted(actual - set(expected))
        details = []
        if missing:
            details.append(f"missing {missing!r}")
        if unexpected:
            details.append(f"unexpected {unexpected!r}")
        raise ProtocolError(f"Worker {action} message has invalid fields ({', '.join(details)})")


def _validate_intermediate_value(value: Any, *, path: str, active: set[int] | None = None) -> None:
    if value is None or type(value) in {bool, int, float, str, bytes}:
        return
    if not isinstance(value, (list, tuple, dict)):
        raise ProtocolError(f"{path} contains unsupported value type {type(value).__qualname__}")
    seen = active if active is not None else set()
    identity = id(value)
    if identity in seen:
        raise ProtocolError(f"{path} contains a recursive value")
    seen.add(identity)
    try:
        if isinstance(value, dict):
            for key, item in value.items():
                if type(key) not in {bool, int, float, str, bytes}:
                    raise ProtocolError(f"{path} contains an unsupported dictionary key")
                _validate_intermediate_value(item, path=f"{path}[{key!r}]", active=seen)
        else:
            for index, item in enumerate(value):
                _validate_intermediate_value(item, path=f"{path}[{index}]", active=seen)
    finally:
        seen.remove(identity)


def _validate_failure_payload(payload: Any, *, expected_task_id: str) -> None:
    expected = {
        "category",
        "message",
        "task_id",
        "call_target",
        "traceback",
        "traceback_frames",
        "remote_exception",
        "worker",
        "exit_code",
        "signal",
        "timeout",
        "elapsed",
        "serialization_context",
    }
    if not isinstance(payload, dict):
        raise ProtocolError("Worker failure payload must be an object")
    _require_fields(set(payload), expected, "failure payload")
    if not isinstance(payload["category"], str) or not payload["category"]:
        raise ProtocolError("Worker failure payload has an invalid category")
    if not isinstance(payload["message"], str):
        raise ProtocolError("Worker failure payload has an invalid message")
    if payload["task_id"] not in (None, expected_task_id):
        raise ProtocolError("Worker failure payload has an inconsistent task ID")
    for field in ("call_target", "traceback", "serialization_context"):
        if payload[field] is not None and not isinstance(payload[field], str):
            raise ProtocolError(f"Worker failure payload has an invalid {field}")
    frames = payload["traceback_frames"]
    if not isinstance(frames, list) or any(not isinstance(frame, str) for frame in frames):
        raise ProtocolError("Worker failure payload has invalid traceback frames")
    _validate_remote_exception(payload["remote_exception"])
    _validate_worker_info(payload["worker"])
    for field in ("exit_code", "signal"):
        if payload[field] is not None and type(payload[field]) is not int:
            raise ProtocolError(f"Worker failure payload has an invalid {field}")
    for field in ("timeout", "elapsed"):
        if payload[field] is not None and (type(payload[field]) not in {int, float} or payload[field] < 0):
            raise ProtocolError(f"Worker failure payload has an invalid {field}")


def _validate_remote_exception(
    payload: Any,
    *,
    active: set[int] | None = None,
    depth: int = 0,
) -> None:
    if payload is None:
        return
    if depth > _MAX_REMOTE_EXCEPTION_DEPTH:
        raise ProtocolError(f"Worker remote exception exceeds maximum nesting depth {_MAX_REMOTE_EXCEPTION_DEPTH}")
    expected = {
        "module",
        "type_name",
        "qualified_name",
        "message",
        "traceback",
        "cause",
        "context",
        "suppress_context",
    }
    if not isinstance(payload, dict):
        raise ProtocolError("Worker failure has an invalid remote exception")
    seen = active if active is not None else set()
    identity = id(payload)
    if identity in seen:
        raise ProtocolError("Worker remote exception contains a recursive reference")
    seen.add(identity)
    try:
        _require_fields(set(payload), expected, "remote exception")
        for field in ("module", "type_name", "qualified_name", "message", "traceback"):
            if payload[field] is not None and not isinstance(payload[field], str):
                raise ProtocolError(f"Worker remote exception has an invalid {field}")
        if type(payload["suppress_context"]) is not bool:
            raise ProtocolError("Worker remote exception has an invalid suppress_context")
        _validate_remote_exception(payload["cause"], active=seen, depth=depth + 1)
        _validate_remote_exception(payload["context"], active=seen, depth=depth + 1)
    finally:
        seen.remove(identity)


def _validate_worker_info(payload: Any) -> None:
    if payload is None:
        return
    expected = {"environment", "index", "pid", "port", "persistent"}
    if not isinstance(payload, dict):
        raise ProtocolError("Worker failure has invalid worker information")
    _require_fields(set(payload), expected, "worker information")
    if payload["environment"] is not None and not isinstance(payload["environment"], str):
        raise ProtocolError("Worker failure has an invalid environment")
    for field in ("index", "pid", "port"):
        if payload[field] is not None and type(payload[field]) is not int:
            raise ProtocolError(f"Worker failure has an invalid worker {field}")
    if payload["persistent"] is not None and type(payload["persistent"]) is not bool:
        raise ProtocolError("Worker failure has an invalid worker persistent flag")
