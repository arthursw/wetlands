"""Self-contained contracts for the Wetlands execution protocol.

This module deliberately has no imports from the ``wetlands`` package so the
worker bootstrap can load the same source file inside an isolated environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

EXECUTION_PROTOCOL_VERSION = 1
WORKER_RUNTIME_VERSION = "2.0.0"

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
        python_version=python_version,
        pid=pid,
        environment_path=environment_path,
        generation_id=generation_id,
        recipe_hash=recipe_hash,
    )


def _is_dotted_identifier(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and all(part.isidentifier() for part in value.split("."))
