"""Portable installed-content receipts for owner-managed environment generations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any


class RuntimeContentUnavailableError(RuntimeError):
    """The selected generation has no valid content admission."""


class EditableRuntimeSourceChangedError(RuntimeContentUnavailableError):
    """An editable source changed since this generation was admitted."""

    def __init__(self, distribution_name: str, source_root: str, expected_digest: str, actual_digest: str) -> None:
        self.distribution_name = distribution_name
        self.source_root = source_root
        self.expected_digest = expected_digest
        self.actual_digest = actual_digest
        super().__init__(
            f"Editable distribution {distribution_name!r} source changed at {source_root!r}; "
            "close its workers and explicitly recreate the environment before reuse."
        )


def _digest_string(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("Runtime content digests must be lowercase SHA-256 strings")
    return value


def _finite_json(value: Any) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is list:
        for item in value:
            _finite_json(item)
        return
    if type(value) is dict and all(type(key) is str for key in value):
        for item in value.values():
            _finite_json(item)
        return
    raise ValueError("Runtime facts admit only finite typed JSON metadata")


def _validate_facts(facts: dict[str, Any]) -> None:
    _finite_json(facts)
    if set(facts) != {"schema_version", "python", "distributions", "resolved_artifacts", "editable_sources"}:
        raise ValueError("Invalid runtime scientific facts fields")
    if type(facts["schema_version"]) is not int or facts["schema_version"] != 1:
        raise ValueError("Unsupported runtime scientific facts version")
    python = facts["python"]
    fields = {"implementation", "version", "cache_tag", "soabi", "platform", "machine", "executable_digest"}
    if not isinstance(python, dict) or set(python) != fields:
        raise ValueError("Invalid runtime interpreter facts")
    if any(not isinstance(python[key], str) for key in fields - {"version"}):
        raise ValueError("Runtime interpreter facts must be strings")
    if (
        not isinstance(python["version"], list)
        or len(python["version"]) != 3
        or any(type(part) is not int or part < 0 for part in python["version"])
    ):
        raise ValueError("Invalid runtime interpreter version")
    _digest_string(python["executable_digest"])
    for category in ("distributions", "resolved_artifacts", "editable_sources"):
        records = facts[category]
        if not isinstance(records, list):
            raise ValueError(f"Invalid runtime {category}")
        names = []
        for item in records:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not item["name"]:
                raise ValueError(f"Invalid runtime {category} member")
            names.append(item["name"])
            if category == "distributions":
                if set(item) != {"name", "version", "content_digest"} or not isinstance(item["version"], str):
                    raise ValueError("Invalid installed distribution facts")
                _digest_string(item["content_digest"])
            elif category == "editable_sources":
                if set(item) != {"name", "content_digest"}:
                    raise ValueError("Invalid editable source facts")
                _digest_string(item["content_digest"])
            else:
                if set(item) != {"name", "version", "build", "subdir", "sha256"} or any(
                    not isinstance(item[field], str) for field in ("version", "build", "subdir")
                ):
                    raise ValueError("Invalid resolved artifact facts")
                if item["sha256"] is not None:
                    _digest_string(item["sha256"])
        if names != sorted(set(names)):
            raise ValueError(f"Runtime {category} must be ordered and unique")


def _validate_editable_roots(receipt: "RuntimeContentReceipt", operational: Any) -> list[dict[str, Any]]:
    """Validate all operational authority before any editable content is read."""
    expected = {item["name"]: item["content_digest"] for item in receipt.to_scientific_facts()["editable_sources"]}
    if not isinstance(operational, list) or len(operational) != len(expected):
        raise ValueError("Invalid editable source authority")
    observed = set()
    for item in operational:
        if not isinstance(item, dict) or set(item) != {"name", "source_root", "import_roots", "content_digest"}:
            raise ValueError("Invalid editable source descriptor")
        name, source, roots = item["name"], item["source_root"], item["import_roots"]
        if (
            not isinstance(name, str)
            or name not in expected
            or name in observed
            or item["content_digest"] != expected[name]
        ):
            raise ValueError("Invalid editable source membership")
        if not isinstance(source, str) or not Path(source).is_absolute() or ".." in Path(source).parts:
            raise ValueError("Invalid editable source root")
        if not isinstance(roots, list) or not roots or any(not isinstance(root, str) for root in roots):
            raise ValueError("Invalid editable import roots")
        if roots != sorted(set(roots)):
            raise ValueError("Editable import roots must be ordered and unique")
        for root in roots:
            if not Path(root).is_absolute() or ".." in Path(root).parts:
                raise ValueError("Invalid editable import root")
            Path(root).relative_to(Path(source))
        observed.add(name)
    return operational


@dataclass(frozen=True, init=False)
class RuntimeContentReceipt:
    """Immutable scientific content with separate operational generation fences.

    The digest excludes generation IDs, absolute prefixes and requested recipe or
    lock hashes. It describes admitted installed content, not arbitrary manual
    mutations or arbitrary Python execution state.
    """

    content_digest: str
    generation_id: str
    recipe_hash: str
    lockfile_hash: str
    _facts_json: str

    def __init__(
        self, *, generation_id: str, recipe_hash: str, lockfile_hash: str, scientific_facts: Mapping[str, Any]
    ) -> None:
        if not isinstance(generation_id, str) or not generation_id:
            raise ValueError("Runtime receipt needs a generation ID")
        _digest_string(recipe_hash)
        _digest_string(lockfile_hash)
        facts = dict(scientific_facts)
        _validate_facts(facts)
        serialized = json.dumps(facts, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        for name, value in (
            ("generation_id", generation_id),
            ("recipe_hash", recipe_hash),
            ("lockfile_hash", lockfile_hash),
            ("_facts_json", serialized),
            ("content_digest", hashlib.sha256(b"wetlands-runtime-content-v1\0" + serialized.encode()).hexdigest()),
        ):
            object.__setattr__(self, name, value)

    def to_scientific_facts(self) -> dict[str, Any]:
        """Return a detached copy of the admitted semantic content facts."""
        return dict(json.loads(self._facts_json))

    def _to_payload(self) -> dict[str, Any]:
        return {
            "generation_id": self.generation_id,
            "recipe_hash": self.recipe_hash,
            "lockfile_hash": self.lockfile_hash,
            "content_digest": self.content_digest,
            "scientific_facts": self.to_scientific_facts(),
        }

    @classmethod
    def _from_payload(cls, value: Any) -> RuntimeContentReceipt:
        if not isinstance(value, dict) or set(value) != {
            "generation_id",
            "recipe_hash",
            "lockfile_hash",
            "content_digest",
            "scientific_facts",
        }:
            raise ValueError("Invalid stored runtime receipt")
        result = cls(
            generation_id=value["generation_id"],
            recipe_hash=value["recipe_hash"],
            lockfile_hash=value["lockfile_hash"],
            scientific_facts=value["scientific_facts"],
        )
        if value["content_digest"] != result.content_digest:
            raise ValueError("Stored runtime content digest mismatch")
        return result
