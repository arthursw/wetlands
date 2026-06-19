from __future__ import annotations

import hashlib
import json
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from wetlands._internal.dependency_manager import Dependencies

ENVIRONMENT_METADATA_SCHEMA_VERSION = 1
ENVIRONMENT_METADATA_DIRECTORY = ".wetlands"
ENVIRONMENT_METADATA_FILENAME = "environment.json"
MANAGED_STATUS = "managed"
UNMANAGED_STATUS = "unmanaged"


def environment_metadata_path(environment_path: Path, *, use_pixi: bool) -> Path:
    """Return the Wetlands metadata sidecar path for an environment path."""
    root = environment_path.parent if use_pixi else environment_path
    return root / ENVIRONMENT_METADATA_DIRECTORY / ENVIRONMENT_METADATA_FILENAME


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sorted_canonical(values: list[Any]) -> list[Any]:
    return sorted(values, key=_canonical_json)


def _deduplicate_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _normalize_dependency_entry(entry: str | dict[str, Any]) -> str | dict[str, Any]:
    if isinstance(entry, str):
        return entry.strip()
    normalized: dict[str, Any] = {}
    for key in ("name", "platforms", "optional", "dependencies"):
        if key not in entry:
            continue
        value = entry[key]
        if key == "name" and isinstance(value, str):
            normalized[key] = value.strip()
        elif key == "platforms" and isinstance(value, list):
            normalized[key] = sorted(str(platform) for platform in value)
        else:
            normalized[key] = value
    return normalized


def _normalize_local_dependency(entry: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "name": str(entry["name"]).strip(),
        "path": str(Path(entry["path"]).resolve()),
        "editable": bool(entry.get("editable", True)),
    }


def normalize_recipe_dependencies(dependencies: Dependencies) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    python = dependencies.get("python")
    if python is not None:
        normalized["python"] = str(python).strip()

    channels = _deduplicate_preserve_order(
        [str(channel).strip() for channel in dependencies.get("channels", []) if str(channel).strip()]
    )
    if channels:
        normalized["channels"] = channels

    for package_manager in ("conda", "pip"):
        entries = [_normalize_dependency_entry(entry) for entry in dependencies.get(package_manager, [])]  # type: ignore[arg-type]
        if entries:
            normalized[package_manager] = _sorted_canonical(entries)

    local_entries = [_normalize_local_dependency(entry) for entry in dependencies.get("local", [])]
    if local_entries:
        normalized["local"] = local_entries

    return normalized


def build_environment_recipe(
    *,
    manager: str,
    platform: str,
    conda_platform: str,
    python_version: str,
    dependencies: Dependencies,
    additional_install_commands: list[str],
) -> dict[str, Any]:
    """Build the canonical recipe stored in environment metadata."""
    return {
        "schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION,
        "manager": manager,
        "platform": platform,
        "conda_platform": conda_platform,
        "python_version": python_version,
        "dependencies": normalize_recipe_dependencies(dependencies),
        "additional_install_commands": list(additional_install_commands),
    }


def hash_environment_recipe(recipe: dict[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_json(recipe).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def read_environment_metadata(environment_path: Path, *, use_pixi: bool) -> tuple[dict[str, Any] | None, str | None]:
    path = environment_metadata_path(environment_path, use_pixi=use_pixi)
    if not path.exists():
        return None, "missing"
    try:
        with open(path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception:
        return None, "unreadable"
    if not isinstance(metadata, dict):
        return None, "unreadable"
    if metadata.get("schema_version") != ENVIRONMENT_METADATA_SCHEMA_VERSION:
        return None, "unsupported_schema"
    status = metadata.get("status")
    if status not in {MANAGED_STATUS, UNMANAGED_STATUS}:
        return None, "invalid_metadata"
    if status == MANAGED_STATUS:
        if not isinstance(metadata.get("recipe_hash"), str) or not isinstance(metadata.get("recipe"), dict):
            return None, "invalid_metadata"
    return metadata, None


def write_environment_metadata(environment_path: Path, *, use_pixi: bool, metadata: dict[str, Any]) -> None:
    path = environment_metadata_path(environment_path, use_pixi=use_pixi)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with open(temporary_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, sort_keys=True)
        f.write("\n")
    temporary_path.replace(path)


def build_managed_environment_metadata(
    *,
    name: str,
    manager: str,
    recipe: dict[str, Any],
    recipe_hash: str,
) -> dict[str, Any]:
    return {
        "schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION,
        "status": MANAGED_STATUS,
        "name": name,
        "manager": manager,
        "recipe_hash": recipe_hash,
        "recipe": recipe,
    }


def mark_environment_metadata_unmanaged(
    environment_path: Path,
    *,
    use_pixi: bool,
    reason: str,
) -> None:
    metadata, read_reason = read_environment_metadata(environment_path, use_pixi=use_pixi)
    if metadata is None:
        metadata = {
            "schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION,
            "status": UNMANAGED_STATUS,
            "unmanaged_reason": reason if read_reason == "missing" else f"{reason}; previous metadata {read_reason}",
        }
    else:
        metadata = {
            **metadata,
            "status": UNMANAGED_STATUS,
            "unmanaged_reason": reason,
        }
    write_environment_metadata(environment_path, use_pixi=use_pixi, metadata=metadata)
