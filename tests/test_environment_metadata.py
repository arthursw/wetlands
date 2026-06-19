from __future__ import annotations

import json
from pathlib import Path

from wetlands._internal.dependency_manager import Dependencies
from wetlands._internal.environment_metadata import (
    ENVIRONMENT_METADATA_SCHEMA_VERSION,
    build_environment_recipe,
    environment_metadata_path,
    hash_environment_recipe,
    read_environment_metadata,
    write_environment_metadata,
)


def make_recipe(
    dependencies: Dependencies,
    *,
    manager: str = "micromamba",
    python_version: str = "3.12.1",
    additional_install_commands: list[str] | None = None,
) -> dict:
    return build_environment_recipe(
        manager=manager,
        platform="linux",
        conda_platform="linux-64",
        python_version=python_version,
        dependencies=dependencies,
        additional_install_commands=additional_install_commands or [],
    )


def test_recipe_hash_is_stable_for_reordered_unordered_dependencies(tmp_path):
    local_a = tmp_path / "local-a"
    local_b = tmp_path / "local-b"
    local_a.mkdir()
    local_b.mkdir()

    left = make_recipe(
        {
            "conda": ["pandas", {"name": "numpy>=2", "platforms": ["linux-64"], "dependencies": False}],
            "pip": ["requests", "zarr"],
            "local": [
                {"name": "local-a", "path": local_a},
                {"name": "local-b", "path": local_b, "editable": False},
            ],
        }
    )
    right = make_recipe(
        {
            "pip": ["zarr", "requests"],
            "conda": [{"dependencies": False, "platforms": ["linux-64"], "name": "numpy>=2"}, "pandas"],
            "local": [
                {"path": local_a, "name": "local-a"},
                {"path": local_b, "editable": False, "name": "local-b"},
            ],
        }
    )

    assert left == right
    assert hash_environment_recipe(left) == hash_environment_recipe(right)


def test_recipe_hash_preserves_channel_order():
    left = make_recipe({"channels": ["bioconda", "conda-forge"], "conda": ["numpy"]})
    right = make_recipe({"channels": ["conda-forge", "bioconda"], "conda": ["numpy"]})

    assert hash_environment_recipe(left) != hash_environment_recipe(right)


def test_recipe_hash_preserves_local_dependency_order(tmp_path):
    local_a = tmp_path / "local-a"
    local_b = tmp_path / "local-b"
    local_a.mkdir()
    local_b.mkdir()
    left = make_recipe(
        {
            "local": [
                {"name": "local-a", "path": local_a},
                {"name": "local-b", "path": local_b},
            ],
        }
    )
    right = make_recipe(
        {
            "local": [
                {"name": "local-b", "path": local_b},
                {"name": "local-a", "path": local_a},
            ],
        }
    )

    assert hash_environment_recipe(left) != hash_environment_recipe(right)


def test_recipe_hash_changes_for_effective_python_version():
    assert hash_environment_recipe(make_recipe({})) != hash_environment_recipe(make_recipe({}, python_version="3.11.9"))


def test_recipe_hash_preserves_additional_install_command_order():
    left = hash_environment_recipe(make_recipe({}, additional_install_commands=["echo one", "echo two"]))
    right = hash_environment_recipe(make_recipe({}, additional_install_commands=["echo two", "echo one"]))

    assert left != right


def test_metadata_path_uses_workspace_parent_for_pixi():
    manifest_path = Path("/tmp/pixi-root/workspaces/demo/pixi.toml")

    assert environment_metadata_path(manifest_path, use_pixi=True) == (
        Path("/tmp/pixi-root/workspaces/demo") / ".wetlands" / "environment.json"
    )


def test_metadata_path_uses_environment_directory_for_micromamba():
    env_path = Path("/tmp/micromamba-root/envs/demo")

    assert environment_metadata_path(env_path, use_pixi=False) == env_path / ".wetlands" / "environment.json"


def test_metadata_write_is_read_back_atomically(tmp_path):
    env_path = tmp_path / "env"
    metadata = {
        "schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION,
        "status": "managed",
        "name": "demo",
        "manager": "micromamba",
        "recipe_hash": "sha256:test",
        "recipe": make_recipe({}),
    }

    write_environment_metadata(env_path, use_pixi=False, metadata=metadata)
    loaded, reason = read_environment_metadata(env_path, use_pixi=False)

    assert reason is None
    assert loaded == metadata


def test_metadata_read_reports_corrupt_json(tmp_path):
    metadata_path = environment_metadata_path(tmp_path / "env", use_pixi=False)
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text("{not json", encoding="utf-8")

    loaded, reason = read_environment_metadata(tmp_path / "env", use_pixi=False)

    assert loaded is None
    assert reason == "unreadable"


def test_metadata_read_reports_unsupported_schema(tmp_path):
    metadata_path = environment_metadata_path(tmp_path / "env", use_pixi=False)
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")

    loaded, reason = read_environment_metadata(tmp_path / "env", use_pixi=False)

    assert loaded is None
    assert reason == "unsupported_schema"


def test_metadata_read_reports_invalid_schema_v1_shape(tmp_path):
    metadata_path = environment_metadata_path(tmp_path / "env", use_pixi=False)
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(json.dumps({"schema_version": ENVIRONMENT_METADATA_SCHEMA_VERSION}), encoding="utf-8")

    loaded, reason = read_environment_metadata(tmp_path / "env", use_pixi=False)

    assert loaded is None
    assert reason == "invalid_metadata"
