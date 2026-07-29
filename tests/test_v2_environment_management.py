from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import wetlands._internal.environment_management as management_module
import wetlands.environment_manager as manager_module
from wetlands._internal.provisioning import (
    OWNER_MARKER,
    READY_SCHEMA_VERSION,
    _create_managed_target,
    _publish_ready,
    _write_target_file,
    environment_lifecycle_gate,
)
from wetlands.environment_info import ManagedEnvironmentInfo, ManagedEnvironmentState
from wetlands.environment_manager import EnvironmentManager, EnvironmentNotReadyError
from wetlands.lifecycle import (
    EnvironmentInUseError,
    EnvironmentNotFoundError,
    UnmanagedTargetError,
)
from wetlands.operation import (
    OperationCanceled,
    OperationEventKind,
    OperationState,
    RemovalError,
    RemovalOperation,
)
from wetlands.managed_environment import ManagedEnvironment
from wetlands.specs import EnvironmentSpec


def _ready_environment(
    manager: EnvironmentManager,
    name: str,
    *,
    generation_id: str = "generation-1",
    recipe_hash: str = "recipe-1",
) -> Path:
    target = manager.environments_root / name
    manager.root.mkdir(parents=True, exist_ok=True)
    identity = _create_managed_target(manager.environments_root, target)
    manifest = b'[project]\nname = "example"\n'
    lock = b"version: 6\n"
    _write_target_file(
        manager.environments_root,
        target,
        OWNER_MARKER,
        b"test-operation\n",
        expected_identity=identity,
        require_marker=False,
    )
    _write_target_file(
        manager.environments_root,
        target,
        "pixi.toml",
        manifest,
        expected_identity=identity,
        require_marker=True,
    )
    _write_target_file(
        manager.environments_root,
        target,
        "pixi.lock",
        lock,
        expected_identity=identity,
        require_marker=True,
    )
    metadata = {
        "schema_version": READY_SCHEMA_VERSION,
        "state": "ready",
        "name": name,
        "canonical_path": str(target.resolve()),
        "recipe_hash": recipe_hash,
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
        "lock_sha256": hashlib.sha256(lock).hexdigest(),
        "generation_id": generation_id,
        "operation_id": "test-operation",
        "pixi_version": "0.74.0",
        "pixi_executable": str(manager.root / "bin" / "pixi"),
    }
    _publish_ready(
        manager.environments_root,
        target,
        json.dumps(metadata).encode(),
        expected_identity=identity,
    )
    return target


def _incomplete_environment(manager: EnvironmentManager, name: str) -> Path:
    target = manager.environments_root / name
    target.mkdir(parents=True)
    (target / OWNER_MARKER).write_text("interrupted-operation\n", encoding="utf-8")
    (target / "partial").write_text("partial\n", encoding="utf-8")
    return target


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("condition was not reached")
        time.sleep(0.01)


def test_discovery_without_a_root_is_side_effect_free(tmp_path: Path) -> None:
    root = tmp_path / "wetlands"
    manager = EnvironmentManager(root)

    assert manager.managed_environments() == ()
    assert not root.exists()


def test_discovery_returns_immutable_ready_and_incomplete_snapshots(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    ready = _ready_environment(manager, "ready")
    incomplete = _incomplete_environment(manager, "Incomplete")
    unmanaged = manager.environments_root / "unmanaged"
    unmanaged.mkdir()

    environments = manager.managed_environments()

    assert environments == (
        ManagedEnvironmentInfo(
            name="Incomplete",
            path=incomplete.resolve(),
            state=ManagedEnvironmentState.INCOMPLETE,
        ),
        ManagedEnvironmentInfo(
            name="ready",
            path=ready.resolve(),
            state=ManagedEnvironmentState.READY,
            generation_id="generation-1",
            recipe_hash="recipe-1",
            pixi_version="0.74.0",
        ),
    )
    assert environments[0].ready is False
    assert environments[1].ready is True
    with pytest.raises(AttributeError):
        environments[1].name = "changed"  # type: ignore[misc]


def test_invalid_ready_metadata_is_discovered_as_incomplete(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _ready_environment(manager, "example")
    (target / "pixi.lock").write_text("tampered\n", encoding="utf-8")

    (info,) = manager.managed_environments()

    assert info.name == "example"
    assert info.state is ManagedEnvironmentState.INCOMPLETE
    assert info.generation_id is None


def test_discovery_rejects_ambiguous_managed_names(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    _incomplete_environment(manager, "Example")
    if (manager.environments_root / "example").exists():
        pytest.skip("Filesystem does not permit portable name aliases")
    _incomplete_environment(manager, "example")

    with pytest.raises(RuntimeError, match="ambiguous portable names"):
        manager.managed_environments()


def test_discovery_rejects_linked_environment_root(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("Creating symlinks requires platform-specific privileges on Windows")
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "wetlands"
    root.mkdir()
    (root / "environments").symlink_to(outside, target_is_directory=True)
    manager = EnvironmentManager(root)

    with pytest.raises(RuntimeError, match="linked or not a directory"):
        manager.managed_environments()


def test_remove_ready_environment_returns_snapshot_and_invalidates_cache(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _ready_environment(manager, "example")
    cached = manager.environment("example")
    journal = manager.state_root / "operations" / "old.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(json.dumps({"target": str(target), "state": "building"}), encoding="utf-8")
    events = []

    operation = manager.remove("example").listen(events.append)
    removed = operation.wait_for()

    assert isinstance(operation, RemovalOperation)
    assert removed.state is ManagedEnvironmentState.READY
    assert removed.generation_id == cached.generation_id
    assert not target.exists()
    assert not journal.exists()
    assert operation.state is OperationState.COMPLETED
    assert any(event.kind is OperationEventKind.CLEANUP for event in events)
    with pytest.raises(EnvironmentNotReadyError):
        manager.environment("example")


def test_remove_incomplete_environment(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")

    removed = manager.remove("example").wait_for()

    assert removed.state is ManagedEnvironmentState.INCOMPLETE
    assert not target.exists()


def test_remove_missing_and_portable_alias_are_explicit(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")

    with pytest.raises(EnvironmentNotFoundError) as missing:
        manager.remove("missing").wait_for()
    assert missing.value.environment == "missing"
    assert missing.value.alias is None

    _incomplete_environment(manager, "Example")
    with pytest.raises(EnvironmentNotFoundError) as aliased:
        manager.remove("example").wait_for()
    assert aliased.value.alias == "Example"
    assert (manager.environments_root / "Example").exists()


def test_remove_refuses_unmanaged_target_without_modifying_it(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = manager.environments_root / "example"
    target.mkdir(parents=True)
    precious = target / "precious.txt"
    precious.write_text("keep\n", encoding="utf-8")

    with pytest.raises(UnmanagedTargetError):
        manager.remove("example").wait_for()

    assert precious.read_text(encoding="utf-8") == "keep\n"


def test_remove_refuses_live_registered_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _ready_environment(manager, "example")
    monkeypatch.setattr(management_module.runtime_state, "reconcile_persistent_pool", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        management_module.runtime_state,
        "live_workers_for_env",
        lambda *args, **kwargs: [{"generation_id": "generation-1"}],
    )

    with pytest.raises(EnvironmentInUseError) as caught:
        manager.remove("example").wait_for()

    assert caught.value.generation_id == "generation-1"
    assert target.exists()


def test_remove_refuses_an_open_pool_even_during_a_registry_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _ready_environment(manager, "example")
    environment = manager.environment("example")
    environment._pools.append(SimpleNamespace(_closed=False))
    monkeypatch.setattr(management_module.runtime_state, "live_workers_for_env", lambda *args, **kwargs: [])

    with pytest.raises(EnvironmentInUseError):
        manager.remove("example").wait_for()

    assert target.exists()


def test_remove_can_be_canceled_while_waiting_for_lifecycle_gate(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")

    with environment_lifecycle_gate(manager, "example"):
        operation = manager.remove("example")
        events = []
        operation.listen(events.append)
        _wait_until(
            lambda: any(event.stage == "lock_wait" for event in events),
        )
        assert operation.cancel()

    with pytest.raises(OperationCanceled):
        operation.wait_for(timeout=2)
    assert operation.state is OperationState.CANCELED
    assert target.exists()


def test_concurrent_remove_calls_are_serialized(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")

    first = manager.remove("example")
    second = manager.remove("example")

    outcomes = []
    for operation in (first, second):
        try:
            outcomes.append(operation.wait_for(timeout=3))
        except EnvironmentNotFoundError as error:
            outcomes.append(error)
    assert sum(isinstance(outcome, ManagedEnvironmentInfo) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, EnvironmentNotFoundError) for outcome in outcomes) == 1
    assert not target.exists()


def test_failed_removal_restores_owner_marker_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    original_remove = management_module._remove_target

    def fail_after_marker_removal(*args, **kwargs) -> None:
        (target / OWNER_MARKER).unlink()
        raise OSError("simulated interruption")

    monkeypatch.setattr(management_module, "_remove_target", fail_after_marker_removal)
    with pytest.raises(RemovalError) as caught:
        manager.remove("example").wait_for()

    assert caught.value.failure.stage == "environment_removal"
    assert (target / OWNER_MARKER).is_file()

    monkeypatch.setattr(management_module, "_remove_target", original_remove)
    manager.remove("example").wait_for()
    assert not target.exists()


def test_removal_cannot_be_canceled_after_destructive_work_is_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    entered = threading.Event()
    release = threading.Event()
    original_remove = management_module._remove_target

    def slow_remove(*args, **kwargs) -> None:
        entered.set()
        assert release.wait(2)
        original_remove(*args, **kwargs)

    monkeypatch.setattr(management_module, "_remove_target", slow_remove)
    operation = manager.remove("example")
    assert entered.wait(2)
    assert operation.cancel() is False
    release.set()

    assert operation.wait_for(timeout=2).name == "example"
    assert operation.state is OperationState.COMPLETED
    assert not target.exists()


def test_cleanup_failure_is_structured_before_target_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _ready_environment(manager, "example")

    def fail_cleanup(*args, **kwargs) -> None:
        raise OSError("registry unavailable")

    monkeypatch.setattr(management_module.runtime_state, "remove_workers_for_env", fail_cleanup)

    with pytest.raises(RemovalError) as caught:
        manager.remove("example").wait_for()

    assert caught.value.failure.stage == "cleanup"
    assert caught.value.failure.cleanup_error == "registry unavailable"
    assert target.exists()


def test_removal_epoch_prevents_late_provision_cache_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    started = threading.Event()
    release = threading.Event()

    def delayed_provision(*args, **kwargs) -> ManagedEnvironment:
        started.set()
        assert release.wait(2)
        return ManagedEnvironment._from_ready(
            manager,
            "example",
            target,
            {
                "generation_id": "generation-1",
                "recipe_hash": "recipe-1",
                "pixi_version": "0.74.0",
                "pixi_executable": str(manager.root / "bin" / "pixi"),
                "lock_sha256": "lock-1",
            },
        )

    monkeypatch.setattr(manager_module, "provision_environment", delayed_provision)
    provision = manager.provision("example", EnvironmentSpec())
    assert started.wait(2)
    manager.remove("example").wait_for(timeout=2)
    release.set()

    provision.wait_for(timeout=2)
    assert "example" not in manager._environments
