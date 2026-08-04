from __future__ import annotations

import ctypes
import hashlib
import json
import os
import platform
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import wetlands._internal.environment_cleanup as cleanup_module
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


def test_failed_quarantine_leaves_original_environment_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    original_quarantine = manager._quarantine_environment

    def fail_before_rename(*args, **kwargs) -> None:
        raise OSError("simulated interruption")

    monkeypatch.setattr(manager, "_quarantine_environment", fail_before_rename)
    with pytest.raises(RemovalError) as caught:
        manager.remove("example").wait_for()

    assert caught.value.failure.stage == "environment_removal"
    assert (target / OWNER_MARKER).is_file()

    monkeypatch.setattr(manager, "_quarantine_environment", original_quarantine)
    manager.remove("example").wait_for()
    assert not target.exists()


def test_post_rename_validation_failure_still_commits_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    original_move = cleanup_module._move_target_into_quarantine

    def fail_after_rename(*args, **kwargs) -> None:
        original_move(*args, **kwargs)
        raise OSError("simulated post-rename validation failure")

    monkeypatch.setattr(cleanup_module, "_move_target_into_quarantine", fail_after_rename)

    removed = manager.remove("example").wait_for(timeout=2)

    assert removed.name == "example"
    assert not target.exists()
    assert manager._environment_epochs["example"] == 1


def test_directory_fsync_failure_after_rename_still_commits_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    seed = _incomplete_environment(manager, "seed")
    cleanup_module.quarantine_environment(manager, seed, operation_id="seed")
    target = _incomplete_environment(manager, "example")

    def fail_fsync(*args, **kwargs) -> None:
        raise OSError("simulated directory fsync failure")

    monkeypatch.setattr(cleanup_module, "_sync_detach_directories", fail_fsync)

    removed = manager.remove("example").wait_for(timeout=2)

    assert removed.name == "example"
    assert not target.exists()


def test_quarantine_rename_never_replaces_an_injected_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name == "nt":
        pytest.skip("The POSIX no-replace primitive is exercised by this test")
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    original_rename = cleanup_module._rename_no_replace

    def inject_destination(source_name, destination_name, *, source_directory_fd, destination_directory_fd) -> None:
        if destination_name.endswith(".tree"):
            os.mkdir(destination_name, dir_fd=destination_directory_fd)
        original_rename(
            source_name,
            destination_name,
            source_directory_fd=source_directory_fd,
            destination_directory_fd=destination_directory_fd,
        )

    monkeypatch.setattr(cleanup_module, "_rename_no_replace", inject_destination)

    with pytest.raises(RemovalError):
        manager.remove("example").wait_for(timeout=2)

    assert target.is_dir()
    quarantine = manager.root / cleanup_module.QUARANTINE_DIRECTORY
    assert len(tuple(quarantine.glob("*.tree"))) == 1


def test_linux_no_replace_rename_falls_back_to_direct_syscall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    class FakeSyscall:
        restype = None

        def __call__(self, *args):
            calls.append(args)
            return 0

    fake_libc = SimpleNamespace(syscall=FakeSyscall())
    monkeypatch.setattr(ctypes, "CDLL", lambda *args, **kwargs: fake_libc)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")

    cleanup_module._rename_no_replace(
        "source",
        "destination",
        source_directory_fd=10,
        destination_directory_fd=11,
    )

    assert calls[0][0].value == 316
    assert calls[0][-1].value == 1


def test_quarantine_root_publication_never_replaces_an_injected_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name == "nt":
        pytest.skip("The POSIX no-replace primitive is exercised by this test")
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    original_rename = cleanup_module._rename_no_replace

    def inject_root(source_name, destination_name, *, source_directory_fd, destination_directory_fd) -> None:
        if destination_name == cleanup_module.QUARANTINE_DIRECTORY:
            os.mkdir(destination_name, dir_fd=destination_directory_fd)
        original_rename(
            source_name,
            destination_name,
            source_directory_fd=source_directory_fd,
            destination_directory_fd=destination_directory_fd,
        )

    monkeypatch.setattr(cleanup_module, "_rename_no_replace", inject_root)

    with pytest.raises(RemovalError):
        manager.remove("example").wait_for(timeout=2)

    assert target.is_dir()
    quarantine = manager.root / cleanup_module.QUARANTINE_DIRECTORY
    assert quarantine.is_dir()
    assert not (quarantine / cleanup_module._QUARANTINE_MARKER).exists()


def test_removal_can_be_canceled_during_quarantine_preparation(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    events = []

    with cleanup_module._metadata_lock(manager):
        operation = manager.remove("example").listen(events.append)
        _wait_until(lambda: any(event.stage == "environment_removal" for event in events))
        assert operation.cancel() is True
        with pytest.raises(OperationCanceled):
            operation.wait_for(timeout=1)
    assert target.exists()
    quarantine = manager.root / cleanup_module.QUARANTINE_DIRECTORY
    assert not tuple(quarantine.glob("*.json"))


def test_removal_cannot_be_canceled_after_destructive_work_is_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    entered = threading.Event()
    release = threading.Event()
    original_move = cleanup_module._move_target_into_quarantine

    def slow_move(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        return original_move(*args, **kwargs)

    monkeypatch.setattr(cleanup_module, "_move_target_into_quarantine", slow_move)
    operation = manager.remove("example")
    assert entered.wait(2)
    assert operation.cancel() is False
    release.set()

    assert operation.wait_for(timeout=2).name == "example"
    assert operation.state is OperationState.COMPLETED
    assert not target.exists()


def test_remove_completes_while_physical_reclamation_is_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    entered = threading.Event()
    release = threading.Event()
    original_remove = cleanup_module._remove_target

    def blocked_remove(*args, **kwargs) -> None:
        entered.set()
        assert release.wait(5)
        original_remove(*args, **kwargs)

    monkeypatch.setattr(cleanup_module, "_remove_target", blocked_remove)
    operation = manager.remove("example")
    assert entered.wait(2)
    try:
        removed = operation.wait_for(timeout=2)
        assert removed.name == "example"
        assert not target.exists()
    finally:
        release.set()
    assert manager._environment_reclaimer.wait_idle(2)


def test_removed_name_can_be_reused_while_old_tree_is_reclaimed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    old_target = _incomplete_environment(manager, "example")
    entered = threading.Event()
    release = threading.Event()
    original_remove = cleanup_module._remove_target

    def blocked_remove(*args, **kwargs) -> None:
        entered.set()
        assert release.wait(5)
        original_remove(*args, **kwargs)

    monkeypatch.setattr(cleanup_module, "_remove_target", blocked_remove)
    manager.remove("example").wait_for(timeout=2)
    assert entered.wait(2)
    replacement = _incomplete_environment(manager, "example")
    replacement_file = replacement / "replacement"
    replacement_file.write_text("new\n", encoding="utf-8")

    release.set()
    assert manager._environment_reclaimer.wait_idle(2)
    assert replacement_file.read_text(encoding="utf-8") == "new\n"
    assert old_target == replacement


def test_reclamation_failure_is_durable_and_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    _incomplete_environment(manager, "example")
    original_remove = cleanup_module._remove_target
    attempts = 0

    def fail_once(*args, **kwargs) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporarily unavailable")
        original_remove(*args, **kwargs)

    monkeypatch.setattr(cleanup_module, "_remove_target", fail_once)
    removed = manager.remove("example").wait_for(timeout=2)
    assert removed.name == "example"
    assert manager._environment_reclaimer.wait_idle(2)
    quarantine = manager.root / cleanup_module.QUARANTINE_DIRECTORY
    assert tuple(quarantine.glob("*.tree"))

    manager._environment_reclaimer.wake()
    assert manager._environment_reclaimer.wait_idle(2)
    assert attempts == 2
    assert not tuple(quarantine.glob("*.tree"))


def test_manager_close_does_not_wait_for_physical_reclamation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    _incomplete_environment(manager, "example")
    entered = threading.Event()
    release = threading.Event()
    original_remove = cleanup_module._remove_target

    def blocked_remove(*args, **kwargs) -> None:
        entered.set()
        assert release.wait(5)
        original_remove(*args, **kwargs)

    monkeypatch.setattr(cleanup_module, "_remove_target", blocked_remove)
    manager.remove("example").wait_for(timeout=2)
    assert entered.wait(2)
    close_thread = threading.Thread(target=manager.close)
    close_thread.start()
    close_thread.join(1)
    try:
        assert not close_thread.is_alive()
    finally:
        release.set()


def test_orphaned_quarantine_is_reclaimed_by_next_mutating_manager(
    tmp_path: Path,
) -> None:
    root = tmp_path / "wetlands"
    first_manager = EnvironmentManager(root)
    orphan = _incomplete_environment(first_manager, "orphan")
    cleanup_module.quarantine_environment(
        first_manager,
        orphan,
        operation_id="interrupted-removal",
    )
    quarantine = first_manager.root / cleanup_module.QUARANTINE_DIRECTORY
    assert tuple(quarantine.glob("*.tree"))

    second_manager = EnvironmentManager(root)
    assert tuple(quarantine.glob("*.tree"))
    _incomplete_environment(second_manager, "trigger")
    second_manager.remove("trigger").wait_for(timeout=2)

    assert second_manager._environment_reclaimer.wait_idle(2)
    assert not tuple(quarantine.glob("*.tree"))


def test_prepared_tombstone_is_reclaimed_after_original_name_is_reused(tmp_path: Path) -> None:
    root = tmp_path / "wetlands"
    first_manager = EnvironmentManager(root)
    original = _incomplete_environment(first_manager, "example")
    record = cleanup_module.quarantine_environment(
        first_manager,
        original,
        operation_id="interrupted-removal",
    )
    quarantine = first_manager.root / cleanup_module.QUARANTINE_DIRECTORY
    cleanup_module._write_record(quarantine, cleanup_module._updated_record(record, "prepared"))
    replacement = _incomplete_environment(first_manager, "example")
    replacement_marker = replacement / "replacement"
    replacement_marker.write_text("new\n", encoding="utf-8")

    second_manager = EnvironmentManager(root)
    second_manager._environment_reclaimer.wake()

    assert second_manager._environment_reclaimer.wait_idle(2)
    assert replacement_marker.read_text(encoding="utf-8") == "new\n"
    assert not tuple(quarantine.glob("*.tree"))


def test_orphaned_quarantine_is_reclaimed_without_environment_root(tmp_path: Path) -> None:
    root = tmp_path / "wetlands"
    first_manager = EnvironmentManager(root)
    target = _incomplete_environment(first_manager, "orphan")
    cleanup_module.quarantine_environment(
        first_manager,
        target,
        operation_id="interrupted-removal",
    )
    quarantine = first_manager.root / cleanup_module.QUARANTINE_DIRECTORY
    first_manager.environments_root.rmdir()

    second_manager = EnvironmentManager(root)
    second_manager._environment_reclaimer.wake()

    assert second_manager._environment_reclaimer.wait_idle(2)
    assert not tuple(quarantine.glob("*.tree"))


def test_reclaimer_does_not_purge_tombstone_with_mismatched_token(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    record = cleanup_module.quarantine_environment(
        manager,
        target,
        operation_id="interrupted-removal",
    )
    quarantine = manager.root / cleanup_module.QUARANTINE_DIRECTORY
    tomb = quarantine / record.tomb_name
    (tomb / cleanup_module._QUARANTINE_TOKEN).write_text("wrong\n", encoding="utf-8")

    manager._environment_reclaimer.wake()

    assert manager._environment_reclaimer.wait_idle(2)
    assert tomb.is_dir()


def test_reclaimer_ignores_malformed_and_unowned_quarantine_entries(
    tmp_path: Path,
) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "owned")
    cleanup_module.quarantine_environment(
        manager,
        target,
        operation_id="owned-removal",
    )
    quarantine = manager.root / cleanup_module.QUARANTINE_DIRECTORY
    unknown_token = "f" * 64
    unknown = quarantine / f"tomb-{unknown_token}.tree"
    unknown.mkdir()
    (unknown / "precious").write_text("keep\n", encoding="utf-8")
    (quarantine / f"tomb-{unknown_token}.json").write_text("{broken", encoding="utf-8")

    manager._environment_reclaimer.wake()

    assert manager._environment_reclaimer.wait_idle(2)
    assert (unknown / "precious").read_text(encoding="utf-8") == "keep\n"


def test_quarantined_child_symlink_is_unlinked_without_touching_destination(
    tmp_path: Path,
) -> None:
    if os.name == "nt":
        pytest.skip("Creating symlinks requires platform-specific privileges on Windows")
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, "example")
    outside = tmp_path / "outside"
    outside.mkdir()
    precious = outside / "precious"
    precious.write_text("keep\n", encoding="utf-8")
    (target / "outside-link").symlink_to(outside, target_is_directory=True)

    manager.remove("example").wait_for(timeout=2)

    assert manager._environment_reclaimer.wait_idle(2)
    assert precious.read_text(encoding="utf-8") == "keep\n"


def test_two_managers_reclaim_one_tombstone_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "wetlands"
    first_manager = EnvironmentManager(root)
    target = _incomplete_environment(first_manager, "example")
    cleanup_module.quarantine_environment(
        first_manager,
        target,
        operation_id="shared-removal",
    )
    second_manager = EnvironmentManager(root)
    original_remove = cleanup_module._remove_target
    calls = 0
    calls_lock = threading.Lock()

    def counted_remove(*args, **kwargs) -> None:
        nonlocal calls
        with calls_lock:
            calls += 1
        original_remove(*args, **kwargs)

    monkeypatch.setattr(cleanup_module, "_remove_target", counted_remove)
    first_manager._environment_reclaimer.wake()
    second_manager._environment_reclaimer.wake()

    assert first_manager._environment_reclaimer.wait_idle(2)
    assert second_manager._environment_reclaimer.wait_idle(2)
    assert calls == 1


def test_legacy_environment_name_does_not_conflict_with_quarantine(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "wetlands")
    target = _incomplete_environment(manager, ".wetlands-gc")

    removed = manager.remove(".wetlands-gc").wait_for(timeout=2)

    assert removed.name == ".wetlands-gc"
    assert not target.exists()
    assert (manager.root / cleanup_module.QUARANTINE_DIRECTORY).is_dir()


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
