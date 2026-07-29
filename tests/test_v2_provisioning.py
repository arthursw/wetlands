from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import wetlands._internal.provisioning as provisioning_module
from wetlands._internal import runtime_state
from wetlands import (
    EnvironmentGenerationChangedError,
    EnvironmentInUseError,
    EnvironmentManager,
    EnvironmentNotReadyError,
    EnvironmentRecipeConflictError,
    EnvironmentSpec,
    LocalPackage,
    OperationCanceled,
    OperationState,
    PostInstallCommand,
    PreparationError,
    ProvisioningError,
    UnmanagedTargetError,
)
from wetlands._internal.provisioning import (
    OWNER_MARKER,
    ProvisioningStage,
    _create_managed_target,
    _publish_ready,
    _read_ready,
    _remove_target,
    _safe_command,
    _write_target_file,
    environment_lifecycle_gate,
)
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION


def _fake_pixi(
    tmp_path: Path,
    *,
    install_delay: float = 0,
    install_exit_code: int = 0,
    mutate_locked: bool = False,
    version_delay: float = 0,
) -> Path:
    executable = tmp_path / "pixi" / "bin" / ("pixi.exe" if os.name == "nt" else "pixi")
    executable.parent.mkdir(parents=True)
    executable.write_text(
        f"""#!/usr/bin/env python3
import pathlib
import subprocess
import sys
import time

arguments = sys.argv[1:]
if arguments == ["--version"]:
    time.sleep({version_delay!r})
    print("pixi 0.48.2")
elif arguments and arguments[0] == "install":
    manifest = pathlib.Path(arguments[arguments.index("--manifest-path") + 1])
    sentinel = pathlib.Path({str(tmp_path / "install-started")!r})
    sentinel.write_text("started", encoding="utf-8")
    time.sleep({install_delay!r})
    if {install_exit_code!r}:
        raise SystemExit({install_exit_code!r})
    lock = manifest.parent / "pixi.lock"
    if "--locked" not in arguments or {mutate_locked!r}:
        lock.write_text("version: 6\\n", encoding="utf-8")
elif arguments and arguments[0] == "shell-hook":
    pass
elif arguments and arguments[0] == "add":
    manifest = pathlib.Path(arguments[arguments.index("--manifest-path") + 1])
    with manifest.open("a", encoding="utf-8") as stream:
        stream.write("# local " + arguments[-1] + "\\n")
    if "--locked" not in arguments:
        (manifest.parent / "pixi.lock").write_text("version: 6\\nlocal: true\\n", encoding="utf-8")
elif arguments and arguments[0] == "run":
    manifest_index = arguments.index("--manifest-path")
    command = arguments[manifest_index + 2:]
    raise SystemExit(subprocess.run(command, check=False).returncode)
else:
    raise SystemExit("unexpected fake Pixi arguments: " + repr(arguments))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable


def test_manager_construction_has_no_filesystem_side_effect(tmp_path: Path) -> None:
    root = tmp_path / "wetlands"
    EnvironmentManager(root)
    assert not root.exists()


def test_network_configuration_accepts_only_proxy_settings(tmp_path: Path) -> None:
    manager = EnvironmentManager(
        tmp_path / "state",
        network={"HTTPS_PROXY": "https://example.invalid", "no_proxy": "localhost"},
    )
    assert manager.network == {
        "https": "https://example.invalid",
        "no_proxy": "localhost",
    }
    with pytest.raises(ValueError, match="network keys"):
        EnvironmentManager(tmp_path / "other", network={"PATH": "/untrusted"})


def test_commands_and_output_are_redacted() -> None:
    rendered = _safe_command(
        (
            "tool",
            "https://user:password@example.invalid/archive",
            "Authorization: Bearer abc123",
            "api_key=secret",
        )
    )

    assert "password" not in rendered
    assert "abc123" not in rendered
    assert "secret" not in rendered


def test_post_install_secret_is_absent_from_events_and_ready_metadata(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    secret = "highly-sensitive-value"
    operation = manager.provision(
        "example",
        EnvironmentSpec(post_install=(PostInstallCommand(("python", "-c", "pass", "--token", secret)),)),
    )
    events = []
    operation.listen(events.append)

    environment = operation.wait_for()
    ready = (environment.path / ".wetlands" / "ready.json").read_text(encoding="utf-8")

    assert secret not in ready
    assert all(secret not in event.message and secret not in (event.line or "") for event in events)


def test_missing_external_pixi_has_structured_preparation_failure(tmp_path: Path) -> None:
    missing = tmp_path / "missing" / "pixi"
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=missing)

    with pytest.raises(PreparationError) as caught:
        manager.prepare().wait_for()

    assert caught.value.failure.stage == ProvisioningStage.PIXI_DISCOVERY.value


def test_prepare_and_provision_with_external_pixi(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    root = tmp_path / "state"
    manager = EnvironmentManager(root, pixi_executable=executable)

    preparation = manager.prepare()
    pixi = preparation.wait_for()
    assert pixi.executable == executable.resolve()
    assert not pixi.managed

    events = []
    operation = manager.provision("example", EnvironmentSpec(python="3.11"))
    operation.listen(events.append)
    environment = operation.wait_for()
    assert operation.state is OperationState.COMPLETED
    assert environment.pixi_manifest_path.is_file()
    assert environment.pixi_lock_path.is_file()
    assert environment.lockfile_hash
    assert manager.environment("example").generation_id == environment.generation_id
    assert events[-1].state is OperationState.COMPLETED
    assert all(event.environment == "example" for event in events)


def test_matching_environment_is_reused(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    spec = EnvironmentSpec(python="3.11")
    first = manager.provision("example", spec).wait_for()
    pool = MagicMock()
    first._pools.append(pool)
    second = manager.provision("example", spec).wait_for()

    assert second is first
    manager.close()
    pool.close.assert_called_once()


def test_different_pixi_identity_requires_explicit_replacement(tmp_path: Path) -> None:
    first_pixi = _fake_pixi(tmp_path / "first")
    second_pixi = _fake_pixi(tmp_path / "second")
    root = tmp_path / "state"
    EnvironmentManager(root, pixi_executable=first_pixi).provision(
        "example",
        EnvironmentSpec(),
    ).wait_for()

    manager = EnvironmentManager(root, pixi_executable=second_pixi)
    with pytest.raises(ProvisioningError, match="different recipe or Pixi identity"):
        manager.provision("example", EnvironmentSpec()).wait_for()


def test_replacement_fails_closed_when_worker_identity_is_uninspectable(
    tmp_path: Path,
) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    original = manager.provision(
        "example",
        EnvironmentSpec(python="3.11"),
    ).wait_for()
    runtime_state.record_worker(
        manager.root,
        env_name="example",
        env_path=original.path,
        worker_index=0,
        pid=os.getpid(),
        port=5001,
        persistent=False,
        pool_id="pool-a",
        generation_id=original.generation_id,
        recipe_hash=original.recipe_hash,
        worker_runtime_version=WORKER_RUNTIME_VERSION,
        protocol_version=EXECUTION_PROTOCOL_VERSION,
    )

    unavailable = runtime_state.WorkerIdentityUnavailableError("Cannot inspect recorded worker PID")
    with (
        patch.object(
            runtime_state,
            "_recorded_process_tree_state",
            side_effect=unavailable,
        ),
        pytest.raises(ProvisioningError, match="Cannot inspect recorded worker"),
    ):
        manager.provision(
            "example",
            EnvironmentSpec(python="3.12"),
            replace_existing=True,
        ).wait_for()

    assert manager.environment("example").generation_id == original.generation_id
    assert runtime_state.load_workers(manager.root)["workers"]


def test_supplied_lock_is_preserved_and_published(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    lock = b"version: 6\npinned: true\n"

    environment = manager.provision(
        "example",
        EnvironmentSpec(python="3.11", pixi_lock=lock),
    ).wait_for()

    assert environment.pixi_lock_path.read_bytes() == lock


def test_local_package_is_added_by_pixi_and_reused(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    package = tmp_path / "package with spaces"
    package.mkdir()
    (package / "pyproject.toml").write_text(
        '[project]\nname = "Example_Local.Package"\nversion = "1.0"\n',
        encoding="utf-8",
    )
    spec = EnvironmentSpec(
        local=(LocalPackage(package, editable=True, extras=("test",)),),
    )

    first = manager.provision("example", spec).wait_for()
    second = manager.provision("example", spec).wait_for()

    assert f"# local example-local-package[test] @ {package.resolve().as_uri()}" in first.pixi_manifest_path.read_text(
        encoding="utf-8"
    )
    assert first.generation_id == second.generation_id


def test_local_package_can_use_a_supplied_lock_without_modifying_it(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    package = tmp_path / "package"
    package.mkdir()
    (package / "pyproject.toml").write_text(
        '[project]\nname = "locked-local"\nversion = "1.0"\n',
        encoding="utf-8",
    )
    lock = b"version: 6\nlocal: locked\n"

    environment = manager.provision(
        "example",
        EnvironmentSpec(
            local=(LocalPackage(package, editable=True),),
            pixi_lock=lock,
        ),
    ).wait_for()

    assert environment.pixi_lock_path.read_bytes() == lock
    assert f"# local locked-local @ {package.resolve().as_uri()}" in environment.pixi_manifest_path.read_text(
        encoding="utf-8"
    )


def test_modified_supplied_lock_fails_and_removes_incomplete_target(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path, mutate_locked=True)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    operation = manager.provision(
        "example",
        EnvironmentSpec(python="3.11", pixi_lock=b"version: 6\npinned: true\n"),
    )

    with pytest.raises(ProvisioningError, match="modified the supplied"):
        operation.wait_for()

    assert operation.state is OperationState.FAILED
    assert not (manager.environments_root / "example").exists()


def test_failed_install_has_structured_failure_and_cleans_target(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path, install_exit_code=17)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    operation = manager.provision("example", EnvironmentSpec(python="3.11"))

    with pytest.raises(ProvisioningError) as caught:
        operation.wait_for()

    assert caught.value.failure.stage == ProvisioningStage.CONDA_INSTALL.value
    assert caught.value.failure.step_id == "pixi-install"
    assert caught.value.failure.returncode == 17
    assert not (manager.environments_root / "example").exists()


def test_cancel_terminates_install_before_publishing_terminal_state(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path, install_delay=30)
    manager = EnvironmentManager(
        tmp_path / "state",
        pixi_executable=executable,
        termination_grace=0.1,
    )
    operation = manager.provision("example", EnvironmentSpec(python="3.11"))
    sentinel = tmp_path / "install-started"
    deadline = time.monotonic() + 5
    while not sentinel.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert sentinel.exists()

    assert operation.cancel()
    with pytest.raises(OperationCanceled):
        operation.wait_for(timeout=5)

    assert operation.state is OperationState.CANCELED
    assert not (manager.environments_root / "example").exists()


def test_cancel_is_rejected_after_ready_publication_is_sealed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    original_atomic_write_at = provisioning_module._atomic_write_at
    cancel_results: list[bool] = []
    operation_holder: dict[str, object] = {}

    def write_then_cancel(directory_fd, name, content, *, guard) -> None:
        original_atomic_write_at(directory_fd, name, content, guard=guard)
        if name == provisioning_module.READY_FILENAME:
            operation = operation_holder["operation"]
            cancel_results.append(operation.cancel())  # type: ignore[attr-defined]

    monkeypatch.setattr(provisioning_module, "_atomic_write_at", write_then_cancel)
    operation = manager.provision("example", EnvironmentSpec())
    operation_holder["operation"] = operation
    environment = operation.wait_for()

    assert cancel_results == [False]
    assert operation.state is OperationState.COMPLETED
    assert manager.environment("example").generation_id == environment.generation_id


def test_cancel_while_waiting_for_shared_preparation(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path, version_delay=1)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    first = manager.prepare()
    second = manager.prepare()

    assert second.cancel()
    with pytest.raises(OperationCanceled):
        second.wait_for(timeout=3)

    assert first.wait_for(timeout=3).version == "0.48.2"


def test_concurrent_provision_calls_reuse_one_published_generation(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path, install_delay=0.2)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    spec = EnvironmentSpec(python="3.11")
    first = manager.provision("example", spec)
    second = manager.provision("example", spec)

    first_environment = first.wait_for(timeout=5)
    second_environment = second.wait_for(timeout=5)
    assert first_environment is second_environment


def test_environment_lifecycle_gate_serializes_portable_name_aliases(
    tmp_path: Path,
) -> None:
    manager = EnvironmentManager(tmp_path / "state")
    acquired = threading.Event()

    def acquire_alias() -> None:
        with environment_lifecycle_gate(manager, "example"):
            acquired.set()

    with environment_lifecycle_gate(manager, "Example"):
        thread = threading.Thread(target=acquire_alias)
        thread.start()
        assert not acquired.wait(0.2)

    assert acquired.wait(2)
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_incomplete_owned_target_and_stale_journal_are_rebuilt(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    target = manager.environments_root / "example"
    target.mkdir(parents=True)
    (target / OWNER_MARKER).write_text("crashed-operation\n", encoding="utf-8")
    (target / "partial").write_text("partial", encoding="utf-8")
    journal = manager.state_root / "operations" / "crashed-operation.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(json.dumps({"target": str(target), "state": "building"}), encoding="utf-8")

    environment = manager.provision("example", EnvironmentSpec(python="3.11")).wait_for()

    assert environment.path == target.resolve()
    assert not (target / "partial").exists()
    assert not journal.exists()


def test_unmanaged_target_is_never_removed(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    target = manager.environments_root / "example"
    target.mkdir(parents=True)
    precious = target / "precious.txt"
    precious.write_text("keep", encoding="utf-8")

    with pytest.raises(UnmanagedTargetError) as caught:
        manager.provision("example", EnvironmentSpec()).wait_for()

    assert caught.value.environment == "example"
    assert caught.value.path == target
    assert precious.read_text(encoding="utf-8") == "keep"


def test_ready_environment_with_conflicting_recipe_has_public_error(
    tmp_path: Path,
) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    existing = manager.provision(
        "example",
        EnvironmentSpec(python="3.11"),
    ).wait_for()
    requested = EnvironmentSpec(python="3.12")

    with pytest.raises(EnvironmentRecipeConflictError) as caught:
        manager.provision("example", requested).wait_for()

    assert caught.value.environment == "example"
    assert caught.value.existing_recipe_hash == existing.recipe_hash
    assert caught.value.requested_recipe_hash == requested.recipe_hash


def test_read_ready_rejects_linked_metadata_parent(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("Creating symlinks requires platform-specific privileges on Windows")
    target = tmp_path / "environments" / "example"
    target.mkdir(parents=True)
    (target / OWNER_MARKER).write_text("owned\n", encoding="utf-8")
    (target / "pixi.toml").write_text("manifest\n", encoding="utf-8")
    (target / "pixi.lock").write_text("lock\n", encoding="utf-8")
    outside = tmp_path / "outside-metadata"
    outside.mkdir()
    (outside / "ready.json").write_text("{}", encoding="utf-8")
    (target / ".wetlands").symlink_to(outside, target_is_directory=True)

    assert _read_ready(target) is None


def test_linked_owner_marker_is_unmanaged_and_never_followed(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("Creating symlinks requires platform-specific privileges on Windows")
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    target = manager.environments_root / "example"
    target.mkdir(parents=True)
    outside_marker = tmp_path / "outside-marker"
    outside_marker.write_text("owned\n", encoding="utf-8")
    (target / OWNER_MARKER).symlink_to(outside_marker)

    with pytest.raises(UnmanagedTargetError):
        manager.provision("example", EnvironmentSpec()).wait_for()

    assert outside_marker.read_text(encoding="utf-8") == "owned\n"
    assert (target / OWNER_MARKER).is_symlink()


@pytest.mark.parametrize("artifact", [OWNER_MARKER, "pixi.toml"])
def test_target_identity_swap_is_rejected_before_project_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact: str,
) -> None:
    if os.name == "nt":
        pytest.skip("Descriptor-relative identity checks are POSIX-specific")
    root = tmp_path / "environments"
    target = root / "example"
    identity = _create_managed_target(root, target)
    if artifact != OWNER_MARKER:
        _write_target_file(
            root,
            target,
            OWNER_MARKER,
            b"owned\n",
            expected_identity=identity,
            require_marker=False,
        )
    moved = root / "moved-example"
    original_atomic_write_at = provisioning_module._atomic_write_at

    def swap_then_write(*args, **kwargs) -> None:
        target.rename(moved)
        target.mkdir()
        original_atomic_write_at(*args, **kwargs)

    monkeypatch.setattr(provisioning_module, "_atomic_write_at", swap_then_write)

    with pytest.raises(RuntimeError, match="identity"):
        _write_target_file(
            root,
            target,
            artifact,
            b"content\n",
            expected_identity=identity,
            require_marker=artifact != OWNER_MARKER,
        )

    assert not (moved / artifact).exists()
    assert not (target / artifact).exists()


def test_target_identity_swap_is_rejected_before_metadata_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name == "nt":
        pytest.skip("Descriptor-relative identity checks are POSIX-specific")
    root = tmp_path / "environments"
    target = root / "example"
    identity = _create_managed_target(root, target)
    _write_target_file(
        root,
        target,
        OWNER_MARKER,
        b"owned\n",
        expected_identity=identity,
        require_marker=False,
    )
    moved = root / "moved-example"
    original_atomic_write_at = provisioning_module._atomic_write_at

    def swap_then_publish(*args, **kwargs) -> None:
        target.rename(moved)
        target.mkdir()
        original_atomic_write_at(*args, **kwargs)

    monkeypatch.setattr(provisioning_module, "_atomic_write_at", swap_then_publish)

    with pytest.raises(RuntimeError, match="identity"):
        _publish_ready(
            root,
            target,
            b"{}",
            expected_identity=identity,
        )

    assert not (moved / ".wetlands" / "ready.json").exists()
    assert not (target / ".wetlands").exists()


def test_renamed_environment_root_stops_deletion_before_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name == "nt":
        pytest.skip("Descriptor-relative identity checks are POSIX-specific")
    root = tmp_path / "environments"
    target = root / "example"
    target.mkdir(parents=True)
    (target / OWNER_MARKER).write_text("owned\n", encoding="utf-8")
    precious = target / "precious.txt"
    precious.write_text("keep\n", encoding="utf-8")
    moved_root = tmp_path / "moved-environments"
    original_remove_contents = provisioning_module._remove_directory_contents_fd

    def rename_then_remove(*args, **kwargs) -> None:
        root.rename(moved_root)
        root.mkdir()
        original_remove_contents(*args, **kwargs)

    monkeypatch.setattr(
        provisioning_module,
        "_remove_directory_contents_fd",
        rename_then_remove,
    )

    with pytest.raises(RuntimeError, match="identity"):
        _remove_target(root, target)

    assert (moved_root / "example" / "precious.txt").read_text(encoding="utf-8") == "keep\n"


def test_cleanup_unlinks_child_symlink_without_touching_destination(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("Creating symlinks requires platform-specific privileges on Windows")
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    target = manager.environments_root / "example"
    target.mkdir(parents=True)
    (target / OWNER_MARKER).write_text("crashed-operation\n", encoding="utf-8")
    destination = tmp_path / "outside.txt"
    destination.write_text("keep", encoding="utf-8")
    (target / "link").symlink_to(destination)

    manager.provision("example", EnvironmentSpec()).wait_for()

    assert destination.read_text(encoding="utf-8") == "keep"


def test_tampered_ready_artifact_is_not_reused(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    spec = EnvironmentSpec(python="3.11")
    first = manager.provision("example", spec).wait_for()
    first.pixi_manifest_path.write_text("tampered = true\n", encoding="utf-8")

    second = manager.provision("example", spec).wait_for()

    assert second.generation_id != first.generation_id


def test_environment_lookup_revalidates_cached_readiness(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    environment = manager.provision("example", EnvironmentSpec()).wait_for()
    environment.pixi_lock_path.write_text("tampered", encoding="utf-8")

    with pytest.raises(EnvironmentNotReadyError):
        manager.environment("example")


def test_portable_name_alias_is_rejected(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    manager.provision("Example", EnvironmentSpec()).wait_for()

    with pytest.raises(ProvisioningError, match="aliases"):
        manager.provision("example", EnvironmentSpec()).wait_for()


def test_worker_pool_executes_qualified_and_equal_stem_path_targets(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    environment = manager.provision("example", EnvironmentSpec(python="3.11")).wait_for()
    first = tmp_path / "first" / "worker.py"
    second = tmp_path / "second" / "worker.py"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_text("def value():\n    return 'first'\n", encoding="utf-8")
    second.write_text("def value():\n    return 'second'\n", encoding="utf-8")

    with environment.start() as pool:
        assert pool.execute_import("builtins:sum", args=([1, 2, 3],)) == 6
        assert pool.execute_path(first, "value") == "first"
        assert pool.execute_path(second, "value") == "second"


def test_persistent_pool_detach_and_exclusive_attach(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    environment = manager.provision("example", EnvironmentSpec(python="3.11")).wait_for()
    pool = environment.start(persistent=True)
    assert pool.execute_import("builtins:sum", args=([1, 2],)) == 3

    with pytest.raises(RuntimeError, match="controlled by another live process"):
        environment.attach_pool(timeout=0.5)

    pool.detach()
    with environment.attach_pool() as attached:
        assert attached.execute_import("builtins:sum", args=([3, 4],)) == 7


def test_replacement_is_rejected_while_nonpersistent_pool_is_live(
    tmp_path: Path,
) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    environment = manager.provision(
        "example",
        EnvironmentSpec(python="3.11"),
    ).wait_for()
    pool = environment.start()
    try:
        with pytest.raises(EnvironmentInUseError) as caught:
            manager.provision(
                "example",
                EnvironmentSpec(python="3.12"),
                replace_existing=True,
            ).wait_for()

        assert caught.value.environment == "example"
        assert caught.value.generation_id == environment.generation_id
        assert pool.execute_import("builtins:sum", args=([1, 2],)) == 3
    finally:
        pool.close()


def test_stale_environment_handle_rejects_start_and_attach(tmp_path: Path) -> None:
    executable = _fake_pixi(tmp_path)
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=executable)
    stale = manager.provision(
        "example",
        EnvironmentSpec(python="3.11"),
    ).wait_for()
    current = manager.provision(
        "example",
        EnvironmentSpec(python="3.12"),
        replace_existing=True,
    ).wait_for()

    with pytest.raises(EnvironmentGenerationChangedError) as start_error:
        stale.start()
    with pytest.raises(EnvironmentGenerationChangedError):
        stale.attach_pool()

    assert start_error.value.expected_generation_id == stale.generation_id
    assert start_error.value.actual_generation_id == current.generation_id
