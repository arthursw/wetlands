from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import io
import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from wetlands import (
    EditableRuntimeSourceChangedError,
    EnvironmentManager,
    EnvironmentNotReadyError,
    RuntimeContentReceipt,
    RuntimeContentUnavailableError,
)
from wetlands._internal.runtime_content import _file_content, capture_runtime_content, editable_content_digest
from wetlands._internal import runtime_content as capture_kernel
from wetlands._internal.provisioning import OWNER_MARKER, READY_SCHEMA_VERSION, _path_identity, _publish_ready
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION


def facts():
    return {
        "schema_version": 1,
        "python": {
            "implementation": "cpython",
            "version": [3, 12, 14],
            "cache_tag": "cpython-312",
            "soabi": "cpython-312-darwin",
            "platform": "darwin",
            "machine": "arm64",
            "executable_digest": "0" * 64,
        },
        "distributions": [{"name": "dependency", "version": "0.1.0", "content_digest": "1" * 64}],
        "resolved_artifacts": [],
        "editable_sources": [],
    }


def receipt(value=None, generation="generation-one"):
    return RuntimeContentReceipt(
        generation_id=generation, recipe_hash="2" * 64, lockfile_hash="3" * 64, scientific_facts=value or facts()
    )


def installed(root, value):
    site = root / "lib" / "python3.12" / "site-packages"
    package, metadata = site / "dependency", site / "dependency-0.1.0.dist-info"
    package.mkdir(parents=True)
    metadata.mkdir()
    (package / "__init__.py").write_text(f"VALUE = {value}\n")
    (metadata / "METADATA").write_text("Metadata-Version: 2.1\nName: dependency\nVersion: 0.1.0\n")
    (metadata / "direct_url.json").write_text(json.dumps({"url": root.as_uri()}))
    (metadata / "INSTALLER").write_text("fixture")
    (metadata / "RECORD").write_text(
        "\n".join(
            f"{name},,"
            for name in (
                "dependency/__init__.py",
                "dependency-0.1.0.dist-info/METADATA",
                "dependency-0.1.0.dist-info/direct_url.json",
                "dependency-0.1.0.dist-info/INSTALLER",
                "dependency-0.1.0.dist-info/RECORD",
            )
        )
    )
    return site


def ready(manager, value, *, roots=None, generation="generation-one"):
    target = manager.environments_root / "proof"
    target.mkdir(parents=True)
    (target / OWNER_MARKER).write_text("owned-test")
    manifest, lock = b'[workspace]\nname="proof"\n', b"version: 6\n"
    (target / "pixi.toml").write_bytes(manifest)
    (target / "pixi.lock").write_bytes(lock)
    r = RuntimeContentReceipt(
        generation_id=generation,
        recipe_hash="2" * 64,
        lockfile_hash=hashlib.sha256(lock).hexdigest(),
        scientific_facts=value,
    )
    payload = {
        "schema_version": READY_SCHEMA_VERSION,
        "name": "proof",
        "state": "ready",
        "canonical_path": str(target.resolve()),
        "recipe_hash": r.recipe_hash,
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
        "lock_sha256": r.lockfile_hash,
        "generation_id": generation,
        "operation_id": "fixture-operation",
        "pixi_version": "fixture",
        "pixi_executable": "fixture-pixi",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "runtime_content": r._to_payload(),
        "editable_roots": roots or [],
    }
    _publish_ready(
        manager.environments_root, target, json.dumps(payload).encode(), expected_identity=_path_identity(target)
    )
    return target, payload


def tree_state(root):
    return {str(path.relative_to(root)): (path.stat().st_mtime_ns, path.stat().st_size) for path in root.rglob("*")}


def test_real_installed_content_changes_even_with_equal_version_recipe_and_lock(tmp_path):
    roots = [tmp_path / "one", tmp_path / "nine"]
    captures = [
        capture_runtime_content(distribution_paths=[str(installed(root, value))], prefix=root)
        for root, value in zip(roots, (1, 9))
    ]
    a, b = [receipt(item["scientific_facts"]) for item in captures]
    assert a.recipe_hash == b.recipe_hash and a.lockfile_hash == b.lockfile_hash
    assert (
        a.to_scientific_facts()["distributions"][0]["version"] == b.to_scientific_facts()["distributions"][0]["version"]
    )
    assert a.content_digest != b.content_digest


def test_equal_contents_ignore_prefix_uuid_record_order_and_generated_metadata(tmp_path):
    roots = [tmp_path / "prefix-one", tmp_path / "prefix-two"]
    sites = [installed(root, 4) for root in roots]
    metadata = sites[1] / "dependency-0.1.0.dist-info"
    record_path = metadata / "RECORD"
    record_path.write_text("\n".join(reversed(record_path.read_text().splitlines())))
    (metadata / "INSTALLER").write_text("another installer")
    cache = sites[1] / "dependency" / "__pycache__"
    cache.mkdir()
    (cache / "generated.pyc").write_bytes(b"not a scientific source")
    a, b = [
        receipt(
            capture_runtime_content(distribution_paths=[str(site)], prefix=root)["scientific_facts"],
            generation=str(index),
        )
        for index, (site, root) in enumerate(zip(sites, roots))
    ]
    assert a.generation_id != b.generation_id and a.content_digest == b.content_digest


def test_receipt_is_detached_and_rejects_wrong_stored_digest():
    value = facts()
    r = receipt(value)
    value["distributions"][0]["content_digest"] = "9" * 64
    detached = r.to_scientific_facts()
    detached["python"]["version"][0] = 99
    assert r.to_scientific_facts() == facts()
    payload = r._to_payload()
    payload["content_digest"] = "0" * 64
    with pytest.raises(ValueError, match="digest mismatch"):
        RuntimeContentReceipt._from_payload(payload)


def test_missing_ready_inspection_is_write_free(tmp_path):
    root = tmp_path / "absent"
    with EnvironmentManager(root) as manager:
        with pytest.raises(EnvironmentNotReadyError):
            manager.environment("missing")
        assert not root.exists()
    assert not root.exists()


def test_ready_receipt_needs_no_process_scan_or_directory_effects(tmp_path, monkeypatch):
    with EnvironmentManager(tmp_path / "manager") as manager:
        target, _ = ready(manager, facts())
        before = tree_state(manager.root)

        def forbidden(*args, **kwargs):
            raise AssertionError("Warm receipt must not launch or scan installed distributions")

        monkeypatch.setattr(subprocess, "Popen", forbidden)
        monkeypatch.setattr("wetlands._internal.runtime_content.capture_runtime_content", forbidden)
        monkeypatch.setattr("wetlands._internal.runtime_content.importlib.metadata.distributions", forbidden)
        first = manager.environment("proof").runtime_content_receipt()
        for _ in range(5):
            assert manager.environment("proof").runtime_content_receipt().content_digest == first.content_digest
        assert tree_state(manager.root) == before
        assert target.exists()


@pytest.mark.parametrize("damage", ["digest", "generation"])
def test_torn_or_foreign_receipt_refuses_without_effects(tmp_path, damage):
    with EnvironmentManager(tmp_path / "manager") as manager:
        target, payload = ready(manager, facts())
        handle = manager.environment("proof")
        payload["runtime_content"]["content_digest" if damage == "digest" else "generation_id"] = "9" * 64
        _publish_ready(
            manager.environments_root, target, json.dumps(payload).encode(), expected_identity=_path_identity(target)
        )
        before = tree_state(manager.root)
        with pytest.raises(RuntimeContentUnavailableError):
            handle.runtime_content_receipt()
        with pytest.raises(EnvironmentNotReadyError):
            manager.environment("proof")
        assert tree_state(manager.root) == before


def test_live_editable_source_change_refuses_until_reprovision(tmp_path):
    source = tmp_path / "editable" / "dependency"
    source.mkdir(parents=True)
    module = source / "__init__.py"
    module.write_text("VALUE = 4\n")
    captured = editable_content_digest([str(source)])
    value = facts()
    value["editable_sources"] = [{"name": "dependency", "content_digest": captured}]
    roots = [
        {
            "name": "dependency",
            "source_root": str(source.parent),
            "import_roots": [str(source)],
            "content_digest": captured,
        }
    ]
    with EnvironmentManager(tmp_path / "manager") as manager:
        ready(manager, value, roots=roots)
        handle = manager.environment("proof")
        assert handle.runtime_content_receipt().to_scientific_facts() == value
        module.write_text("VALUE = 99\n")
        before = tree_state(manager.root)
        with pytest.raises(EditableRuntimeSourceChangedError) as failure:
            handle.runtime_content_receipt()
        assert failure.value.distribution_name == "dependency"
        assert failure.value.expected_digest == captured
        assert failure.value.actual_digest != captured
        assert tree_state(manager.root) == before


def test_hatch_editable_without_top_level_uses_owned_activation_and_actual_members(tmp_path, monkeypatch):
    captures = []
    for label in ("one", "two"):
        source = tmp_path / label / "project"
        package = source / "src" / "receipt_editable_fixture"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("VALUE = 4\n")
        prefix = tmp_path / label / "prefix"
        site = prefix / "site-packages"
        metadata = site / "receipt_editable_fixture-0.1.dist-info"
        metadata.mkdir(parents=True)
        (metadata / "METADATA").write_text("Metadata-Version: 2.1\nName: receipt-editable-fixture\nVersion: 0.1\n")
        (metadata / "direct_url.json").write_text(json.dumps({"url": source.as_uri(), "dir_info": {"editable": True}}))
        (site / "_receipt_editable_fixture.pth").write_text(str(source / "src") + "\n")
        (metadata / "RECORD").write_text(
            "receipt_editable_fixture-0.1.dist-info/METADATA,,\nreceipt_editable_fixture-0.1.dist-info/direct_url.json,,\n_receipt_editable_fixture.pth,,\n"
        )
        monkeypatch.syspath_prepend(str(source / "src"))
        captures.append(capture_runtime_content(distribution_paths=[str(site)], prefix=prefix))
    assert captures[0]["editable_roots"][0]["import_roots"] == [
        str(tmp_path / "one/project/src/receipt_editable_fixture")
    ]
    assert (
        receipt(captures[0]["scientific_facts"]).content_digest
        == receipt(captures[1]["scientific_facts"]).content_digest
    )


def test_editable_symlink_directory_and_root_are_not_silently_omitted(tmp_path):
    package = tmp_path / "package"
    package.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    (external / "data.py").write_text("VALUE = 9\n")
    linked = package / "importable"
    linked.symlink_to(external, target_is_directory=True)
    with pytest.raises(ValueError, match="unowned link"):
        editable_content_digest([str(package)])
    linked.unlink()
    root_link = tmp_path / "root-link"
    root_link.symlink_to(package, target_is_directory=True)
    with pytest.raises(ValueError, match="physical authority"):
        editable_content_digest([str(root_link)])


@pytest.mark.parametrize("directory", ["build", "dist"])
def test_import_root_build_and_dist_are_scientific_content(tmp_path, directory):
    package = tmp_path / "package"
    nested = package / directory
    nested.mkdir(parents=True)
    module = nested / "__init__.py"
    module.write_text("VALUE = 4\n")
    before = editable_content_digest([str(package)])
    module.write_text("VALUE = 9\n")
    assert editable_content_digest([str(package)]) != before


@pytest.mark.parametrize("launcher", ["trampoline", "windows-entrypoint"])
def test_generated_launchers_ignore_only_admitted_interpreter_prefix(tmp_path, monkeypatch, launcher):
    captures = []
    for label, body in (("one", b"print(4)\n"), ("two", b"print(4)\n"), ("three", b"print(9)\n")):
        prefix = tmp_path / label
        site = installed(prefix, 4)
        scripts = prefix / ("bin" if launcher == "trampoline" else "Scripts")
        scripts.mkdir()
        executable = scripts / ("python" if launcher == "trampoline" else "python.exe")
        executable.write_bytes(b"same-interpreter-bytes")
        monkeypatch.setattr(sys, "executable", str(executable))
        if launcher == "trampoline":
            payload = f"#!/bin/sh\n'''exec' '{executable}' \"$0\" \"$@\"\n' '''\n".encode() + body
            command = scripts / "example"
        else:
            archive_bytes = io.BytesIO()
            with zipfile.ZipFile(archive_bytes, "w") as archive:
                archive.writestr(zipfile.ZipInfo("__main__.py"), body)
            payload = b"MZverified-fixture-stub" + f'#!"{executable}"\n'.encode() + archive_bytes.getvalue()
            command = scripts / "example.exe"
        command.write_bytes(payload)
        metadata = site / "dependency-0.1.0.dist-info"
        (metadata / "entry_points.txt").write_text("[console_scripts]\nexample = dependency:main\n")
        record_path = metadata / "RECORD"
        record_path.write_text(
            record_path.read_text()
            + f"\ndependency-0.1.0.dist-info/entry_points.txt,,\n../../../{scripts.name}/{command.name},,\n"
        )
        captures.append(
            receipt(capture_runtime_content(distribution_paths=[str(site)], prefix=prefix)["scientific_facts"])
        )
    assert captures[0].content_digest == captures[1].content_digest
    assert captures[0].content_digest != captures[2].content_digest


@pytest.mark.parametrize(
    "damage",
    [None, "identity", "descriptor", "path", "windows_executable", "windows_descriptor_mode", "windows_permission"],
)
def test_file_capture_preserves_identity_and_each_metadata_api_fence(tmp_path, monkeypatch, damage):
    path = tmp_path / "member.py"
    payload = b"VALUE = 4\n"
    path.write_bytes(payload)
    real = path.stat()

    def metadata(*, ctime, inode=real.st_ino, mode=real.st_mode):
        return SimpleNamespace(
            st_dev=real.st_dev,
            st_ino=inode,
            st_mode=mode,
            st_size=real.st_size,
            st_mtime_ns=real.st_mtime_ns,
            st_ctime_ns=ctime,
        )

    windows = damage is not None and damage.startswith("windows_")
    path_mode = real.st_mode | 0o111 if windows else real.st_mode
    descriptor_mode = real.st_mode & ~0o111 if windows else real.st_mode
    path_stats = iter(
        [metadata(ctime=10, mode=path_mode), metadata(ctime=11 if damage == "path" else 10, mode=path_mode)]
    )
    fd_stats = iter(
        [
            metadata(ctime=20, inode=real.st_ino + 1 if damage == "identity" else real.st_ino, mode=descriptor_mode),
            metadata(
                ctime=21 if damage == "descriptor" else 20,
                mode=descriptor_mode ^ 0o100 if damage == "windows_descriptor_mode" else descriptor_mode,
            ),
        ]
    )
    original_stat = Path.stat
    monkeypatch.setattr(
        Path, "stat", lambda self, *a, **kw: next(path_stats) if self == path else original_stat(self, *a, **kw)
    )
    if damage == "windows_permission":
        fd_stats = iter([metadata(ctime=20, mode=descriptor_mode ^ 0o200)] * 2)
    monkeypatch.setattr(
        capture_kernel, "os", SimpleNamespace(name="nt" if windows else os.name, fstat=lambda fd: next(fd_stats))
    )
    if damage in {None, "windows_executable"}:
        assert _file_content(path) == (len(payload), hashlib.sha256(payload).digest())
    else:
        with pytest.raises(ValueError, match="changed while being captured"):
            _file_content(path)
