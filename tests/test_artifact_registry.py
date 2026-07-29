from __future__ import annotations

import inspect
import re
from pathlib import Path

from wetlands._internal import artifact_registry, provisioning


EXPECTED_PIXI_TARGETS = {
    "pixi-aarch64-apple-darwin.tar.gz",
    "pixi-aarch64-pc-windows-msvc.zip",
    "pixi-aarch64-unknown-linux-musl.tar.gz",
    "pixi-x86_64-apple-darwin.tar.gz",
    "pixi-x86_64-pc-windows-msvc.zip",
    "pixi-x86_64-unknown-linux-musl.tar.gz",
}


def test_registry_has_exactly_one_checksum_for_every_supported_target():
    assert set(artifact_registry.PIXI_SHA256) == EXPECTED_PIXI_TARGETS
    assert len(artifact_registry.PIXI_SHA256) == len(EXPECTED_PIXI_TARGETS)


def test_every_registered_digest_is_normalized_sha256():
    assert all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in artifact_registry.PIXI_SHA256.values())


def test_runtime_provisioner_uses_generated_registry() -> None:
    source = inspect.getsource(provisioning)

    assert "CHECKSUMS_BASE_DIR" not in source
    assert '".sha256"' not in source
    assert "PIXI_SHA256" in source
    assert "PIXI_VERSION" in source


def test_current_platform_target_has_a_registered_checksum() -> None:
    assert provisioning._pixi_target() in artifact_registry.PIXI_SHA256


def test_registry_is_an_importable_python_package_resource():
    registry_path = Path(artifact_registry.__file__)

    assert registry_path.name == "artifact_registry.py"
    assert registry_path.is_file()
