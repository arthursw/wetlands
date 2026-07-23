from __future__ import annotations

import inspect
import re
from pathlib import Path
from unittest.mock import patch

import pytest

from wetlands._internal import artifact_registry
from wetlands._internal import install

INSTALL_MICROMAMBA = install.installMicromamba
INSTALL_PIXI = install.installPixi


EXPECTED_PIXI_TARGETS = {
    "pixi-aarch64-apple-darwin.tar.gz",
    "pixi-aarch64-pc-windows-msvc.zip",
    "pixi-aarch64-unknown-linux-musl.tar.gz",
    "pixi-x86_64-apple-darwin.tar.gz",
    "pixi-x86_64-pc-windows-msvc.zip",
    "pixi-x86_64-unknown-linux-musl.tar.gz",
}
EXPECTED_MICROMAMBA_TARGETS = {
    "micromamba-linux-64",
    "micromamba-linux-aarch64",
    "micromamba-linux-ppc64le",
    "micromamba-osx-64",
    "micromamba-osx-arm64",
    "micromamba-win-64",
}


def test_registry_has_exactly_one_checksum_for_every_supported_target():
    assert set(artifact_registry.PIXI_SHA256) == EXPECTED_PIXI_TARGETS
    assert len(artifact_registry.PIXI_SHA256) == len(EXPECTED_PIXI_TARGETS)
    assert set(artifact_registry.MICROMAMBA_SHA256) == EXPECTED_MICROMAMBA_TARGETS
    assert len(artifact_registry.MICROMAMBA_SHA256) == len(EXPECTED_MICROMAMBA_TARGETS)


def test_every_registered_digest_is_normalized_sha256():
    digests = [
        *artifact_registry.PIXI_SHA256.values(),
        *artifact_registry.MICROMAMBA_SHA256.values(),
        artifact_registry.VC_REDIST_SHA256,
    ]
    assert artifact_registry.VC_REDIST_ARTIFACT_NAME == "VC_redist.x64.exe"
    assert all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in digests)


def test_default_micromamba_version_uses_matching_registry_entry(tmp_path, monkeypatch):
    artifact_name = "micromamba-linux-64"

    monkeypatch.setattr(install, "get_micromamba_platform_info", lambda: ("linux", "64"))

    def fake_download(url, path, expected_checksum, proxies):
        assert artifact_registry.MICROMAMBA_VERSION in url
        assert expected_checksum == artifact_registry.MICROMAMBA_SHA256[artifact_name]
        path.write_bytes(b"micromamba")

    monkeypatch.setattr(install, "downloadAndVerify", fake_download)

    executable = INSTALL_MICROMAMBA(tmp_path)

    assert executable.read_bytes() == b"micromamba"


def test_default_pixi_version_uses_matching_registry_entry(tmp_path, monkeypatch):
    artifact_name = "pixi-x86_64-unknown-linux-musl.tar.gz"
    seen = {}

    monkeypatch.setattr(install, "get_pixi_target", lambda: artifact_name)

    def stop_after_checksum_selection(url, path, expected_checksum, proxies):
        seen["url"] = url
        seen["checksum"] = expected_checksum
        raise RuntimeError("stop after selection")

    monkeypatch.setattr(install, "downloadAndVerify", stop_after_checksum_selection)

    with pytest.raises(Exception, match="Pixi installation failed"):
        INSTALL_PIXI(tmp_path)

    assert artifact_registry.PIXI_VERSION in seen["url"]
    assert seen["checksum"] == artifact_registry.PIXI_SHA256[artifact_name]


@pytest.mark.parametrize(
    ("installer", "version", "tool"),
    [
        (install.installPixi, "v0.49.0", "Pixi"),
        (install.installPixi, "latest", "Pixi"),
        (install.installMicromamba, "2.4.0-1", "Micromamba"),
        (install.installMicromamba, "latest", "Micromamba"),
    ],
)
def test_unknown_and_latest_versions_fail_before_download(tmp_path, installer, version, tool):
    with patch("wetlands._internal.install.downloadFile") as download:
        with pytest.raises(ValueError, match=rf"No trusted checksums are registered for {tool} {re.escape(version)}"):
            installer(tmp_path, version=version)
    download.assert_not_called()


def test_missing_pixi_registry_entry_is_clear_and_precedes_download(tmp_path, monkeypatch):
    artifact_name = "pixi-x86_64-unknown-linux-musl.tar.gz"
    monkeypatch.setattr(install, "get_pixi_target", lambda: artifact_name)
    monkeypatch.delitem(install.PIXI_SHA256, artifact_name)

    with patch("wetlands._internal.install.downloadFile") as download:
        with pytest.raises(ValueError, match=rf"No trusted checksum is registered.*{re.escape(artifact_name)}"):
            INSTALL_PIXI(tmp_path)
    download.assert_not_called()


def test_runtime_installer_has_no_checksum_resource_access():
    source = inspect.getsource(install)

    assert "CHECKSUMS_BASE_DIR" not in source
    assert "VC_REDIST_CHECKSUM_PATH" not in source
    assert '".sha256"' not in source
    assert "Path(__file__)" not in source
    assert "artifact_registry" in source


def test_registry_is_an_importable_python_package_resource():
    registry_path = Path(artifact_registry.__file__)

    assert registry_path.name == "artifact_registry.py"
    assert registry_path.is_file()
