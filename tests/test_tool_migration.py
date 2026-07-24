from __future__ import annotations

import io
import subprocess
import tarfile
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from wetlands._internal import install
from wetlands._internal.artifact_registry import MICROMAMBA_VERSION, PIXI_VERSION
from wetlands.environment_manager import EnvironmentManager


INSTALL_MICROMAMBA = install.installMicromamba
INSTALL_PIXI = install.installPixi
ENSURE_CONDA_TOOL = install.ensure_conda_tool


@pytest.mark.parametrize(
    ("tool", "output", "expected"),
    [
        ("pixi", "pixi 0.73.0\n", "0.73.0"),
        ("pixi", "pixi v0.73.0\n", "0.73.0"),
        ("pixi", "pixi version 0.73.0\n", "0.73.0"),
        ("micromamba", "2.8.1\n", "2.8.1"),
        ("micromamba", "micromamba 2.8.1\n", "2.8.1"),
        ("micromamba", "micromamba version 2.8.1\n", "2.8.1"),
        ("pixi", "warning\npixi 0.73.0\n", "0.73.0"),
        ("pixi", "pixi latest\n", None),
        ("micromamba", "micromamba version unknown\n", None),
        ("pixi", "pixi 0.72.0\npixi 0.73.0\n", None),
    ],
)
def test_tool_version_output_parser(tool, output, expected):
    assert install._parse_tool_version_output(tool, output) == expected


def test_detect_tool_version_uses_absolute_executable_and_timeout(tmp_path, monkeypatch):
    executable = tmp_path / "bin" / "pixi"
    executable.parent.mkdir()
    executable.write_bytes(b"pixi")
    run = MagicMock(return_value=subprocess.CompletedProcess([], 0, "pixi 0.73.0\n", ""))
    monkeypatch.setattr(install.subprocess, "run", run)

    assert install.detect_tool_version(executable, "pixi") == "0.73.0"
    run.assert_called_once_with(
        [str(executable), "--version"],
        check=False,
        capture_output=True,
        text=True,
        errors="replace",
        timeout=install.TOOL_VERSION_TIMEOUT_SECONDS,
    )


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.CompletedProcess([], 1, "", "failed"),
        subprocess.TimeoutExpired(["pixi", "--version"], 10),
        OSError("not executable"),
    ],
)
def test_detect_tool_version_rejects_failed_probe(tmp_path, monkeypatch, failure):
    executable = tmp_path / "pixi"

    if isinstance(failure, subprocess.CompletedProcess):
        monkeypatch.setattr(install.subprocess, "run", lambda *args, **kwargs: failure)
    else:

        def fail(*args, **kwargs):
            raise failure

        monkeypatch.setattr(install.subprocess, "run", fail)

    assert install.detect_tool_version(executable, "pixi") is None


def _write_dummy_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def test_matching_registered_tool_is_retained_without_installing(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "pixi")
    _write_dummy_executable(executable, "existing")
    install._write_tool_release_marker(tmp_path, "pixi", PIXI_VERSION)
    monkeypatch.setattr(install, "detect_tool_version", lambda path, tool: PIXI_VERSION.removeprefix("v"))
    installer = MagicMock()
    monkeypatch.setattr(install, "installPixi", installer)

    assert ENSURE_CONDA_TOOL(tmp_path, use_pixi=True) == executable
    installer.assert_not_called()


def test_missing_tool_is_installed_and_exact_release_is_recorded(tmp_path, monkeypatch):
    expected_version = PIXI_VERSION.removeprefix("v")

    def fake_install(install_path, version, proxies):
        executable = install.get_tool_executable_path(install_path, "pixi")
        _write_dummy_executable(executable, "new")
        return executable

    monkeypatch.setattr(install, "installPixi", fake_install)
    monkeypatch.setattr(
        install,
        "detect_tool_version",
        lambda path, tool: expected_version if path.is_file() else None,
    )

    executable = ENSURE_CONDA_TOOL(tmp_path, use_pixi=True)

    assert executable.read_text(encoding="utf-8") == "new"
    assert install.get_tool_release_marker_path(tmp_path, "pixi").read_text(encoding="utf-8") == (f"{PIXI_VERSION}\n")


@pytest.mark.parametrize(
    ("use_pixi", "tool", "release_version", "old_version"),
    [
        (True, "pixi", PIXI_VERSION, "0.1.0"),
        (False, "micromamba", MICROMAMBA_VERSION, "99.0.0"),
    ],
)
def test_mismatched_tool_is_replaced(tmp_path, monkeypatch, use_pixi, tool, release_version, old_version):
    executable = install.get_tool_executable_path(tmp_path, tool)
    _write_dummy_executable(executable, "old")
    install._write_tool_release_marker(tmp_path, tool, "old-release")
    expected_version = install.get_expected_executable_version(tool, release_version)

    def fake_detect(path, detected_tool):
        return old_version if path.read_text(encoding="utf-8") == "old" else expected_version

    def fake_install(install_path, version, proxies):
        destination = install.get_tool_executable_path(install_path, tool)
        destination.write_text("new", encoding="utf-8")
        return destination

    monkeypatch.setattr(install, "detect_tool_version", fake_detect)
    monkeypatch.setattr(install, "installPixi" if use_pixi else "installMicromamba", fake_install)

    assert ENSURE_CONDA_TOOL(tmp_path, use_pixi=use_pixi).read_text(encoding="utf-8") == "new"
    assert install.get_tool_release_marker_path(tmp_path, tool).read_text(encoding="utf-8").strip() == (release_version)


def test_markerless_micromamba_is_migrated_even_when_binary_version_matches(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "micromamba")
    _write_dummy_executable(executable, "existing")
    expected_version = install.get_expected_executable_version("micromamba", MICROMAMBA_VERSION)
    monkeypatch.setattr(install, "detect_tool_version", lambda path, tool: expected_version)
    installer = MagicMock(return_value=executable)
    monkeypatch.setattr(install, "installMicromamba", installer)

    ENSURE_CONDA_TOOL(tmp_path, use_pixi=False)

    installer.assert_called_once_with(tmp_path, version=MICROMAMBA_VERSION, proxies=None)
    assert install.get_tool_release_marker_path(tmp_path, "micromamba").read_text(encoding="utf-8").strip() == (
        MICROMAMBA_VERSION
    )


def test_failed_migration_keeps_existing_release_marker(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "pixi")
    _write_dummy_executable(executable, "old")
    install._write_tool_release_marker(tmp_path, "pixi", "v0.1.0")
    monkeypatch.setattr(install, "detect_tool_version", lambda path, tool: "0.1.0")
    monkeypatch.setattr(install, "installPixi", MagicMock(side_effect=RuntimeError("download failed")))

    with pytest.raises(RuntimeError, match="download failed"):
        ENSURE_CONDA_TOOL(tmp_path, use_pixi=True)

    assert executable.read_text(encoding="utf-8") == "old"
    assert install.get_tool_release_marker_path(tmp_path, "pixi").read_text(encoding="utf-8").strip() == "v0.1.0"


def test_concurrent_initialization_installs_once(tmp_path, monkeypatch):
    expected_version = PIXI_VERSION.removeprefix("v")
    installer_entered = threading.Event()
    allow_installer_to_finish = threading.Event()
    calls = []

    def fake_detect(path, tool):
        return expected_version if path.is_file() else None

    def fake_install(install_path, version, proxies):
        calls.append(version)
        installer_entered.set()
        assert allow_installer_to_finish.wait(timeout=5)
        executable = install.get_tool_executable_path(install_path, "pixi")
        _write_dummy_executable(executable, "new")
        return executable

    monkeypatch.setattr(install, "detect_tool_version", fake_detect)
    monkeypatch.setattr(install, "installPixi", fake_install)
    results = []
    errors = []

    def ensure():
        try:
            results.append(ENSURE_CONDA_TOOL(tmp_path, use_pixi=True))
        except Exception as error:
            errors.append(error)

    first = threading.Thread(target=ensure)
    second = threading.Thread(target=ensure)
    first.start()
    assert installer_entered.wait(timeout=5)
    second.start()
    allow_installer_to_finish.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not errors
    assert len(results) == 2
    assert calls == [PIXI_VERSION]


def test_micromamba_failed_staged_validation_preserves_executable_and_config(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "micromamba")
    _write_dummy_executable(executable, "old executable")
    config = tmp_path / ".mambarc"
    config.write_text("custom: true\n", encoding="utf-8")
    monkeypatch.setattr(install, "get_micromamba_platform_info", lambda: ("linux", "64"))

    def fake_download(url, path, expected_checksum, proxies):
        path.write_text("new executable", encoding="utf-8")

    monkeypatch.setattr(install, "downloadAndVerify", fake_download)
    monkeypatch.setattr(
        install,
        "_require_expected_executable_version",
        MagicMock(side_effect=RuntimeError("wrong staged version")),
    )

    with pytest.raises(RuntimeError, match="existing executable was left unchanged"):
        INSTALL_MICROMAMBA(tmp_path)

    assert executable.read_text(encoding="utf-8") == "old executable"
    assert config.read_text(encoding="utf-8") == "custom: true\n"


def test_micromamba_replacement_is_atomic_and_preserves_config(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "micromamba")
    _write_dummy_executable(executable, "old executable")
    config = tmp_path / ".mambarc"
    config.write_text("custom: true\n", encoding="utf-8")
    monkeypatch.setattr(install, "get_micromamba_platform_info", lambda: ("linux", "64"))

    def fake_download(url, path, expected_checksum, proxies):
        path.write_text("new executable", encoding="utf-8")

    monkeypatch.setattr(install, "downloadAndVerify", fake_download)
    monkeypatch.setattr(install, "_require_expected_executable_version", lambda *args: None)

    assert INSTALL_MICROMAMBA(tmp_path) == executable
    assert executable.read_text(encoding="utf-8") == "new executable"
    assert config.read_text(encoding="utf-8") == "custom: true\n"


def test_pixi_failed_download_preserves_existing_executable(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "pixi")
    _write_dummy_executable(executable, "old executable")
    monkeypatch.setattr(install, "get_pixi_target", lambda: "pixi-x86_64-unknown-linux-musl.tar.gz")

    def fail_download(url, path, expected_checksum, proxies):
        path.write_bytes(b"partial")
        raise RuntimeError("download failed")

    monkeypatch.setattr(install, "downloadAndVerify", fail_download)

    with pytest.raises(RuntimeError, match="existing executable was left unchanged"):
        INSTALL_PIXI(tmp_path)

    assert executable.read_text(encoding="utf-8") == "old executable"


def test_pixi_replacement_extracts_to_staging_before_replace(tmp_path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "pixi")
    _write_dummy_executable(executable, "old executable")
    artifact_name = "pixi-x86_64-unknown-linux-musl.tar.gz"
    monkeypatch.setattr(install, "get_pixi_target", lambda: artifact_name)

    def fake_download(url, path, expected_checksum, proxies):
        with tarfile.open(path, "w:gz") as archive:
            content = b"new executable"
            member = tarfile.TarInfo("pixi")
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))

    monkeypatch.setattr(install, "downloadAndVerify", fake_download)
    monkeypatch.setattr(install, "_require_expected_executable_version", lambda *args: None)

    assert INSTALL_PIXI(tmp_path) == executable
    assert executable.read_text(encoding="utf-8") == "new executable"


def test_environment_manager_delegates_tool_migration(tmp_path, monkeypatch):
    ensure = MagicMock()
    monkeypatch.setattr("wetlands.environment_manager.ensure_conda_tool", ensure)

    EnvironmentManager(
        wetlands_instance_path=tmp_path / "wetlands",
        conda_path=tmp_path / "managed-pixi",
        manager="pixi",
        log_file_path=None,
    )

    ensure.assert_called_once_with((tmp_path / "managed-pixi").resolve(), use_pixi=True, proxies=None)
