from __future__ import annotations

from pathlib import Path

import pytest

from wetlands.specs import EnvironmentSpec, LocalPackage, PostInstallCommand, validate_environment_name
from wetlands._internal.provisioning import render_pixi_manifest


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "a/b",
        "a\\b",
        "CON",
        "con.txt",
        "name.",
        "name ",
        "C:relative",
        "a:b",
        "a?b",
        "a*b",
        "\x00bad",
    ],
)
def test_invalid_environment_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError):
        validate_environment_name(name)


def test_environment_spec_snapshots_lockfile(tmp_path: Path) -> None:
    lock = tmp_path / "pixi.lock"
    lock.write_bytes(b"version: 6\n")
    spec = EnvironmentSpec(
        python="3.11",
        conda=("numpy>=2",),
        pypi=("requests>=2",),
        post_install=(PostInstallCommand(("python", "-V")),),
        pixi_lock=lock,
    )
    lock.write_bytes(b"changed")
    assert spec.lock_bytes == b"version: 6\n"
    assert spec.normalized()["pixi_lock_sha256"]


def test_shell_post_install_requires_safe_display() -> None:
    with pytest.raises(ValueError):
        PostInstallCommand(("echo secret",), shell=True)


def test_environment_spec_rejects_invalid_entries_immediately() -> None:
    with pytest.raises(ValueError, match="PyPI dependency"):
        EnvironmentSpec(pypi=("not a valid @ requirement",))
    with pytest.raises(ValueError, match="markers"):
        EnvironmentSpec(pypi=('example; python_version > "3.10"',))
    with pytest.raises(ValueError, match="credentials"):
        EnvironmentSpec(pypi=("example @ https://user:secret@example.invalid/example.whl",))
    with pytest.raises(ValueError, match="query"):
        EnvironmentSpec(pypi=("example @ https://example.invalid/example.whl?token=secret",))
    with pytest.raises(ValueError, match="Duplicate PyPI"):
        EnvironmentSpec(pypi=("my-package>=1", "my_package<3"))
    with pytest.raises(ValueError, match="Duplicate Conda"):
        EnvironmentSpec(conda=("conda-forge::NumPy>=1", "numpy<3"))
    with pytest.raises(ValueError, match="Invalid Conda"):
        EnvironmentSpec(conda=("::numpy",))
    with pytest.raises(ValueError, match="Channel"):
        EnvironmentSpec(channels=("",))
    with pytest.raises(TypeError, match="LocalPackage"):
        EnvironmentSpec(local=(object(),))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="sequence"):
        EnvironmentSpec(conda="numpy")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="sequence"):
        PostInstallCommand("python -V")  # type: ignore[arg-type]
    combined = EnvironmentSpec(
        local=(LocalPackage(Path.cwd()),),
        pixi_lock=b"version: 6\n",
    )
    assert combined.lock_bytes == b"version: 6\n"


def test_manifest_renders_pypi_extras_and_direct_urls() -> None:
    manifest = render_pixi_manifest(
        "example",
        EnvironmentSpec(
            pypi=(
                "requests[socks]>=2",
                "example @ https://example.invalid/example.whl",
            )
        ),
    ).decode()

    assert '"requests" = { version = ">=2", extras = ["socks"] }' in manifest
    assert '"example" = { url = "https://example.invalid/example.whl" }' in manifest
