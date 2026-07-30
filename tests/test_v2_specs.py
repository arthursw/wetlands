from __future__ import annotations

from pathlib import Path

import pytest

from wetlands.specs import (
    EnvironmentSpec,
    LocalPackage,
    LocalPackageValidationError,
    MANAGED_DEBUGPY_VERSION,
    PostInstallCommand,
    validate_environment_name,
)
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
    lock.write_bytes(b"version: 6\n- name: debugpy\n")
    spec = EnvironmentSpec(
        python="3.11",
        conda=("numpy>=2",),
        pypi=("requests>=2",),
        post_install=(PostInstallCommand(("python", "-V")),),
        pixi_lock=lock,
    )
    lock.write_bytes(b"changed")
    assert spec.lock_bytes == b"version: 6\n- name: debugpy\n"
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
    with pytest.raises(ValueError, match="channels="):
        EnvironmentSpec(conda=("conda-forge::NumPy>=1",))
    with pytest.raises(ValueError, match="channels="):
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
        pixi_lock=b"version: 6\n- name: debugpy\n",
    )
    assert combined.lock_bytes == b"version: 6\n- name: debugpy\n"


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
    assert f'"debugpy" = "=={MANAGED_DEBUGPY_VERSION}"' in manifest
    assert manifest.startswith("[workspace]\n")
    assert "[project]" not in manifest


def test_manifest_renders_pinned_git_dependency_for_pixi() -> None:
    revision = "2b90b9f5ceec907a663497b9df6b8a1d7b5bd94d"
    manifest = render_pixi_manifest(
        "example",
        EnvironmentSpec(
            pypi=(f"sam-2[notebooks] @ git+https://github.com/facebookresearch/sam2.git@{revision}",),
        ),
    ).decode()

    assert (
        f'"sam-2" = {{ git = "https://github.com/facebookresearch/sam2.git", '
        f'rev = "{revision}", extras = ["notebooks"] }}'
    ) in manifest


@pytest.mark.parametrize(
    ("requirement", "message"),
    [
        (
            "example @ git+http://example.invalid/repository.git@main",
            r"must use git\+https",
        ),
        (
            "example @ git+ssh://example.invalid/repository.git@main",
            r"must use git\+https",
        ),
        (
            "example @ git+https://example.invalid/repository.git",
            "explicit revision",
        ),
        (
            "example @ git+https://example.invalid/repository.git@main#subdirectory=package",
            "fragments",
        ),
        (
            "example @ git+https://user:secret@example.invalid/repository.git@main",
            "credentials",
        ),
        (
            "example @ git+https://example.invalid/repository.git@main?token=secret",
            "query",
        ),
    ],
)
def test_environment_spec_rejects_unsafe_or_ambiguous_git_dependencies(
    requirement: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        EnvironmentSpec(pypi=(requirement,))


def test_manifest_materializes_local_packages_before_install(
    tmp_path: Path,
) -> None:
    package = tmp_path / "package with spaces"
    package.mkdir()
    (package / "pyproject.toml").write_text(
        '[project]\nname = "Local_Package"\nversion = "1.0"\n',
        encoding="utf-8",
    )

    manifest = render_pixi_manifest(
        "example",
        EnvironmentSpec(
            pypi=("requests>=2",),
            local=(LocalPackage(package, editable=True, extras=("test",)),),
        ),
    ).decode()

    assert '"requests" = ">=2"' in manifest
    assert f'"local-package" = {{ path = "{package.resolve()}", editable = true, extras = ["test"] }}' in manifest


def test_environment_spec_rejects_local_dependency_name_collisions(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    for package in (first, second):
        package.mkdir()
        (package / "pyproject.toml").write_text(
            '[project]\nname = "Same_Package"\nversion = "1.0"\n',
            encoding="utf-8",
        )

    with pytest.raises(ValueError, match="duplicates a declared PyPI"):
        EnvironmentSpec(
            pypi=("same-package>=1",),
            local=(LocalPackage(first),),
        )
    with pytest.raises(ValueError, match="Duplicate local package"):
        EnvironmentSpec(
            local=(LocalPackage(first), LocalPackage(second)),
        )


def test_managed_debugger_dependency_cannot_be_overridden(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="managed by the Wetlands worker runtime"):
        EnvironmentSpec(pypi=("debugpy>=1",))
    with pytest.raises(ValueError, match="managed by the Wetlands worker runtime"):
        EnvironmentSpec(conda=("debugpy",))
    local = tmp_path / "debugpy"
    local.mkdir()
    (local / "pyproject.toml").write_text('[project]\nname = "debugpy"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="managed by the Wetlands worker runtime"):
        EnvironmentSpec(local=(LocalPackage(local),))


def test_local_package_discovers_and_canonicalizes_distribution_name(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    (package / "pyproject.toml").write_text(
        '[project]\nname = "My_Distribution.Name"\nversion = "1.0"\n',
        encoding="utf-8",
    )

    local = LocalPackage(package, editable=True, extras=("test",))
    original_recipe = EnvironmentSpec(local=(local,)).recipe_hash

    assert local.source == package.resolve()
    assert local.distribution_name == "my-distribution-name"
    assert EnvironmentSpec(local=(local,)).normalized()["local"] == [
        {
            "source": str(package.resolve()),
            "distribution_name": "my-distribution-name",
            "editable": True,
            "extras": ["test"],
        }
    ]
    (package / "pyproject.toml").write_text(
        '[project]\nname = "renamed-distribution"\nversion = "1.0"\n',
        encoding="utf-8",
    )
    renamed_recipe = EnvironmentSpec(local=(LocalPackage(package),)).recipe_hash
    assert renamed_recipe != original_recipe


@pytest.mark.parametrize(
    "pyproject, message",
    [
        (None, "must contain pyproject.toml"),
        ("[build-system]\nrequires = []\n", r"declare a non-empty \[project\]\.name"),
        ('[project]\nname = ""\n', r"declare a non-empty \[project\]\.name"),
        ('[project]\nname = "invalid name!"\n', r"invalid \[project\]\.name"),
        ("[project\n", "valid TOML"),
    ],
)
def test_local_package_rejects_missing_or_invalid_project_name(
    tmp_path: Path,
    pyproject: str | None,
    message: str,
) -> None:
    package = tmp_path / "package"
    package.mkdir()
    if pyproject is not None:
        (package / "pyproject.toml").write_text(pyproject, encoding="utf-8")

    with pytest.raises(LocalPackageValidationError, match=message):
        LocalPackage(package)
