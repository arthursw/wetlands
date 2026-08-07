from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

import wetlands.specs as specs_module

from wetlands.specs import (
    EnvironmentSpec,
    LocalPackage,
    LocalPackageValidationError,
    MANAGED_DEBUGPY_VERSION,
    PostInstallCommand,
    local_package_content_identity,
    validate_environment_name,
)
from wetlands._internal.provisioning import render_pixi_manifest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised by Python 3.9/3.10 compatibility jobs
    import tomli as tomllib


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


def test_manifest_accepts_full_sha256_git_commit() -> None:
    revision = "0123456789abcdef" * 4
    manifest = render_pixi_manifest(
        "example",
        EnvironmentSpec(
            pypi=(f"example @ git+https://example.invalid/repository.git@{revision}",),
        ),
    ).decode()

    assert f'rev = "{revision}"' in manifest


@pytest.mark.parametrize(
    ("requirement", "message"),
    [
        (
            "example @ git+http://example.invalid/repository.git@0123456789abcdef0123456789abcdef01234567",
            r"must use git\+https",
        ),
        (
            "example @ git+ssh://example.invalid/repository.git@0123456789abcdef0123456789abcdef01234567",
            r"must use git\+https",
        ),
        (
            "example @ git+https://example.invalid/repository.git",
            "full 40- or 64-character hexadecimal commit SHA",
        ),
        (
            "example @ git+https://example.invalid/repository.git@main",
            "full 40- or 64-character hexadecimal commit SHA",
        ),
        (
            "example @ git+https://example.invalid/repository.git@v1.2.3",
            "full 40- or 64-character hexadecimal commit SHA",
        ),
        (
            "example @ git+https://example.invalid/repository.git@0123456789abcdef",
            "full 40- or 64-character hexadecimal commit SHA",
        ),
        (
            "example @ git+https://example.invalid/repository.git@0123456789abcdef0123456789abcdef0123456g",
            "full 40- or 64-character hexadecimal commit SHA",
        ),
        (
            "example @ git+https://example.invalid/repository.git@0123456789abcdef0123456789abcdef01234567"
            "#subdirectory=package",
            "fragments",
        ),
        (
            "example @ git+https://user:secret@example.invalid/repository.git@0123456789abcdef0123456789abcdef01234567",
            "credentials",
        ),
        (
            "example @ git+https://example.invalid/repository.git@"
            "0123456789abcdef0123456789abcdef01234567?token=secret",
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

    manifest = tomllib.loads(
        render_pixi_manifest(
            "example",
            EnvironmentSpec(
                pypi=("requests>=2",),
                local=(LocalPackage(package, editable=True, extras=("test",)),),
            ),
        ).decode()
    )

    assert manifest["pypi-dependencies"]["requests"] == ">=2"
    assert manifest["pypi-dependencies"]["local-package"] == {
        "path": str(package.resolve()),
        "editable": True,
        "extras": ["test"],
    }


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


def test_local_package_content_identity_is_relocatable_and_recipe_stable(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    for package in (first, second):
        (package / "nested").mkdir(parents=True)
        (package / "pyproject.toml").write_text(
            '[project]\nname = "example"\nversion = "1.0"\n',
            encoding="utf-8",
        )
        (package / "nested" / "data.txt").write_text("content", encoding="utf-8")

    identity = local_package_content_identity(first)
    first_package = LocalPackage(first, content_identity=identity)
    second_package = LocalPackage(second, content_identity=identity.upper().replace("SHA256", "sha256"))

    assert identity.startswith("sha256:")
    assert local_package_content_identity(second) == identity
    assert EnvironmentSpec(local=(first_package,)).recipe_hash == EnvironmentSpec(local=(second_package,)).recipe_hash
    assert "source" not in EnvironmentSpec(local=(first_package,)).normalized()["local"][0]


def test_local_package_content_identity_changes_with_path_or_content(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    (package / "pyproject.toml").write_text('[project]\nname = "example"\n', encoding="utf-8")
    original = local_package_content_identity(package)

    (package / "data.txt").write_text("first", encoding="utf-8")
    with_file = local_package_content_identity(package)
    (package / "data.txt").write_text("second", encoding="utf-8")
    changed_content = local_package_content_identity(package)
    (package / "renamed.txt").write_bytes((package / "data.txt").read_bytes())
    (package / "data.txt").unlink()
    changed_path = local_package_content_identity(package)

    assert len({original, with_file, changed_content, changed_path}) == 4


def test_local_package_content_identity_includes_empty_directories(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    (package / "pyproject.toml").write_text('[project]\nname = "example"\n', encoding="utf-8")
    without_empty_directory = local_package_content_identity(package)

    empty = package / "empty"
    empty.mkdir()
    with_empty_directory = local_package_content_identity(package)
    empty.rmdir()

    assert with_empty_directory != without_empty_directory
    assert local_package_content_identity(package) == without_empty_directory


@pytest.mark.skipif(__import__("os").name == "nt", reason="POSIX executable modes are unavailable")
def test_local_package_content_identity_and_copy_include_file_modes(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    pyproject = package / "pyproject.toml"
    pyproject.write_text('[project]\nname = "example"\n', encoding="utf-8")
    pyproject.chmod(0o644)
    regular_identity = local_package_content_identity(package)

    (package / "empty").mkdir()
    directory_identity = local_package_content_identity(package)
    pyproject.chmod(0o755)
    executable_identity = local_package_content_identity(package)
    destination = tmp_path / "copy"
    specs_module._copy_local_package_content(package, destination, executable_identity)

    assert directory_identity != regular_identity
    assert executable_identity != directory_identity
    assert local_package_content_identity(destination) == executable_identity
    assert (destination / "empty").is_dir()
    assert (destination / "pyproject.toml").stat().st_mode & 0o777 == 0o755


def test_content_identified_local_package_rejects_editable_or_invalid_identity(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    (package / "pyproject.toml").write_text('[project]\nname = "example"\n', encoding="utf-8")
    identity = local_package_content_identity(package)

    with pytest.raises(ValueError, match="cannot be editable"):
        LocalPackage(package, editable=True, content_identity=identity)
    with pytest.raises(ValueError, match="sha256"):
        LocalPackage(package, content_identity="not-a-digest")


def test_local_package_content_identity_rejects_links_and_special_files(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    link = package / "escape"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Symbolic links are not available")

    with pytest.raises(LocalPackageValidationError, match="links or reparse points"):
        local_package_content_identity(package)

    with pytest.raises(LocalPackageValidationError, match="cannot be a link"):
        LocalPackage(link, content_identity=f"sha256:{'0' * 64}")

    link.unlink()
    if hasattr(os, "mkfifo"):
        fifo = package / "pipe"
        os.mkfifo(fifo)
        with pytest.raises(LocalPackageValidationError, match="special files"):
            local_package_content_identity(package)


def test_local_package_content_identity_rejects_case_collisions(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    first = package / "entry"
    second = package / "ENTRY"
    first.write_text("first", encoding="utf-8")
    second.write_text("second", encoding="utf-8")
    if first.samefile(second):
        pytest.skip("The test filesystem is case-insensitive")

    with pytest.raises(LocalPackageValidationError, match="case-colliding"):
        local_package_content_identity(package)


def test_local_package_content_identity_rejects_mutation_during_scan(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    source = package / "data.txt"
    source.write_text("first", encoding="utf-8")
    original_read = specs_module._read_local_file

    def mutate_after_read(*args, **kwargs):
        result = original_read(*args, **kwargs)
        source.write_text("changed", encoding="utf-8")
        return result

    with (
        patch.object(specs_module, "_read_local_file", side_effect=mutate_after_read),
        pytest.raises(LocalPackageValidationError, match="changed"),
    ):
        local_package_content_identity(package)


def test_local_package_content_identity_allows_path_descriptor_timestamp_differences(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    package = tmp_path / "package"
    package.mkdir()
    source = package / "data.txt"
    source.write_text("content", encoding="utf-8")
    original_fstat = specs_module.os.fstat

    class DescriptorMetadata:
        def __init__(self, metadata: os.stat_result) -> None:
            self._metadata = metadata
            self.st_mtime_ns = metadata.st_mtime_ns + 1
            self.st_ctime_ns = metadata.st_ctime_ns + 1

        def __getattr__(self, name: str):
            return getattr(self._metadata, name)

    monkeypatch.setattr(
        specs_module.os,
        "fstat",
        lambda descriptor: DescriptorMetadata(original_fstat(descriptor)),
    )

    assert local_package_content_identity(package).startswith("sha256:")


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
