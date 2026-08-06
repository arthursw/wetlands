"""Immutable Wetlands 2.0 public configuration types."""

from __future__ import annotations

import hashlib
import enum
import json
import os
import re
import stat
import unicodedata
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import InvalidName, canonicalize_name

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised by Python 3.9/3.10 compatibility jobs
    import tomli as tomllib  # type: ignore[import-not-found,no-redef]

_WINDOWS_RESERVED = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
}
_WINDOWS_INVALID_CHARACTERS = frozenset('<>:"|?*')
_PORTABLE_EXTRA = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_FULL_GIT_COMMIT_SHA = re.compile(r"(?:[0-9A-Fa-f]{40}|[0-9A-Fa-f]{64})")
MANAGED_DEBUGPY_VERSION = "1.8.20"
MANAGED_RUNTIME_PYPI = (f"debugpy=={MANAGED_DEBUGPY_VERSION}",)
_MANAGED_RUNTIME_PACKAGE_NAMES = frozenset({"debugpy"})
_LOCAL_PACKAGE_IDENTITY_PREFIX = "sha256:"
_LOCAL_PACKAGE_IDENTITY_PATTERN = re.compile(r"sha256:[0-9a-fA-F]{64}")
_LOCAL_PACKAGE_HASH_DOMAIN = b"wetlands-local-package-v1\0"


def _parse_pinned_git_url(url: str) -> tuple[str, str]:
    parsed_url = urllib.parse.urlsplit(url)
    if parsed_url.username or parsed_url.password or parsed_url.query:
        raise ValueError("PyPI direct URLs cannot contain credentials or query parameters")
    if parsed_url.scheme != "git+https":
        raise ValueError("PyPI Git dependencies must use git+https URLs")
    if parsed_url.fragment:
        raise ValueError("PyPI Git dependencies cannot contain URL fragments")
    repository_path, separator, revision = parsed_url.path.rpartition("@")
    if not separator or not repository_path or _FULL_GIT_COMMIT_SHA.fullmatch(revision) is None:
        raise ValueError("PyPI Git dependencies must pin a full 40- or 64-character hexadecimal commit SHA after '@'")
    repository_url = urllib.parse.urlunsplit(
        (
            "https",
            parsed_url.netloc,
            repository_path,
            "",
            "",
        )
    )
    return repository_url, revision


class LocalPackageValidationError(ValueError):
    """A local package cannot be represented as a deterministic Pixi requirement."""


@dataclass(frozen=True)
class _LocalTreeEntry:
    relative: str
    is_directory: bool
    signature: tuple[int, int, int, int, int, int]


def _metadata_signature(metadata: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _is_link_or_reparse(metadata: os.stat_result) -> bool:
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return stat.S_ISLNK(metadata.st_mode) or bool(getattr(metadata, "st_file_attributes", 0) & reparse_flag)


def _local_tree_entries(source: Path) -> tuple[tuple[int, int, int, int, int, int], tuple[_LocalTreeEntry, ...]]:
    try:
        root_metadata = source.lstat()
    except OSError as error:
        raise LocalPackageValidationError(f"Could not inspect local package source {source}: {error}") from error
    if _is_link_or_reparse(root_metadata) or not stat.S_ISDIR(root_metadata.st_mode):
        raise LocalPackageValidationError(f"Local package source must be an unlinked directory: {source}")

    entries: list[_LocalTreeEntry] = []
    portable_names: dict[str, str] = {}

    def visit(directory: Path) -> None:
        try:
            children = tuple(os.scandir(directory))
        except OSError as error:
            raise LocalPackageValidationError(f"Could not inspect local package directory {directory}: {error}") from error
        for child in sorted(children, key=lambda item: item.name):
            path = Path(child.path)
            try:
                metadata = child.stat(follow_symlinks=False)
                relative = path.relative_to(source).as_posix()
                relative.encode("utf-8")
            except (OSError, UnicodeError, ValueError) as error:
                raise LocalPackageValidationError(f"Could not safely identify local package entry {path}: {error}") from error
            if relative in {"", ".", ".."} or relative.startswith("../"):
                raise LocalPackageValidationError(f"Local package entry escapes its source directory: {path}")
            portable_key = unicodedata.normalize("NFC", relative).casefold()
            collision = portable_names.get(portable_key)
            if collision is not None and collision != relative:
                raise LocalPackageValidationError(
                    f"Local package contains case-colliding paths {collision!r} and {relative!r}"
                )
            portable_names[portable_key] = relative
            if _is_link_or_reparse(metadata):
                raise LocalPackageValidationError(f"Local package cannot contain links or reparse points: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                is_directory = True
            elif stat.S_ISREG(metadata.st_mode):
                is_directory = False
            else:
                raise LocalPackageValidationError(f"Local package cannot contain special files: {relative}")
            entries.append(
                _LocalTreeEntry(
                    relative=relative,
                    is_directory=is_directory,
                    signature=_metadata_signature(metadata),
                )
            )
            if is_directory:
                visit(path)

    visit(source)
    return _metadata_signature(root_metadata), tuple(sorted(entries, key=lambda item: item.relative))


def _read_local_file(source: Path, entry: _LocalTreeEntry, destination: BinaryIO | None) -> bytes:
    path = source / Path(entry.relative)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise LocalPackageValidationError(f"Could not safely open local package file {entry.relative!r}: {error}") from error
    digest = hashlib.sha256()
    try:
        if _metadata_signature(os.fstat(descriptor)) != entry.signature:
            raise LocalPackageValidationError(f"Local package file changed during inspection: {entry.relative}")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            if destination is not None:
                destination.write(chunk)
        if _metadata_signature(os.fstat(descriptor)) != entry.signature:
            raise LocalPackageValidationError(f"Local package file changed while it was read: {entry.relative}")
    finally:
        os.close(descriptor)
    try:
        after = path.lstat()
    except OSError as error:
        raise LocalPackageValidationError(f"Local package file disappeared during inspection: {entry.relative}") from error
    if _is_link_or_reparse(after) or _metadata_signature(after) != entry.signature:
        raise LocalPackageValidationError(f"Local package file changed while it was read: {entry.relative}")
    return digest.digest()


def _local_package_tree_identity(source: Path, destination: Path | None = None) -> str:
    root_signature, entries = _local_tree_entries(source)
    if destination is not None:
        destination.mkdir()
        for entry in entries:
            if entry.is_directory:
                (destination / Path(entry.relative)).mkdir()

    digest = hashlib.sha256(_LOCAL_PACKAGE_HASH_DOMAIN)
    for entry in entries:
        if entry.is_directory:
            continue
        relative_bytes = entry.relative.encode("utf-8")
        digest.update(len(relative_bytes).to_bytes(8, "big"))
        digest.update(relative_bytes)
        digest.update(entry.signature[3].to_bytes(8, "big"))
        destination_stream: BinaryIO | None = None
        try:
            if destination is not None:
                destination_path = destination / Path(entry.relative)
                destination_stream = destination_path.open("xb")
            digest.update(_read_local_file(source, entry, destination_stream))
        finally:
            if destination_stream is not None:
                destination_stream.close()
        if destination is not None:
            (destination / Path(entry.relative)).chmod(stat.S_IMODE(entry.signature[2]))

    final_root_signature, final_entries = _local_tree_entries(source)
    if final_root_signature != root_signature or final_entries != entries:
        raise LocalPackageValidationError("Local package source changed while its content identity was computed")
    identity = f"{_LOCAL_PACKAGE_IDENTITY_PREFIX}{digest.hexdigest()}"
    if destination is not None and local_package_content_identity(destination) != identity:
        raise LocalPackageValidationError("Copied local package content does not match its source identity")
    return identity


def local_package_content_identity(source: str | os.PathLike[str]) -> str:
    """Return a deterministic content identity for an immutable local package tree.

    Only regular files and directories are accepted. Paths and file contents are
    hashed in a canonical order; links, special files, portable path collisions,
    and concurrent source mutation are rejected.
    """

    path = Path(os.path.abspath(Path(source).expanduser()))
    return _local_package_tree_identity(path)


def _copy_local_package_content(source: Path, destination: Path, expected_identity: str) -> None:
    actual_identity = _local_package_tree_identity(source, destination)
    if actual_identity != expected_identity:
        raise LocalPackageValidationError(
            f"Local package content identity changed: expected {expected_identity}, found {actual_identity}"
        )


class ProvisioningStage(enum.Enum):
    """A stable provisioning stage identifier used by operation events."""

    LOCK_WAIT = "lock_wait"
    PIXI_DISCOVERY = "pixi_discovery"
    PIXI_DOWNLOAD = "pixi_download"
    PIXI_VERIFY = "pixi_verify"
    PIXI_INSTALL = "pixi_install"
    TARGET_INSPECTION = "target_inspection"
    INCOMPLETE_REMOVAL = "incomplete_removal"
    PROJECT_MATERIALIZATION = "project_materialization"
    LOCK_RESOLUTION = "lock_resolution"
    CONDA_INSTALL = "conda_install"
    PYPI_INSTALL = "pypi_install"
    LOCAL_INSTALL = "local_install"
    POST_INSTALL = "post_install"
    VALIDATION = "validation"
    METADATA_PUBLICATION = "metadata_publication"
    CLEANUP = "cleanup"


@dataclass(frozen=True)
class PixiInfo:
    """Information about the validated Pixi executable used by Wetlands."""

    executable: Path
    version: str
    managed: bool


def validate_environment_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise ValueError("Environment name must be a non-empty string")
    normalized = unicodedata.normalize("NFC", name)
    if normalized in {".", ".."}:
        raise ValueError("Environment name cannot be '.' or '..'")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("Environment name must be a single path component")
    if any(character in _WINDOWS_INVALID_CHARACTERS for character in normalized):
        raise ValueError("Environment name contains a character that is not portable")
    if normalized[-1:] in {" ", "."}:
        raise ValueError("Environment name cannot end with a space or dot")
    if any(ord(character) < 32 or character == "\x7f" for character in normalized):
        raise ValueError("Environment name cannot contain control characters")
    if re.match(r"^(?:[A-Za-z]:|//|\\\\|\\\\\\?\\|\\\\\\.\\)", normalized):
        raise ValueError("Environment name cannot be rooted or use a Windows device path")
    stem = normalized.split(".", 1)[0].casefold()
    if stem in _WINDOWS_RESERVED:
        raise ValueError(f"Environment name uses reserved device alias {stem!r}")
    return normalized


def environment_name_key(name: str) -> str:
    """Return the portable comparison key used for managed names."""

    return validate_environment_name(name).casefold()


@dataclass(frozen=True)
class LocalPackage:
    """An installable local Python package included in an environment recipe.

    The source must contain a PEP 621 ``pyproject.toml`` with ``[project].name``.
    """

    source: Path
    editable: bool = False
    extras: tuple[str, ...] = ()
    content_identity: str | None = None
    distribution_name: str = field(init=False)

    def __post_init__(self) -> None:
        source = Path(self.source).resolve()
        pyproject = source / "pyproject.toml"
        if not source.is_dir():
            raise LocalPackageValidationError(f"Local package source must be an existing directory: {source}")
        if not pyproject.is_file():
            raise LocalPackageValidationError(f"Local package {source} must contain pyproject.toml with [project].name")
        try:
            document = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
            raise LocalPackageValidationError(
                f"Could not read valid TOML from local package {pyproject}: {error}"
            ) from error
        project = document.get("project")
        declared_name = project.get("name") if isinstance(project, dict) else None
        if not isinstance(declared_name, str) or not declared_name:
            raise LocalPackageValidationError(f"Local package {pyproject} must declare a non-empty [project].name")
        try:
            distribution_name = canonicalize_name(declared_name, validate=True)
        except InvalidName as error:
            raise LocalPackageValidationError(
                f"Local package {pyproject} has invalid [project].name {declared_name!r}"
            ) from error
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "distribution_name", distribution_name)
        content_identity = self.content_identity
        if content_identity is not None:
            if not isinstance(content_identity, str) or _LOCAL_PACKAGE_IDENTITY_PATTERN.fullmatch(content_identity) is None:
                raise ValueError("Local package content_identity must be 'sha256:' followed by 64 hexadecimal digits")
            if self.editable:
                raise ValueError("A content-identified local package cannot be editable")
            object.__setattr__(self, "content_identity", content_identity.lower())
        if isinstance(self.extras, str):
            raise TypeError("Local package extras must be a sequence of names, not a string")
        extras = tuple(str(extra) for extra in self.extras)
        if any(not _PORTABLE_EXTRA.fullmatch(extra) for extra in extras):
            raise ValueError("Local package extras must be valid Python distribution extras")
        object.__setattr__(self, "extras", extras)


@dataclass(frozen=True)
class PostInstallCommand:
    """A command run after Pixi installs the environment dependencies."""

    argv: tuple[str, ...]
    shell: bool = False
    display: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.argv, str):
            raise TypeError("Post-install argv must be a sequence of arguments, not a string")
        argv = tuple(str(item) for item in self.argv)
        if not argv:
            raise ValueError("Post-install command argv cannot be empty")
        if self.shell and self.display is None:
            raise ValueError("Shell post-install commands require an explicit safe display string")
        object.__setattr__(self, "argv", argv)


@dataclass(frozen=True)
class EnvironmentSpec:
    """The complete immutable recipe for a managed Pixi environment.

    Dependency strings use Pixi's Conda syntax in :attr:`conda` and PEP 508
    requirement syntax in :attr:`pypi`.
    """

    python: str = ">=3.9"
    conda: tuple[str, ...] = ()
    pypi: tuple[str, ...] = ()
    channels: tuple[str, ...] = ("conda-forge",)
    local: tuple[LocalPackage, ...] = ()
    post_install: tuple[PostInstallCommand, ...] = ()
    pixi_lock: bytes | os.PathLike[str] | None = field(default=None, repr=False)
    _lock_bytes: bytes | None = field(init=False, default=None, repr=False, compare=True)

    def __post_init__(self) -> None:
        python = str(self.python).strip()
        if not python:
            raise ValueError("Python constraint cannot be empty")
        conda = _nonempty_strings(self.conda, "Conda dependency")
        conda_names: set[str] = set()
        for dependency in conda:
            if "::" in dependency:
                raise ValueError(
                    f"Channel-qualified Conda dependencies are not supported: {dependency!r}. "
                    "Declare channels with EnvironmentSpec(channels=...)."
                )
            match = re.match(r"^([A-Za-z0-9_.-]+)", dependency)
            if match is None:
                raise ValueError(f"Invalid Conda dependency: {dependency!r}")
            package = canonicalize_name(match.group(1))
            if package in _MANAGED_RUNTIME_PACKAGE_NAMES:
                raise ValueError(f"Conda package {match.group(1)!r} is managed by the Wetlands worker runtime")
            if package in conda_names:
                raise ValueError(f"Duplicate Conda dependency for package {match.group(1)!r}")
            conda_names.add(package)
        pypi = _nonempty_strings(self.pypi, "PyPI dependency")
        pypi_names: set[str] = set()
        for dependency in pypi:
            try:
                requirement = Requirement(dependency)
            except InvalidRequirement as error:
                raise ValueError(f"Invalid PyPI dependency: {dependency!r}") from error
            if requirement.marker is not None:
                raise ValueError(f"PyPI environment markers are not supported in EnvironmentSpec: {dependency!r}")
            if requirement.url is not None:
                parsed_url = urllib.parse.urlsplit(requirement.url)
                if parsed_url.username or parsed_url.password or parsed_url.query:
                    raise ValueError("PyPI direct URLs cannot contain credentials or query parameters")
                if parsed_url.scheme.startswith("git+"):
                    _parse_pinned_git_url(requirement.url)
            package = canonicalize_name(requirement.name)
            if package in _MANAGED_RUNTIME_PACKAGE_NAMES:
                raise ValueError(f"PyPI package {requirement.name!r} is managed by the Wetlands worker runtime")
            if package in pypi_names:
                raise ValueError(f"Duplicate PyPI dependency for package {requirement.name!r}")
            pypi_names.add(package)
        channels = tuple(dict.fromkeys(_nonempty_strings(self.channels, "Channel")))
        if not channels:
            raise ValueError("At least one Pixi channel is required")
        local = tuple(self.local)
        if any(not isinstance(package, LocalPackage) for package in local):
            raise TypeError("local entries must be LocalPackage instances")
        local_names: set[str] = set()
        for local_package in local:
            if local_package.distribution_name in _MANAGED_RUNTIME_PACKAGE_NAMES:
                raise ValueError(
                    f"Local package {local_package.distribution_name!r} is managed by the Wetlands worker runtime"
                )
            if local_package.distribution_name in pypi_names:
                raise ValueError(
                    f"Local package {local_package.distribution_name!r} duplicates a declared PyPI dependency"
                )
            if local_package.distribution_name in local_names:
                raise ValueError(f"Duplicate local package {local_package.distribution_name!r}")
            local_names.add(local_package.distribution_name)
        post_install = tuple(self.post_install)
        if any(not isinstance(command, PostInstallCommand) for command in post_install):
            raise TypeError("post_install entries must be PostInstallCommand instances")
        object.__setattr__(self, "python", python)
        object.__setattr__(self, "conda", conda)
        object.__setattr__(self, "pypi", pypi)
        object.__setattr__(self, "channels", channels)
        object.__setattr__(self, "local", local)
        object.__setattr__(self, "post_install", post_install)
        lock = self.pixi_lock
        if lock is None:
            lock_bytes = None
        elif isinstance(lock, bytes):
            lock_bytes = bytes(lock)
        else:
            lock_bytes = Path(lock).read_bytes()
        object.__setattr__(self, "_lock_bytes", lock_bytes)
        object.__setattr__(self, "pixi_lock", None)

    @property
    def lock_bytes(self) -> bytes | None:
        """Return an independent copy of the supplied lockfile bytes, if any."""
        return self._lock_bytes

    def normalized(self) -> dict[str, Any]:
        """Return the canonical recipe representation used for identity."""
        return {
            "python": self.python.strip(),
            "conda": sorted(set(self.conda)),
            "pypi": sorted(set(self.pypi)),
            "channels": list(self.channels),
            "local": [
                {
                    **(
                        {"content_identity": package.content_identity}
                        if package.content_identity is not None
                        else {"source": str(package.source)}
                    ),
                    "distribution_name": package.distribution_name,
                    "editable": package.editable,
                    "extras": list(package.extras),
                }
                for package in self.local
            ],
            "post_install": [
                {
                    "argv": list(command.argv),
                    "shell": command.shell,
                    "display": command.display,
                }
                for command in self.post_install
            ],
            "pixi_lock_sha256": (
                hashlib.sha256(self._lock_bytes).hexdigest() if self._lock_bytes is not None else None
            ),
            "managed_runtime": {
                "pypi": list(MANAGED_RUNTIME_PYPI),
            },
        }

    @property
    def recipe_hash(self) -> str:
        """Return the SHA-256 identity of the normalized recipe."""
        payload = json.dumps(self.normalized(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()


def _nonempty_strings(values: Any, label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} values must be a sequence, not a string")
    normalized = tuple(str(value).strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"{label} cannot be empty")
    return normalized
