"""Immutable Wetlands 2.0 public configuration types."""

from __future__ import annotations

import hashlib
import enum
import json
import os
import re
import unicodedata
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import InvalidName, canonicalize_name

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised by Python 3.9/3.10 compatibility jobs
    import tomli as tomllib  # type: ignore[no-redef]

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


class LocalPackageValidationError(ValueError):
    """A local package cannot be represented as a deterministic Pixi requirement."""


class ProvisioningStage(enum.Enum):
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
    executable: Path
    version: str
    managed: bool


@dataclass(frozen=True)
class ProvisioningStep:
    id: str
    stage: ProvisioningStage
    argv: tuple[str, ...]
    cwd: Path | None = None
    environment: Mapping[str, str] | None = field(default=None, repr=False)
    display: str | None = None
    shell: bool = False


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
    source: Path
    editable: bool = False
    extras: tuple[str, ...] = ()
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
        if isinstance(self.extras, str):
            raise TypeError("Local package extras must be a sequence of names, not a string")
        extras = tuple(str(extra) for extra in self.extras)
        if any(not _PORTABLE_EXTRA.fullmatch(extra) for extra in extras):
            raise ValueError("Local package extras must be valid Python distribution extras")
        object.__setattr__(self, "extras", extras)


@dataclass(frozen=True)
class PostInstallCommand:
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
            package = canonicalize_name(requirement.name)
            if package in pypi_names:
                raise ValueError(f"Duplicate PyPI dependency for package {requirement.name!r}")
            pypi_names.add(package)
        channels = tuple(dict.fromkeys(_nonempty_strings(self.channels, "Channel")))
        if not channels:
            raise ValueError("At least one Pixi channel is required")
        local = tuple(self.local)
        if any(not isinstance(package, LocalPackage) for package in local):
            raise TypeError("local entries must be LocalPackage instances")
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
        return self._lock_bytes

    def normalized(self) -> dict[str, Any]:
        return {
            "python": self.python.strip(),
            "conda": sorted(set(self.conda)),
            "pypi": sorted(set(self.pypi)),
            "channels": list(self.channels),
            "local": [
                {
                    "source": str(package.source),
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
        }

    @property
    def recipe_hash(self) -> str:
        payload = json.dumps(self.normalized(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()


def _nonempty_strings(values: Any, label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} values must be a sequence, not a string")
    normalized = tuple(str(value).strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"{label} cannot be empty")
    return normalized
