from __future__ import annotations

import contextlib
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple

import yaml

from wetlands._internal.artifact_registry import (
    MICROMAMBA_SHA256,
    MICROMAMBA_VERSION,
    PIXI_SHA256,
    PIXI_VERSION,
    VC_REDIST_ARTIFACT_NAME,
    VC_REDIST_SHA256,
    VC_REDIST_URL,
)
from wetlands._internal.shell import shell_quote

ToolName = Literal["pixi", "micromamba"]
TOOL_VERSION_TIMEOUT_SECONDS = 10
INSTALL_LOCK_FILENAME = ".wetlands-install.lock"

# --- Helper Functions ---


def downloadFile(url: str, dest_path: Path, proxies: Optional[Dict[str, str]] = None) -> None:
    """
    Downloads a file from a URL to a destination path using urllib.

    Note: For more complex scenarios, consider using the 'requests' library.
    """
    print(f"Downloading {url} to {dest_path}...")
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    proxy_handler = urllib.request.ProxyHandler(proxies)
    opener = urllib.request.build_opener(proxy_handler)
    urllib.request.install_opener(opener)

    try:
        with urllib.request.urlopen(url, timeout=120) as response, open(dest_path, "wb") as outFile:
            shutil.copyfileobj(response, outFile)
        print(f"Successfully downloaded {dest_path.name}.")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download {url}. Reason: {e.reason}") from e


def calculate_sha256(file_path: Path) -> str:
    """Calculates the SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files efficiently.
            for byteBlock in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byteBlock)
        return sha256_hash.hexdigest()
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Cannot calculate checksum, file not found: {file_path}") from e


def verify_checksum(file_path: Path, expected_checksum: str) -> None:
    """Verify a file against a trusted SHA-256 digest embedded in the registry."""
    print(f"Verifying checksum for {file_path.name}...")
    actual_checksum = calculate_sha256(file_path)

    if actual_checksum == expected_checksum:
        print(f"Checksum OK for {file_path.name}.")
    else:
        raise ValueError(
            f"Checksum MISMATCH for {file_path.name}!\n  Expected: {expected_checksum}\n  Actual:   {actual_checksum}"
        )


def downloadAndVerify(url: str, download_path: Path, expected_checksum: str, proxies: Optional[Dict[str, str]]) -> None:
    """A helper to chain download and verification, with cleanup on failure."""
    try:
        downloadFile(url, download_path, proxies)
        verify_checksum(download_path, expected_checksum)
    except (RuntimeError, ValueError) as e:
        print(f"Error during download or verification: {e}", file=sys.stderr)
        # Clean up partially downloaded file on failure
        if download_path.exists():
            download_path.unlink()
        raise


def get_tool_executable_path(install_path: Path, tool: ToolName) -> Path:
    """Return the platform-specific path for a Wetlands-managed tool."""
    suffix = ".exe" if platform.system() == "Windows" else ""
    return install_path / "bin" / f"{tool}{suffix}"


def get_expected_executable_version(tool: ToolName, release_version: str) -> str:
    """Convert an upstream release tag to the version printed by its executable."""
    if tool == "pixi":
        return release_version.removeprefix("v")
    return release_version.split("-", 1)[0]


def _parse_tool_version_output(tool: ToolName, output: str) -> str | None:
    """Parse the supported forms of ``--version`` output for one tool."""
    patterns = {
        "pixi": r"(?:pixi(?:\s+version)?\s+)?v?([0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)",
        "micromamba": (
            r"(?:micromamba(?:\s+version)?\s+)?v?"
            r"([0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)"
        ),
    }
    matches = []
    for line in output.splitlines():
        match = re.fullmatch(rf"\s*{patterns[tool]}\s*", line, flags=re.IGNORECASE)
        if match:
            matches.append(match.group(1))
    return matches[0] if len(matches) == 1 else None


def detect_tool_version(executable_path: Path, tool: ToolName) -> str | None:
    """Return an executable's version, or ``None`` when it cannot be trusted."""
    try:
        result = subprocess.run(
            [str(executable_path), "--version"],
            check=False,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=TOOL_VERSION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return _parse_tool_version_output(tool, "\n".join((result.stdout, result.stderr)))


def _require_expected_executable_version(executable_path: Path, tool: ToolName, release_version: str) -> None:
    expected_version = get_expected_executable_version(tool, release_version)
    actual_version = detect_tool_version(executable_path, tool)
    if actual_version != expected_version:
        display_name = "Pixi" if tool == "pixi" else "Micromamba"
        raise RuntimeError(
            f"{display_name} {release_version} produced an unexpected executable version at "
            f"{executable_path}. Expected: {expected_version}. Detected: {actual_version or 'unavailable'}."
        )


def get_tool_release_marker_path(install_path: Path, tool: ToolName) -> Path:
    """Return the marker that records the exact installed upstream release."""
    return install_path / "bin" / f".wetlands-{tool}-version"


def _read_tool_release_marker(install_path: Path, tool: ToolName) -> str | None:
    try:
        return get_tool_release_marker_path(install_path, tool).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _write_tool_release_marker(install_path: Path, tool: ToolName, release_version: str) -> None:
    """Atomically record the exact release installed by Wetlands."""
    marker_path = get_tool_release_marker_path(install_path, tool)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{marker_path.name}.", dir=marker_path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as marker:
            marker.write(f"{release_version}\n")
            marker.flush()
            os.fsync(marker.fileno())
        os.replace(temporary_name, marker_path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(temporary_name)
        raise


@contextlib.contextmanager
def _installation_lock(install_path: Path) -> Iterator[None]:
    """Serialize version checks and installations for one managed tool root."""
    install_path.mkdir(parents=True, exist_ok=True)
    lock_path = install_path / INSTALL_LOCK_FILENAME
    with open(lock_path, "a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            if lock_file.tell() == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def ensure_conda_tool(install_path: Path, use_pixi: bool, proxies: Optional[Dict[str, str]] = None) -> Path:
    """Install or migrate the configured tool to Wetlands' trusted release."""
    tool: ToolName = "pixi" if use_pixi else "micromamba"
    display_name = "Pixi" if use_pixi else "Micromamba"
    release_version = PIXI_VERSION if use_pixi else MICROMAMBA_VERSION
    expected_version = get_expected_executable_version(tool, release_version)
    executable_path = get_tool_executable_path(install_path, tool)

    with _installation_lock(install_path):
        installed_version = detect_tool_version(executable_path, tool) if executable_path.is_file() else None
        installed_release = _read_tool_release_marker(install_path, tool)
        if installed_version == expected_version and installed_release == release_version:
            return executable_path

        if executable_path.exists():
            print(
                f"Migrating {display_name} at {executable_path}. "
                f"Installed version: {installed_version or 'unavailable'}; required release: {release_version}."
            )
        else:
            print(f"{display_name} is not installed at {executable_path}; installing {release_version}.")

        installer = installPixi if use_pixi else installMicromamba
        installed_path = installer(install_path, version=release_version, proxies=proxies)
        _require_expected_executable_version(installed_path, tool, release_version)
        _write_tool_release_marker(install_path, tool, release_version)
        return installed_path


# --- Micromamba ---


def get_micromamba_platform_info() -> Tuple[str, str]:
    """Determines the OS platform and architecture for micromamba URLs."""
    system = platform.system()
    arch = platform.machine().lower()

    system_map = {"Linux": "linux", "Darwin": "osx", "Windows": "win"}
    platform_os = system_map.get(system)
    if not platform_os:
        raise ValueError(f"Unsupported operating system: {system}")

    arch_map = {
        "aarch64": "aarch64",
        "ppc64le": "ppc64le",
        "arm64": "arm64",  # For macOS
        "x86_64": "64",
        "amd64": "64",
    }
    platform_arch = arch_map.get(arch)
    if (not platform_arch) or (platform_os == "win" and platform_arch != "64"):
        print(f"Warning: Detected architecture '{arch}', defaulting to '64'.")
        platform_arch = "64"

    # Validate the final combination
    valid_combinations = {"linux-aarch64", "linux-ppc64le", "linux-64", "osx-arm64", "osx-64", "win-64"}
    if f"{platform_os}-{platform_arch}" not in valid_combinations:
        raise ValueError(f"Unsupported OS-Architecture combination: {platform_os}-{platform_arch}")

    return platform_os, platform_arch


def get_micromamba_url(platform_os: str, platform_arch: str, version: str) -> Tuple[str, str]:
    """Constructs the micromamba download URL."""
    _require_registered_version("Micromamba", version, MICROMAMBA_VERSION)
    base_name = f"micromamba-{platform_os}-{platform_arch}"
    base_url = "https://github.com/mamba-org/micromamba-releases/releases"
    return f"{base_url}/download/{version}/{base_name}", base_name


def _require_registered_version(tool: str, requested_version: str, supported_version: str) -> None:
    if requested_version != supported_version:
        raise ValueError(
            f"No trusted checksums are registered for {tool} {requested_version}.\n"
            f"Supported version: {supported_version}."
        )


def _registered_checksum(tool: str, version: str, artifact_name: str, registry: Dict[str, str]) -> str:
    try:
        return registry[artifact_name]
    except KeyError as e:
        raise ValueError(
            f"No trusted checksum is registered for {tool} artifact {artifact_name} in version {version}."
        ) from e


def install_vc_redist_windows(proxies: Optional[Dict[str, str]]) -> None:
    """Downloads, verifies, and silently installs VC Redistributable on Windows."""
    print("\n--- Starting VC Redistributable Setup ---")

    with tempfile.TemporaryDirectory() as tmpDir:
        vc_redist_path = Path(tmpDir) / VC_REDIST_ARTIFACT_NAME

        downloadAndVerify(VC_REDIST_URL, vc_redist_path, VC_REDIST_SHA256, proxies)

        print(f"Installing {VC_REDIST_ARTIFACT_NAME}...")
        try:
            # Prepare the PowerShell command to launch the installer with -Wait
            ps_command = [
                "powershell",
                "-Command",
                f"Start-Process -FilePath {shell_quote(vc_redist_path)} -ArgumentList '/install','/passive','/norestart' -Wait -NoNewWindow",
            ]

            result = subprocess.run(
                ps_command,
                check=False,  # We check returncode manually for success codes
                capture_output=True,
                text=True,
            )

            # Successful exit codes for vc_redist are 0 (success) or 3010 (reboot required)
            if result.returncode in [0, 3010]:
                print(f"{VC_REDIST_ARTIFACT_NAME} installation successful. Code: {result.returncode}")
            else:
                raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
        except subprocess.CalledProcessError as e:
            error_message = (
                f"Error: {VC_REDIST_ARTIFACT_NAME} installation failed with code {e.returncode}.\n"
                f"  Stdout: {e.stdout}\n"
                f"  Stderr: {e.stderr}"
            )
            raise RuntimeError(error_message) from e


def create_mamba_config_file(mamba_path):
    """Create the default Mamba config without replacing an existing configuration."""
    config_path = mamba_path / ".mambarc"
    if config_path.exists():
        return
    with open(config_path, "w") as f:
        mamba_settings = dict(
            channel_priority="flexible",
            channels=["conda-forge", "nodefaults"],
            default_channels=["conda-forge"],
        )
        yaml.safe_dump(mamba_settings, f)


def installMicromamba(
    install_path: Path, version: str = MICROMAMBA_VERSION, proxies: Optional[Dict[str, str]] = None
) -> Path:
    """High-level function to orchestrate Micromamba installation."""
    _require_registered_version("Micromamba", version, MICROMAMBA_VERSION)
    currentOs, currentArch = get_micromamba_platform_info()
    micromambaBaseName = f"micromamba-{currentOs}-{currentArch}"
    expected_checksum = _registered_checksum("Micromamba", version, micromambaBaseName, MICROMAMBA_SHA256)

    print(f"\n--- Starting Micromamba Setup for {currentOs}-{currentArch} ---")
    micromambaUrl, micromambaBaseName = get_micromamba_url(currentOs, currentArch, version)
    print(f"Target Micromamba URL: {micromambaUrl}")

    micromamba_full_path = get_tool_executable_path(install_path, "micromamba")
    bin_dir = micromamba_full_path.parent
    bin_dir.mkdir(exist_ok=True, parents=True)

    try:
        with tempfile.TemporaryDirectory(prefix=".micromamba-install-", dir=bin_dir) as tmpDir:
            staged_path = Path(tmpDir) / micromamba_full_path.name
            downloadAndVerify(micromambaUrl, staged_path, expected_checksum, proxies)

            if currentOs != "win":
                staged_path.chmod(0o755)
            if currentOs == "win":
                install_vc_redist_windows(proxies)

            _require_expected_executable_version(staged_path, "micromamba", version)
            create_mamba_config_file(install_path)
            os.replace(staged_path, micromamba_full_path)
    except Exception as e:
        raise RuntimeError(
            f"Micromamba {version} installation failed; the existing executable was left unchanged."
        ) from e

    print(f"Micromamba successfully set up at {micromamba_full_path}")
    return micromamba_full_path


# --- Pixi ---


def get_pixi_target(architecture=None) -> str:
    """
    Determines the target triple for Pixi downloads.
    """
    platform_system = platform.system()
    platform_machine = platform.machine().lower()

    if architecture is None:
        architecture = "x86_64"
        if platform_machine in ("aarch64", "arm64"):
            architecture = "aarch64"

    platform_name = "unknown-linux-musl"
    archive_extension = ".tar.gz"
    if platform_system == "Windows":
        platform_name = "pc-windows-msvc"
        archive_extension = ".zip"
    elif platform_system == "Darwin":
        platform_name = "apple-darwin"

    return f"pixi-{architecture}-{platform_name}{archive_extension}"


def installPixi(install_path: Path, version: str = PIXI_VERSION, proxies: Optional[Dict[str, str]] = None) -> Path:
    """Downloads, verifies, and installs a specific version of Pixi."""
    _require_registered_version("Pixi", version, PIXI_VERSION)
    binary_filename = get_pixi_target()
    expected_checksum = _registered_checksum("Pixi", version, binary_filename, PIXI_SHA256)

    pixi_repo_url = "https://github.com/prefix-dev/pixi"
    download_url = f"{pixi_repo_url}/releases/download/{version}/{binary_filename}"

    bin_dir = install_path / "bin"

    print(f"Preparing to install Pixi ({version}, {binary_filename}).")
    print(f"  URL: {download_url}")
    print(f"  Destination: {bin_dir}")

    pixi_full_path = get_tool_executable_path(install_path, "pixi")
    bin_dir.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory(prefix=".pixi-install-", dir=bin_dir) as tmpDir:
            archive_path = Path(tmpDir) / binary_filename
            staged_path = Path(tmpDir) / pixi_full_path.name
            downloadAndVerify(download_url, archive_path, expected_checksum, proxies)

            if binary_filename.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_ref:
                    zip_members = [
                        member
                        for member in zip_ref.infolist()
                        if not member.is_dir() and Path(member.filename).name in {"pixi", "pixi.exe"}
                    ]
                    if len(zip_members) != 1:
                        raise RuntimeError(f"Expected exactly one Pixi executable in {binary_filename}.")
                    with zip_ref.open(zip_members[0]) as source, open(staged_path, "wb") as destination:
                        shutil.copyfileobj(source, destination)
            else:
                with tarfile.open(archive_path, "r:gz") as tar_ref:
                    tar_members = [
                        member
                        for member in tar_ref.getmembers()
                        if member.isfile() and Path(member.name).name in {"pixi", "pixi.exe"}
                    ]
                    if len(tar_members) != 1:
                        raise RuntimeError(f"Expected exactly one Pixi executable in {binary_filename}.")
                    tar_source = tar_ref.extractfile(tar_members[0])
                    if tar_source is None:
                        raise RuntimeError(f"Could not read the Pixi executable from {binary_filename}.")
                    with tar_source, open(staged_path, "wb") as destination:
                        shutil.copyfileobj(tar_source, destination)

            if platform.system() != "Windows":
                staged_path.chmod(0o755)
            _require_expected_executable_version(staged_path, "pixi", version)
            os.replace(staged_path, pixi_full_path)
    except Exception as e:
        raise RuntimeError(f"Pixi {version} installation failed; the existing executable was left unchanged.") from e

    print(f"Pixi installed successfully at {pixi_full_path}.")
    return pixi_full_path
