import hashlib
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import yaml

# --- Configuration ---
PIXI_VERSION = "v0.48.2"
MICROMAMBA_VERSION = "2.3.0-1"

VC_REDIST_ARTIFACT_NAME = "VC_redist.x64.exe"
VC_REDIST_URL_DEFAULT = f"https://download.visualstudio.microsoft.com/download/pr/7ebf5fdb-36dc-4145-b0a0-90d3d5990a61/CC0FF0EB1DC3F5188AE6300FAEF32BF5BEEBA4BDD6E8E445A9184072096B713B/{VC_REDIST_ARTIFACT_NAME}"

SCRIPT_DIR = Path(__file__).parent.resolve()
CHECKSUMS_BASE_DIR = SCRIPT_DIR / "checksums"

VC_REDIST_CHECKSUM_PATH = CHECKSUMS_BASE_DIR / "vc_redist.x64.exe.sha256"

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


def verify_checksum(file_path: Path, checksum_file_path: Path) -> None:
    """Verifies the SHA256 checksum of a file against an expected value from a file."""
    print(f"Verifying checksum for {file_path.name} using {checksum_file_path}...")

    try:
        with open(checksum_file_path, "r") as f:
            expected_checksum = f.read().strip().split()[0].lower()
    except (FileNotFoundError, IndexError) as e:
        raise ValueError(f"Could not read expected checksum from {checksum_file_path}") from e

    actual_checksum = calculate_sha256(file_path)

    if actual_checksum == expected_checksum:
        print(f"Checksum OK for {file_path.name}.")
    else:
        raise ValueError(
            f"Checksum MISMATCH for {file_path.name}!\n  Expected: {expected_checksum}\n  Actual:   {actual_checksum}"
        )


def downloadAndVerify(url: str, download_path: Path, checksum_path: Path, proxies: Optional[Dict[str, str]]) -> None:
    """A helper to chain download and verification, with cleanup on failure."""
    try:
        downloadFile(url, download_path, proxies)
        verify_checksum(download_path, checksum_path)
    except (RuntimeError, ValueError) as e:
        print(f"Error during download or verification: {e}", file=sys.stderr)
        # Clean up partially downloaded file on failure
        if download_path.exists():
            download_path.unlink()
        raise


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
    base_name = f"micromamba-{platform_os}-{platform_arch}"
    base_url = "https://github.com/mamba-org/micromamba-releases/releases"

    if version:
        return f"{base_url}/download/{version}/{base_name}", base_name
    return f"{base_url}/latest/download/{base_name}", base_name


def install_vc_redist_windows(proxies: Optional[Dict[str, str]]) -> None:
    """Downloads, verifies, and silently installs VC Redistributable on Windows."""
    print("\n--- Starting VC Redistributable Setup ---")

    with tempfile.TemporaryDirectory() as tmpDir:
        vc_redist_path = Path(tmpDir) / VC_REDIST_ARTIFACT_NAME

        downloadAndVerify(VC_REDIST_URL_DEFAULT, vc_redist_path, VC_REDIST_CHECKSUM_PATH, proxies)

        print(f"Installing {VC_REDIST_ARTIFACT_NAME}...")
        try:
            # Prepare the PowerShell command to launch the installer with -Wait
            ps_command = [
                "powershell",
                "-Command",
                f"Start-Process -FilePath '{vc_redist_path}' -ArgumentList '/install','/passive','/norestart' -Wait -NoNewWindow",
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
    """Create Mamba config file .mambarc in conda_path, with nodefaults and conda-forge channels."""
    with open(mamba_path / ".mambarc", "w") as f:
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
    currentOs, currentArch = get_micromamba_platform_info()

    if currentOs == "win":
        install_vc_redist_windows(proxies)

    print(f"\n--- Starting Micromamba Setup for {currentOs}-{currentArch} ---")
    micromambaUrl, micromambaBaseName = get_micromamba_url(currentOs, currentArch, version)
    print(f"Target Micromamba URL: {micromambaUrl}")

    suffix = ".exe" if currentOs == "win" else ""
    micromamba_full_path = install_path / "bin" / f"micromamba{suffix}"
    micromamba_full_path.parent.mkdir(exist_ok=True, parents=True)

    # Use the combined helper to download and verify
    downloadAndVerify(micromambaUrl, micromamba_full_path, CHECKSUMS_BASE_DIR / f"{micromambaBaseName}.sha256", proxies)

    # Ensure the file is executable and properly named on Windows
    if currentOs == "win":
        # On Windows, verify the file exists and has the correct extension
        if not micromamba_full_path.exists():
            raise Exception(f"Micromamba executable not found at {micromamba_full_path}")
        # Make sure it's readable and not locked
        try:
            micromamba_full_path.stat()
        except Exception as e:
            raise Exception(f"Failed to access micromamba executable at {micromamba_full_path}: {e}") from e
    else:
        micromamba_full_path.chmod(0o755)  # rwxr-xr-x
        print(f"Made {micromamba_full_path} executable.")

    print(f"Micromamba successfully set up at {micromamba_full_path}")

    create_mamba_config_file(install_path)
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

    binary_filename = get_pixi_target()

    pixi_repo_url = "https://github.com/prefix-dev/pixi"

    if version == "latest":
        download_url = f"{pixi_repo_url}/releases/latest/download/{binary_filename}"
    else:
        download_url = f"{pixi_repo_url}/releases/download/{version}/{binary_filename}"

    bin_dir = install_path / "bin"

    print(f"Preparing to install Pixi ({version}, {binary_filename}).")
    print(f"  URL: {download_url}")
    print(f"  Destination: {bin_dir}")

    checksum_path = CHECKSUMS_BASE_DIR / f"{binary_filename}.sha256"
    if not checksum_path.exists():
        raise Exception(f"Error: Checksum file not found at {checksum_path}")

    try:
        with tempfile.TemporaryDirectory() as tmpDir:
            archive_path = Path(tmpDir) / binary_filename
            downloadAndVerify(download_url, archive_path, checksum_path, proxies)

            print(f"Extracting {archive_path.name} to {bin_dir}...")
            bin_dir.mkdir(parents=True, exist_ok=True)

            if binary_filename.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_ref:
                    zip_ref.extractall(bin_dir)
            else:  # .tar.gz
                with tarfile.open(archive_path, "r:gz") as tar_ref:
                    if sys.version_info >= (3, 12):
                        tar_ref.extractall(bin_dir, filter="data")
                    else:
                        # Emulate 'filter="data"' for 3.10–3.11
                        for member in tar_ref.getmembers():
                            if member.isfile():  # Only extract files, not symlinks/devices/etc
                                tar_ref.extract(member, path=bin_dir)

            print("Pixi installed successfully.")

    except (RuntimeError, ValueError, FileNotFoundError) as e:
        raise Exception("Pixi installation failed") from e

    # Find the actual executable - it may be named 'pixi' or 'pixi.exe' depending on the zip contents
    # and the platform
    is_windows = platform.system() == "Windows"

    # On Windows, the executable might be named just 'pixi' in the zip, so we need to rename it to 'pixi.exe'
    # to ensure it can be executed properly
    pixi_without_ext = bin_dir / "pixi"
    pixi_with_ext = bin_dir / "pixi.exe"

    if pixi_without_ext.is_file():
        if is_windows:
            # Rename to add .exe extension if it doesn't have one
            if not pixi_with_ext.exists():
                pixi_without_ext.rename(pixi_with_ext)
            else:
                pixi_without_ext.unlink()  # Remove the non-.exe version
            return pixi_with_ext
        else:
            pixi_without_ext.chmod(0o755)  # Make executable on Unix-like systems
            return pixi_without_ext

    if pixi_with_ext.is_file():
        return pixi_with_ext

    raise Exception(f"Pixi executable not found. Checked locations: {pixi_without_ext}, {pixi_with_ext}")


# --- Main Execution ---


def main():
    """
    Main function to demonstrate script usage.
    """
    # Example: Install Micromamba
    micromamba_install_dir = SCRIPT_DIR / "micromamba_install"
    print(f"--- Example: Installing Micromamba to {micromamba_install_dir} ---")
    try:
        installMicromamba(micromamba_install_dir)
    except (RuntimeError, ValueError, FileNotFoundError) as e:
        print(f"\nFATAL ERROR during Micromamba setup: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n" + "=" * 50 + "\n")

    # Example: Install Pixi
    pixi_install_dir = SCRIPT_DIR / "pixi_install"
    print(f"--- Example: Installing Pixi to {pixi_install_dir} ---")
    try:
        installPixi(pixi_install_dir, version="0.21.0")  # Use a specific version
    except (RuntimeError, ValueError, FileNotFoundError) as e:
        print(f"\nFATAL ERROR during Pixi setup: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
