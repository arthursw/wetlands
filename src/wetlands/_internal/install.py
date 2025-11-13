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


def downloadFile(url: str, destPath: Path, proxies: Optional[Dict[str, str]] = None) -> None:
    """
    Downloads a file from a URL to a destination path using urllib.

    Note: For more complex scenarios, consider using the 'requests' library.
    """
    print(f"Downloading {url} to {destPath}...")
    destPath.parent.mkdir(parents=True, exist_ok=True)

    proxyHandler = urllib.request.ProxyHandler(proxies)
    opener = urllib.request.build_opener(proxyHandler)
    urllib.request.install_opener(opener)

    try:
        with urllib.request.urlopen(url, timeout=120) as response, open(destPath, "wb") as outFile:
            shutil.copyfileobj(response, outFile)
        print(f"Successfully downloaded {destPath.name}.")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download {url}. Reason: {e.reason}") from e


def calculateSha256(filePath: Path) -> str:
    """Calculates the SHA256 checksum of a file."""
    sha256Hash = hashlib.sha256()
    try:
        with open(filePath, "rb") as f:
            # Read in chunks to handle large files efficiently.
            for byteBlock in iter(lambda: f.read(4096), b""):
                sha256Hash.update(byteBlock)
        return sha256Hash.hexdigest()
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Cannot calculate checksum, file not found: {filePath}") from e


def verifyChecksum(filePath: Path, checksumFilePath: Path) -> None:
    """Verifies the SHA256 checksum of a file against an expected value from a file."""
    print(f"Verifying checksum for {filePath.name} using {checksumFilePath}...")

    try:
        with open(checksumFilePath, "r") as f:
            expectedChecksum = f.read().strip().split()[0].lower()
    except (FileNotFoundError, IndexError) as e:
        raise ValueError(f"Could not read expected checksum from {checksumFilePath}") from e

    actualChecksum = calculateSha256(filePath)

    if actualChecksum == expectedChecksum:
        print(f"Checksum OK for {filePath.name}.")
    else:
        raise ValueError(
            f"Checksum MISMATCH for {filePath.name}!\n  Expected: {expectedChecksum}\n  Actual:   {actualChecksum}"
        )


def downloadAndVerify(url: str, downloadPath: Path, checksumPath: Path, proxies: Optional[Dict[str, str]]) -> None:
    """A helper to chain download and verification, with cleanup on failure."""
    try:
        downloadFile(url, downloadPath, proxies)
        verifyChecksum(downloadPath, checksumPath)
    except (RuntimeError, ValueError) as e:
        print(f"Error during download or verification: {e}", file=sys.stderr)
        # Clean up partially downloaded file on failure
        if downloadPath.exists():
            downloadPath.unlink()
        raise


# --- Micromamba ---


def getMicromambaPlatformInfo() -> Tuple[str, str]:
    """Determines the OS platform and architecture for micromamba URLs."""
    system = platform.system()
    arch = platform.machine().lower()

    systemMap = {"Linux": "linux", "Darwin": "osx", "Windows": "win"}
    platformOs = systemMap.get(system)
    if not platformOs:
        raise ValueError(f"Unsupported operating system: {system}")

    archMap = {
        "aarch64": "aarch64",
        "ppc64le": "ppc64le",
        "arm64": "arm64",  # For macOS
        "x86_64": "64",
        "amd64": "64",
    }
    platformArch = archMap.get(arch)
    if (not platformArch) or (platformOs == "win" and platformArch != "64"):
        print(f"Warning: Detected architecture '{arch}', defaulting to '64'.")
        platformArch = "64"

    # Validate the final combination
    validCombinations = {"linux-aarch64", "linux-ppc64le", "linux-64", "osx-arm64", "osx-64", "win-64"}
    if f"{platformOs}-{platformArch}" not in validCombinations:
        raise ValueError(f"Unsupported OS-Architecture combination: {platformOs}-{platformArch}")

    return platformOs, platformArch


def getMicromambaUrl(platformOs: str, platformArch: str, version: str) -> Tuple[str, str]:
    """Constructs the micromamba download URL."""
    baseName = f"micromamba-{platformOs}-{platformArch}"
    baseUrl = "https://github.com/mamba-org/micromamba-releases/releases"

    if version:
        return f"{baseUrl}/download/{version}/{baseName}", baseName
    return f"{baseUrl}/latest/download/{baseName}", baseName


def installVcRedistWindows(proxies: Optional[Dict[str, str]]) -> None:
    """Downloads, verifies, and silently installs VC Redistributable on Windows."""
    print("\n--- Starting VC Redistributable Setup ---")

    with tempfile.TemporaryDirectory() as tmpDir:
        vcRedistPath = Path(tmpDir) / VC_REDIST_ARTIFACT_NAME

        downloadAndVerify(VC_REDIST_URL_DEFAULT, vcRedistPath, VC_REDIST_CHECKSUM_PATH, proxies)

        print(f"Installing {VC_REDIST_ARTIFACT_NAME}...")
        try:
            # Prepare the PowerShell command to launch the installer with -Wait
            ps_command = [
                "powershell",
                "-Command",
                f"Start-Process -FilePath '{vcRedistPath}' -ArgumentList '/install','/passive','/norestart' -Wait -NoNewWindow",
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


def createMambaConfigFile(mambaPath):
    """Create Mamba config file .mambarc in condaPath, with nodefaults and conda-forge channels."""
    with open(mambaPath / ".mambarc", "w") as f:
        mambaSettings = dict(
            channel_priority="flexible",
            channels=["conda-forge", "nodefaults"],
            default_channels=["conda-forge"],
        )
        yaml.safe_dump(mambaSettings, f)


def installMicromamba(
    installPath: Path, version: str = MICROMAMBA_VERSION, proxies: Optional[Dict[str, str]] = None
) -> Path:
    """High-level function to orchestrate Micromamba installation."""
    currentOs, currentArch = getMicromambaPlatformInfo()

    if currentOs == "win":
        installVcRedistWindows(proxies)

    print(f"\n--- Starting Micromamba Setup for {currentOs}-{currentArch} ---")
    micromambaUrl, micromambaBaseName = getMicromambaUrl(currentOs, currentArch, version)
    print(f"Target Micromamba URL: {micromambaUrl}")

    suffix = ".exe" if currentOs == "win" else ""
    micromambaFullPath = installPath / "bin" / f"micromamba{suffix}"
    micromambaFullPath.parent.mkdir(exist_ok=True, parents=True)

    # Use the combined helper to download and verify
    downloadAndVerify(micromambaUrl, micromambaFullPath, CHECKSUMS_BASE_DIR / f"{micromambaBaseName}.sha256", proxies)

    # Ensure the file is executable and properly named on Windows
    if currentOs == "win":
        # On Windows, verify the file exists and has the correct extension
        if not micromambaFullPath.exists():
            raise Exception(f"Micromamba executable not found at {micromambaFullPath}")
        # Make sure it's readable and not locked
        try:
            micromambaFullPath.stat()
        except Exception as e:
            raise Exception(f"Failed to access micromamba executable at {micromambaFullPath}: {e}") from e
    else:
        micromambaFullPath.chmod(0o755)  # rwxr-xr-x
        print(f"Made {micromambaFullPath} executable.")

    print(f"Micromamba successfully set up at {micromambaFullPath}")

    createMambaConfigFile(installPath)
    return micromambaFullPath


# --- Pixi ---


def getPixiTarget(architecture=None) -> str:
    """
    Determines the target triple for Pixi downloads.
    """
    platformSystem = platform.system()
    platformMachine = platform.machine().lower()

    if architecture is None:
        architecture = "x86_64"
        if platformMachine in ("aarch64", "arm64"):
            architecture = "aarch64"

    platformName = "unknown-linux-musl"
    archiveExtension = ".tar.gz"
    if platformSystem == "Windows":
        platformName = "pc-windows-msvc"
        archiveExtension = ".zip"
    elif platformSystem == "Darwin":
        platformName = "apple-darwin"

    return f"pixi-{architecture}-{platformName}{archiveExtension}"


def installPixi(installPath: Path, version: str = PIXI_VERSION, proxies: Optional[Dict[str, str]] = None) -> Path:
    """Downloads, verifies, and installs a specific version of Pixi."""

    binaryFilename = getPixiTarget()

    pixiRepoUrl = "https://github.com/prefix-dev/pixi"

    if version == "latest":
        downloadUrl = f"{pixiRepoUrl}/releases/latest/download/{binaryFilename}"
    else:
        downloadUrl = f"{pixiRepoUrl}/releases/download/{version}/{binaryFilename}"

    binDir = installPath / "bin"

    print(f"Preparing to install Pixi ({version}, {binaryFilename}).")
    print(f"  URL: {downloadUrl}")
    print(f"  Destination: {binDir}")

    checksumPath = CHECKSUMS_BASE_DIR / f"{binaryFilename}.sha256"
    if not checksumPath.exists():
        raise Exception(f"Error: Checksum file not found at {checksumPath}")

    try:
        with tempfile.TemporaryDirectory() as tmpDir:
            archive_path = Path(tmpDir) / binaryFilename
            downloadAndVerify(downloadUrl, archive_path, checksumPath, proxies)

            print(f"Extracting {archive_path.name} to {binDir}...")
            binDir.mkdir(parents=True, exist_ok=True)

            if binaryFilename.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_ref:
                    zip_ref.extractall(binDir)
            else:  # .tar.gz
                with tarfile.open(archive_path, "r:gz") as tar_ref:
                    if sys.version_info >= (3, 12):
                        tar_ref.extractall(binDir, filter="data")
                    else:
                        # Emulate 'filter="data"' for 3.10–3.11
                        for member in tar_ref.getmembers():
                            if member.isfile():  # Only extract files, not symlinks/devices/etc
                                tar_ref.extract(member, path=binDir)

            print("Pixi installed successfully.")

    except (RuntimeError, ValueError, FileNotFoundError) as e:
        raise Exception("Pixi installation failed") from e

    # Find the actual executable - it may be named 'pixi' or 'pixi.exe' depending on the zip contents
    # and the platform
    is_windows = platform.system() == "Windows"

    # On Windows, the executable might be named just 'pixi' in the zip, so we need to rename it to 'pixi.exe'
    # to ensure it can be executed properly
    pixi_without_ext = binDir / "pixi"
    pixi_with_ext = binDir / "pixi.exe"

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
    micromambaInstallDir = SCRIPT_DIR / "micromamba_install"
    print(f"--- Example: Installing Micromamba to {micromambaInstallDir} ---")
    try:
        installMicromamba(micromambaInstallDir)
    except (RuntimeError, ValueError, FileNotFoundError) as e:
        print(f"\nFATAL ERROR during Micromamba setup: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n" + "=" * 50 + "\n")

    # Example: Install Pixi
    pixiInstallDir = SCRIPT_DIR / "pixi_install"
    print(f"--- Example: Installing Pixi to {pixiInstallDir} ---")
    try:
        installPixi(pixiInstallDir, version="0.21.0")  # Use a specific version
    except (RuntimeError, ValueError, FileNotFoundError) as e:
        print(f"\nFATAL ERROR during Pixi setup: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
