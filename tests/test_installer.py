import platform
import subprocess
from pathlib import Path

import pytest

from wetlands._internal import install
from wetlands._internal.artifact_registry import MICROMAMBA_VERSION, PIXI_VERSION
from wetlands._internal.install import ensure_conda_tool, installMicromamba, installPixi


pytestmark = [pytest.mark.integration, pytest.mark.manual, pytest.mark.slow]


def test_install_micromamba(tmp_path: Path):
    print(f"--- Running Micromamba install test in temporary directory: {tmp_path} ---")

    # 1. ARRANGE: The tmp_path fixture handles setup. The installation
    # directory is ready and isolated.
    install_root_dir = tmp_path
    version = MICROMAMBA_VERSION

    # 2. ACT: Call the function to be tested.
    # Any exception here will automatically fail the test.
    executable_path = installMicromamba(install_root_dir, version)

    # 3. ASSERT: Verify the results of the action.

    # Assert that the function returned a valid path and the file exists.
    assert executable_path is not None, "installMicromamba should return the path to the executable"
    assert executable_path.is_file(), f"Executable file should exist at {executable_path}"

    # Assert that the installed file is executable by running it.
    # The subprocess.run() will raise CalledProcessError if the command
    # returns a non-zero exit code, which pytest will catch as a test failure.
    print(f"Verifying by running '{executable_path} --version'")
    result = subprocess.run(
        [str(executable_path), "--version"],
        capture_output=True,
        text=True,
        check=True,  # Fails the test if the command fails
    )

    # Assert that the command's output is what we expect.
    stdout = result.stdout.strip().lower()
    version_number = version.split("-")[0]
    assert version_number in stdout, f"The output of '--version' should contain {version_number}"

    print(f"--- Test successful. Micromamba version output: {result.stdout.strip()} ---")


def test_install_pixi(tmp_path: Path):
    print(f"--- Running Pixi install test in temporary directory: {tmp_path} ---")

    # 1. ARRANGE: The tmp_path fixture handles setup. The installation
    # directory is ready and isolated.
    install_root_dir = tmp_path
    version = PIXI_VERSION

    # 2. ACT: Call the function to be tested.
    # Any exception here will automatically fail the test.
    executable_path = installPixi(install_root_dir, version)

    # 3. ASSERT: Verify the results of the action.

    # Assert that the function returned a valid path and the file exists.
    assert executable_path is not None, "installPixi should return the path to the executable"
    assert executable_path.is_file(), f"Executable file should exist at {executable_path}"

    # Assert that the installed file is executable by running it.
    # The subprocess.run() will raise CalledProcessError if the command
    # returns a non-zero exit code, which pytest will catch as a test failure.
    print(f"Verifying by running '{executable_path} --version'")
    result = subprocess.run(
        [str(executable_path), "--version"],
        capture_output=True,
        text=True,
        check=True,  # Fails the test if the command fails
    )

    # Assert that the command's output is what we expect.
    stdout = result.stdout.strip().lower()
    version_number = version[1:]
    assert version_number in stdout, f"The output of '--version' should contain {version_number}"

    print(f"--- Test successful. Micromamba version output: {result.stdout.strip()} ---")


@pytest.mark.skipif(platform.system() == "Windows", reason="The legacy executable fixture is a POSIX shell script.")
def test_existing_pixi_is_migrated_once(tmp_path: Path, monkeypatch):
    executable = install.get_tool_executable_path(tmp_path, "pixi")
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\necho 'pixi 0.1.0'\n", encoding="utf-8")
    executable.chmod(0o755)

    migrated = ensure_conda_tool(tmp_path, use_pixi=True)

    assert install.detect_tool_version(migrated, "pixi") == PIXI_VERSION.removeprefix("v")
    assert install.get_tool_release_marker_path(tmp_path, "pixi").read_text(encoding="utf-8").strip() == PIXI_VERSION

    monkeypatch.setattr(install, "installPixi", pytest.fail)
    assert ensure_conda_tool(tmp_path, use_pixi=True) == migrated
