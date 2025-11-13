import subprocess
from pathlib import Path

import pytest

from wetlands._internal.install import installMicromamba, installPixi


@pytest.mark.integration
def test_install_micromamba(tmp_path: Path):
    print(f"--- Running Micromamba install test in temporary directory: {tmp_path} ---")

    # 1. ARRANGE: The tmp_path fixture handles setup. The installation
    # directory is ready and isolated.
    install_root_dir = tmp_path
    version = "2.3.0-1"

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
    versionNumber = version.split("-")[0]
    assert versionNumber in stdout, f"The output of '--version' should contain {versionNumber}"

    print(f"--- Test successful. Micromamba version output: {result.stdout.strip()} ---")


@pytest.mark.integration
def test_install_pixi(tmp_path: Path):
    print(f"--- Running Pixi install test in temporary directory: {tmp_path} ---")

    # 1. ARRANGE: The tmp_path fixture handles setup. The installation
    # directory is ready and isolated.
    install_root_dir = tmp_path
    version = "v0.48.2"

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
    versionNumber = version[1:]
    assert versionNumber in stdout, f"The output of '--version' should contain {versionNumber}"

    print(f"--- Test successful. Micromamba version output: {result.stdout.strip()} ---")
