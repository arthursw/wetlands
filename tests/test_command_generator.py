import re
from unittest.mock import patch
import pytest

from wetlands._internal.command_generator import CommandGenerator

# mock_settings_manager and mock_dependency_manager is defined in conftest.py

@pytest.fixture
def command_generator_pixi(mock_settings_manager_pixi):
    return CommandGenerator(mock_settings_manager_pixi)


@pytest.fixture
def command_generator_micromamba(mock_settings_manager_micromamba):
    return CommandGenerator(mock_settings_manager_micromamba)


@patch("pathlib.Path.exists", return_value=True)
def test_get_install_conda_commands_exists(mock_exists, command_generator_pixi):
    assert command_generator_pixi.getInstallCondaCommands() == []


@patch("platform.system", return_value="Windows")
def test_get_install_conda_commands_windows_pixi(mock_platform, command_generator_pixi):
    commands = command_generator_pixi.getInstallCondaCommands()
    condaPath, condaBinPath = command_generator_pixi.settingsManager.getCondaPaths()
    assert any(re.match(r" *Invoke-Webrequest.*-URI.*pixi", cmd) for cmd in commands)

@patch("platform.system", return_value="Windows")
def test_get_install_conda_commands_windows_micromamba(mock_platform, command_generator_micromamba):
    commands = command_generator_micromamba.getInstallCondaCommands()
    condaPath, condaBinPath = command_generator_micromamba.settingsManager.getCondaPaths()
    assert any(re.match(r" *Invoke-Webrequest.*-URI.*micromamba", cmd) for cmd in commands)

@patch("platform.system", return_value="Linux")
def test_get_install_conda_commands_linux_pixi(mock_platform, command_generator_pixi):
    commands = command_generator_pixi.getInstallCondaCommands()
    condaPath, condaBinPath = command_generator_pixi.settingsManager.getCondaPaths()
    assert any(re.match(r"curl.*pixi", cmd) for cmd in commands)

@patch("platform.system", return_value="Linux")
def test_get_install_conda_commands_linux_micromamba(mock_platform, command_generator_micromamba):
    command_generator_micromamba.settingsManager.setCondaPath('micromamba', False)
    commands = command_generator_micromamba.getInstallCondaCommands()
    condaPath, condaBinPath = command_generator_micromamba.settingsManager.getCondaPaths()
    assert any(re.match(r"curl.*micromamba", cmd) for cmd in commands)


@patch("platform.system", return_value="Darwin")
def test_get_platform_common_name_mac(mock_platform, command_generator_pixi):
    assert command_generator_pixi.getPlatformCommonName() == "mac"


@patch("platform.system", return_value="Linux")
def test_get_platform_common_name_linux(mock_platform, command_generator_pixi):
    assert command_generator_pixi.getPlatformCommonName() == "linux"


@patch("platform.system", return_value="Windows")
def test_get_platform_common_name_windows(mock_platform, command_generator_pixi):
    assert command_generator_pixi.getPlatformCommonName() == "windows"


@pytest.mark.parametrize(
    "additional_commands, expected",
    [
        (
            {"all": ["common_cmd"], "linux": ["linux_cmd"], "windows": ["win_cmd"]},
            ["common_cmd", "linux_cmd"],
        ),
        ({"windows": ["win_cmd"]}, []),
        ({"linuxisnotlinux": ["linux_cmd"]}, []),
        ({"linux": ["linux_cmd"]}, ["linux_cmd"]),
        ({}, []),
    ],
)
@patch("platform.system", return_value="Linux")
def test_get_commands_for_current_platform(mock_platform, command_generator_pixi, additional_commands, expected):
    assert command_generator_pixi.getCommandsForCurrentPlatform(additional_commands) == expected
