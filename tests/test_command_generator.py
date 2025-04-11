import re
from unittest.mock import patch
import pytest
from wetlands._internal.command_generator import CommandGenerator

# mock_settings_manager and mock_dependency_manager is defined in conftest.py


@pytest.fixture
def command_generator(mock_settings_manager, mock_dependency_manager):
    return CommandGenerator(mock_settings_manager, mock_dependency_manager)


@patch("pathlib.Path.exists", return_value=True)
def test_get_install_conda_commands_exists(mock_exists, command_generator):
    assert command_generator.getInstallCondaCommands() == []


@patch("platform.system", return_value="Windows")
def test_get_install_conda_commands_windows(mock_platform, command_generator, mock_settings_manager):
    commands = command_generator.getInstallCondaCommands()
    condaPath, condaBinPath = mock_settings_manager.getCondaPaths()
    assert any(re.match(r"Invoke-Webrequest.*-URI.*micromamba", cmd) for cmd in commands)
    assert any(re.match(rf'\$Env\:MAMBA_ROOT_PREFIX\="{condaPath}"', cmd) for cmd in commands)
    assert any(
        re.match(
            rf"\.\\{condaBinPath} shell hook -s powershell \| Out-String \| Invoke-Expression",
            cmd,
        )
        for cmd in commands
    )


@patch("platform.system", return_value="Linux")
def test_get_install_conda_commands_linux(mock_platform, command_generator, mock_settings_manager):
    commands = command_generator.getInstallCondaCommands()
    condaPath, condaBinPath = mock_settings_manager.getCondaPaths()
    assert any(re.match(r"curl.* -Ls.*micromamba", cmd) for cmd in commands)
    assert any(re.match(rf'export MAMBA_ROOT_PREFIX="{condaPath}"', cmd) for cmd in commands)
    assert any(re.match(rf'eval "\$\({condaBinPath} shell hook -s posix\)"', cmd) for cmd in commands)


@patch("platform.system", return_value="Darwin")
def test_get_platform_common_name_mac(mock_platform, command_generator):
    assert command_generator.getPlatformCommonName() == "mac"


@patch("platform.system", return_value="Linux")
def test_get_platform_common_name_linux(mock_platform, command_generator):
    assert command_generator.getPlatformCommonName() == "linux"


@patch("platform.system", return_value="Windows")
def test_get_platform_common_name_windows(mock_platform, command_generator):
    assert command_generator.getPlatformCommonName() == "windows"


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
def test_get_commands_for_current_platform(mock_platform, command_generator, additional_commands, expected):
    assert command_generator.getCommandsForCurrentPlatform(additional_commands) == expected
