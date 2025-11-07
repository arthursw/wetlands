import re
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from wetlands._internal.command_generator import CommandGenerator

# mock_settings_manager and mock_dependency_manager is defined in conftest.py


@pytest.fixture
def command_generator_pixi(mock_settings_manager_pixi):
    return CommandGenerator(mock_settings_manager_pixi)


@pytest.fixture
def command_generator_micromamba(mock_settings_manager_micromamba):
    return CommandGenerator(mock_settings_manager_micromamba)


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


def test_mixed_dependencies_with_and_without_channels(command_generator_pixi):
    """Test a mix of dependencies, some with channels and some without."""
    environment = MagicMock()
    environment.name = "base"
    environment.path = Path("/tmp/base")
    dependencies = ["conda-forge::requests", "python", "nvidia::cuda-toolkit", "conda-forge::scipy"]
    # Channels are sorted alphabetically and unique
    expected_channels = "conda-forge", "nvidia", "bioconda"
    expected_command = rf'pixi project channel add --manifest-path ".*" --no-progress --prepend'
    commands = command_generator_pixi.getAddChannelsCommands(
        environment, ["bioconda"], dependencies, activateConda=True
    )
    assert re.search(expected_command, commands[-1])
    assert all(ec in commands[-1] for ec in expected_channels)


def test_mixed_dependencies_with_and_without_channels_micromamba(command_generator_micromamba):
    """Test a mix of dependencies, some with channels and some without."""
    environment = MagicMock()
    environment.name = "base"
    environment.path = Path("/tmp/base")
    dependencies = ["conda-forge::requests", "python", "nvidia::cuda-toolkit", "conda-forge::scipy"]
    # Only bioconda will be added since the others are handled by conda for each package
    expected_channels = "bioconda"
    expected_command = rf"micromamba --rc-file ~/.mambarc config --add channels"
    commands = command_generator_micromamba.getAddChannelsCommands(
        environment, ["bioconda"], dependencies, activateConda=True
    )
    assert re.search(expected_command, commands[-1])
    assert all(ec in commands[-1] for ec in expected_channels)
