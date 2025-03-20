import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from cema.command_generator import CommandGenerator
from cema.dependency_manager import DependencyManager
from cema.settings_manager import SettingsManager

@pytest.fixture
def mock_settings_manager():
    mock = MagicMock(spec=SettingsManager)
    mock.getCondaPaths.return_value = (Path("/mock/conda"), "micromamba")
    mock.getProxyEnvironmentVariablesCommands.return_value = []
    mock.getProxyString.return_value = None
    mock.condaBin = "micromamba"
    return mock

@pytest.fixture
def mock_dependency_manager():
    return MagicMock(spec=DependencyManager)

@pytest.fixture
def command_generator(mock_settings_manager, mock_dependency_manager):
    return CommandGenerator(mock_settings_manager, mock_dependency_manager)

@patch("platform.system", return_value="Windows")
def test_get_shell_hook_commands_windows(mock_platform, command_generator):
    expected = [
        'Set-Location -Path "/mock/conda"',
        '$Env:MAMBA_ROOT_PREFIX="/mock/conda"',
        '.\micromamba shell hook -s powershell | Out-String | Invoke-Expression',
        'Set-Location -Path "' + str(Path.cwd().resolve()) + '"',
    ]
    assert command_generator.getShellHookCommands() == expected

@patch("platform.system", return_value="Linux")
def test_get_shell_hook_commands_linux(mock_platform, command_generator):
    expected = [
        'cd "/mock/conda"',
        'export MAMBA_ROOT_PREFIX="/mock/conda"',
        'eval "$(micromamba shell hook -s posix)"',
        'cd "' + str(Path.cwd().resolve()) + '"',
    ]
    assert command_generator.getShellHookCommands() == expected

@patch("pathlib.Path.exists", return_value=True)
def test_get_install_conda_commands_exists(mock_exists, command_generator):
    assert command_generator.getInstallCondaCommands() == []

@patch("platform.system", return_value="Windows")
def test_get_install_conda_commands_windows(mock_platform, command_generator, mock_settings_manager):
    mock_settings_manager.getProxyString.return_value = "http://user:pass@proxyserver"
    expected_proxy_commands = [
        '$proxyUsername = "user"',
        '$proxyPassword = "pass"',
        "$securePassword = ConvertTo-SecureString $proxyPassword -AsPlainText -Force",
        "$proxyCredentials = New-Object System.Management.Automation.PSCredential($proxyUsername, $securePassword)",
    ]
    commands = command_generator.getInstallCondaCommands()
    assert any(cmd in commands for cmd in expected_proxy_commands)
    assert any("Invoke-WebRequest" in cmd for cmd in commands)

@patch("platform.system", return_value="Linux")
def test_get_install_conda_commands_linux(mock_platform, command_generator):
    commands = command_generator.getInstallCondaCommands()
    assert any("curl -Ls" in cmd for cmd in commands)
    assert any("tar -xvj" in cmd for cmd in commands)

@patch("platform.system", return_value="Darwin")
def test_get_platform_common_name_mac(mock_platform, command_generator):
    assert command_generator.getPlatformCommonName() == "mac"

@patch("platform.system", return_value="Linux")
def test_get_platform_common_name_linux(mock_platform, command_generator):
    assert command_generator.getPlatformCommonName() == "linux"

@patch("platform.system", return_value="Windows")
def test_get_platform_common_name_windows(mock_platform, command_generator):
    assert command_generator.getPlatformCommonName() == "windows"

@pytest.mark.parametrize("additional_commands, expected", [
    ({"all": ["common_cmd"], "linux": ["linux_cmd"]}, ["common_cmd", "linux_cmd"]),
    ({"windows": ["win_cmd"]}, ["win_cmd"]),
    ({"mac": ["mac_cmd"]}, ["mac_cmd"]),
    ({}, []),
])
@patch("platform.system", return_value="Linux")
def test_get_commands_for_current_platform(mock_platform, command_generator, additional_commands, expected):
    assert command_generator.getCommandsForCurrentPlatform(additional_commands) == expected
