from pathlib import Path
import platform
import re

try:
    from typing import NotRequired, TypedDict  # type: ignore
except ImportError:
    from typing_extensions import NotRequired, TypedDict  # type: ignore

from typing import Union

import yaml

from wetlands._internal.settings_manager import SettingsManager


class CommandsDict(TypedDict):
    all: NotRequired[list[str]]
    linux: NotRequired[list[str]]
    mac: NotRequired[list[str]]
    windows: NotRequired[list[str]]


Commands = Union[CommandsDict, list[str]]


class CommandGenerator:
    """Generate Conda commands."""

    def __init__(self, settingsManager: SettingsManager):
        self.settingsManager = settingsManager

    def getShellHookCommands(self) -> list[str]:
        """Generates shell commands for Conda initialization. Only relevant when using micromamba.

        Returns:
                OS-specific commands to activate Conda shell hooks.
        """
        if self.settingsManager.usePixi: return []
        condaPath, condaBinPath = self.settingsManager.getCondaPaths()
        if platform.system() == "Windows":
            return [
                f'$Env:MAMBA_ROOT_PREFIX="{condaPath}"',
                f".\\{condaBinPath} shell hook -s powershell | Out-String | Invoke-Expression",
            ]
        else:
            return [
                f'export MAMBA_ROOT_PREFIX="{condaPath}"',
                f'eval "$({condaBinPath} shell hook -s posix)"',
            ]
    

    def createMambaConfigFile(self, condaPath):
        """Create Mamba config file .mambarc in condaPath, with nodefaults and conda-forge channels."""
        if self.settingsManager.usePixi: return
        with open(condaPath / ".mambarc", "w") as f:
            mambaSettings = dict(
                channel_priority="flexible",
                channels=["conda-forge", "nodefaults"],
                default_channels=["conda-forge"],
            )
            yaml.safe_dump(mambaSettings, f)

    def getPlatformCommonName(self) -> str:
        """Gets common platform name (mac/linux/windows)."""
        return "mac" if platform.system() == "Darwin" else platform.system().lower()

    def toCommandsDict(self, commands: Commands) -> CommandsDict:
        return {"all": commands} if isinstance(commands, list) else commands

    def getCommandsForCurrentPlatform(self, additionalCommands: Commands = {}) -> list[str]:
        """Selects platform-specific commands from a dictionary.

        Args:
                additionalCommands: Dictionary mapping platforms to command lists (e.g. dict(all=[], linux=['wget "http://something.cool"']) ).

        Returns:
                Merged list of commands for 'all' and current platform.
        """
        commands = []
        if additionalCommands is None:
            return commands
        additionalCommandsDict = self.toCommandsDict(additionalCommands)
        for name in ["all", self.getPlatformCommonName()]:
            commands += additionalCommandsDict.get(name, [])
        return commands

    def getInstallCondaCommands(self) -> list[str]:
        """Generates commands to install micromamba if missing.

        Returns:
                List of installation commands for the current OS.
        """
        condaPath, condaBinPath = self.settingsManager.getCondaPaths()
        if (condaPath / condaBinPath).exists():
            return []
        if platform.system() not in ["Windows", "Linux", "Darwin"]:
            raise Exception(f"Platform {platform.system()} is not supported.")

        condaPath.mkdir(exist_ok=True, parents=True)
        self.createMambaConfigFile(condaPath)

        commands = self.settingsManager.getProxyEnvironmentVariablesCommands()
        proxyString = self.settingsManager.getProxyString()

        if platform.system() == "Windows":
            proxyCredentials = ""
            if proxyString is not None:
                match = re.search(r"^[a-zA-Z]+://(.*?):(.*?)@", proxyString)
                if match:
                    username, password = match.groups()
                    commands += [
                        f'$proxyUsername = "{username}"',
                        f'$proxyPassword = "{password}"',
                        "$securePassword = ConvertTo-SecureString $proxyPassword -AsPlainText -Force",
                        "$proxyCredentials = New-Object System.Management.Automation.PSCredential($proxyUsername, $securePassword)",
                    ]
                    proxyCredentials = f"-ProxyCredential $proxyCredentials"
            proxyArgs = f"-Proxy {proxyString} {proxyCredentials}" if proxyString is not None else ""
            if self.settingsManager.usePixi:
                commands += [
                    'echo "Installing pixi..."',
                    '$tempFile = "$env:TEMP\\pixi-install.ps1"',
                    'try {',
                        f'Invoke-Webrequest {proxyArgs} -UseBasicParsing -Uri https://pixi.sh/install.ps1',
                        f'& $tempFile -PixiHome {condaPath} -NoPathUpdate',
                    '} finally {',
                        'Remove-Item $tempFile -ErrorAction SilentlyContinue',
                    '}',
                ]
            else:
                commands += [
                    f'Set-Location -Path "{condaPath}"',
                    f'echo "Installing Visual C++ Redistributable if necessary..."',
                    f'Invoke-WebRequest {proxyArgs} -URI "https://aka.ms/vs/17/release/vc_redist.x64.exe" -OutFile "$env:Temp\\vc_redist.x64.exe"; Start-Process "$env:Temp\\vc_redist.x64.exe" -ArgumentList "/quiet /norestart" -Wait; Remove-Item "$env:Temp\\vc_redist.x64.exe"',
                    f'echo "Installing micromamba..."',
                    f"Invoke-Webrequest {proxyArgs} -URI https://github.com/mamba-org/micromamba-releases/releases/download/2.0.4-0/micromamba-win-64 -OutFile micromamba.exe",
                ]
        else:
            system = "osx" if platform.system() == "Darwin" else "linux"
            machine = platform.machine()
            machine = "64" if machine == "x86_64" else machine
            proxyArgs = f'--proxy "{proxyString}"' if proxyString is not None else ""
            if self.settingsManager.usePixi:
                commands += [
                    f'cd "{condaPath}"',
                    f'echo "Installing pixi..."',
                    f'curl {proxyArgs} -fsSL https://pixi.sh/install.sh | PIXI_HOME={condaPath} PIXI_NO_PATH_UPDATE=1 bash'
                ]
            else:
                commands += [
                    f'cd "{condaPath}"',
                    f'echo "Installing micromamba..."',
                    f'curl {proxyArgs} -fsSL https://micro.mamba.pm/api/micromamba/{system}-{machine}/latest | tar -xvj bin/micromamba',
                ]
        return commands

    def getActivateCondaCommands(self) -> list[str]:
        """Generates commands to install (if needed) and activate Conda."""
        commands = self.getInstallCondaCommands()
        return commands + self.getShellHookCommands()

    def getActivateEnvironmentCommands(
        self, environment: str | None, additionalActivateCommands: Commands = {}, activateConda = True
    ) -> list[str]:
        """Generates commands to activate the given environment

        Args:
                environment: Environment name to launch. If none, the resulting command list will be empty.
                additionalActivateCommands: Platform-specific activation commands.
                activateConda: Whether to activate conda (micromamba) or not.

        Returns:
                List of commands to activate the environment
        """
        if environment is None:
            return []
        commands = self.getActivateCondaCommands() if activateConda else []
        if self.settingsManager.usePixi:
            manifestPath = self.settingsManager.getManifestPath(environment)
            if platform.system() != "Windows":
                commands += [f'eval "$({self.settingsManager.condaBin} shell-hook --manifest-path {manifestPath})"']
            else:
                commands += [f".\\{self.settingsManager.condaBin} shell-hook --manifest-path {manifestPath} | Out-String | Invoke-Expression",]
        else:
            commands += [f"{self.settingsManager.condaBin} activate {environment}"]
        return commands + self.getCommandsForCurrentPlatform(additionalActivateCommands)
