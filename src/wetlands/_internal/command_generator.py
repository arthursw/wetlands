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
        """Generates shell commands for Conda initialization.

        Returns:
                OS-specific commands to activate Conda shell hooks.
        """
        currentPath = Path.cwd().resolve()
        condaPath, condaBinPath = self.settingsManager.getCondaPaths()
        if self.settingsManager.usePixi:
            if platform.system() == "Windows":
                return [f'$env:PATH = "{condaPath / condaBinPath.parent};" + $env:PATH']
            else:
                return [f'export PATH="{condaPath / condaBinPath.parent}:$PATH"']
        if platform.system() == "Windows":
            return [
                f'Set-Location -Path "{condaPath}"',
                f'$Env:MAMBA_ROOT_PREFIX="{condaPath}"',
                f".\\{condaBinPath} shell hook -s powershell | Out-String | Invoke-Expression",
                f'Set-Location -Path "{currentPath}"',
            ]
        else:
            return [
                f'cd "{condaPath}"',
                f'export MAMBA_ROOT_PREFIX="{condaPath}"',
                f'eval "$({condaBinPath} shell hook -s posix)"',
                f'cd "{currentPath}"',
            ]

    def createMambaConfigFile(self, condaPath):
        """Create Mamba config file .mambarc in condaPath, with nodefaults and conda-forge channels."""
        if self.settingsManager.usePixi:
            return
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

        # Only install if conda is not already installed
        if platform.system() == "Windows":
            commands += [
                f'if (-not (Test-Path "{condaPath / condaBinPath}")) {{',
            ]
        else:
            commands += [f'if [ ! -f "{condaPath / condaBinPath}" ]; then']

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
                    "try {",
                    f"Invoke-Webrequest {proxyArgs} -UseBasicParsing -URI https://pixi.sh/install.ps1",
                    f'& $tempFile -PixiHome "{condaPath}" -NoPathUpdate',
                    "} finally {",
                    "Remove-Item $tempFile -ErrorAction SilentlyContinue",
                    "}",
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
                    f'curl {proxyArgs} -fsSL https://pixi.sh/install.sh | PIXI_HOME="{condaPath}" PIXI_NO_PATH_UPDATE=1 bash',
                ]
            else:
                commands += [
                    f'cd "{condaPath}"',
                    f'echo "Installing micromamba..."',
                    f"curl {proxyArgs} -fsSL https://micro.mamba.pm/api/micromamba/{system}-{machine}/latest | tar -xvj bin/micromamba",
                ]
        # Close the if which checks if conda is already installed
        commands += ["}"] if platform.system() == "Windows" else ["fi"]

        return commands

    def getActivateCondaCommands(self) -> list[str]:
        """Generates commands to install (if needed) and activate Conda."""
        commands = self.getInstallCondaCommands()
        return commands + self.getShellHookCommands()

    def getActivateEnvironmentCommands(
        self, environment: str | None, additionalActivateCommands: Commands = {}, activateConda: bool = True
    ) -> list[str]:
        """Generates commands to activate the given environment

        Args:
                environment: Environment name to launch. If none, the resulting command list will be empty.
                additionalActivateCommands: Platform-specific activation commands.
                activateConda: Whether to activate Conda or not.

        Returns:
                List of commands to activate the environment
        """
        if environment is None:
            return []
        commands = self.getActivateCondaCommands() if activateConda else []
        if self.settingsManager.usePixi:
            manifestPath = self.settingsManager.getManifestPath(environment)
            if platform.system() != "Windows":
                commands += [f'eval "$({self.settingsManager.condaBin} shell-hook --manifest-path "{manifestPath}")"']
            else:
                commands += [
                    f'.\\{self.settingsManager.condaBin} shell-hook --manifest-path "{manifestPath}" | Out-String | Invoke-Expression'
                ]
        else:
            commands += [f"{self.settingsManager.condaBin} activate {environment}"]
        return commands + self.getCommandsForCurrentPlatform(additionalActivateCommands)

    def getAddChannelsCommands(self, environment: str, condaDependencies: list[str], activateConda: bool = True) -> list[str]:
        """Add Conda channels in manifest file when using Pixi (`pixi add channelName::packageName` is not enough, channelName must be in manifest file).
        The returned command will be something like `pixi project add --manifest-path "/path/to/pixi.toml" --prepend channel1 channel2`.

        Args:
                environment: Environment name.
                condaDependencies: The conda dependecies to install (e.g. ["bioimageit::atlas", "openjdk"]).
                activateConda: Whether to activate conda or not.

        Returns:
                List of commands to add required channels
        """
        if not self.settingsManager.usePixi:
            return []
        channels = set([dep.split("::")[0].replace('"', "") for dep in condaDependencies if "::" in dep])
        if len(channels) == 0:
            return []
        manifestPath = self.settingsManager.getManifestPath(environment)
        commands = self.getActivateCondaCommands() if activateConda else []
        commands += [
            f'{self.settingsManager.condaBin} project channel add --manifest-path "{manifestPath}" --no-progress --prepend '
            + " ".join(channels)
        ]
        return commands
