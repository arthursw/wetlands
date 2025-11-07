from pathlib import Path
import platform
from typing import TYPE_CHECKING, Union

try:
    from typing import NotRequired, TypedDict  # type: ignore
except ImportError:
    from typing_extensions import NotRequired, TypedDict  # type: ignore

if TYPE_CHECKING:
    from wetlands.environment import Environment

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

    def getActivateCondaCommands(self) -> list[str]:
        """Generates commands to activate Conda"""
        # Previouly, this function was also installing Conda if necessary
        return self.getShellHookCommands()

    def getActivateEnvironmentCommands(
        self, environment: "Environment", additionalActivateCommands: Commands = {}, activateConda: bool = True
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
            # Warning: Use `pixi shell-hook` instead of `pixi shell` since `pixi shell` creates a new shell (and we want to keep the same shell)
            if platform.system() != "Windows":
                commands += [
                    f'eval "$({self.settingsManager.condaBin} shell-hook --manifest-path "{environment.path}")"'
                ]
            else:
                commands += [
                    f'{self.settingsManager.condaBin} shell-hook --manifest-path "{environment.path}" | Out-String | Invoke-Expression'
                ]
        else:
            commands += [f"{self.settingsManager.condaBin} activate {environment.path}"]
        return commands + self.getCommandsForCurrentPlatform(additionalActivateCommands)

    def getAddChannelsCommands(
        self, environment: "Environment", channels: list[str], condaDependencies: list[str], activateConda: bool = True
    ) -> list[str]:
        """Add Conda channels in manifest file when using Pixi (`pixi add channelName::packageName` is not enough, channelName must be in manifest file).
        The returned command will be something like `pixi project add --manifest-path "/path/to/pixi.toml" --prepend channel1 channel2`.

        Args:
                environment: Environment name.
                condaChannels: The channels to add.
                condaDependencies: The conda dependecies to install (e.g. ["bioimageit::atlas", "openjdk"]).
                activateConda: Whether to activate conda or not.

        Returns:
                List of commands to add required channels
        """
        if not self.settingsManager.usePixi:
            if len(channels) > 0:
                return [f"{self.settingsManager.condaBinConfig} config --add channels " + " ".join(channels)]
            else:
                return []
        channels += set([dep.split("::")[0].replace('"', "") for dep in condaDependencies if "::" in dep])
        if len(channels) == 0:
            return []
        commands = self.getActivateCondaCommands() if activateConda else []
        commands += [
            f'{self.settingsManager.condaBin} project channel add --manifest-path "{environment.path}" --no-progress --prepend '
            + " ".join(channels)
        ]
        return commands
