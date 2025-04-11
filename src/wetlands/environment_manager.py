import json
import re
import platform
from importlib import metadata
from pathlib import Path
import subprocess
import sys
from typing import Any, Literal

from wetlands.internal_environment import InternalEnvironment
from wetlands._internal.dependency_manager import Dependencies, DependencyManager
from wetlands._internal.command_executor import CommandExecutor
from wetlands._internal.command_generator import Commands, CommandGenerator
from wetlands._internal.settings_manager import SettingsManager
from wetlands.environment import Environment
from wetlands.external_environment import ExternalEnvironment


class EnvironmentManager:
    """Manages Conda environments using micromamba for isolation and dependency management.

    Attributes:
            mainEnvironment: The main conda environment in which wetlands is installed.
            installedPackages: map of the installed packaged (e.g. {"pip": {"numpy":2.2.4"}, "conda":{"icu":"75.1"}})
            environments: map of the environments

            settingsManager: SettingsManager(condaPath)
            dependencyManager: DependencyManager(settingsManager)
            commandGenerator: CommandGenerator(settingsManager, dependencyManager)
            commandExecutor: CommandExecutor()
    """

    mainEnvironment: InternalEnvironment
    installedPackages: dict[str, dict[str, str]] = {}
    environments: dict[str, Environment] = {}

    def __init__(
        self, condaPath: str | Path = Path("micromamba"), mainCondaEnvironmentPath: str | Path | None = None
    ) -> None:
        """Initializes the EnvironmentManager with a micromamba path.

        Args:
                condaPath: Path to the micromamba binary. Defaults to "micromamba".
                mainCondaEnvironmentPath: Path of the main conda environment in which wetlands is installed, used to check whether it is necessary to create new environments (only when dependencies are not already available in the main environment).
        """
        self.mainEnvironment = InternalEnvironment(mainCondaEnvironmentPath, self)
        self.settingsManager = SettingsManager(condaPath)
        self.dependencyManager = DependencyManager(self.settingsManager)
        self.commandGenerator = CommandGenerator(self.settingsManager, self.dependencyManager)
        self.commandExecutor = CommandExecutor()

    def setCondaPath(self, condaPath: str | Path) -> None:
        """Updates the micromamba path and loads proxy settings if exists.

        Args:
                condaPath: New path to micromamba binary.

        Side Effects:
                Updates self.settingsManager.condaBinConfig, and self.settingsManager.proxies from the .mambarc file.
        """
        self.settingsManager.setCondaPath(condaPath)

    def setProxies(self, proxies: dict[str, str]) -> None:
        """Configures proxy settings for Conda operations.

        Args:
                proxies: Proxy configuration dictionary (e.g., {"http": "...", "https": "..."}).

        Side Effects:
                Updates .mambarc configuration file with proxy settings.
        """
        self.settingsManager.setProxies(proxies)

    def _removeChannel(self, condaDependency: str) -> str:
        """Removes channel prefix from a Conda dependency string (e.g., "channel::package" -> "package")."""
        return condaDependency.split("::")[1] if "::" in condaDependency else condaDependency

    def _checkRequirement(self, dependency: str, packageManager: Literal["pip", "conda"]) -> bool:
        """Check if dependency is installed (exists in self.installedPackages[packageManager])"""
        if packageManager == "conda":
            dependency = self._removeChannel(dependency)
        nameVersion = dependency.split("==")
        return any(
            [
                nameVersion[0] == name and (len(nameVersion) == 1 or version.startswith(nameVersion[1]))
                for name, version in self.installedPackages[packageManager].items()
            ]
        )

    def _dependenciesAreInstalled(self, dependencies: Dependencies) -> bool:
        """Verifies if all specified dependencies are installed in the main environment.

        Args:
                dependencies: Dependencies to check.

        Returns:
                True if all dependencies are installed, False otherwise.
        """

        if not sys.version.startswith(dependencies.get("python", "").replace("=", "")):
            return False

        condaDependencies, condaDependenciesNoDeps, hasCondaDependencies = self.dependencyManager.formatDependencies(
            "conda", dependencies, False, False
        )
        pipDependencies, pipDependenciesNoDeps, hasPipDependencies = self.dependencyManager.formatDependencies(
            "pip", dependencies, False, False
        )

        if hasCondaDependencies:
            if self.mainEnvironment.name is None:
                return False
            elif "conda" not in self.installedPackages:
                commands = self.commandGenerator.getActivateCondaCommands() + [
                    f"{self.settingsManager.condaBin} activate {self.mainEnvironment.name}",
                    f"{self.settingsManager.condaBin} list --json",
                ]
                condaList = self.commandExecutor.executeCommandAndGetOutput(commands, log=False)

                json_output = "".join(condaList)
                conda_list = json.loads(json_output)

                for package_info in conda_list:
                    name = package_info.get("name")
                    version = package_info.get("version")
                    if name and version:
                        if "conda" not in self.installedPackages:
                            self.installedPackages["conda"] = {}
                        self.installedPackages["conda"][name] = version

            if not all([self._checkRequirement(d, "conda") for d in condaDependencies + condaDependenciesNoDeps]):
                return False
        if not hasPipDependencies:
            return True

        if "pip" not in self.installedPackages:
            if self.mainEnvironment.name is not None:
                commands = self.commandGenerator.getActivateCondaCommands() + [
                    f"{self.settingsManager.condaBin} activate {self.mainEnvironment.name}",
                    f"pip freeze --all",
                ]
                output = self.commandExecutor.executeCommandAndGetOutput(commands, log=False)
                parsedOutput = [o.split("==") for o in output if "==" in o]
                self.installedPackages["pip"] = {name: version for name, version in parsedOutput}
            else:
                self.installedPackages["pip"] = {
                    dist.metadata["Name"]: dist.version for dist in metadata.distributions()
                }

        return all([self._checkRequirement(d, "pip") for d in pipDependencies + pipDependenciesNoDeps])

    def environmentExists(self, environment: str) -> bool:
        """Checks if a Conda environment exists.

        Args:
                environment: Environment name to check.

        Returns:
                True if environment exists, False otherwise.
        """
        condaMeta = Path(self.settingsManager.condaPath) / "envs" / environment / "conda-meta"
        return condaMeta.is_dir()

    def create(
        self,
        environment: str,
        dependencies: Dependencies = {},
        additionalInstallCommands: Commands = {},
        forceExternal: bool = False,
    ) -> Environment:
        """Creates a new Conda environment with specified dependencie or the main environment if dependencies are met in the main environment and forceExternal is False (in which case additional install commands will not be called). Return the existing environment if it was already created.

        Args:
                environment: Name for the new environment. Ignore if dependencies are already installed in the main environment and forceExternal is False.
                dependencies: Dependencies to install, in the form dict(python="3.12.7", conda=["conda-forge::pyimagej==1.5.0", dict(name="openjdk=11", platforms=["osx-64", "osx-arm64", "win-64", "linux-64"], dependencies=True, optional=False)], pip=["numpy==1.26.4"]).
                additionalInstallCommands: Platform-specific commands during installation (e.g. {"mac": ["cd ...", "wget https://...", "unzip ..."], "all"=[], ...}).
                forceExternal: force create external environment even if dependencies are met in main environment

        Returns:
                The created environment (InternalEnvironment if dependencies are met in the main environment and not forceExternal, ExternalEnvironment otherwise).
        """
        if self.environmentExists(environment):
            if environment not in self.environments:
                self.environments[environment] = ExternalEnvironment(environment, self)
            return self.environments[environment]
        if not forceExternal and self._dependenciesAreInstalled(dependencies):
            return self.mainEnvironment
        pythonVersion = dependencies.get("python", "").replace("=", "")
        match = re.search(r"(\d+)\.(\d+)", pythonVersion)
        if match and (int(match.group(1)) < 3 or int(match.group(2)) < 9):
            raise Exception("Python version must be greater than 3.8")
        pythonRequirement = " python=" + (pythonVersion if len(pythonVersion) > 0 else platform.python_version())
        createEnvCommands = self.commandGenerator.getActivateCondaCommands()
        createEnvCommands += [f"{self.settingsManager.condaBinConfig} create -n {environment}{pythonRequirement} -y"]
        createEnvCommands += self.dependencyManager.getInstallDependenciesCommands(environment, dependencies)
        createEnvCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
        self.commandExecutor.executeCommandAndGetOutput(createEnvCommands)
        self.environments[environment] = ExternalEnvironment(environment, self)
        return self.environments[environment]

    def install(
        self, environmentName: str | None, dependencies: Dependencies, additionalInstallCommands: Commands = {}
    ) -> list[str]:
        """Installs dependencies.
        See [`EnvironmentManager.create`][wetlands.environment_manager.EnvironmentManager.create] for more details on the ``dependencies`` and ``additionalInstallCommands`` parameters.

        Args:
                environmentName: The environment to install dependencies.
                dependencies: Dependencies to install.
                additionalInstallCommands: Platform-specific commands during installation.

        Returns:
                Output lines of the installation commands.
        """
        installCommands = self.commandGenerator.getActivateCondaCommands()
        installCommands += self.dependencyManager.getInstallDependenciesCommands(environmentName, dependencies)
        installCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
        return self.commandExecutor.executeCommandAndGetOutput(installCommands)

    def executeCommands(
        self,
        environmentName: str | None,
        commands: Commands,
        additionalActivateCommands: Commands = {},
        popenKwargs: dict[str, Any] = {},
    ) -> subprocess.Popen:
        """Executes the given commands in the given environment.

        Args:
                environmentName: The environment in which to execute commands.
                commands: The commands to execute in the environment.
                additionalActivateCommands: Platform-specific activation commands.
                popenKwargs: Keyword arguments for subprocess.Popen() (see [Popen documentation](https://docs.python.org/3/library/subprocess.html#popen-constructor)). Defaults are: dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, encoding="utf-8", errors="replace", bufsize=1).

        Returns:
                The launched process.
        """
        activateCommands = self.commandGenerator.getActivateEnvironmentCommands(
            environmentName, additionalActivateCommands
        )
        platformCommands = self.commandGenerator.getCommandsForCurrentPlatform(commands)
        return self.commandExecutor.executeCommands(activateCommands + platformCommands, popenKwargs=popenKwargs)

    def _removeEnvironment(self, environment: Environment) -> None:
        """Remove an environment.

        Args:
                environment: instance to remove.
        """
        if environment.name in self.environments:
            del self.environments[environment.name]
