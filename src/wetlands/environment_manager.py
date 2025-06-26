import re
import platform
from importlib import metadata
from pathlib import Path
import subprocess
import sys
from typing import Any, Literal

from wetlands._internal.install import installMicromamba, installPixi
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
            environments: map of the environments

            settingsManager: SettingsManager(condaPath)
            commandGenerator: CommandGenerator(settingsManager)
            dependencyManager: DependencyManager(commandGenerator)
            commandExecutor: CommandExecutor()
    """

    mainEnvironment: InternalEnvironment
    environments: dict[str, Environment] = {}

    def __init__(
        self,
        condaPath: str | Path = Path("pixi"),
        usePixi=True,
        mainCondaEnvironmentPath: Path | None = None,
        acceptAllCondaPaths=False,
    ) -> None:
        """Initializes the EnvironmentManager with a micromamba path.

        Args:
                condaPath: Path to the micromamba binary. Defaults to "micromamba". Warning: cannot contain any space character on Windows.
                usePixi: Whether to use Pixi as the conda manager.
                mainCondaEnvironmentPath: Path of the main conda environment in which Wetlands is installed, used to check whether it is necessary to create new environments (only when dependencies are not already available in the main environment). When using Pixi, this must point to the folder containing the pixi.toml (or pyproject.toml) file.
                acceptAllCondaPaths: Whether to accept Conda path containing "pixi" when using micromamba or "micromamba" when using pixi.
        """
        condaPath = Path(condaPath)
        if platform.system() == "Windows" and (not usePixi) and " " in str(condaPath) and not condaPath.exists():
            raise Exception(
                f'The Micromamba path cannot contain any space character on Windows (given path is "{condaPath}").'
            )
        condaName = "pixi" if usePixi else "micromamba"
        otherName = "micromamba" if usePixi else "pixi"
        if (not acceptAllCondaPaths) and otherName in str(condaPath):
            raise Exception(
                f'You provided the condaPath "{condaPath}" which contains "{otherName}", but you asked to use {condaName}. Use acceptAllCondaPaths to use this path anyway.'
            )
        self.mainEnvironment = InternalEnvironment(mainCondaEnvironmentPath, self)
        self.settingsManager = SettingsManager(condaPath, usePixi)
        self.installConda()
        self.commandGenerator = CommandGenerator(self.settingsManager)
        self.dependencyManager = DependencyManager(self.commandGenerator)
        self.commandExecutor = CommandExecutor()

    def installConda(self):
        """Install Pixi or Micromamba (depending on settingsManager.usePixi)"""

        condaPath, condaBinPath = self.settingsManager.getCondaPaths()
        if (condaPath / condaBinPath).exists():
            return []

        condaPath.mkdir(exist_ok=True, parents=True)

        if self.settingsManager.usePixi:
            installPixi(condaPath, proxies=self.settingsManager.proxies)
        else:
            installMicromamba(condaPath, proxies=self.settingsManager.proxies)
        return

    def setCondaPath(self, condaPath: str | Path, usePixi: bool = True) -> None:
        """Updates the micromamba path and loads proxy settings if exists.

        Args:
                condaPath: New path to micromamba binary.
                usePixi: Whether to use Pixi or Micromamba

        Side Effects:
                Updates self.settingsManager.condaBinConfig, and self.settingsManager.proxies from the .mambarc file.
        """
        self.settingsManager.setCondaPath(condaPath, usePixi)

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

    def getInstalledPackages(self, environment: str | Path) -> list[dict[str, str]]:
        """Get the list of the packages installed in the environment

        Args:
                environment: The environment name.

        Returns:
                A list of dict containing the installed packages [{"kind":"conda|pypi", "name": "numpy", "version", "2.1.3"}, ...].
        """
        if self.settingsManager.usePixi:
            manifestPath = self.settingsManager.getManifestPath(environment)
            commands = self.commandGenerator.getActivateCondaCommands()
            commands += [f'{self.settingsManager.condaBin} list --json --manifest-path "{manifestPath}"']
            return self.commandExecutor.executeCommandAndGetJsonOutput(commands, log=False)
        else:
            commands = self.commandGenerator.getActivateEnvironmentCommands(str(environment)) + [
                f"{self.settingsManager.condaBin} list --json",
            ]
            packages = self.commandExecutor.executeCommandAndGetJsonOutput(commands, log=False)
            for package in packages:
                package["kind"] = "conda"

            commands = self.commandGenerator.getActivateEnvironmentCommands(str(environment)) + [
                f"pip freeze --all",
            ]
            output = self.commandExecutor.executeCommandAndGetOutput(commands, log=False)
            parsedOutput = [o.split("==") for o in output if "==" in o]
            packages += [{"name": name, "version": version, "kind": "pypi"} for name, version in parsedOutput]
            return packages

    def _checkRequirement(
        self, dependency: str, packageManager: Literal["pip", "conda"], installedPackages: list[dict[str, str]]
    ) -> bool:
        """Check if dependency is installed (exists in installedPackages)"""
        if packageManager == "conda":
            dependency = self._removeChannel(dependency)
        nameVersion = dependency.split("==")
        packageManagerName = "conda" if packageManager == "conda" else "pypi"
        return any(
            [
                nameVersion[0] == package["name"]
                and (len(nameVersion) == 1 or package["version"].startswith(nameVersion[1]))
                and packageManagerName == package["kind"]
                for package in installedPackages
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
        if not hasPipDependencies and not hasCondaDependencies:
            return True
        if hasCondaDependencies and self.mainEnvironment.name is None:
            return False
        installedPackages = []
        if hasPipDependencies and self.mainEnvironment.name is None:
            installedPackages = [
                {"name": dist.metadata["Name"], "version": dist.version, "kind": "pypi"}
                for dist in metadata.distributions()
            ]

        if self.mainEnvironment.name is not None:
            installedPackages = self.getInstalledPackages(Path(self.mainEnvironment.name))

        condaSatisfied = all(
            [self._checkRequirement(d, "conda", installedPackages) for d in condaDependencies + condaDependenciesNoDeps]
        )
        pipSatisfied = all(
            [self._checkRequirement(d, "pip", installedPackages) for d in pipDependencies + pipDependenciesNoDeps]
        )

        return condaSatisfied and pipSatisfied

    def environmentExists(self, environment: str | Path) -> bool:
        """Checks if a Conda environment exists.

        Args:
                environment: Environment name to check. If environment is a string, it will be considered as a name; if it is a pathlib.Path, it will be considered as a path to an existing environment.

        Returns:
                True if environment exists, False otherwise.
        """
        if self.settingsManager.usePixi:
            manifestPath = self.settingsManager.getManifestPath(environment)
            condaMeta = manifestPath.parent / ".pixi" / "envs" / "default" / "conda-meta"
            return manifestPath.is_file() and condaMeta.is_dir()
        else:
            if isinstance(environment, Path):
                condaMeta = environment / "conda-meta"
            else:
                condaMeta = Path(self.settingsManager.condaPath) / "envs" / environment / "conda-meta"
            return condaMeta.is_dir()

    def create(
        self,
        environment: str | Path,
        dependencies: Dependencies = {},
        additionalInstallCommands: Commands = {},
        forceExternal: bool = False,
    ) -> Environment:
        """Creates a new Conda environment with specified dependencie or the main environment if dependencies are met in the main environment and forceExternal is False (in which case additional install commands will not be called). Return the existing environment if it was already created.

        Args:
                environment: Name for the new environment. Ignore if dependencies are already installed in the main environment and forceExternal is False. Can also be a pathlib.Path to an existing Conda environment (or the folder containing the pixi.toml or pyproject.toml when using Pixi). If environment is a string, it will be considered as a name; if it is a pathlib.Path, it will be considered as a path to an existing environment (will raise an exception if the environment does not exist).
                dependencies: Dependencies to install, in the form dict(python="3.12.7", conda=["conda-forge::pyimagej==1.5.0", dict(name="openjdk=11", platforms=["osx-64", "osx-arm64", "win-64", "linux-64"], dependencies=True, optional=False)], pip=["numpy==1.26.4"]).
                additionalInstallCommands: Platform-specific commands during installation (e.g. {"mac": ["cd ...", "wget https://...", "unzip ..."], "all"=[], ...}).
                forceExternal: force create external environment even if dependencies are met in main environment

        Returns:
                The created environment (InternalEnvironment if dependencies are met in the main environment and not forceExternal, ExternalEnvironment otherwise).
        """
        if self.environmentExists(environment):
            environment = str(environment)
            if environment not in self.environments:
                self.environments[environment] = ExternalEnvironment(environment, self)
            return self.environments[environment]
        if isinstance(environment, Path):
            raise Exception(f"The environment {environment.resolve()} was not found.")
        if not forceExternal and self._dependenciesAreInstalled(dependencies):
            return self.mainEnvironment
        pythonVersion = dependencies.get("python", "").replace("=", "")
        match = re.search(r"(\d+)\.(\d+)", pythonVersion)
        if match and (int(match.group(1)) < 3 or int(match.group(2)) < 9):
            raise Exception("Python version must be greater than 3.8")
        pythonRequirement = " python=" + (pythonVersion if len(pythonVersion) > 0 else platform.python_version())
        createEnvCommands = self.commandGenerator.getActivateCondaCommands()
        if self.settingsManager.usePixi:
            manifestPath = self.settingsManager.getManifestPath(environment)
            if not manifestPath.exists():
                platformArgs = f"--platform win-64" if platform.system() == "Windows" else ""
                createEnvCommands += [
                    f'{self.settingsManager.condaBin} init --no-progress {platformArgs} "{manifestPath.parent}"'
                ]
            createEnvCommands += [
                f'{self.settingsManager.condaBin} add --no-progress --manifest-path "{manifestPath}" {pythonRequirement}'
            ]
        else:
            createEnvCommands += [
                f"{self.settingsManager.condaBinConfig} create -n {environment}{pythonRequirement} -y"
            ]
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
        installCommands = self.dependencyManager.getInstallDependenciesCommands(environmentName, dependencies)
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
