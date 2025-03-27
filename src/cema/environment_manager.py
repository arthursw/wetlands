import re
import platform
from importlib import metadata
from pathlib import Path

from cema.environment import Environment, InternalEnvironment, ExternalEnvironment
from cema.dependency_manager import Dependencies, DependencyManager
from cema.command_executor import CommandExecutor
from cema.command_generator import CommandGenerator
from cema.settings_manager import SettingsManager


class EnvironmentManager:
    """Manages Conda environments using micromamba for isolation and dependency management.

    Attributes:
            mainEnvironment: Path of the main conda environment in which cema is installed, used to check whether it is necessary to create new environments (only when dependencies are not already available in the main environment).
            installedPackages: map of the installed packaged (e.g. {pip: ['numpy==2.2.4'], conda=['icu==75.1']})
            environments: map of the environments

            settingsManager: SettingsManager(condaPath)
            dependencyManager: DependencyManager(settingsManager)
            commandGenerator: CommandGenerator(settingsManager, dependencyManager)
            commandExecutor: CommandExecutor()
    """

    mainEnvironment: Path = None
    installedPackages: dict[str, list[str]] = {}
    environments: dict[str, Environment] = {}

    def __init__(self, condaPath: str | Path = Path("micromamba"), mainEnvironment: str | Path = None) -> None:
        """Initializes the EnvironmentManager with a micromamba path.

        Args:
                condaPath: Path to the micromamba binary. Defaults to "micromamba".
        """
        self.mainEnvironment = Path(mainEnvironment) if isinstance(mainEnvironment, str) else mainEnvironment
        self.settingsManager = SettingsManager(condaPath)
        self.dependencyManager = DependencyManager(self.settingsManager)
        self.commandGenerator = CommandGenerator(
            self.settingsManager, self.dependencyManager
        )
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
                proxies: Proxy configuration dictionary (e.g., {'http': '...', 'https': '...'}).

        Side Effects:
                Updates .mambarc configuration file with proxy settings.
        """
        self.settingsManager.setProxies(proxies)

    def _removeChannel(self, condaDependency: str) -> str:
        """Removes channel prefix from a Conda dependency string (e.g., 'channel::package' -> 'package')."""
        return (
            condaDependency.split("::")[1]
            if "::" in condaDependency
            else condaDependency
        )

    def _dependenciesAreInstalled(self, dependencies: Dependencies) -> bool:
        """Verifies if all specified dependencies are installed in the main environment.

        Args:
                dependencies: Dependencies to check.

        Returns:
                True if all dependencies are installed, False otherwise.
        """
        condaDependencies, condaDependenciesNoDeps, hasCondaDependencies = (
            self.dependencyManager.formatDependencies("conda", dependencies, False)
        )
        pipDependencies, pipDependenciesNoDeps, hasPipDependencies = (
            self.dependencyManager.formatDependencies("pip", dependencies, False)
        )
        
        if hasCondaDependencies:
            if self.mainEnvironment is not None and "conda" not in self.installedPackages:
                commands = self.commandGenerator.getActivateCondaCommands() + [
                    f"{self.settingsManager.condaBin} activate {self.mainEnvironment}",
                    f"{self.settingsManager.condaBin} list -y",
                ]
                self.installedPackages["conda"] = self.commandExecutor.executeCommandAndGetOutput(commands, log=False)
            if not all(
                [
                    self._removeChannel(d) in self.installedPackages["conda"]
                    for d in condaDependencies + condaDependenciesNoDeps
                ]
            ):
                return False
        if not hasPipDependencies:
            return True

        if "pip" not in self.installedPackages:
            if self.mainEnvironment is not None:
                commands = self.commandGenerator.getActivateCondaCommands() + [f"{self.settingsManager.condaBin} activate {self.mainEnvironment}", f"pip freeze --all"]
                self.installedPackages["pip"] = self.commandExecutor.executeCommandAndGetOutput(commands, log=False)
            else:
                self.installedPackages["pip"] = [
                    f"{dist.metadata['Name']}=={dist.version}"
                    for dist in metadata.distributions()
                ]

        return all(
            [
                d in self.installedPackages["pip"]
                for d in pipDependencies + pipDependenciesNoDeps
            ]
        )

    def environmentExists(self, environment: str) -> bool:
        """Checks if a Conda environment exists.

        Args:
                environment: Environment name to check.

        Returns:
                True if environment exists, False otherwise.
        """
        condaMeta = (
            Path(self.settingsManager.condaPath) / "envs" / environment / "conda-meta"
        )
        return condaMeta.is_dir()

    def create(
        self,
        environment: str,
        dependencies: Dependencies,
        additionalInstallCommands: dict[str, list[str]] = {},
        forceExternal=False
    ) -> Environment:
        """Creates a new Conda environment with specified dependencie or a fake environment if dependencies are met in the main environment (in which case additional install commands will not be called). Return the existing environment if it was already created.

        Args:
                environment: Name for the new environment.
                dependencies: Dependencies to install, in the form dict(python='3.12.7', conda=['conda-forge::pyimagej==1.5.0', dict(name='openjdk=11', platforms=['osx-64', 'osx-arm64', 'win-64', 'linux-64'], dependencies=True, optional=False)], pip=['numpy==1.26.4']).
                additionalInstallCommands: Platform-specific commands during installation (e.g. {'mac': ['cd ...', 'wget https://...', 'unzip ...'], 'all'=[], ...}).
                forceExternal: force create external environment even if dependencies are met in main environment

        Returns:
                The created environment (InternalEnvironment if dependencies are met in the main environment and not forceExternal, ExternalEnvironment otherwise).
        """
        if self.environmentExists(environment):
            if environment not in self.environments:
                self.environments[environment] = ExternalEnvironment(environment, self)
            return self.environments[environment]
        if not forceExternal and self._dependenciesAreInstalled(dependencies):
            self.environments[environment] = InternalEnvironment(environment, self)
            return self.environments[environment]
        pythonVersion = (
            str(dependencies.get("python", "")).replace("=", "")
            if "python" in dependencies and dependencies["python"]
            else ""
        )
        match = re.search(r"(\d+)\.(\d+)", pythonVersion)
        if match and (int(match.group(1)) < 3 or int(match.group(2)) < 9):
            raise Exception("Python version must be greater than 3.8")
        pythonRequirement = " python=" + (
            pythonVersion if len(pythonVersion) > 0 else platform.python_version()
        )
        createEnvCommands = self.commandGenerator.getActivateCondaCommands()
        createEnvCommands += [
            f"{self.settingsManager.condaBin} create -n {environment}{pythonRequirement} -y"
        ]
        createEnvCommands += self.dependencyManager.getInstallDependenciesCommands(
            environment, dependencies
        )
        createEnvCommands += self.commandGenerator.getCommandsForCurrentPlatform(
            additionalInstallCommands
        )
        self.commandExecutor.executeCommandAndGetOutput(createEnvCommands)
        self.environments[environment] = ExternalEnvironment(environment, self)
        return self.environments[environment]
    
    def _removeEnvironment(self, environment: Environment) -> None:
        """Remove an environment.

        Args:
                environment: instance to remove.
        """
        if environment.name in self.environments:
            del self.environments[environment.name]
