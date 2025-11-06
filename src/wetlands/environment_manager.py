import json
import re
import platform
from importlib import metadata
from pathlib import Path
import subprocess
import sys
from typing import Any, Literal, cast, Union

from wetlands._internal.install import installMicromamba, installPixi
from wetlands.internal_environment import InternalEnvironment
from wetlands._internal.dependency_manager import Dependencies, DependencyManager
from wetlands._internal.command_executor import CommandExecutor
from wetlands._internal.command_generator import Commands, CommandGenerator
from wetlands._internal.settings_manager import SettingsManager
from wetlands._internal.config_parser import ConfigParser
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
    wetlandsInstancePath: Path
    debug: bool

    def __init__(
        self,
        condaPath: str | Path = Path("pixi"),
        usePixi=True,
        mainCondaEnvironmentPath: Path | None = None,
        acceptAllCondaPaths=False,
        wetlandsInstancePath: Path | None = None,
        debug: bool = False,
    ) -> None:
        """Initializes the EnvironmentManager with a micromamba path.

        Args:
                condaPath: Path to the micromamba binary. Defaults to "micromamba". Warning: cannot contain any space character on Windows.
                usePixi: Whether to use Pixi as the conda manager.
                mainCondaEnvironmentPath: Path of the main conda environment in which Wetlands is installed, used to check whether it is necessary to create new environments (only when dependencies are not already available in the main environment). When using Pixi, this must point to the folder containing the pixi.toml (or pyproject.toml) file.
                acceptAllCondaPaths: Whether to accept Conda path containing "pixi" when using micromamba or "micromamba" when using pixi.
                wetlandsInstancePath: Path to the folder which will contain the state of this wetlands instance (environment process debug ports, stored in debug_ports.json). Default is condaPath / 'wetlands'.
                debug: When true, processes will listen to debugpy ( debugpy.listen(0) ) to enable debugging, and their ports will be sorted in  wetlandsInstancePath / debug_ports.json
        """
        self.environments: dict[str, Environment] = {}
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
        if wetlandsInstancePath is None:
            self.wetlandsInstancePath = self.settingsManager.condaPath / "wetlands"
        else:
            self.wetlandsInstancePath = cast(Path, wetlandsInstancePath).resolve()
        self.debug = debug
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
            output = self.commandExecutor.executeCommandsAndGetOutput(commands, log=False)
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

    def _addDebugpyInDependencies(self, dependencies: Dependencies) -> None:
        """Add debugpy in the dependencies to be able to debug in debug mode. Does nothing when not in debug mode.

        Args:
                dependencies: Dependencies to install.
        """
        if not self.debug:
            return
        # Check that debugpy is not already in dependencies
        for packageManager in ["pip", "conda"]:
            if packageManager in dependencies:
                for dep in dependencies[packageManager]:
                    import re

                    pattern = r"debugpy(?==|$)"
                    if isinstance(dep, str):
                        if bool(re.search(pattern, dep)):
                            return
                    elif dep["name"] == "debugpy":
                        return
        # Add debugpy without version because we need one compatible with the required python version
        # Use conda (conda forge) since there are more versions available (especially for python 3.9 on macOS arm64)
        debugpy = "debugpy"
        if "conda" in dependencies:
            dependencies["conda"].append(debugpy)
        else:
            dependencies["conda"] = [debugpy]
        return

    def _parseDependenciesFromConfig(
        self,
        config_path: Union[str, Path],
        environmentName: str | None = None,
        optionalDependencies: list[str] | None = None,
    ) -> Dependencies:
        """Parse dependencies from a config file (pixi.toml, pyproject.toml, or environment.yml).

        Args:
                config_path: Path to configuration file
                environmentName: Environment name for pixi/pyproject configs
                optionalDependencies: Optional dependency groups for pyproject configs

        Returns:
                Dependencies dict

        Raises:
                FileNotFoundError: If config file doesn't exist
                ValueError: If config format is invalid or parameters are missing
        """
        config_path = Path(config_path)
        parser = ConfigParser()

        # Detect and validate config file type
        try:
            file_type = parser.detectConfigFileType(config_path)
        except ValueError as e:
            raise ValueError(f"Unsupported config file: {e}")

        # Validate required parameters for specific file types
        if file_type == "pixi" and not environmentName:
            raise ValueError(
                f"environmentName is required for pixi.toml files. "
                f"Please provide the environment name to extract dependencies from."
            )

        if file_type == "pyproject" and not environmentName and not optionalDependencies:
            raise ValueError(
                f"For pyproject.toml, provide either environmentName (for pixi config) "
                f"or optionalDependencies (for optional dependency groups)."
            )

        # Parse the config file
        return parser.parse(
            config_path,
            environmentName=environmentName,
            optionalDependencies=optionalDependencies,
        )

    def create(
        self,
        name: str,
        dependencies: Union[Dependencies, None] = None,
        additionalInstallCommands: Commands = {},
        forceExternal: bool = False,
    ) -> Environment:
        """Creates a new Conda environment with specified dependencies or the main environment if dependencies are met in the main environment and forceExternal is False (in which case additional install commands will not be called). Return the existing environment if it was already created.

        Args:
                name: Name for the new environment.
                dependencies: Dependencies to install. Can be one of:
                    - A Dependencies dict: dict(python="3.12.7", conda=["numpy"], pip=["requests"])
                    - None (no dependencies to install)
                additionalInstallCommands: Platform-specific commands during installation (e.g. {"mac": ["cd ...", "wget https://...", "unzip ..."], "all"=[], ...}).
                forceExternal: force create external environment even if dependencies are met in main environment

        Returns:
                The created environment (InternalEnvironment if dependencies are met in the main environment and not forceExternal, ExternalEnvironment otherwise).
        """
        if "/" in name or "\\" in name:
            raise Exception("Environments name cannot contain any forward nor backward slash.")
        if self.environmentExists(name):
            if name not in self.environments:
                self.environments[name] = ExternalEnvironment(name, self)
            return self.environments[name]

        if dependencies is None:
            dependencies = {}
        elif not isinstance(dependencies, dict):
            raise ValueError(f"Unsupported dependencies type: {type(dependencies)}")

        self._addDebugpyInDependencies(dependencies)
        if not forceExternal and self._dependenciesAreInstalled(dependencies):
            return self.mainEnvironment
        pythonVersion = dependencies.get("python", "").replace("=", "")
        match = re.search(r"(\d+)\.(\d+)", pythonVersion)
        if match and (int(match.group(1)) < 3 or int(match.group(2)) < 9):
            raise Exception("Python version must be greater than 3.8")
        pythonRequirement = " python=" + (pythonVersion if len(pythonVersion) > 0 else platform.python_version())
        createEnvCommands = self.commandGenerator.getActivateCondaCommands()
        if self.settingsManager.usePixi:
            manifestPath = self.settingsManager.getManifestPath(name)
            if not manifestPath.exists():
                platformArgs = f"--platform win-64" if platform.system() == "Windows" else ""
                createEnvCommands += [
                    f'{self.settingsManager.condaBin} init --no-progress {platformArgs} "{manifestPath.parent}"'
                ]
            createEnvCommands += [
                f'{self.settingsManager.condaBin} add --no-progress --manifest-path "{manifestPath}" {pythonRequirement}'
            ]
        else:
            createEnvCommands += [f"{self.settingsManager.condaBinConfig} create -n {name}{pythonRequirement} -y"]
        createEnvCommands += self.dependencyManager.getInstallDependenciesCommands(name, dependencies)
        createEnvCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
        self.commandExecutor.executeCommandsAndGetOutput(createEnvCommands)
        self.environments[name] = ExternalEnvironment(name, self)
        return self.environments[name]

    def createFromConfig(
        self,
        name: str,
        configPath: str | Path,
        optionalDependencies: list[str] | None = None,
        additionalInstallCommands: Commands = {},
        forceExternal: bool = False,
    ) -> Environment:
        """Creates a new Conda environment from a config file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt).

        Args:
                name: Name for the new environment.
                configPath: Path to configuration file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt).
                optionalDependencies: List of optional dependency groups to extract from pyproject.toml.
                additionalInstallCommands: Platform-specific commands during installation.
                forceExternal: force create external environment even if dependencies are met in main environment

        Returns:
                The created environment (InternalEnvironment if dependencies are met in the main environment and not forceExternal, ExternalEnvironment otherwise).
        """
        if "/" in name or "\\" in name:
            raise Exception("Environments name cannot contain any forward nor backward slash.")

        # Parse config file
        dependencies = self._parseDependenciesFromConfig(
            configPath, environmentName=name, optionalDependencies=optionalDependencies
        )

        # Use create() with parsed dependencies
        return self.create(name, dependencies, additionalInstallCommands, forceExternal)

    def load(
        self,
        name: str,
        environmentPath: Path,
    ) -> Environment:
        """Load an existing Conda environment from disk.

        Args:
                name: Name for the environment instance.
                environmentPath: Path to an existing Conda environment, or the folder containing the pixi.toml/pyproject.toml when using Pixi.

        Returns:
                The loaded environment (ExternalEnvironment if using Pixi or micromamba with a path, InternalEnvironment otherwise).

        Raises:
                Exception: If the environment does not exist.
        """
        environmentPath = environmentPath.resolve()

        if not self.environmentExists(environmentPath):
            raise Exception(f"The environment {environmentPath} was not found.")

        if environmentPath not in self.environments:
            self.environments[environmentPath] = ExternalEnvironment(environmentPath, self)
        return self.environments[environmentPath]

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
        return self.commandExecutor.executeCommandsAndGetOutput(installCommands)

    def executeCommands(
        self,
        environmentName: str | None,
        commands: Commands,
        additionalActivateCommands: Commands = {},
        popenKwargs: dict[str, Any] = {},
        wait: bool = False,
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
        return self.commandExecutor.executeCommands(
            activateCommands + platformCommands, popenKwargs=popenKwargs, wait=wait
        )

    def registerEnvironment(self, environment: ExternalEnvironment, debugPort: int, moduleExecutorPath: Path) -> None:
        """
        Register the environment (save its debug port to `wetlandsInstancePath / debug_ports.json`) so that it can be debugged later.

        Args:
                environment: The external environment object to register
                debugPort: The debug port to save
        """
        if environment.process is None:
            return
        wetlands_debug_ports_path = self.wetlandsInstancePath / "debug_ports.json"
        wetlands_debug_ports_path.parent.mkdir(exist_ok=True, parents=True)
        wetlands_debug_ports = {}
        try:
            if wetlands_debug_ports_path.exists():
                with open(wetlands_debug_ports_path, "r") as f:
                    wetlands_debug_ports = json.load(f)
            wetlands_debug_ports[environment.name] = dict(
                debugPort=debugPort, moduleExecutorPath=str(moduleExecutorPath)
            )
            with open(wetlands_debug_ports_path, "w") as f:
                json.dump(wetlands_debug_ports, f)
        except Exception as e:
            e.add_note(f"Error while updating the debug ports file {wetlands_debug_ports_path}.")
            raise e
        return

    def _removeEnvironment(self, environment: Environment) -> None:
        """Remove an environment.

        Args:
                environment: instance to remove.
        """
        if environment.name in self.environments:
            del self.environments[environment.name]
