import json
import re
import platform
from importlib import metadata
from pathlib import Path
import subprocess
import sys
from typing import Any, Literal, cast, Union
from venv import logger
from packaging.specifiers import SpecifierSet
from packaging.version import Version, InvalidVersion

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
        wetlandsInstancePath: Path = Path("wetlands"),
        condaPath: str | Path | None = None,
        mainCondaEnvironmentPath: Path | None = None,
        debug: bool = False,
        manager="auto",
    ) -> None:
        """Initializes the EnvironmentManager.

        The wetlandsInstancePath directory will contain:
        - logs (managed by logger.py)
        - debug_ports.json (for debug port tracking)
        - conda installation (by default at wetlandsInstancePath / "pixi" or "micromamba")

        Args:
                wetlandsInstancePath: Path to the folder which will contain the state of this wetlands instance (logs, debug ports stored in debug_ports.json, and conda installation). Defaults to "wetlands".
                condaPath: Path to the micromamba or pixi installation path. If None, defaults to wetlandsInstancePath / "pixi". Warning: cannot contain any space character on Windows when using micromamba.
                mainCondaEnvironmentPath: Path of the main conda environment in which Wetlands is installed, used to check whether it is necessary to create new environments (only when dependencies are not already available in the main environment). When using Pixi, this must point to the pixi.toml or pyproject.toml file.
                debug: When true, processes will listen to debugpy ( debugpy.listen(0) ) to enable debugging, and their ports will be sorted in  wetlandsInstancePath / debug_ports.json
                manager: Use "pixi" to use Pixi as the conda manager, "micromamba" to use Micromamba and "auto" to infer from condaPath (will look for "pixi" or "micromamba" in the path).
        """
        from wetlands.logger import setLogFilePath

        self.environments: dict[str | Path, Environment] = {}
        self.wetlandsInstancePath = cast(Path, wetlandsInstancePath).resolve()

        # Set default condaPath if not provided
        if condaPath is None:
            condaPath = self.wetlandsInstancePath / "pixi"

        condaPath = Path(condaPath)

        # Initialize logger to use the wetlandsInstancePath for logs
        setLogFilePath(self.wetlandsInstancePath / "wetlands.log")

        usePixi = self._initManager(manager, condaPath)

        if platform.system() == "Windows" and (not usePixi) and " " in str(condaPath) and not condaPath.exists():
            raise Exception(
                f'The Micromamba path cannot contain any space character on Windows (given path is "{condaPath}").'
            )

        self.mainEnvironment = InternalEnvironment("wetlands_main", mainCondaEnvironmentPath, self)
        self.environments["wetlands_main"] = self.mainEnvironment
        self.settingsManager = SettingsManager(condaPath, usePixi)
        self.debug = debug
        self.installConda()
        self.commandGenerator = CommandGenerator(self.settingsManager)
        self.dependencyManager = DependencyManager(self.commandGenerator)
        self.commandExecutor = CommandExecutor()

    def _initManager(self, manager: str, condaPath: Path) -> bool:
        if manager not in ["auto", "pixi", "micromamba"]:
            raise Exception(f'Invalid manager "{manager}", must be "auto", "pixi" or "micromamba".')
        if manager == "auto":
            if "pixi" in str(condaPath).lower():
                usePixi = True
            elif "micromamba" in str(condaPath).lower():
                usePixi = False
            else:
                raise Exception(
                    'When using manager="auto", the condaPath must contain either "pixi" or "micromamba" to infer the manager to use.'
                )
        elif manager == "pixi":
            usePixi = True
        else:
            usePixi = False
        return usePixi

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

    def getInstalledPackages(self, environment: Environment) -> list[dict[str, str]]:
        """Get the list of the packages installed in the environment

        Args:
                environment: The environment name.

        Returns:
                A list of dict containing the installed packages [{"kind":"conda|pypi", "name": "numpy", "version", "2.1.3"}, ...].
        """
        if self.settingsManager.usePixi:
            commands = self.commandGenerator.getActivateCondaCommands()
            commands += [f'{self.settingsManager.condaBin} list --json --manifest-path "{environment.path}"']
            return self.commandExecutor.executeCommandAndGetJsonOutput(commands, log=False)
        else:
            commands = self.commandGenerator.getActivateEnvironmentCommands(environment) + [
                f"{self.settingsManager.condaBin} list --json",
            ]
            packages = self.commandExecutor.executeCommandAndGetJsonOutput(commands, log=False)
            for package in packages:
                package["kind"] = "conda"

            commands = self.commandGenerator.getActivateEnvironmentCommands(environment) + [
                f"pip freeze --all",
            ]
            output = self.commandExecutor.executeCommandsAndGetOutput(commands, log=False)
            parsedOutput = [o.split("==") for o in output if "==" in o]
            packages += [{"name": name, "version": version, "kind": "pypi"} for name, version in parsedOutput]
            return packages

    def _checkRequirement(
        self, dependency: str, packageManager: Literal["pip", "conda"], installedPackages: list[dict[str, str]]
    ) -> bool:
        """Check if dependency is installed (exists in installedPackages).

        Supports PEP 440 version specifiers like:
        - "numpy" (any version)
        - "numpy==1.20.0" (exact version)
        - "numpy>=1.20,<2.0" (version range)
        - "numpy~=2.28" (compatible release)
        - "numpy!=1.5.0" (any except specific version)
        """
        if packageManager == "conda":
            dependency = self._removeChannel(dependency)

        packageManagerName = "conda" if packageManager == "conda" else "pypi"

        # Parse dependency string to extract package name and version specifier
        # Package name is followed by optional version specifier (starts with ==, >=, <=, >, <, !=, ~=)
        match = re.match(r"^([a-zA-Z0-9._-]+)((?:[<>=!~].*)?)", dependency)
        if not match:
            return False

        package_name = match.group(1)
        version_spec = match.group(2).strip()

        # Find matching package
        for package in installedPackages:
            if package_name != package["name"] or packageManagerName != package["kind"]:
                continue

            # If no version specified, just match on name
            if not version_spec:
                return True

            # Check version against specifier using packaging library
            try:
                installed_version = Version(package["version"])
                specifier_set = SpecifierSet(version_spec)
                if installed_version in specifier_set:
                    return True
            except InvalidVersion:
                # If version parsing fails, continue to next package
                continue

        return False

    def _environmentValidatesRequirements(self, environment: Environment, dependencies: Dependencies) -> bool:
        """Verifies if all specified dependencies are installed in the given environment.

        Applies special handling for main environment with None path (uses metadata.distributions() for pip packages).

        Args:
                environment: The environment to check.
                dependencies: Dependencies to verify.

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

        # Special handling for main environment with None path
        isMainEnvironment = environment == self.mainEnvironment
        if isMainEnvironment and environment.path is None:
            if hasCondaDependencies:
                return False
            if hasPipDependencies:
                installedPackages = [
                    {"name": dist.metadata["Name"], "version": dist.version, "kind": "pypi"}
                    for dist in metadata.distributions()
                ]
            else:
                return True
        else:
            # Get installed packages for the environment
            installedPackages = self.getInstalledPackages(environment)

        condaSatisfied = (
            all(
                [
                    self._checkRequirement(d, "conda", installedPackages)
                    for d in condaDependencies + condaDependenciesNoDeps
                ]
            )
            if hasCondaDependencies
            else True
        )
        pipSatisfied = (
            all([self._checkRequirement(d, "pip", installedPackages) for d in pipDependencies + pipDependenciesNoDeps])
            if hasPipDependencies
            else True
        )

        return condaSatisfied and pipSatisfied

    def environmentExists(self, environmentPath: Path) -> bool:
        """Checks if a Conda environment exists.

        Args:
                environmentPath: Environment name to check.

        Returns:
                True if environment exists, False otherwise.
        """
        if self.settingsManager.usePixi:
            condaMeta = environmentPath.parent / ".pixi" / "envs" / "default" / "conda-meta"
            return environmentPath.is_file() and condaMeta.is_dir()
        else:
            condaMeta = environmentPath / "conda-meta"
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
        useExisting: bool = False,
    ) -> Environment:
        """Creates a new Conda environment with specified dependencies or returns an existing one.

        Args:
                name: Name for the new environment.
                dependencies: Dependencies to install. Can be one of:
                    - A Dependencies dict: dict(python="3.12.7", conda=["numpy"], pip=["requests"])
                    - None (no dependencies to install)
                additionalInstallCommands: Platform-specific commands during installation (e.g. {"mac": ["cd ...", "wget https://...", "unzip ..."], "all"=[], ...}).
                useExisting: if True, search through existing environments and return the first one that satisfies the dependencies instead of creating a new one.

        Returns:
                The created or existing environment (ExternalEnvironment if created, or an existing environment if useExisting=True and match found).
        """
        if isinstance(name, Path):
            raise Exception(
                "Environment name cannot be a Path, use EnvironmentManager.load() to load an existing environment."
            )

        if name in self.environments:
            logger.debug(f"Environment '{name}' already exists, returning existing instance.")
            return self.environments[name]

        if dependencies is None:
            dependencies = {}
        elif not isinstance(dependencies, dict):
            raise ValueError(f"Unsupported dependencies type: {type(dependencies)}")

        self._addDebugpyInDependencies(dependencies)

        # Try to find existing environment if useExisting=True
        if useExisting:
            envs = [self.mainEnvironment] + [env for env in self.environments.values() if env != self.mainEnvironment]
            for env in envs:
                try:
                    if self._environmentValidatesRequirements(env, dependencies):
                        logger.debug(f"Environment '{env.name}' satisfies dependencies for '{name}', returning it.")
                        return env
                except Exception as e:
                    logger.debug(f"Error checking environment '{env.name}': {e}")
                    continue

        # Create new environment
        pythonVersion = dependencies.get("python", "").replace("=", "")
        match = re.search(r"(\d+)\.(\d+)", pythonVersion)
        if match and (int(match.group(1)) < 3 or int(match.group(2)) < 9):
            raise Exception("Python version must be greater than 3.8")
        pythonRequirement = " python=" + (pythonVersion if len(pythonVersion) > 0 else platform.python_version())
        createEnvCommands = self.commandGenerator.getActivateCondaCommands()
        path = self.settingsManager.getEnvironmentPathFromName(name)
        if self.settingsManager.usePixi:
            manifestPath = path
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
        environment = ExternalEnvironment(name, path, self)
        self.environments[name] = environment
        createEnvCommands += self.dependencyManager.getInstallDependenciesCommands(environment, dependencies)
        createEnvCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
        self.commandExecutor.executeCommandsAndGetOutput(createEnvCommands)
        return self.environments[name]

    def createFromConfig(
        self,
        name: str,
        configPath: str | Path,
        optionalDependencies: list[str] | None = None,
        additionalInstallCommands: Commands = {},
        useExisting: bool = False,
    ) -> Environment:
        """Creates a new Conda environment from a config file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt) or returns an existing one.

        Args:
                name: Name for the new environment.
                configPath: Path to configuration file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt).
                optionalDependencies: List of optional dependency groups to extract from pyproject.toml.
                additionalInstallCommands: Platform-specific commands during installation.
                useExisting: if True, search through existing environments and return the first one that satisfies the dependencies instead of creating a new one.

        Returns:
                The created or existing environment (ExternalEnvironment if created, or an existing environment if useExisting=True and match found).
        """

        # Parse config file
        dependencies = self._parseDependenciesFromConfig(
            configPath, environmentName=name, optionalDependencies=optionalDependencies
        )

        # Use create() with parsed dependencies
        return self.create(name, dependencies, additionalInstallCommands, useExisting)

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

        if name not in self.environments:
            self.environments[name] = ExternalEnvironment(name, environmentPath, self)
        return self.environments[name]

    def install(
        self, environment: Environment, dependencies: Dependencies, additionalInstallCommands: Commands = {}
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
        if environment == self.mainEnvironment and self.settingsManager.usePixi:
            raise Exception("Cannot install packages in an InternalEnvironment when using Pixi.")
        if environment == self.mainEnvironment and environment.path is None:
            raise Exception("Cannot install packages in an InternalEnvironment with no conda path.")

        installCommands = self.dependencyManager.getInstallDependenciesCommands(environment, dependencies)
        installCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
        return self.commandExecutor.executeCommandsAndGetOutput(installCommands)

    def executeCommands(
        self,
        environment: Environment,
        commands: Commands,
        additionalActivateCommands: Commands = {},
        popenKwargs: dict[str, Any] = {},
        wait: bool = False,
    ) -> subprocess.Popen:
        """Executes the given commands in the given environment.

        Args:
                environment: The environment in which to execute commands.
                commands: The commands to execute in the environment.
                additionalActivateCommands: Platform-specific activation commands.
                popenKwargs: Keyword arguments for subprocess.Popen() (see [Popen documentation](https://docs.python.org/3/library/subprocess.html#popen-constructor)). Defaults are: dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, encoding="utf-8", errors="replace", bufsize=1).

        Returns:
                The launched process.
        """
        activateCommands = self.commandGenerator.getActivateEnvironmentCommands(environment, additionalActivateCommands)
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
                debugPort=debugPort, moduleExecutorPath=moduleExecutorPath.as_posix()
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
