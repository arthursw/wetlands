import re
import platform
from importlib import metadata
from pathlib import Path
import subprocess
import sys
from typing import Any, Literal, cast, Union
import json5
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
from wetlands._internal.process_logger import ProcessLogger
from wetlands.logger import logger, enable_file_logging, LOG_SOURCE_ENVIRONMENT


class EnvironmentManager:
    """Manages Conda environments using micromamba for isolation and dependency management.

    Attributes:
            main_environment: The main conda environment in which wetlands is installed.
            environments: map of the environments

            settings_manager: SettingsManager(conda_path)
            command_generator: CommandGenerator(settings_manager)
            dependency_manager: DependencyManager(command_generator)
            command_executor: CommandExecutor()
    """

    main_environment: InternalEnvironment
    wetlands_instance_path: Path
    debug: bool

    def __init__(
        self,
        wetlands_instance_path: Path = Path("wetlands"),
        conda_path: str | Path | None = None,
        main_conda_environment_path: Path | None = None,
        debug: bool = False,
        manager="auto",
        log_file_path=Path("wetlands.log"),
    ) -> None:
        """Initializes the EnvironmentManager.

        The wetlands_instance_path directory will contain:
        - logs (managed by logger.py)
        - debug_ports.json (for debug port tracking)
        - conda installation (by default at wetlands_instance_path / "pixi" or "micromamba")

        Args:
                wetlands_instance_path: Path to the folder which will contain the state of this wetlands instance (logs, debug ports stored in debug_ports.json, and conda installation). Defaults to "wetlands".
                conda_path: Path to the micromamba or pixi installation path. If None, defaults to wetlands_instance_path / "pixi". Warning: cannot contain any space character on Windows when using micromamba.
                main_conda_environment_path: Path of the main conda environment in which Wetlands is installed, used to check whether it is necessary to create new environments (only when dependencies are not already available in the main environment). When using Pixi, this must point to the pixi.toml or pyproject.toml file.
                debug: When true, processes will listen to debugpy ( debugpy.listen(0) ) to enable debugging, and their ports will be sorted in  wetlands_instance_path / debug_ports.json
                manager: Use "pixi" to use Pixi as the conda manager, "micromamba" to use Micromamba and "auto" to infer from conda_path (will look for "pixi" or "micromamba" in the path).
                log_file_path: Path to the log file where logs will be stored. Use relative path to wetlands_instance_path, or absolute path. Set to None to disable file logging.
        """

        self.environments: dict[str | Path, Environment] = {}
        self.wetlands_instance_path = cast(Path, wetlands_instance_path).resolve()

        # Set default conda_path if not provided
        if conda_path is None:
            conda_path = self.wetlands_instance_path / "pixi"

        conda_path = Path(conda_path)

        # Initialize logger to use the wetlands_instance_path for logs
        if log_file_path is not None:
            enable_file_logging(
                log_file_path if log_file_path.is_absolute() else self.wetlands_instance_path / log_file_path
            )

        use_pixi = self._init_manager(manager, conda_path)

        if platform.system() == "Windows" and (not use_pixi) and " " in str(conda_path) and not conda_path.exists():
            raise Exception(
                f'The Micromamba path cannot contain any space character on Windows (given path is "{conda_path}").'
            )

        self.main_environment = InternalEnvironment("wetlands_main", main_conda_environment_path, self)
        self.environments["wetlands_main"] = self.main_environment
        self.settings_manager = SettingsManager(conda_path, use_pixi)
        self.debug = debug
        self.install_conda()
        self.command_generator = CommandGenerator(self.settings_manager)
        self.dependency_manager = DependencyManager(self.command_generator)
        self.command_executor = CommandExecutor(self.wetlands_instance_path / "command_executions" if debug else None)

        if log_file_path is not None:
            logger.info("Wetlands initialized at %s", str(self.wetlands_instance_path))

    def _init_manager(self, manager: str, conda_path: Path) -> bool:
        if manager not in ["auto", "pixi", "micromamba"]:
            raise Exception(f'Invalid manager "{manager}", must be "auto", "pixi" or "micromamba".')
        if manager == "auto":
            if "pixi" in str(conda_path).lower():
                use_pixi = True
            elif "micromamba" in str(conda_path).lower():
                use_pixi = False
            else:
                raise Exception(
                    'When using manager="auto", the conda_path must contain either "pixi" or "micromamba" to infer the manager to use.'
                )
        elif manager == "pixi":
            use_pixi = True
        else:
            use_pixi = False
        return use_pixi

    def install_conda(self):
        """Install Pixi or Micromamba (depending on settings_manager.use_pixi)"""

        conda_path, conda_bin_path = self.settings_manager.get_conda_paths()
        if (conda_path / conda_bin_path).exists():
            return []

        conda_path.mkdir(exist_ok=True, parents=True)

        if self.settings_manager.use_pixi:
            installPixi(conda_path, proxies=self.settings_manager.proxies)
        else:
            installMicromamba(conda_path, proxies=self.settings_manager.proxies)
        return

    def set_conda_path(self, conda_path: str | Path, use_pixi: bool = True) -> None:
        """Updates the micromamba path and loads proxy settings if exists.

        Args:
                conda_path: New path to micromamba binary.
                use_pixi: Whether to use Pixi or Micromamba

        Side Effects:
                Updates self.settings_manager.conda_bin_config, and self.settings_manager.proxies from the .mambarc file.
        """
        self.settings_manager.set_conda_path(conda_path, use_pixi)

    def set_proxies(self, proxies: dict[str, str]) -> None:
        """Configures proxy settings for Conda operations.

        Args:
                proxies: Proxy configuration dictionary (e.g., {"http": "...", "https": "..."}).

        Side Effects:
                Updates .mambarc configuration file with proxy settings.
        """
        self.settings_manager.set_proxies(proxies)

    def _remove_channel(self, conda_dependency: str) -> str:
        """Removes channel prefix from a Conda dependency string (e.g., "channel::package" -> "package")."""
        return conda_dependency.split("::")[1] if "::" in conda_dependency else conda_dependency

    def get_installed_packages(self, environment: Environment) -> list[dict[str, str]]:
        """Get the list of the packages installed in the environment

        Args:
                environment: The environment name.

        Returns:
                A list of dict containing the installed packages [{"kind":"conda|pypi", "name": "numpy", "version", "2.1.3"}, ...].
        """
        if self.settings_manager.use_pixi:
            commands = self.command_generator.get_activate_conda_commands()
            commands += [f'{self.settings_manager.conda_bin} list --json --manifest-path "{environment.path}"']
            return self.command_executor.execute_commands_and_get_json_output(commands)
        else:
            commands = self.command_generator.get_activate_environment_commands(environment) + [
                f"{self.settings_manager.conda_bin} list --json",
            ]
            packages = self.command_executor.execute_commands_and_get_json_output(commands)
            for package in packages:
                package["kind"] = "conda"

            commands = self.command_generator.get_activate_environment_commands(environment) + [
                f"pip freeze --all",
            ]
            output = self.command_executor.execute_commands_and_get_output(commands)
            parsed_output = [o.split("==") for o in output if "==" in o]
            packages += [{"name": name, "version": version, "kind": "pypi"} for name, version in parsed_output]
            return packages

    def _check_requirement(
        self, dependency: str, package_manager: Literal["pip", "conda"], installed_packages: list[dict[str, str]]
    ) -> bool:
        """Check if dependency is installed (exists in installed_packages).

        Supports PEP 440 version specifiers like:
        - "numpy" (any version)
        - "numpy==1.20.0" (exact version)
        - "numpy>=1.20,<2.0" (version range)
        - "numpy~=2.28" (compatible release)
        - "numpy!=1.5.0" (any except specific version)
        """
        if package_manager == "conda":
            dependency = self._remove_channel(dependency)

        package_manager_name = "conda" if package_manager == "conda" else "pypi"

        # Parse dependency string to extract package name and version specifier
        # Package name is followed by optional version specifier (starts with ==, >=, <=, >, <, !=, ~=)
        match = re.match(r"^([a-zA-Z0-9._-]+)((?:[<>=!~].*)?)", dependency)
        if not match:
            return False

        package_name = match.group(1)
        version_spec = match.group(2).strip()

        # Find matching package
        for package in installed_packages:
            if package_name != package["name"] or package_manager_name != package["kind"]:
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

    def _environment_validates_requirements(self, environment: Environment, dependencies: Dependencies) -> bool:
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

        conda_dependencies, condaDependenciesNoDeps, hasCondaDependencies = self.dependency_manager.format_dependencies(
            "conda", dependencies, False, False
        )
        pipDependencies, pipDependenciesNoDeps, hasPipDependencies = self.dependency_manager.format_dependencies(
            "pip", dependencies, False, False
        )
        if not hasPipDependencies and not hasCondaDependencies:
            return True

        # Special handling for main environment with None path
        is_main_environment = environment == self.main_environment
        if is_main_environment and environment.path is None:
            if hasCondaDependencies:
                return False
            if hasPipDependencies:
                installed_packages = [
                    {"name": dist.metadata["Name"], "version": dist.version, "kind": "pypi"}
                    for dist in metadata.distributions()
                ]
            else:
                return True
        else:
            # Get installed packages for the environment
            installed_packages = self.get_installed_packages(environment)

        conda_satisfied = (
            all(
                [
                    self._check_requirement(d, "conda", installed_packages)
                    for d in conda_dependencies + condaDependenciesNoDeps
                ]
            )
            if hasCondaDependencies
            else True
        )
        pip_satisfied = (
            all(
                [self._check_requirement(d, "pip", installed_packages) for d in pipDependencies + pipDependenciesNoDeps]
            )
            if hasPipDependencies
            else True
        )

        return conda_satisfied and pip_satisfied

    def environment_exists(self, environment_path: Path) -> bool:
        """Checks if a Conda environment exists.

        Args:
                environment_path: Environment name to check.

        Returns:
                True if environment exists, False otherwise.
        """
        if self.settings_manager.use_pixi:
            conda_meta = environment_path.parent / ".pixi" / "envs" / "default" / "conda-meta"
            return environment_path.is_file() and conda_meta.is_dir()
        else:
            conda_meta = environment_path / "conda-meta"
            return conda_meta.is_dir()

    def _add_debugpy_in_dependencies(self, dependencies: Dependencies) -> None:
        """Add debugpy in the dependencies to be able to debug in debug mode. Does nothing when not in debug mode.

        Args:
                dependencies: Dependencies to install.
        """
        if not self.debug:
            return
        # Check that debugpy is not already in dependencies
        for package_manager in ["pip", "conda"]:
            if package_manager in dependencies:
                for dep in dependencies[package_manager]:
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

    def _parse_dependencies_from_config(
        self,
        config_path: Union[str, Path],
        environment_name: str | None = None,
        optional_dependencies: list[str] | None = None,
    ) -> Dependencies:
        """Parse dependencies from a config file (pixi.toml, pyproject.toml, or environment.yml).

        Args:
                config_path: Path to configuration file
                environment_name: Environment name for pixi/pyproject configs
                optional_dependencies: Optional dependency groups for pyproject configs

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
            file_type = parser.detect_config_file_type(config_path)
        except ValueError as e:
            raise ValueError(f"Unsupported config file: {e}")

        # Validate required parameters for specific file types
        if file_type == "pixi" and not environment_name:
            raise ValueError(
                f"environment_name is required for pixi.toml files. "
                f"Please provide the environment name to extract dependencies from."
            )

        if file_type == "pyproject" and not environment_name and not optional_dependencies:
            raise ValueError(
                f"For pyproject.toml, provide either environment_name (for pixi config) "
                f"or optional_dependencies (for optional dependency groups)."
            )

        # Parse the config file
        return parser.parse(
            config_path,
            environment_name=environment_name,
            optional_dependencies=optional_dependencies,
        )

    def create(
        self,
        name: str,
        dependencies: Union[Dependencies, None] = None,
        additional_install_commands: Commands = {},
        use_existing: bool = False,
    ) -> Environment:
        """Creates a new Conda environment with specified dependencies or returns an existing one.

        Args:
                name: Name for the new environment.
                dependencies: Dependencies to install. Can be one of:
                    - A Dependencies dict: dict(python="3.12.7", conda=["numpy"], pip=["requests"])
                    - None (no dependencies to install)
                additional_install_commands: Platform-specific commands during installation (e.g. {"mac": ["cd ...", "wget https://...", "unzip ..."], "all"=[], ...}).
                use_existing: if True, search through existing environments and return the first one that satisfies the dependencies instead of creating a new one.

        Returns:
                The created or existing environment (ExternalEnvironment if created, or an existing environment if use_existing=True and match found).
        """
        if isinstance(name, Path):
            raise Exception(
                "Environment name cannot be a Path, use EnvironmentManager.load() to load an existing environment."
            )

        # Check if environment already exists on disk
        path = self.settings_manager.get_environment_path_from_name(name)
        if self.environment_exists(path) and name not in self.environments:
            logger.log_environment(f"Loading existing environment '{name}' from '{path}'", name, stage="create")
            self.environments[name] = ExternalEnvironment(name, path, self)

        if name in self.environments:
            logger.debug(f"Environment '{name}' already exists, returning existing instance.")
            return self.environments[name]

        if dependencies is None:
            dependencies = {}
        elif not isinstance(dependencies, dict):
            raise ValueError(f"Unsupported dependencies type: {type(dependencies)}")

        self._add_debugpy_in_dependencies(dependencies)

        # Try to find existing environment if use_existing=True
        if use_existing:
            envs = [self.main_environment] + [env for env in self.environments.values() if env != self.main_environment]
            for env in envs:
                try:
                    if self._environment_validates_requirements(env, dependencies):
                        logger.log_environment(
                            f"Environment '{env.name}' satisfies dependencies for '{name}', returning it.",
                            name,
                            stage="create",
                        )
                        return env
                except Exception as e:
                    logger.debug(f"Error checking environment '{env.name}': {e}")
                    continue

        # Create new environment
        python_version = dependencies.get("python", "").replace("=", "")
        match = re.search(r"(\d+)\.(\d+)", python_version)
        if match and (int(match.group(1)) < 3 or int(match.group(2)) < 9):
            raise Exception("Python version must be greater than 3.8")
        python_requirement = " python=" + (python_version if len(python_version) > 0 else platform.python_version())
        create_env_commands = self.command_generator.get_activate_conda_commands()

        if self.settings_manager.use_pixi:
            manifest_path = path
            if not manifest_path.exists():
                platform_args = f"--platform win-64" if platform.system() == "Windows" else ""
                create_env_commands += [
                    f'{self.settings_manager.conda_bin} init --no-progress {platform_args} "{manifest_path.parent}"'
                ]
            create_env_commands += [
                f'{self.settings_manager.conda_bin} add --no-progress --manifest-path "{manifest_path}" {python_requirement}'
            ]
        else:
            create_env_commands += [f"{self.settings_manager.conda_bin_config} create -n {name}{python_requirement} -y"]
        environment = ExternalEnvironment(name, path, self)
        self.environments[name] = environment
        create_env_commands += self.dependency_manager.get_install_dependencies_commands(environment, dependencies)
        create_env_commands += self.command_generator.get_commands_for_current_platform(additional_install_commands)

        logger.log_environment(f"Creating environment '{name}'", name, stage="create")
        log_context = {"log_source": LOG_SOURCE_ENVIRONMENT, "env_name": name, "stage": "install"}
        self.command_executor.execute_commands(create_env_commands, wait=True, log_context=log_context)
        logger.log_environment(f"Environment '{name}' created successfully", name, stage="create")
        return self.environments[name]

    def create_from_config(
        self,
        name: str,
        config_path: str | Path,
        optional_dependencies: list[str] | None = None,
        additional_install_commands: Commands = {},
        use_existing: bool = False,
    ) -> Environment:
        """Creates a new Conda environment from a config file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt) or returns an existing one.

        Args:
                name: Name for the new environment.
                config_path: Path to configuration file (pixi.toml, pyproject.toml, environment.yml, or requirements.txt).
                optional_dependencies: List of optional dependency groups to extract from pyproject.toml.
                additional_install_commands: Platform-specific commands during installation.
                use_existing: if True, search through existing environments and return the first one that satisfies the dependencies instead of creating a new one.

        Returns:
                The created or existing environment (ExternalEnvironment if created, or an existing environment if use_existing=True and match found).
        """

        # Parse config file
        dependencies = self._parse_dependencies_from_config(
            config_path, environment_name=name, optional_dependencies=optional_dependencies
        )

        # Use create() with parsed dependencies
        return self.create(name, dependencies, additional_install_commands, use_existing)

    def load(
        self,
        name: str,
        environment_path: Path,
    ) -> Environment:
        """Load an existing Conda environment from disk.

        Args:
                name: Name for the environment instance.
                environment_path: Path to an existing Conda environment, or the folder containing the pixi.toml/pyproject.toml when using Pixi.

        Returns:
                The loaded environment (ExternalEnvironment if using Pixi or micromamba with a path, InternalEnvironment otherwise).

        Raises:
                Exception: If the environment does not exist.
        """
        environment_path = environment_path.resolve()

        if not self.environment_exists(environment_path):
            raise Exception(f"The environment {environment_path} was not found.")

        if name not in self.environments:
            self.environments[name] = ExternalEnvironment(name, environment_path, self)
        return self.environments[name]

    def install(
        self, environment: Environment, dependencies: Dependencies, additional_install_commands: Commands = {}
    ) -> list[str]:
        """Installs dependencies.
        See [`EnvironmentManager.create`][wetlands.environment_manager.EnvironmentManager.create] for more details on the ``dependencies`` and ``additional_install_commands`` parameters.

        Args:
                environment_name: The environment to install dependencies.
                dependencies: Dependencies to install.
                additional_install_commands: Platform-specific commands during installation.

        Returns:
                Output lines of the installation commands.
        """
        if environment == self.main_environment and self.settings_manager.use_pixi:
            raise Exception("Cannot install packages in an InternalEnvironment when using Pixi.")
        if environment == self.main_environment and environment.path is None:
            raise Exception("Cannot install packages in an InternalEnvironment with no conda path.")

        install_commands = self.dependency_manager.get_install_dependencies_commands(environment, dependencies)
        install_commands += self.command_generator.get_commands_for_current_platform(additional_install_commands)

        logger.log_environment(
            f"Installing dependencies in environment '{environment.name}'", environment.name, stage="install"
        )
        log_context = {"log_source": LOG_SOURCE_ENVIRONMENT, "env_name": environment.name, "stage": "install"}
        return self.command_executor.execute_commands_and_get_output(install_commands, log_context=log_context)

    def execute_commands(
        self,
        environment: Environment,
        commands: Commands,
        additional_activate_commands: Commands = {},
        popen_kwargs: dict[str, Any] = {},
        wait: bool = False,
        log_context: dict[str, Any] | None = None,
        log: bool = True,
    ) -> subprocess.Popen:
        """Executes the given commands in the given environment.

        Args:
                environment: The environment in which to execute commands.
                commands: The commands to execute in the environment.
                additional_activate_commands: Platform-specific activation commands.
                popen_kwargs: Keyword arguments for subprocess.Popen() (see [Popen documentation](https://docs.python.org/3/library/subprocess.html#popen-constructor)). Defaults are: dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, encoding="utf-8", errors="replace", bufsize=1).
                log_context: Optional context dict to attach to logs via ProcessLogger.
                log: Whether to log the process output.

        Returns:
                The launched process.
        """
        activate_commands = self.command_generator.get_activate_environment_commands(
            environment, additional_activate_commands
        )
        platform_commands = self.command_generator.get_commands_for_current_platform(commands)
        return self.command_executor.execute_commands(
            activate_commands + platform_commands,
            popen_kwargs=popen_kwargs,
            wait=wait,
            log_context=log_context,
            log=log,
        )

    def get_process_logger(self, process: subprocess.Popen) -> ProcessLogger:
        """Get a ProcessLogger for the given process.

        Args:
                process: The process to create a ProcessLogger for.
        Returns:
                The created ProcessLogger.
        """
        return self.command_executor.get_process_logger(process)

    def register_environment(
        self, environment: ExternalEnvironment, debug_port: int, module_executor_path: Path
    ) -> None:
        """
        Register the environment (save its debug port to `wetlands_instance_path / debug_ports.json`) so that it can be debugged later.

        Args:
                environment: The external environment object to register
                debug_port: The debug port to save
        """
        if environment.process is None:
            return
        wetlands_debug_ports_path = self.wetlands_instance_path / "debug_ports.json"
        wetlands_debug_ports_path.parent.mkdir(exist_ok=True, parents=True)
        wetlands_debug_ports = {}
        try:
            if wetlands_debug_ports_path.exists():
                with open(wetlands_debug_ports_path, "r") as f:
                    wetlands_debug_ports = json5.load(f)
            wetlands_debug_ports[environment.name] = dict(
                debug_port=debug_port, module_executor_path=module_executor_path.as_posix()
            )
            with open(wetlands_debug_ports_path, "w") as f:
                json5.dump(wetlands_debug_ports, f, indent=4, quote_keys=True)
        except Exception as e:
            e.add_note(f"Error while updating the debug ports file {wetlands_debug_ports_path}.")
            raise e
        return

    def _remove_environment(self, environment: Environment) -> None:
        """Remove an environment.

        Args:
                environment: instance to remove.
        """
        if environment.name in self.environments:
            del self.environments[environment.name]

    def exit(self) -> None:
        """Exit all environments"""
        for env in self.environments.values():
            env.exit()
