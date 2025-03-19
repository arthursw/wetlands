import re
import platform
import tempfile
import subprocess
import threading
from importlib import metadata
from pathlib import Path

from cema import logger
from cema.environment import Environment, DirectEnvironment, ClientEnvironment
from cema.dependency_manager import Dependencies, DependencyManager
from cema.command_executor import CommandExecutor
from cema.command_generator import CommandGenerator
from cema.settings_manager import SettingsManager

class EnvironmentManager:
	"""Manages Conda environments using micromamba for isolation and dependency management.
	
	Attributes:
		condaBin (str): Name of the micromamba binary.
		condaBinConfig (str): Configuration command for micromamba.
		environments (dict[str, Environment]): Active environments managed by this instance.
		proxies (dict[str, str] | None): Proxy configuration for network requests.
	"""

	environments: dict[str, Environment] = {}

	def __init__(self, condaPath: str | Path = Path("micromamba")) -> None:
		"""Initializes the EnvironmentManager with a micromamba path.
		
		Args:
			condaPath: Path to the micromamba binary. Defaults to "micromamba".
		"""
		self.settingsManager = SettingsManager(condaPath)
		self.dependencyManager = DependencyManager(self.settingsManager)
		self.commandGenerator = CommandGenerator(self.settingsManager, self.dependencyManager)
		self.commandExecutor = CommandExecutor()

	def setCondaPath(self, condaPath: str | Path) -> None:
		"""Updates the micromamba path and loads proxy settings if exists.
		
		Args:
			condaPath: New path to micromamba binary.
			
		Side Effects:
			Updates condaBinConfig and proxies from the .mambarc file.
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

	def dependenciesAreInstalled(self, environment: str, dependencies: Dependencies) -> bool:
		"""Verifies if all specified dependencies are installed in the given environment.
		
		Args:
			environment: Target environment name.
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

		installedDependencies = (
			self.environments[environment].installedDependencies
			if environment in self.environments
			else {}
		)
		if hasCondaDependencies:
			if "conda" not in installedDependencies:
				installedDependencies["conda"] = self.commandExecutor.executeCommandAndGetOutput(
					self.commandGenerator.getActivateCondaComands()
					+ [
						f"{self.settingsManager.condaBin} activate {environment}",
						f"{self.settingsManager.condaBin} list -y",
					],
					log=False,
				)
			if not all(
				[
					self._removeChannel(d) in installedDependencies["conda"]
					for d in condaDependencies + condaDependenciesNoDeps
				]
			):
				return False
		if not hasPipDependencies:
			return True

		if "pip" not in installedDependencies:
			if environment is not None:
				installedDependencies["pip"] = self.commandExecutor.executeCommandAndGetOutput(
					self.commandGenerator.getActivateCondaComands()
					+ [f"{self.settingsManager.condaBin} activate {environment}", f"pip freeze"],
					log=False,
				)
			else:
				installedDependencies["pip"] = [
					f"{dist.metadata['Name']}=={dist.version}"
					for dist in metadata.distributions()
				]

		return all(
			[
				d in installedDependencies["pip"]
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
		condaMeta = Path(self.settingsManager.condaPath) / "envs" / environment / "conda-meta"
		return condaMeta.is_dir()

	def install(self, environment: str, dependencies: Dependencies, additionalInstallCommands: dict[str, list[str]] = {}) -> None:
		"""Installs dependencies into a Conda environment. 
		See :meth:`EnvironmentManager.create` for more details on the ``dependencies`` and ``additionalInstallCommands`` parameters.
		
		Args:
			environment: Target environment name.
			dependencies: Dependencies to install.
			additionalInstallCommands: Platform-specific commands during installation.
		"""

		installCommands = self.commandGenerator.getActivateCondaComands()
		installCommands += self.commandGenerator.getInstallDependenciesCommands(environment, dependencies)
		installCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
		self.commandExecutor.executeCommandAndGetOutput(installCommands)
		self.environments[environment].installedDependencies = {}

	def create(
		self,
		environment: str,
		dependencies: Dependencies,
		additionalInstallCommands: dict[str, list[str]] = {},
		mainEnvironment: str | None = None,
		errorIfExists: bool = False,
	) -> bool:
		"""Creates a new Conda environment with specified dependencies.
		
		Args:
			environment: Name for the new environment.
			dependencies: Dependencies to install, in the form dict(python='3.12.7', conda=['conda-forge::pyimagej==1.5.0', dict(name='openjdk=11', platforms=['osx-64', 'osx-arm64', 'win-64', 'linux-64'], dependencies=True, optional=False)], pip=['numpy==1.26.4']).
			additionalInstallCommands: Platform-specific commands during installation (e.g. {'mac': ['cd ...', 'wget https://...', 'unzip ...'], 'all'=[], ...}).
			mainEnvironment: Environment to check for existing dependencies.
			errorIfExists: Whether to raise error if environment exists.
			
		Returns:
			True if environment was created, False if dependencies already met.
			
		Raises:
			Exception: For existing environments when errorIfExists=True.
		"""
		if mainEnvironment is not None and self.dependenciesAreInstalled(
			mainEnvironment, dependencies
		):
			return False
		if self.environmentExists(environment):
			if errorIfExists:
				raise Exception(f"Error: the environment {environment} already exists.")
			else:
				return True
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
		createEnvCommands = self.commandGenerator.getActivateCondaComands()
		createEnvCommands += [f"{self.settingsManager.condaBinConfig} create -n {environment}{pythonRequirement} -y"]
		createEnvCommands += self.dependencyManager.getInstallDependenciesCommands(environment, dependencies)
		createEnvCommands += self.commandGenerator.getCommandsForCurrentPlatform(additionalInstallCommands)
		self.commandExecutor.executeCommandAndGetOutput(createEnvCommands)
		return True

	def environmentIsLaunched(self, environment: str) -> bool:
		"""Checks if an environment is currently running.
		
		Args:
			environment: Environment name to check.
			
		Returns:
			True if environment process is active.
		"""
		return (
			environment in self.environments
			and self.environments[environment].launched()
		)

	def logOutput(self, process: subprocess.Popen, stopEvent: threading.Event) -> None:
		"""Logs output from a subprocess until stopped.
		
		Args:
			process: Subprocess to monitor.
			stopEvent: Event to signal stopping logging.
		"""
		if process.stdout is None or process.stdout.readline is None: return
		try:
			for line in iter(process.stdout.readline, ""):  # Use iter to avoid buffering issues
				if stopEvent is not None and stopEvent.is_set():
					break
				logger.info(line.strip())
		except Exception as e:
			logger.error(f"Exception in logging thread: {e}")
		return
	
	def executeCommandsInEnvironment(
		self,
		environment: str,
		commands: list[str],
		additionalActivateCommands: dict[str, list[str]] = {},
		environmentVariables: dict[str, str] | None = None,
	) -> subprocess.Popen:
		"""Executes the given commands in the specified environment.
		
		Args:
			environment: Environment name to launch.
			commands: The commands to execute in the environment.
			additionalActivateCommands: Platform-specific activation commands.
			environmentVariables: Environment variables for the process.
			
		Returns:
			The launched process.
		"""
		commands = self.commandGenerator.getActivateEnvironmentCommands(environment, additionalActivateCommands) + commands
		return self.commandExecutor.executeCommands(commands, env=environmentVariables)

	def launch(
		self,
		environment: str,
		additionalActivateCommands: dict[str, list[str]] = {},
		environmentVariables: dict[str, str] | None = None,
		logOutput: bool = True,
	) -> Environment:
		"""Launches a server listening for orders in the specified environment.
		
		Args:
			environment: Environment name to launch.
			additionalActivateCommands: Platform-specific activation commands.
			environmentVariables: Environment variables for the process.
			logOutput: Enable logging of process output.
			
		Returns:
			ClientEnvironment instance for the launched process.
		"""
		if self.environmentIsLaunched(environment):
			return self.environments[environment]

		moduleCallerPath = Path(__file__).parent.resolve() / "module_caller.py"
		process = self.executeCommandsInEnvironment(environment, [f'python -u "{moduleCallerPath}" {environment}'], additionalActivateCommands, environmentVariables)

		port = -1
		if process.stdout is not None:
			try:
				for line in process.stdout:
					logger.info(line.strip())
					if line.strip().startswith("Listening port "):
						port = int(line.strip().replace("Listening port ", ""))
						break
			except Exception as e:
				process.stdout.close()
				raise e
		if process.poll() is not None:
			if process.stdout is not None:
				process.stdout.close()
			raise Exception(f"Process exited with return code {process.returncode}.")
		ce = ClientEnvironment(environment, port, process)
		if logOutput:
			threading.Thread(
				target=self.logOutput, args=[process, ce.stopEvent]
			).start()
		self.environments[environment] = ce
		ce.initialize()
		return ce

	def createAndLaunch(
		self,
		environment: str,
		dependencies: Dependencies,
		environmentVariables: dict[str, str] | None = None,
		additionalInstallCommands: dict[str, list[str]] = {},
		additionalActivateCommands: dict[str, list[str]] = {},
		mainEnvironment: str | None = None,
	) -> Environment:
		"""Creates and/or launches an environment.
		
		Args:
			environment: Environment name.
			dependencies: Dependencies to install.
			environmentVariables: Environment variables for the process.
			additionalInstallCommands: Platform-specific install commands.
			additionalActivateCommands: Platform-specific activation commands.
			mainEnvironment: Environment to check for existing dependencies.
			
		Returns:
			Environment instance (ClientEnvironment or DirectEnvironment).
		"""
		environmentIsRequired = self.create(
			environment,
			dependencies,
			additionalInstallCommands=additionalInstallCommands,
			mainEnvironment=mainEnvironment,
		)
		if environmentIsRequired:
			return self.launch(
				environment,
				environmentVariables=environmentVariables,
				additionalActivateCommands=additionalActivateCommands,
			)
		else:
			return DirectEnvironment(environment)

	def exit(self, environment: Environment | str) -> None:
		"""Terminates an environment process and cleans up.
		
		Args:
			environment: Environment name or instance to terminate.
		"""
		environmentName = (
			environment if isinstance(environment, str) else environment.name
		)
		if environmentName in self.environments:
			self.environments[environmentName]._exit()
			del self.environments[environmentName]