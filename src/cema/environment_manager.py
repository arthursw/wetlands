import re
import platform
import tempfile
import subprocess
import threading
from importlib import metadata
from pathlib import Path

from cema import logger
from cema.environment import Environment, DirectEnvironment, ClientEnvironment
from cema.dependencies import Dependencies, Dependency
from cema.exceptions import IncompatibilityException

class EnvironmentManager:
	"""Manages Conda environments using micromamba for isolation and dependency management.
	
	Attributes:
		condaBin (str): Name of the micromamba binary.
		condaBinConfig (str): Configuration command for micromamba.
		environments (dict[str, Environment]): Active environments managed by this instance.
		proxies (dict[str, str] | None): Proxy configuration for network requests.
	"""

	# Default settings for conda binaries
	condaBin = "micromamba"
	condaBinConfig = "micromamba --rc-file ~/.mambarc"

	environments: dict[str, Environment] = {}
	proxies: dict[str, str] | None = None

	def __init__(self, condaPath: str | Path = Path("micromamba")) -> None:
		"""Initializes the EnvironmentManager with a micromamba path.
		
		Args:
			condaPath: Path to the micromamba binary. Defaults to "micromamba".
		"""
		self.setCondaPath(condaPath)

	def setCondaPath(self, condaPath: str | Path) -> None:
		"""Updates the micromamba path and loads proxy settings if exists.
		
		Args:
			condaPath: New path to micromamba binary.
			
		Side Effects:
			Updates condaBinConfig and proxies from the .mambarc file.
		"""
		self.condaPath = Path(condaPath).resolve()
		condaConfigPath = self.condaPath / ".mambarc"
		self.condaBinConfig = f'{self.condaBin} --rc-file "{condaConfigPath}"'
		import yaml

		if condaConfigPath.exists():
			with open(condaConfigPath, "r") as f:
				condaConfig = yaml.safe_load(f)
				if "proxies" in condaConfig:
					self.proxies = condaConfig["proxies"]

	def _insertCommandErrorChecks(self, commands: list[str]) -> list[str]:
		"""Inserts error checking commands after each shell command.
		
		Args:
			commands: List of original shell commands.
			
		Returns:
			Augmented command list with error checking logic.
		"""
		commandsWithChecks = []
		errorMessage = "Errors encountered during execution. Exited with status:"
		windowsChecks = ["", "if (! $?) { exit 1 } "]
		posixChecks = [
			"",
			"return_status=$?",
			"if [ $return_status -ne 0 ]",
			"then",
			f'    echo "{errorMessage} $return_status"',
			"    exit 1",
			"fi",
			"",
		]
		checks = windowsChecks if self._isWindows() else posixChecks
		for command in commands:
			commandsWithChecks.append(command)
			commandsWithChecks += checks
		return commandsWithChecks

	def _getOutput(
		self, process: subprocess.Popen, commands: list[str], log: bool = True, strip: bool = True
	) -> list[str]:
		"""Captures and processes output from a subprocess.
		
		Args:
			process: Subprocess to monitor.
			commands: Commands that were executed (for error messages).
			log: Whether to log output lines.
			strip: Whether to strip whitespace from output lines.
			
		Returns:
			Putput lines.
			
		Raises:
			Exception: If CondaSystemExit is detected or non-zero exit code.
		"""
		prefix: str = "[...] " if len(str(commands)) > 150 else ""
		commandString = (
			prefix + str(commands)[-150:]
			if commands is not None and len(commands) > 0
			else ""
		)
		outputs = []
		if process.stdout is not None:
			for line in process.stdout:
				if strip:
					line = line.strip()
				if log:
					logger.info(line)
				if "CondaSystemExit" in line:
					process.kill()
					raise Exception(f'The execution of the commands "{commandString}" failed.')
				outputs.append(line)
		process.wait()
		if process.returncode != 0:
			raise Exception(f'The execution of the commands "{commandString}" failed.')
		return outputs

	def setProxies(self, proxies: dict[str, str]) -> None:
		"""Configures proxy settings for Conda operations.
		
		Args:
			proxies: Proxy configuration dictionary (e.g., {'http': '...', 'https': '...'}).
			
		Side Effects:
			Updates .mambarc configuration file with proxy settings.
		"""
		self.proxies = proxies
		condaConfigPath = self.condaPath / ".mambarc"
		condaConfig = dict()
		import yaml

		if condaConfigPath.exists():
			with open(condaConfigPath, "r") as f:
				condaConfig = yaml.safe_load(f)
			condaConfig["proxy_servers"] = proxies
			with open(condaConfigPath, "w") as f:
				yaml.safe_dump(condaConfig, f)

	def executeCommands(
		self,
		commands: list[str],
		env: dict[str, str] | None = None,
		exitIfCommandError: bool = True
	) -> subprocess.Popen:
		"""Executes shell commands in a subprocess.
		
		Args:
			commands: List of shell commands to execute.
			env: Environment variables for the subprocess.
			exitIfCommandError: Whether to insert error checking.
			
		Returns:
			Subprocess handle for the executed commands.
		"""
		logger.debug(f"Execute commands: {commands}")
		with tempfile.NamedTemporaryFile(
			suffix=".ps1" if self._isWindows() else ".sh", mode="w", delete=False
		) as tmp:
			if exitIfCommandError:
				commands = self._insertCommandErrorChecks(commands)
			tmp.write("\n".join(commands))
			tmp.flush()
			tmp.close()
			executeFile = (
				[
					"powershell", "-WindowStyle", "Hidden", "-NoProfile", "-ExecutionPolicy", "ByPass", "-File", tmp.name
				]
				if self._isWindows()
				else ["/bin/bash", tmp.name]
			)
			if not self._isWindows():
				subprocess.run(["chmod", "u+x", tmp.name])
			logger.debug(f"Script file: {tmp.name}")
			process = subprocess.Popen(
				executeFile,
				env=env,
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				stdin=subprocess.DEVNULL,
				encoding="utf-8",
				errors="replace",
				bufsize=1,
			)
			return process

	def executeCommandAndGetOutput(
		self,
		commands: list[str],
		env: dict[str, str] | None = None,
		exitIfCommandError: bool = True,
		log: bool = True,
	) -> list[str]:
		"""Executes commands and captures their output.
		
		Args:
			commands: Shell commands to execute.
			env: Environment variables for the subprocess.
			exitIfCommandError: Enable automatic error checking.
			log: Enable logging of command output.
			
		Returns:
			Output lines.
		"""
		rawCommands = commands.copy()
		process = self.executeCommands(commands, env, exitIfCommandError)
		with process:
			return self._getOutput(process, rawCommands, log=log)
		return

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
			self._formatDependencies("conda", dependencies, False)
		)
		pipDependencies, pipDependenciesNoDeps, hasPipDependencies = (
			self._formatDependencies("pip", dependencies, False)
		)

		installedDependencies = (
			self.environments[environment].installedDependencies
			if environment in self.environments
			else {}
		)
		if hasCondaDependencies:
			if "conda" not in installedDependencies:
				installedDependencies["conda"] = self.executeCommandAndGetOutput(
					self._getActivateCondaComands()
					+ [
						f"{self.condaBin} activate {environment}",
						f"{self.condaBin} list -y",
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
				installedDependencies["pip"] = self.executeCommandAndGetOutput(
					self._getActivateCondaComands()
					+ [f"{self.condaBin} activate {environment}", f"pip freeze"],
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

	def _getPlatformCommonName(self) -> str:
		"""Gets common platform name (mac/linux/windows)."""
		return "mac" if platform.system() == "Darwin" else platform.system().lower()

	def _isWindows(self) -> bool:
		"""Checks if the current OS is Windows."""
		return platform.system() == "Windows"

	def _getCondaPaths(self) -> tuple[Path, Path]:
		"""Gets micromamba root path and binary path.
		
		Returns:
			Tuple of (conda directory path, binary relative path).
		"""
		return self.condaPath.resolve(), Path(
			"bin/micromamba" if platform.system() != "Windows" else "micromamba.exe"
		)

	def _setupCondaChannels(self) -> list[str]:
		"""Configures default Conda channels.
		
		Returns:
			List of channel configuration commands.
		"""
		return [
			f"{self.condaBinConfig} config append channels conda-forge",
			f"{self.condaBinConfig} config append channels nodefaults",
			f"{self.condaBinConfig} config set channel_priority flexible",
		]

	def _getShellHookCommands(self) -> list[str]:
		"""Generates shell commands for Conda initialization.
		
		Returns:
			OS-specific commands to activate Conda shell hooks.
		"""
		currentPath = Path.cwd().resolve()
		condaPath, condaBinPath = self._getCondaPaths()
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

	def _getInstallCondaCommands(self) -> list[str]:
		"""Generates commands to install micromamba if missing.
		
		Returns:
			List of installation commands for the current OS.
		"""
		condaPath, condaBinPath = self._getCondaPaths()
		if (condaPath / condaBinPath).exists():
			return []
		if platform.system() not in ["Windows", "Linux", "Darwin"]:
			raise Exception(f"Platform {platform.system()} is not supported.")
		condaPath.mkdir(exist_ok=True, parents=True)
		commands = self._getProxyEnvironmentVariablesCommands()
		proxyString = self._getProxyString()

		if platform.system() == "Windows":
			if proxyString is not None:
				match = re.search(r"^[a-zA-Z]+://(.*?):(.*?)@", proxyString)
				proxyCredentials = ""
				if match:
					username, password = match.groups()
					commands += [
						f'$proxyUsername = "{username}"',
						f'$proxyPassword = "{password}"',
						"$securePassword = ConvertTo-SecureString $proxyPassword -AsPlainText -Force",
						"$proxyCredentials = New-Object System.Management.Automation.PSCredential($proxyUsername, $securePassword)",
					]
					proxyCredentials = f"-ProxyCredential $proxyCredentials"
			proxyArgs = (
				f"-Proxy {proxyString} {proxyCredentials}"
				if proxyString is not None
				else ""
			)
			commands += [
				f'Set-Location -Path "{condaPath}"',
				f'echo "Installing Visual C++ Redistributable if necessary..."',
				f'Invoke-WebRequest {proxyArgs} -URI "https://aka.ms/vs/17/release/vc_redist.x64.exe" -OutFile "$env:Temp\\vc_redist.x64.exe"; Start-Process "$env:Temp\\vc_redist.x64.exe" -ArgumentList "/quiet /norestart" -Wait; Remove-Item "$env:Temp\\vc_redist.x64.exe"',
				f'echo "Installing micromamba..."',
				f"Invoke-Webrequest {proxyArgs} -URI https://github.com/mamba-org/micromamba-releases/releases/download/2.0.4-0/micromamba-win-64 -OutFile micromamba.exe",
				f"New-Item .mambarc -type file",
			]
		else:
			system = "osx" if platform.system() == "Darwin" else "linux"
			machine = platform.machine()
			machine = "64" if machine == "x86_64" else machine
			proxyArgs = f'--proxy "{proxyString}"' if proxyString is not None else ""
			commands += [
				f'cd "{condaPath}"',
				f'echo "Installing micromamba..."',
				f"curl {proxyArgs} -Ls https://micro.mamba.pm/api/micromamba/{system}-{machine}/latest | tar -xvj bin/micromamba",
				f"touch .mambarc",
			]
		commands += self._getShellHookCommands()
		return commands + self._setupCondaChannels()

	def _getActivateCondaComands(self) -> list[str]:
		"""Generates commands to install (if needed) and activate Conda."""
		commands = self._getInstallCondaCommands()
		return commands + self._getShellHookCommands()

	def environmentExists(self, environment: str) -> bool:
		"""Checks if a Conda environment exists.
		
		Args:
			environment: Environment name to check.
			
		Returns:
			True if environment exists, False otherwise.
		"""
		condaMeta = Path(self.condaPath) / "envs" / environment / "conda-meta"
		return condaMeta.is_dir()

	def install(self, environment: str, dependencies: Dependencies, additionalInstallCommands: dict[str, list[str]] = {}) -> None:
		"""Installs dependencies into a Conda environment. 
		See :meth:`EnvironmentManager.create` for more details on the ``dependencies`` and ``additionalInstallCommands`` parameters.
		
		Args:
			environment: Target environment name.
			dependencies: Dependencies to install.
			additionalInstallCommands: Platform-specific commands during installation.
		"""

		installCommands = self._getActivateCondaComands()
		installCommands += self._getInstallDependenciesCommands(environment, dependencies)
		installCommands += self._getCommandsForCurrentPlatform(additionalInstallCommands)
		self.executeCommandAndGetOutput(installCommands)
		self.environments[environment].installedDependencies = {}

	def _platformCondaFormat(self) -> str:
		"""Get conda-compatible platform string (e.g., 'linux-64', 'osx-arm64', 'win-64')."""
		machine = platform.machine()
		machine = "64" if machine == "x86_64" or machine == "AMD64" else machine
		system = dict(Darwin="osx", Windows="win", Linux="linux")[platform.system()]
		return f'{system}-{machine}'

	def _formatDependencies(
		self,
		package_manager: str,
		dependencies: Dependencies,
		raiseIncompatibilityError: bool = True,
	) -> tuple[list[str], list[str], bool]:
		"""Formats dependencies for installation with platform checks.
		
		Args:
			package_manager: 'conda' or 'pip'.
			dependencies: Dependencies to process.
			raiseIncompatibilityError: Whether to raise on incompatible platforms.
			
		Returns:
			Tuple of (dependencies, no-deps dependencies, has_dependencies).
			
		Raises:
			IncompatibilityException: For non-optional incompatible dependencies.
		"""
		dependencyList: list[str | Dependency] = dependencies.get(package_manager, [])  # type: ignore
		finalDependencies: list[str] = []
		finalDependenciesNoDeps: list[str] = []
		for dependency in dependencyList:
			if isinstance(dependency, str):
				finalDependencies.append(dependency)
			else:
				currentPlatform = self._platformCondaFormat()
				platforms = dependency["platforms"]
				if (
					currentPlatform in platforms
					or platforms == "all"
					or len(platforms) == 0
					or not raiseIncompatibilityError
				):
					if "dependencies" not in dependency or dependency["dependencies"]:
						finalDependencies.append(dependency["name"])
					else:
						finalDependenciesNoDeps.append(dependency["name"])
				elif not dependency["optional"]:
					platformsString = ", ".join(platforms)
					raise IncompatibilityException(
						f"Error: the library {dependency['name']} is not available on this platform ({currentPlatform}). It is only available on the following platforms: {platformsString}."
					)
		return (
			[f'"{d}"' for d in finalDependencies],
			[f'"{d}"' for d in finalDependenciesNoDeps],
			len(finalDependencies) + len(finalDependenciesNoDeps) > 0,
		)

	def _getProxyEnvironmentVariablesCommands(self) -> list[str]:
		"""Generates proxy environment variable commands.
		
		Returns:
			List of OS-specific proxy export commands.
		"""
		if self.proxies is None:
			return []
		return [
			f'export {name.lower()}_proxy="{value}"'
			if not self._isWindows()
			else f'$Env:{name.lower()}_proxy="{value}"'
			for name, value in self.proxies.items()
		]

	def _getProxyString(self) -> str | None:
		"""Gets active proxy string from configuration (HTTPS preferred, fallback to HTTP)."""
		if self.proxies is None:
			return None
		return self.proxies.get("https", self.proxies.get("http", None))

	def _getInstallDependenciesCommands(self, environment: str, dependencies: Dependencies) -> list[str]:
		"""Generates commands to install dependencies in the given environment. Note: this does not activate conda, use self._getActivateCondaComands() first.
		
		Args:
			environment: Target environment name.
			dependencies: Dependencies to install.
			
		Returns:
			List of installation commands.
			
		Raises:
			Exception: If pip dependencies contain Conda channel syntax.
		"""
		condaDependencies, condaDependenciesNoDeps, hasCondaDependencies = (
			self._formatDependencies("conda", dependencies)
		)
		pipDependencies, pipDependenciesNoDeps, hasPipDependencies = (
			self._formatDependencies("pip", dependencies)
		)
		if any("::" in d for d in pipDependencies + pipDependenciesNoDeps):
			raise Exception(
				f'One pip dependency has a channel specifier "::". Is it a conda dependency?\n\n({dependencies["pip"]})'
			)
		installDepsCommands = self._getProxyEnvironmentVariablesCommands()
		installDepsCommands += (
			[
				f'echo "Activating environment {environment}..."',
				f"{self.condaBin} activate {environment}",
			]
			if hasCondaDependencies or hasPipDependencies
			else []
		)
		installDepsCommands += (
			[
				f'echo "Installing conda dependencies..."',
				f"{self.condaBinConfig} install {' '.join(condaDependencies)} -y",
			]
			if len(condaDependencies) > 0
			else []
		)
		installDepsCommands += (
			[
				f'echo "Installing conda dependencies without their dependencies..."',
				f"{self.condaBinConfig} install --no-deps {' '.join(condaDependenciesNoDeps)} -y",
			]
			if len(condaDependenciesNoDeps) > 0
			else []
		)
		proxyString = self._getProxyString()
		proxyArgs = f"--proxy {proxyString}" if proxyString is not None else ""
		installDepsCommands += (
			[
				f'echo "Installing pip dependencies..."',
				f"pip install {proxyArgs} {' '.join(pipDependencies)}",
			]
			if len(pipDependencies) > 0
			else []
		)
		installDepsCommands += (
			[
				f'echo "Installing pip dependencies without their dependencies..."',
				f"pip install {proxyArgs} --no-dependencies {' '.join(pipDependenciesNoDeps)}",
			]
			if len(pipDependenciesNoDeps) > 0
			else []
		)
		if environment in self.environments:
			self.environments[environment].installedDependencies = {}
		return installDepsCommands

	def _getCommandsForCurrentPlatform(
		self, additionalCommands: dict[str, list[str]] = {}
	) -> list[str]:
		"""Selects platform-specific commands from a dictionary.
		
		Args:
			additionalCommands: Dictionary mapping platforms to command lists.
			
		Returns:
			Merged list of commands for 'all' and current platform.
		"""
		commands = []
		if additionalCommands is None: return commands
		for name in ['all', self._getPlatformCommonName()]:
			if name in additionalCommands:
				commands += additionalCommands[name]
		return commands

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
		createEnvCommands = self._getActivateCondaComands()
		createEnvCommands += [f"{self.condaBinConfig} create -n {environment}{pythonRequirement} -y"]
		createEnvCommands += self._getInstallDependenciesCommands(environment, dependencies)
		createEnvCommands += self._getCommandsForCurrentPlatform(additionalInstallCommands)
		self.executeCommandAndGetOutput(createEnvCommands)
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
	
	def _getActivateEnvironmentCommands(self, environment: str, additionalActivateCommands: dict[str, list[str]] = {}):
		"""Generates commands to activate the given environment
		
		Args:
			environment: Environment name to launch.
			additionalActivateCommands: Platform-specific activation commands.
			
		Returns:
			List of commands to activate the environment
		"""
		commands = self._getActivateCondaComands() + [f"{self.condaBin} activate {environment}"]
		return commands + self._getCommandsForCurrentPlatform(additionalActivateCommands)

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
		commands = self._getActivateEnvironmentCommands(environment, additionalActivateCommands) + commands
		return self.executeCommands(commands, env=environmentVariables)

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