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
	condaBin = "micromamba"
	condaBinConfig = "micromamba --rc-file ~/.mambarc"

	environments: dict[str, Environment] = {}
	proxies: dict[str, str] | None = None

	def __init__(self, condaPath: str | Path = Path("micromamba")) -> None:
		self.setCondaPath(condaPath)

	def _configFileOption(self, doubleQuotes: bool = False) -> str:
		quotes = '"' if doubleQuotes else "'"
		return f"--rc-file {quotes}{self.condaPath / '.mambarc'}{quotes}"

	def setCondaPath(self, condaPath: str | Path) -> None:
		self.condaPath = Path(condaPath).resolve()
		self.condaBinConfig = f"{self.condaBin} {self._configFileOption(True)}"
		# Set proxy from condaPath
		condaConfigPath = self.condaPath / ".mambarc"
		import yaml

		if condaConfigPath.exists():
			with open(condaConfigPath, "r") as f:
				condaConfig = yaml.safe_load(f)
				if "proxies" in condaConfig:
					self.proxies = condaConfig["proxies"]

	def _insertCommandErrorChecks(self, commands: list[str]) -> list[str]:
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
	) -> tuple[list[str], int]:
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
		return (outputs, process.returncode)

	def setProxies(self, proxies: dict[str, str]) -> None:
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
				["powershell", "-WindowStyle", "Hidden", "-NoProfile", "-ExecutionPolicy", "ByPass", "-File", tmp.name]
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
	) -> tuple[list[str], int]:
		rawCommands = commands.copy()
		process = self.executeCommands(commands, env, exitIfCommandError)
		with process:
			return self._getOutput(process, rawCommands, log=log)
		return
	
	def _removeChannel(self, condaDependency: str) -> str:
		return (
			condaDependency.split("::")[1]
			if "::" in condaDependency
			else condaDependency
		)

	def dependenciesAreInstalled(self, environment: str, dependencies: Dependencies) -> bool:
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
				installedDependencies["conda"], _ = self.executeCommandAndGetOutput(
					self._activateConda()
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
				installedDependencies["pip"], _ = self.executeCommandAndGetOutput(
					self._activateConda()
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
		return "mac" if platform.system() == "Darwin" else platform.system().lower()

	def _isWindows(self) -> bool:
		return platform.system() == "Windows"

	def _getCondaPaths(self) -> tuple[Path, Path]:
		return self.condaPath.resolve(), Path(
			"bin/micromamba" if platform.system() != "Windows" else "micromamba.exe"
		)

	def _setupCondaChannels(self) -> list[str]:
		return [
			f"{self.condaBinConfig} config append channels conda-forge",
			f"{self.condaBinConfig} config append channels nodefaults",
			f"{self.condaBinConfig} config set channel_priority flexible",
		]
	
	def _shellHook(self) -> list[str]:
		currentPath = Path.cwd().resolve()
		condaPath, condaBinPath = self._getCondaPaths()
		if platform.system() == "Windows":
			return [
				f'Set-Location -Path "{condaPath}"',
				f'$Env:MAMBA_ROOT_PREFIX="{condaPath}"',
				f".\\{condaBinPath} {self._configFileOption(True)} shell hook -s powershell | Out-String | Invoke-Expression",
				f'Set-Location -Path "{currentPath}"',
			]
		else:
			return [
				f'cd "{condaPath}"',
				f'export MAMBA_ROOT_PREFIX="{condaPath}"',
				f'eval "$({condaBinPath} {self._configFileOption(False)} shell hook -s posix)"',
				f'cd "{currentPath}"',
			]

	def _installCondaIfNecessary(self) -> list[str]:
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
		commands += self._shellHook()
		return commands + self._setupCondaChannels()

	def _activateConda(self) -> list[str]:
		commands = self._installCondaIfNecessary()
		return commands + self._shellHook()

	def environmentExists(self, environment: str) -> bool:
		condaMeta = Path(self.condaPath) / "envs" / environment / "conda-meta"
		# we could also check for the condaMeta / history file.
		return condaMeta.is_dir()

	def install(self, environment: str, package: str, channel: str | None = None) -> None:
		channel = channel + "::" if channel is not None else ""
		self.executeCommandAndGetOutput(
			self._activateConda()
			+ [
				f"{self.condaBin} activate {environment}",
				f"{self.condaBinConfig} install {channel}{package} -y",
			]
		)
		self.environments[environment].installedDependencies = {}

	def _platformCondaFormat(self) -> str:
		machine = platform.machine()
		machine = "64" if machine == "x86_64" or machine == "AMD64" else machine
		return (
			dict(Darwin="osx", Windows="win", Linux="linux")[platform.system()]
			+ "-"
			+ machine
		)

	def _formatDependencies(
		self,
		package_manager: str,
		dependencies: Dependencies,
		raiseIncompatibilityError: bool = True,
	) -> tuple[list[str], list[str], bool]:
		dependencyList: list[str | Dependency] = dependencies.get(package_manager, []) # type: ignore
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
		if self.proxies is None:
			return []
		return [
			f'export {name.lower()}_proxy="{value}"'
			if not self._isWindows()
			else f'$Env:{name.lower()}_proxy="{value}"'
			for name, value in self.proxies.items()
		]

	def _getProxyString(self) -> str | None:
		if self.proxies is None:
			return None
		return (
			self.proxies.get("https", self.proxies.get("http", None))
		)

	def installDependencies(self, environment: str, dependencies: Dependencies) -> list[str]:
		condaDependencies, condaDependenciesNoDeps, hasCondaDependencies = (
			self._formatDependencies("conda", dependencies)
		)
		pipDependencies, pipDependenciesNoDeps, hasPipDependencies = (
			self._formatDependencies("pip", dependencies)
		)
		if any("::" in d for d in pipDependencies + pipDependenciesNoDeps):
			raise Exception(
				f'One pip dependency has a channel specifier "::" is it a conda dependency?\n\n({dependencies["pip"]})'
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
	)-> list[str]:
		commands = []
		if additionalCommands is not None and "all" in additionalCommands:
			commands += additionalCommands["all"]
		if (
			additionalCommands is not None
			and self._getPlatformCommonName() in additionalCommands
		):
			commands += additionalCommands[self._getPlatformCommonName()]
		return commands

	def create(
		self,
		environment: str,
		dependencies: Dependencies,
		additionalInstallCommands: dict[str, list[str]] = {},
		additionalActivateCommands: dict[str, list[str]] = {},
		mainEnvironment: str | None = None,
		errorIfExists: bool = False,
	) -> bool:
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
		createEnvCommands = self._activateConda() + [
			f"{self.condaBinConfig} create -n {environment}{pythonRequirement} -y"
		]
		createEnvCommands += self.installDependencies(environment, dependencies)
		createEnvCommands += self._getCommandsForCurrentPlatform(additionalInstallCommands)
		createEnvCommands += self._getCommandsForCurrentPlatform(additionalActivateCommands)
		self.executeCommandAndGetOutput(createEnvCommands)
		return True

	def environmentIsLaunched(self, environment: str) -> bool:
		return (
			environment in self.environments
			and self.environments[environment].launched()
		)

	def logOutput(self, process: subprocess.Popen, stopEvent: threading.Event) -> None:
		if process.stdout is None or process.stdout.readline is None: return
		try:
			for line in iter(process.stdout.readline, ""):  # Use iter to avoid buffering issues
				if stopEvent.is_set():
					break
				logger.info(line.strip())
		except Exception as e:
			logger.error(f"Exception in logging thread: {e}")
		return

	def launch(
		self,
		environment: str,
		customCommand: str | None = None,
		environmentVariables: dict[str, str] | None = None,
		condaEnvironment: bool = True,
		additionalActivateCommands: dict[str, list[str]] = {},
		logOutput: bool = True,
	) -> Environment:
		if self.environmentIsLaunched(environment):
			return self.environments[environment]

		moduleCallerPath = Path(__file__).parent.resolve() / "module_caller.py"
		commands = (
			self._activateConda() + [f"{self.condaBin} activate {environment}"]
			if condaEnvironment
			else []
		)
		commands += self._getCommandsForCurrentPlatform(additionalActivateCommands)
		commands += [
			f'python -u "{moduleCallerPath}" {environment}'
			if customCommand is None
			else customCommand
		]
		port = -1
		process = self.executeCommands(commands, env=environmentVariables)
		# The python command is called with the -u (unbuffered) option, we can wait for a specific print before letting the process run by itself
		# if the unbuffered option is not set, the following can wait for the whole python process to finish
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
		# If process is finished: check if error
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
		customCommand: str | None = None,
		environmentVariables: dict[str, str] | None = None,
		additionalInstallCommands: dict[str, list[str]] = {},
		additionalActivateCommands: dict[str, list[str]] = {},
		mainEnvironment: str | None = None,
	) -> Environment:
		environmentIsRequired = self.create(
			environment,
			dependencies,
			additionalInstallCommands=additionalInstallCommands,
			additionalActivateCommands=additionalActivateCommands,
			mainEnvironment=mainEnvironment,
		)
		if environmentIsRequired:
			return self.launch(
				environment,
				customCommand,
				environmentVariables=environmentVariables,
				additionalActivateCommands=additionalActivateCommands,
			)
		else:
			return DirectEnvironment(environment)

	def exit(self, environment: Environment | str) -> None:
		environmentName = (
			environment if isinstance(environment, str) else environment.name
		)
		if environmentName in self.environments:
			self.environments[environmentName]._exit()
			del self.environments[environmentName]