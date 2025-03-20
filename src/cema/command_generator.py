from pathlib import Path
import platform
import re
from typing import NotRequired, TypedDict

import yaml

from cema.settings_manager import SettingsManager
from cema.dependency_manager import DependencyManager

class Command(TypedDict):
    all: NotRequired[str]
    linux: NotRequired[list[str]]
    mac: NotRequired[list[str]]
    windows: NotRequired[list[str]]

class CommandGenerator:
	"""Generate Conda commands."""

	def __init__(self, settingsManager:SettingsManager, dependencyManager: DependencyManager):
		self.settingsManager = settingsManager
		self.dependencyManager = dependencyManager

	def getShellHookCommands(self) -> list[str]:
		"""Generates shell commands for Conda initialization.
		
		Returns:
			OS-specific commands to activate Conda shell hooks.
		"""
		currentPath = Path.cwd().resolve()
		condaPath, condaBinPath = self.settingsManager.getCondaPaths()
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

	def getInstallCondaCommands(self) -> list[str]:
		"""Generates commands to install micromamba if missing.
		
		Returns:
			List of installation commands for the current OS.
		"""
		condaPath, condaBinPath = self.settingsManager.getCondaPaths()
		if (condaPath / condaBinPath).exists():
			return []
		if platform.system() not in ["Windows", "Linux", "Darwin"]:
			raise Exception(f"Platform {platform.system()} is not supported.")
		
		condaPath.mkdir(exist_ok=True, parents=True)
		with open(condaPath / '.mambarc', 'w') as f:
			mambaSettings = dict(channel_priority='flexible', channels=['conda-forge', 'nodefaults'], default_channels='conda-forge')
			yaml.safe_dump(mambaSettings, f)

		commands = self.settingsManager.getProxyEnvironmentVariablesCommands()
		proxyString = self.settingsManager.getProxyString()

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
			]
		commands += self.getShellHookCommands()
		return commands

	def getActivateCondaComands(self) -> list[str]:
		"""Generates commands to install (if needed) and activate Conda."""
		commands = self.getInstallCondaCommands()
		return commands + self.getShellHookCommands()

	def getActivateEnvironmentCommands(self, environment: str, additionalActivateCommands: dict[str, list[str]] = {}):
		"""Generates commands to activate the given environment
		
		Args:
			environment: Environment name to launch.
			additionalActivateCommands: Platform-specific activation commands.
			
		Returns:
			List of commands to activate the environment
		"""
		commands = self.getActivateCondaComands() + [f"{self.settingsManager.condaBin} activate {environment}"]
		return commands + self.getCommandsForCurrentPlatform(additionalActivateCommands)

	def getPlatformCommonName(self) -> str:
		"""Gets common platform name (mac/linux/windows)."""
		return "mac" if platform.system() == "Darwin" else platform.system().lower()

	def getCommandsForCurrentPlatform(
		self, additionalCommands: Command = {}
	) -> list[str]:
		"""Selects platform-specific commands from a dictionary.
		
		Args:
			additionalCommands: Dictionary mapping platforms to command lists (e.g. dict(all=[], linux=['wget "http://something.cool"']) ).
			
		Returns:
			Merged list of commands for 'all' and current platform.
		"""
		commands = []
		if additionalCommands is None: return commands
		for name in ['all', self.getPlatformCommonName()]:
			if name in additionalCommands:
				commands += additionalCommands[name]
		return commands
	