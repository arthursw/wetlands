import platform
import subprocess
import tempfile
import threading
from cema import logger

class CommandExecutor:
	"""Handles execution of shell commands with error checking and logging."""

	def _isWindows(self) -> bool:
		"""Checks if the current OS is Windows."""
		return platform.system() == "Windows"

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

	def getOutput(
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
			return self.getOutput(process, rawCommands, log=log)
		return
