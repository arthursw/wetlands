import platform
import json
import subprocess
import tempfile
from typing import Any
import psutil
from wetlands.logger import logger


class CommandExecutor:
    """Handles execution of shell commands with error checking and logging."""

    @staticmethod
    def killProcess(process) -> None:
        """Terminates the process and its children"""
        if process is None:
            return
        parent = psutil.Process(process.pid)
        for child in parent.children(recursive=True):  # Get all child processes
            if child.is_running():
                child.kill()
        if parent.is_running():
            parent.kill()

    def _isWindows(self) -> bool:
        """Checks if the current OS is Windows."""
        return platform.system() == "Windows"

    def _insertCommandErrorChecks(self, commands: list[str]) -> list[str]:
        """Inserts error checking commands after each shell command.
        Note: could also use [`set -e`](https://stackoverflow.com/questions/3474526/stop-on-first-error),
        and [`$ErrorActionPreference = "Stop"`](https://stackoverflow.com/questions/9948517/how-to-stop-a-powershell-script-on-the-first-error).

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

    def _commandsExcerpt(self, commands: list[str]) -> str:
        """Returns the command list as a string but cap the length at 150 characters
        (for example to be able to display it in a dialog window)."""
        if commands is None or len(commands) == 0:
            return ""
        prefix: str = "[...] " if len(str(commands)) > 150 else ""
        return prefix + str(commands)[-150:]

    def getOutput(
        self,
        process: subprocess.Popen,
        commands: list[str],
        log: bool = True,
        strip: bool = True,
    ) -> list[str]:
        """Captures and processes output from a subprocess.

        Args:
                process: Subprocess to monitor.
                commands: Commands that were executed (for error messages).
                log: Whether to log output lines.
                strip: Whether to strip whitespace from output lines.

        Returns:
                Output lines.

        Raises:
                Exception: If CondaSystemExit is detected or non-zero exit code.
        """
        outputs = []
        if process.stdout is not None:
            for line in process.stdout:
                if strip:
                    line = line.strip()
                if log:
                    logger.info(line)
                if "CondaSystemExit" in line:  # Sometime conda exists with a CondaSystemExit and a return code 0
                    # we want to stop our script when this happens (and not run the later commands)
                    self.killProcess(process)
                    raise Exception(f'The execution of the commands "{self._commandsExcerpt(commands)}" failed.')
                outputs.append(line)
        process.wait()
        if process.returncode != 0:
            raise Exception(f'The execution of the commands "{self._commandsExcerpt(commands)}" failed.')
        return outputs

    def executeCommands(
        self,
        commands: list[str],
        exitIfCommandError: bool = True,
        popenKwargs: dict[str, Any] = {},
        wait: bool = False,
    ) -> subprocess.Popen:
        """Executes shell commands in a subprocess. Warning: does not wait for completion unless ``wait`` is True.

        Args:
                commands: List of shell commands to execute.
                exitIfCommandError: Whether to insert error checking after each command to make sure the whole command chain stops if an error occurs (otherwise the script will be executed entirely even when one command fails at the beginning).
                popenKwargs: Keyword arguments for subprocess.Popen() (see [Popen documentation](https://docs.python.org/3/library/subprocess.html#popen-constructor)). Defaults are: dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, encoding="utf-8", errors="replace", bufsize=1).
                wait: Whether to wait for the process to complete before returning.

        Returns:
                Subprocess handle for the executed commands.
        """
        commandsString = "\n\t\t".join(commands)
        logger.debug(f"Execute commands:\n\n\t\t{commandsString}\n")
        with tempfile.NamedTemporaryFile(suffix=".ps1" if self._isWindows() else ".sh", mode="w", delete=False) as tmp:
            if exitIfCommandError:
                commands = self._insertCommandErrorChecks(commands)
            tmp.write("\n".join(commands))
            tmp.flush()
            tmp.close()
            executeFile = (
                [
                    "powershell",
                    "-WindowStyle",
                    "Hidden",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "ByPass",
                    "-File",
                    tmp.name,
                ]
                if self._isWindows()
                else ["/bin/bash", tmp.name]
            )
            if not self._isWindows():
                subprocess.run(["chmod", "u+x", tmp.name])
            logger.debug(f"Script file: {tmp.name}")
            defaultPopenKwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,  # Merge stderr and stdout to handle all them with a single loop
                "stdin": subprocess.DEVNULL,  # Prevent the command to wait for input: instead we want to stop if this happens
                "encoding": "utf-8",
                "errors": "replace",  # Determines how encoding and decoding errors should be handled: replaces invalid characters with a placeholder (e.g., ? in ASCII).
                "bufsize": 1,  # 1 means line buffered
            }
            process = subprocess.Popen(executeFile, **(defaultPopenKwargs | popenKwargs))
            if wait:
                process.wait()
            return process

    def executeCommandsAndGetOutput(
        self, commands: list[str], exitIfCommandError: bool = True, log: bool = True, popenKwargs: dict[str, Any] = {}
    ) -> list[str]:
        """Executes commands and captures their output. See [`CommandExecutor.executeCommands`][wetlands._internal.command_executor.CommandExecutor.executeCommands] for more details on the arguments.

        Args:
                commands: Shell commands to execute.
                exitIfCommandError: Whether to insert error checking.
                log: Enable logging of command output.
                popenKwargs: Keyword arguments for subprocess.Popen().

        Returns:
                Output lines.
        """
        rawCommands = commands.copy()
        process = self.executeCommands(commands, exitIfCommandError, popenKwargs)
        with process:
            output = self.getOutput(process, rawCommands, log=log)
            return output

    def executeCommandAndGetJsonOutput(
        self, commands: list[str], exitIfCommandError: bool = True, log: bool = True, popenKwargs: dict[str, Any] = {}
    ) -> list[dict[str, str]]:
        """Execute [`CommandExecutor.executeCommandsAndGetOutput`][wetlands._internal.command_executor.CommandExecutor.executeCommandsAndGetOutput] and parse the json output.

        Returns:
                Output json.
        """
        output = self.executeCommandsAndGetOutput(commands, exitIfCommandError, log, popenKwargs)
        return json.loads("".join(output))
