from pathlib import Path
import platform
import json
import subprocess
import tempfile
from typing import Any
import psutil
from wetlands.logger import logger
from wetlands._internal.process_logger import ProcessLogger


class CommandExecutor:
    """Handles execution of shell commands with error checking and logging."""

    def __init__(self, scripts_path: Path | None = None) -> None:
        """scripts_path: Path where to create temporary script files for command execution (useful for debugging). If None, use the system default temp directory."""
        self.scripts_path = scripts_path
        if scripts_path is not None:
            scripts_path.mkdir(parents=True, exist_ok=True)
        self._process_loggers: dict[int, ProcessLogger] = {}  # Map process PID to ProcessLogger

    @staticmethod
    def kill_process(process) -> None:
        """Terminates the process and its children"""
        if process is None:
            return
        try:
            parent = psutil.Process(process.pid)
        except psutil.NoSuchProcess:
            return
        try:
            for child in parent.children(recursive=True):  # Get all child processes
                if child.is_running():
                    child.kill()
            if parent.is_running():
                parent.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    def _is_windows(self) -> bool:
        """Checks if the current OS is Windows."""
        return platform.system() == "Windows"

    def _insert_command_error_checks(self, commands: list[str]) -> list[str]:
        """Inserts error checking commands after each shell command.
        Note: could also use [`set -e`](https://stackoverflow.com/questions/3474526/stop-on-first-error),
        and [`$ErrorActionPreference = "Stop"`](https://stackoverflow.com/questions/9948517/how-to-stop-a-powershell-script-on-the-first-error).

        Args:
                commands: List of original shell commands.

        Returns:
                Augmented command list with error checking logic.
        """
        commands_with_checks = []
        error_message = "Errors encountered during execution. Exited with status:"
        windows_checks = ["", "if (! $?) { exit 1 } "]
        posix_checks = [
            "",
            "return_status=$?",
            "if [ $return_status -ne 0 ]",
            "then",
            f'    echo "{error_message} $return_status"',
            "    exit 1",
            "fi",
            "",
        ]
        checks = windows_checks if self._is_windows() else posix_checks
        for command in commands:
            commands_with_checks.append(command)
            commands_with_checks += checks
        return commands_with_checks

    def _commands_excerpt(self, commands: list[str]) -> str:
        """Returns the command list as a string but cap the length at 150 characters
        (for example to be able to display it in a dialog window)."""
        if commands is None or len(commands) == 0:
            return ""
        prefix: str = "[...] " if len(str(commands)) > 150 else ""
        return prefix + str(commands)[-150:]

    def _create_process_logger(
        self,
        process: subprocess.Popen,
        log_context: dict[str, Any] | None = None,
    ) -> ProcessLogger:
        """Create and start a ProcessLogger for a subprocess.

        This enables non-blocking, real-time logging with context metadata.

        Args:
                process: Subprocess to monitor.
                log_context: Dictionary of context to attach to all logs.

        Returns:
                ProcessLogger instance (already started).
        """
        if log_context is None:
            log_context = {}

        process_logger = ProcessLogger(process, log_context, logger)
        process_logger.start_reading()
        # Store reference for later retrieval
        self._process_loggers[process.pid] = process_logger
        return process_logger

    def get_process_logger(
        self,
        process: subprocess.Popen,
    ) -> ProcessLogger:
        """Get the ProcessLogger for a subprocess.

        Args:
                process: Subprocess whose logger to retrieve.

        Returns:
                ProcessLogger instance if found, raise exception if not found.
        """
        return self._process_loggers[process.pid]

    def _get_complete_process_logger(self, process: subprocess.Popen) -> ProcessLogger | None:
        """Get the process logger and wait for the reader thread to finish processing all output.

        Args:
                process: The completed subprocess.

        Returns:
                ProcessLogger instance with all output read, or None if not found.
        """
        if process.pid not in self._process_loggers:
            return None

        process_logger = self._process_loggers[process.pid]
        # Wait for reader thread to finish processing all output
        if process_logger._reader_thread is not None and process_logger._reader_thread.is_alive():
            process_logger._reader_thread.join(timeout=5.0)

        return process_logger

    def execute_commands(
        self,
        commands: list[str],
        exit_if_command_error: bool = True,
        popen_kwargs: dict[str, Any] = {},
        wait: bool = False,
        remove_python_env_vars: bool = True,
        log_context: dict[str, Any] | None = None,
        log: bool = True,
    ) -> subprocess.Popen:
        """Executes shell commands in a subprocess with automatic logging via ProcessLogger.

        Warning: does not wait for completion unless ``wait`` is True. Output is logged in real-time via a background thread.

        Args:
                commands: List of shell commands to execute.
                exit_if_command_error: Whether to insert error checking after each command to make sure the whole command chain stops if an error occurs (otherwise the script will be executed entirely even when one command fails at the beginning).
                popen_kwargs: Keyword arguments for subprocess.Popen() (see [Popen documentation](https://docs.python.org/3/library/subprocess.html#popen-constructor)). Defaults are: dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, encoding="utf-8", errors="replace", bufsize=1).
                wait: Whether to wait for the process to complete before returning.
                remove_python_env_vars: Whether to remove PYTHONEXECUTABLE, PYTHONHOME and PYTHONPATH from the environment variables to avoid interference with conda/pixi environment activation.
                log_context: Optional context dict to attach to logs. If provided, ProcessLogger will emit logs with this context.
                log: Whether to enable logging of command output. Defaults to True.

        Returns:
                Subprocess handle for the executed commands. Output is logged in real-time via ProcessLogger.
        """
        import os

        commands_string = "\n\t\t".join(commands)
        logger.debug(f"Execute commands:\n\n\t\t{commands_string}\n")
        with tempfile.NamedTemporaryFile(
            dir=self.scripts_path, suffix=".ps1" if self._is_windows() else ".sh", mode="w", delete=False
        ) as tmp:
            if exit_if_command_error:
                commands = self._insert_command_error_checks(commands)
            tmp.write("\n".join(commands))
            tmp.flush()
            tmp.close()
            execute_file = (
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
                if self._is_windows()
                else ["/bin/bash", tmp.name]
            )
            if not self._is_windows():
                subprocess.run(["chmod", "u+x", tmp.name])
            logger.debug(f"Script file: {tmp.name}")

            if remove_python_env_vars:
                # Remove environment variables that can interfere with conda/pixi activation
                # These are typically set by the parent application (e.g., napari) and can cause
                # Python to use the wrong interpreter or libraries instead of the isolated environment
                env = popen_kwargs.get("env")
                vars_to_remove = ["PYTHONEXECUTABLE", "PYTHONHOME", "PYTHONPATH"]
                # warn if mergedKwargs had an env variable which can cause issues with env activation
                if env is not None and any(var in vars_to_remove for var in env):
                    logger.warning(f"Removing variables {vars_to_remove} from env.")

                if env is None:
                    env = os.environ.copy()

                for var in vars_to_remove:
                    env.pop(var, None)
                popen_kwargs["env"] = env

            default_popen_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,  # Merge stderr and stdout to handle all them with a single loop
                "stdin": subprocess.DEVNULL,  # Prevent the command to wait for input: instead we want to stop if this happens
                "encoding": "utf-8",
                "errors": "replace",  # Determines how encoding and decoding errors should be handled: replaces invalid characters with a placeholder (e.g., ? in ASCII).
                "bufsize": 1,  # 1 means line buffered
            }
            process = subprocess.Popen(execute_file, **(default_popen_kwargs | popen_kwargs))

            # Create ProcessLogger to handle stdout in background if logging is enabled
            if log:
                process_logger = self._create_process_logger(process, log_context)

                # Subscribe to detect CondaSystemExit in real-time during execution
                def conda_exit_detector(line: str, _context: dict) -> None:
                    if "CondaSystemExit" in line:
                        # Kill process immediately and mark it
                        self.kill_process(process)
                        process._conda_exit_detected = True  # type: ignore[attr-defined]

                process_logger.subscribe(conda_exit_detector)

            if wait:
                process.wait()
            return process

    def execute_commands_and_get_output(
        self,
        commands: list[str],
        exit_if_command_error: bool = True,
        popen_kwargs: dict[str, Any] = {},
        log_context: dict[str, Any] | None = None,
    ) -> list[str]:
        """Executes commands and captures their output. See [`CommandExecutor.execute_commands`][wetlands._internal.command_executor.CommandExecutor.execute_commands] for more details on the arguments.

        Args:
                commands: Shell commands to execute.
                exit_if_command_error: Whether to insert error checking.
                popen_kwargs: Keyword arguments for subprocess.Popen().
                log_context: Optional context dict to attach to logs via ProcessLogger.

        Returns:
                Output lines (stripped of whitespace).

        Raises:
                Exception: If CondaSystemExit detected or non-zero exit code.
        """
        # Always create ProcessLogger to capture output (set log=True internally)
        process = self.execute_commands(
            commands,
            exit_if_command_error=exit_if_command_error,
            popen_kwargs=popen_kwargs,
            wait=True,
            log=True,
            log_context=log_context,
        )

        # Get output from ProcessLogger (always created above)
        process_logger = self._get_complete_process_logger(process)
        if process_logger is None:
            return []

        # Check if CondaSystemExit was detected during execution
        if getattr(process, "_conda_exit_detected", False):
            raise Exception(f'The execution of the commands "{self._commands_excerpt(commands)}" failed.')

        output = process_logger.get_output()
        # Strip whitespace from each line
        stripped_output = [line.strip() for line in output]

        # Check exit code
        if process.returncode != 0:
            raise Exception(f'The execution of the commands "{self._commands_excerpt(commands)}" failed.')

        return stripped_output

    def execute_commands_and_get_json_output(
        self, commands: list[str], exit_if_command_error: bool = True, popen_kwargs: dict[str, Any] = {}
    ) -> list[dict[str, str]]:
        """Execute commands and parse the json output.

        Args:
                commands: Shell commands to execute.
                exit_if_command_error: Whether to insert error checking.
                popen_kwargs: Keyword arguments for subprocess.Popen().

        Returns:
                Output json.
        """
        # Execute with wait=True to block until completion
        process = self.execute_commands(
            commands, exit_if_command_error=exit_if_command_error, popen_kwargs=popen_kwargs, wait=True, log=True
        )

        # Get output from ProcessLogger
        process_logger = self._get_complete_process_logger(process)
        if process_logger is None:
            return []

        output = process_logger.get_output()
        return json.loads("".join(output))
