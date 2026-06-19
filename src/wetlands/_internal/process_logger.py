"""ProcessLogger handles non-blocking stdout/stderr reading from subprocesses with log context tracking."""

import subprocess
import threading
import logging
from typing import Callable, Any, Optional
from collections.abc import Callable as CallableType


class ProcessLogger:
    """Reads subprocess stdout/stderr in background threads and emits logs with context metadata.

    This solves the problem of multiple threads competing for process pipes and enables
    real-time log emission with attached context (log_source, env_name, stream, etc.).

    Usage:
        process = subprocess.Popen([...], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger = ProcessLogger(process, log_context={"log_source": "environment", "env_name": "cellpose"})
        logger.subscribe(my_callback)
        logger.start_reading()
    """

    def __init__(self, process: subprocess.Popen, log_context: dict[str, Any], base_logger: logging.LoggerAdapter):
        """Initialize ProcessLogger.

        Args:
            process: The subprocess.Popen instance to read from
            log_context: Dictionary of context to attach to all logs (log_source, env_name, stage, etc.)
            base_logger: The logging.Logger instance to emit logs to
        """
        self.process = process
        self.log_context = log_context.copy() if log_context else {}
        self.base_logger = base_logger
        self._subscribers: list[CallableType[[str, dict], None]] = []
        self._reader_thread: Optional[threading.Thread] = None
        self._stderr_reader_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._output: list[str] = []  # Accumulate all output lines
        self._stdout_output: list[str] = []
        self._stderr_output: list[str] = []

    def subscribe(self, callback: CallableType[[str, dict], None], include_history: bool = True) -> None:
        """Register a callback to be notified of each log line.

        Args:
            callback: Function with signature callback(line: str, context: dict) called for each log line
            include_history: whether to execute callback on all messages which where produced by the process until now (True), or only the futur ones (False)
        """
        with self._lock:
            self._subscribers.append(callback)
            if include_history:
                for line in self._output:
                    callback(line, self.log_context)

    def update_log_context(self, context_update: dict[str, Any]) -> None:
        """Update log context with thread safety.

        Useful for dynamically updating context during execution (e.g., changing call_target).

        Args:
            context_update: Dictionary with keys to update in log_context
        """
        with self._lock:
            self.log_context.update(context_update)

    def start_reading(self) -> None:
        """Start reading process stdout/stderr in background daemon threads."""
        if self._reader_thread is not None or self._stderr_reader_thread is not None:
            return

        if self.process.stdout is not None:
            self._reader_thread = threading.Thread(
                target=self._read_stream,
                args=(self.process.stdout, "stdout", logging.INFO),
                daemon=True,
            )
            self._reader_thread.start()

        if self.process.stderr is not None:
            self._stderr_reader_thread = threading.Thread(
                target=self._read_stream,
                args=(self.process.stderr, "stderr", logging.ERROR),
                daemon=True,
            )
            self._stderr_reader_thread.start()

    def _read_stream(self, stream, stream_name: str, level: int) -> None:
        """Read a process stream line-by-line and emit logs with context."""
        try:
            for line in iter(stream.readline, ""):
                line = line.strip()
                if not line:
                    continue

                # Accumulate output
                with self._lock:
                    self._output.append(line)
                    if stream_name == "stderr":
                        self._stderr_output.append(line)
                    else:
                        self._stdout_output.append(line)

                # Emit to logger with context attached via extra
                extra = self.log_context.copy()
                extra["stream"] = stream_name
                self.base_logger.log(level, line, extra=extra)

                # Notify subscribers
                with self._lock:
                    for callback in self._subscribers:
                        try:
                            callback(line, self.log_context)
                        except Exception as e:
                            self.base_logger.error(f"Error in log callback: {e}")

        except (IOError, OSError, ValueError):
            # File/pipe closed, which is normal when process exits
            # ValueError can be raised for operations on closed files
            pass
        except Exception as e:
            self.base_logger.error(f"Exception in ProcessLogger reader thread: {e}")

    def get_output(self) -> list[str]:
        """Get all accumulated output lines read so far.

        Returns:
            List of output lines (may be incomplete if process still running)
        """
        with self._lock:
            return self._output.copy()

    def get_stdout_output(self) -> list[str]:
        """Get accumulated stdout lines read so far."""
        with self._lock:
            return self._stdout_output.copy()

    def get_stderr_output(self) -> list[str]:
        """Get accumulated stderr lines read so far."""
        with self._lock:
            return self._stderr_output.copy()

    def wait_for_line(
        self, predicate: Callable[[str], bool], timeout: Optional[float] = None, include_history: bool = True
    ) -> Optional[str]:
        """Wait for a line matching predicate and return it.

        Useful for waiting on custom subprocess log lines such as "Custom server ready".

        Args:
            predicate: Function that takes a line and returns True if it's the line we're waiting for
            timeout: Maximum seconds to wait (None = wait forever)
            include_history: whether to execute callback on all messages which where produced by the process until now (True), or only the futur ones (False)

        Returns:
            The first line matching predicate, or None if timeout occurs
        """
        found_event = threading.Event()
        found_line = [None]

        def callback(line: str, context: dict) -> None:
            if predicate(line):
                found_line[0] = line  # type: ignore
                found_event.set()

        self.subscribe(callback, include_history=include_history)
        found_event.wait(timeout=timeout)

        return found_line[0]
