import logging
from pathlib import Path
from collections.abc import Callable
from typing import Any

# Constants for log sources
LOG_SOURCE_GLOBAL = "global"
LOG_SOURCE_ENVIRONMENT = "environment"
LOG_SOURCE_EXECUTION = "execution"

_logger: logging.Logger | None = None
_log_file_path: Path | None = None


class WetlandsLogger(logging.Logger):
    """Extended logger with convenience methods for attaching context metadata."""

    def log_global(self, msg: str, stage: str | None = None, **kwargs: Any) -> None:
        """Log a global operation (not specific to any environment or execution).

        Args:
            msg: The log message
            stage: Optional stage identifier (e.g., "search", "create")
            **kwargs: Additional context to attach to the log
        """
        extra = {"log_source": LOG_SOURCE_GLOBAL, "stage": stage, **kwargs}
        self.info(msg, extra=extra)

    def log_environment(
        self, msg: str, env_name: str, stage: str | None = None, **kwargs: Any
    ) -> None:
        """Log an environment-related operation (creation, update, deletion).

        Args:
            msg: The log message
            env_name: Name of the environment
            stage: Optional stage identifier (e.g., "download", "install", "configure")
            **kwargs: Additional context to attach to the log
        """
        extra = {
            "log_source": LOG_SOURCE_ENVIRONMENT,
            "env_name": env_name,
            "stage": stage,
            **kwargs,
        }
        self.info(msg, extra=extra)

    def log_execution(
        self, msg: str, env_name: str, func_name: str | None = None, **kwargs: Any
    ) -> None:
        """Log an execution operation (running functions or scripts in an environment).

        Args:
            msg: The log message
            env_name: Name of the environment
            func_name: Optional name of the function being executed
            **kwargs: Additional context to attach to the log
        """
        extra = {
            "log_source": LOG_SOURCE_EXECUTION,
            "env_name": env_name,
            "func_name": func_name,
            **kwargs,
        }
        self.info(msg, extra=extra)


def _initializeLogger(log_file_path=None):
    """Initialize the logger with the specified log file path.

    Args:
        log_file_path: Path to the log file. If None, defaults to "wetlands.log" in the current directory.
    """
    global _logger, _log_file_path

    if _logger is not None:
        return

    if log_file_path is None:
        log_file_path = Path("wetlands.log")
    else:
        log_file_path = Path(log_file_path)

    _log_file_path = log_file_path

    # Ensure parent directory exists
    log_file_path.parent.mkdir(exist_ok=True, parents=True)

    # Set WetlandsLogger as the logger class
    logging.setLoggerClass(WetlandsLogger)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(log_file_path, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    _logger = logging.getLogger("wetlands")


def getLogger() -> WetlandsLogger:
    if _logger is None:
        _initializeLogger()
    assert _logger is not None
    return _logger  # type: ignore


def setLogLevel(level):
    getLogger().setLevel(level)


def setLogFilePath(log_file_path):
    """Set the log file path for wetlands logging.

    This should be called early in the application lifecycle, preferably before
    creating any environments or executing commands.

    Args:
        log_file_path: Path where logs should be written.
    """
    global _logger, _log_file_path

    # Reset the logger if it's already been initialized so we can reinitialize with new path
    if _logger is not None:
        # Remove old file handlers
        for handler in list(_logger.handlers):
            if isinstance(handler, logging.FileHandler):
                handler.close()
                _logger.removeHandler(handler)
        _logger = None

    _initializeLogger(log_file_path)


logger: WetlandsLogger = getLogger()  # type: ignore


class CustomHandler(logging.Handler):
    def __init__(self, log) -> None:
        logging.Handler.__init__(self=self)
        self.log = log

    def emit(self, record: logging.LogRecord) -> None:
        formatter = (
            self.formatter
            if self.formatter is not None
            else logger.handlers[0].formatter
            if len(logger.handlers) > 0 and logger.handlers[0].formatter is not None
            else logging.root.handlers[0].formatter
        )
        if formatter is not None:
            self.log(formatter.format(record))


def attachLogHandler(log: Callable[[str], None], logLevel=logging.INFO) -> None:
    logger.setLevel(logLevel)
    ch = CustomHandler(log)
    ch.setLevel(logLevel)
    logger.addHandler(ch)
    return
