import logging
from pathlib import Path
from collections.abc import Callable

_logger: logging.Logger | None = None
_log_file_path: Path | None = None


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

    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(log_file_path, mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    _logger = logging.getLogger("wetlands")


def getLogger() -> logging.Logger:
    if _logger is None:
        _initializeLogger()
    assert _logger is not None
    return _logger


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


logger: logging.Logger = getLogger()


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
