from __future__ import annotations

import logging
import sys
from pathlib import Path
from collections.abc import Callable

# Constants for log sources
LOG_SOURCE_GLOBAL = "global"
LOG_SOURCE_ENVIRONMENT = "environment"
LOG_SOURCE_EXECUTION = "execution"


class WetlandsAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Ensures 'extra' exists and merges adapter’s context
        extra = kwargs.setdefault("extra", {})
        extra.update(self.extra)
        return msg, kwargs

    # --- Convenience methods ---
    def log_global(self, msg, stage=None, **kwargs):
        extra = {"log_source": "global", "stage": stage}
        extra.update(kwargs)
        self.logger.info(msg, extra=extra)

    def log_environment(self, msg, env_name, stage=None, **kwargs):
        extra = {"log_source": "environment", "env_name": env_name, "stage": stage}
        extra.update(kwargs)
        self.logger.info(msg, extra=extra)

    def log_execution(self, msg, env_name, call_target=None, **kwargs):
        extra = {"log_source": "execution", "env_name": env_name, "call_target": call_target}
        extra.update(kwargs)
        self.logger.info(msg, extra=extra)


# create a base logger and wrap it
_base = logging.getLogger("wetlands")
logger = WetlandsAdapter(_base, {})


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def create_split_stream_handlers(
    fmt: str = "%(levelname)s: %(message)s",
    stdout_level: int = logging.DEBUG,
    stderr_level: int = logging.WARNING,
) -> tuple[logging.StreamHandler, logging.StreamHandler]:
    """Create console handlers that route DEBUG/INFO to stdout and WARNING+ to stderr."""
    formatter = logging.Formatter(fmt)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(stdout_level)
    stdout_handler.addFilter(_MaxLevelFilter(logging.INFO))
    stdout_handler.setFormatter(formatter)
    stdout_handler._wetlands_split_stream_handler = True  # type: ignore[attr-defined]

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(stderr_level)
    stderr_handler.setFormatter(formatter)
    stderr_handler._wetlands_split_stream_handler = True  # type: ignore[attr-defined]

    return stdout_handler, stderr_handler


def enable_console_logging(
    level: int = logging.INFO,
    fmt: str = "%(levelname)s: %(message)s",
) -> tuple[logging.StreamHandler, logging.StreamHandler]:
    """Enable Wetlands console logging with DEBUG/INFO on stdout and WARNING+ on stderr."""
    existing_handlers: list[logging.StreamHandler] = []
    for handler in _base.handlers:
        if isinstance(handler, logging.StreamHandler) and getattr(handler, "_wetlands_split_stream_handler", False):
            existing_handlers.append(handler)

    if len(existing_handlers) >= 2:
        _base.setLevel(level)
        _base.propagate = False
        return existing_handlers[0], existing_handlers[1]

    handlers = create_split_stream_handlers(fmt=fmt)
    for handler in handlers:
        _base.addHandler(handler)
    _base.setLevel(level)
    _base.propagate = False
    return handlers


def enable_file_logging(
    filepath: str | Path,
    level: int = logging.INFO,
    fmt: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
) -> None:
    """
    Optional helper for users who want file logging without configuring
    the Python logging system manually.
    """

    # Prevent duplicate handlers
    for h in _base.handlers:
        if isinstance(h, logging.FileHandler) and h.baseFilename == filepath:
            return

    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    filepath = str(filepath)
    handler = logging.FileHandler(filepath)
    handler.setFormatter(logging.Formatter(fmt))
    handler.setLevel(level)

    _base.setLevel(level)
    _base.addHandler(handler)

    _base.debug("File logging enabled at %s", filepath)


class CustomHandler(logging.Handler):
    def __init__(self, log) -> None:
        super().__init__()
        self.log = log

    def emit(self, record: logging.LogRecord) -> None:
        formatter = self.formatter

        if formatter is None:
            for h in _base.handlers:
                if h.formatter:
                    formatter = h.formatter
                    break

        if formatter is None:
            formatter = logging.Formatter("%(levelname)s: %(message)s")
        self.log(formatter.format(record))


def attach_log_handler(
    log: Callable[[str], None], log_level=logging.INFO, filter: logging.Filter | None = None
) -> CustomHandler:
    _base.setLevel(log_level)
    ch = CustomHandler(log)
    if filter is not None:
        ch.addFilter(filter)
    ch.setLevel(log_level)
    _base.addHandler(ch)
    return ch
