import logging
from collections.abc import Callable

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("wetlands.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


def getLogger():
    return logging.getLogger("wetlands")


def setLogLevel(level):
    getLogger().setLevel(level)


logger = getLogger()


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
