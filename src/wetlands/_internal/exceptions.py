class ExecutionException(Exception):
    """Exception raised when the environment raises an error when executing the requested function.

    Attributes:
            message: explanation of the error
    """

    def __init__(self, message):
        super().__init__(message)
        self.exception = message["exception"] if "exception" in message else None
        self.traceback = message["traceback"] if "traceback" in message else None


class IncompatibilityException(Exception):
    pass
