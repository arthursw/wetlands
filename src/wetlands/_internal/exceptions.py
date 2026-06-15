from __future__ import annotations

from typing import Any

from wetlands._internal.diagnostics import TaskFailure


class ExecutionException(Exception):
    """Exception raised when an environment task fails."""

    def __init__(self, failure: TaskFailure | dict[str, Any] | BaseException | str):
        self.failure = TaskFailure.normalize(failure)
        super().__init__(self.failure.summary())

        # Compatibility aliases. New code should prefer ``failure``.
        self.error = self.failure
        self.exception = (
            self.failure.remote_exception.message
            if self.failure.remote_exception is not None
            else self.failure.message
        )
        self.traceback = self.failure.traceback
        self.category = self.failure.category
        self.worker = self.failure.worker
        self.exit_code = self.failure.exit_code
        self.signal = self.failure.signal

    def __str__(self) -> str:
        return self.failure.summary()


class IncompatibilityException(Exception):
    pass
