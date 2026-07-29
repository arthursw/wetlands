"""Structured public diagnostics for failed worker executions."""

from __future__ import annotations

import enum
import traceback as traceback_module
from dataclasses import dataclass, field
from typing import Any


class ExecutionFailureCategory(str, enum.Enum):
    """Stable categories for failures observed at the execution boundary."""

    REMOTE_EXCEPTION = "remote_exception"
    INTERNAL_EXCEPTION = "internal_exception"
    SERIALIZATION = "serialization"
    WORKER_CONNECTION = "worker_connection"
    WORKER_DIED = "worker_died"
    TIMEOUT = "timeout"
    ENVIRONMENT = "environment"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RemoteExceptionInfo:
    """Serializable identity and traceback information for a remote exception."""

    module: str | None = None
    type_name: str | None = None
    qualified_name: str | None = None
    message: str | None = None
    traceback: str | None = None
    cause: "RemoteExceptionInfo | None" = None
    context: "RemoteExceptionInfo | None" = None
    suppress_context: bool = False

    @classmethod
    def from_exception(cls, exc: BaseException) -> "RemoteExceptionInfo":
        exc_type = type(exc)
        return cls(
            module=exc_type.__module__,
            type_name=exc_type.__name__,
            qualified_name=getattr(exc_type, "__qualname__", exc_type.__name__),
            message=str(exc),
            traceback="".join(traceback_module.format_exception(exc_type, exc, exc.__traceback__, chain=False)),
            cause=cls.from_exception(exc.__cause__) if exc.__cause__ is not None else None,
            context=cls.from_exception(exc.__context__) if exc.__context__ is not None else None,
            suppress_context=bool(getattr(exc, "__suppress_context__", False)),
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "RemoteExceptionInfo | None":
        if not payload:
            return None
        return cls(
            module=payload.get("module"),
            type_name=payload.get("type_name"),
            qualified_name=payload.get("qualified_name"),
            message=payload.get("message"),
            traceback=payload.get("traceback"),
            cause=cls.from_payload(payload.get("cause")),
            context=cls.from_payload(payload.get("context")),
            suppress_context=bool(payload.get("suppress_context", False)),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "module": self.module,
            "type_name": self.type_name,
            "qualified_name": self.qualified_name,
            "message": self.message,
            "traceback": self.traceback,
            "cause": self.cause.to_payload() if self.cause is not None else None,
            "context": self.context.to_payload() if self.context is not None else None,
            "suppress_context": self.suppress_context,
        }


@dataclass(frozen=True)
class WorkerInfo:
    """Worker identity attached to an execution failure."""

    environment: str | None = None
    index: int | None = None
    pid: int | None = None
    port: int | None = None
    persistent: bool | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "WorkerInfo | None":
        if not payload:
            return None
        return cls(
            environment=payload.get("environment"),
            index=payload.get("index"),
            pid=payload.get("pid"),
            port=payload.get("port"),
            persistent=payload.get("persistent"),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "index": self.index,
            "pid": self.pid,
            "port": self.port,
            "persistent": self.persistent,
        }


@dataclass(frozen=True)
class ExecutionFailure:
    """Structured details for a failed worker execution."""

    category: ExecutionFailureCategory
    message: str
    task_id: str | None = None
    call_target: str | None = None
    traceback: str | None = None
    traceback_frames: list[str] = field(default_factory=list)
    remote_exception: RemoteExceptionInfo | None = None
    worker: WorkerInfo | None = None
    exit_code: int | None = None
    signal: int | None = None
    timeout: float | None = None
    elapsed: float | None = None
    serialization_context: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def normalize(
        cls,
        value: "ExecutionFailure | dict[str, Any] | BaseException | str",
        *,
        traceback: list[str] | str | None = None,
        task_id: str | None = None,
        call_target: str | None = None,
    ) -> "ExecutionFailure":
        if isinstance(value, ExecutionFailure):
            return value.with_defaults(task_id=task_id, call_target=call_target)
        if isinstance(value, BaseException):
            return cls.from_exception(value, task_id=task_id, call_target=call_target)
        if isinstance(value, dict):
            return cls.from_payload(value, task_id=task_id, call_target=call_target)
        traceback_string = _traceback_to_string(traceback)
        return cls(
            category=ExecutionFailureCategory.UNKNOWN,
            message=str(value),
            task_id=task_id,
            call_target=call_target,
            traceback=traceback_string,
            traceback_frames=_traceback_to_frames(traceback),
        )

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        *,
        category: ExecutionFailureCategory = ExecutionFailureCategory.REMOTE_EXCEPTION,
        task_id: str | None = None,
        call_target: str | None = None,
        serialization_context: str | None = None,
    ) -> "ExecutionFailure":
        exc_type = type(exc)
        return cls(
            category=category,
            message=str(exc),
            task_id=task_id,
            call_target=call_target,
            traceback="".join(traceback_module.format_exception(exc_type, exc, exc.__traceback__, chain=True)),
            traceback_frames=traceback_module.format_tb(exc.__traceback__),
            remote_exception=RemoteExceptionInfo.from_exception(exc),
            serialization_context=serialization_context,
        )

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
        *,
        task_id: str | None = None,
        call_target: str | None = None,
    ) -> "ExecutionFailure":
        failure_payload = payload.get("failure") if "failure" in payload else payload
        if not isinstance(failure_payload, dict):
            return cls.normalize(str(failure_payload), task_id=task_id, call_target=call_target)

        legacy_traceback = failure_payload.get("traceback")
        category_value = failure_payload.get("category")
        try:
            category = (
                ExecutionFailureCategory(category_value)
                if category_value is not None
                else ExecutionFailureCategory.REMOTE_EXCEPTION
            )
        except ValueError:
            category = ExecutionFailureCategory.UNKNOWN

        remote_exception = RemoteExceptionInfo.from_payload(failure_payload.get("remote_exception"))
        message = failure_payload.get("message")
        if message is None:
            message = failure_payload.get("exception")
        if message is None and remote_exception is not None:
            message = remote_exception.message
        if message is None:
            message = "Unknown task failure"

        return cls(
            category=category,
            message=str(message),
            task_id=failure_payload.get("task_id") or payload.get("task_id") or task_id,
            call_target=failure_payload.get("call_target") or payload.get("_call_target") or call_target,
            traceback=_traceback_to_string(legacy_traceback),
            traceback_frames=_traceback_to_frames(failure_payload.get("traceback_frames", legacy_traceback)),
            remote_exception=remote_exception,
            worker=WorkerInfo.from_payload(failure_payload.get("worker")),
            exit_code=failure_payload.get("exit_code"),
            signal=failure_payload.get("signal"),
            timeout=failure_payload.get("timeout"),
            elapsed=failure_payload.get("elapsed"),
            serialization_context=failure_payload.get("serialization_context"),
            raw=failure_payload,
        )

    @classmethod
    def environment(
        cls,
        message: str,
        *,
        task_id: str | None = None,
        call_target: str | None = None,
    ) -> "ExecutionFailure":
        return cls(ExecutionFailureCategory.ENVIRONMENT, message, task_id=task_id, call_target=call_target)

    @classmethod
    def serialization(
        cls,
        message: str,
        *,
        task_id: str | None = None,
        call_target: str | None = None,
        context: str | None = None,
        worker: WorkerInfo | None = None,
    ) -> "ExecutionFailure":
        return cls(
            ExecutionFailureCategory.SERIALIZATION,
            message,
            task_id=task_id,
            call_target=call_target,
            serialization_context=context,
            worker=worker,
        )

    @classmethod
    def worker_connection(
        cls,
        message: str,
        *,
        task_id: str | None = None,
        call_target: str | None = None,
        worker: WorkerInfo | None = None,
    ) -> "ExecutionFailure":
        return cls(
            ExecutionFailureCategory.WORKER_CONNECTION,
            message,
            task_id=task_id,
            call_target=call_target,
            worker=worker,
        )

    @classmethod
    def worker_died(
        cls,
        *,
        task_id: str | None = None,
        call_target: str | None = None,
        worker: WorkerInfo | None = None,
        returncode: int | None = None,
    ) -> "ExecutionFailure":
        exit_code = returncode if returncode is not None and returncode >= 0 else None
        signal = -returncode if returncode is not None and returncode < 0 else None
        if signal is not None:
            message = f"Worker process died with signal {signal}"
        elif exit_code is not None:
            message = f"Worker process died with exit code {exit_code}"
        else:
            message = "Worker process died"
        return cls(
            ExecutionFailureCategory.WORKER_DIED,
            message,
            task_id=task_id,
            call_target=call_target,
            worker=worker,
            exit_code=exit_code,
            signal=signal,
        )

    @classmethod
    def timeout_failure(
        cls,
        *,
        task_id: str | None = None,
        call_target: str | None = None,
        worker: WorkerInfo | None = None,
        timeout: float | None = None,
        elapsed: float | None = None,
    ) -> "ExecutionFailure":
        message = (
            f"Task timed out after {elapsed:.1f}s without worker activity"
            if elapsed is not None
            else "Task timed out without worker activity"
        )
        return cls(
            ExecutionFailureCategory.TIMEOUT,
            message,
            task_id=task_id,
            call_target=call_target,
            worker=worker,
            timeout=timeout,
            elapsed=elapsed,
        )

    def with_defaults(self, *, task_id: str | None = None, call_target: str | None = None) -> "ExecutionFailure":
        if (task_id is None or self.task_id is not None) and (call_target is None or self.call_target is not None):
            return self
        return ExecutionFailure(
            category=self.category,
            message=self.message,
            task_id=self.task_id or task_id,
            call_target=self.call_target or call_target,
            traceback=self.traceback,
            traceback_frames=list(self.traceback_frames),
            remote_exception=self.remote_exception,
            worker=self.worker,
            exit_code=self.exit_code,
            signal=self.signal,
            timeout=self.timeout,
            elapsed=self.elapsed,
            serialization_context=self.serialization_context,
            raw=self.raw,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "category": self.category.value,
            "message": self.message,
            "task_id": self.task_id,
            "call_target": self.call_target,
            "traceback": self.traceback,
            "traceback_frames": list(self.traceback_frames),
            "remote_exception": self.remote_exception.to_payload() if self.remote_exception else None,
            "worker": self.worker.to_payload() if self.worker else None,
            "exit_code": self.exit_code,
            "signal": self.signal,
            "timeout": self.timeout,
            "elapsed": self.elapsed,
            "serialization_context": self.serialization_context,
        }

    def summary(self) -> str:
        if self.category in (
            ExecutionFailureCategory.REMOTE_EXCEPTION,
            ExecutionFailureCategory.INTERNAL_EXCEPTION,
        ):
            prefix = "Remote" if self.category == ExecutionFailureCategory.REMOTE_EXCEPTION else "Local"
            if self.remote_exception is not None:
                exc_name = self.remote_exception.qualified_name or self.remote_exception.type_name or "Exception"
                module = self.remote_exception.module
                source = f" from {module}" if module else ""
                message = self.remote_exception.message or self.message
                return f"{prefix} {exc_name}{source}: {message}"
        if self.category == ExecutionFailureCategory.WORKER_DIED:
            worker = _worker_label(self.worker)
            if self.signal is not None:
                return f"{worker}died with signal {self.signal}"
            if self.exit_code is not None:
                return f"{worker}died with exit code {self.exit_code}"
        if self.category == ExecutionFailureCategory.TIMEOUT:
            if self.elapsed is not None:
                return f"Task timed out after {self.elapsed:.1f}s without worker activity"
            return "Task timed out without worker activity"
        if self.category == ExecutionFailureCategory.SERIALIZATION:
            context = f" while serializing {self.serialization_context}" if self.serialization_context else ""
            return f"Task serialization failure{context}: {self.message}"
        return self.message


def _traceback_to_string(value: list[str] | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return "".join(value)


def _traceback_to_frames(value: list[str] | str | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _worker_label(worker: WorkerInfo | None) -> str:
    if worker is None:
        return "Worker "
    parts = ["Worker"]
    if worker.index is not None:
        parts.append(str(worker.index))
    if worker.pid is not None:
        parts.append(f"pid {worker.pid}")
    return " ".join(parts) + " "


__all__ = [
    "ExecutionFailure",
    "ExecutionFailureCategory",
    "RemoteExceptionInfo",
    "WorkerInfo",
]
