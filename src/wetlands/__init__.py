from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from wetlands._internal.value_codec import ValueDecodingError, ValueEncodingError
from wetlands.debugging import DebugEndpoint, RunningWorker
from wetlands.diagnostics import ExecutionFailure, ExecutionFailureCategory, RemoteExceptionInfo, WorkerInfo
from wetlands.environment_info import ManagedEnvironmentInfo, ManagedEnvironmentState
from wetlands.environment_manager import EnvironmentManager, EnvironmentNotReadyError
from wetlands.lifecycle import (
    EnvironmentGenerationChangedError,
    EnvironmentInUseError,
    EnvironmentNotFoundError,
    EnvironmentRecipeConflictError,
    ManagerCloseError,
    ManagerCloseTimeoutError,
    UnmanagedTargetError,
    WorkerStartError,
)
from wetlands.managed_environment import ManagedEnvironment, WorkerPool
from wetlands.operation import (
    ExecutionError,
    Operation,
    OperationCanceled,
    OperationError,
    OperationEvent,
    OperationEventKind,
    OperationFailure,
    OperationState,
    PreparationError,
    PreparationOperation,
    ProvisioningError,
    ProvisioningOperation,
    RemovalError,
    RemovalOperation,
)
from wetlands.provisioning import PixiInfo, ProvisioningStage
from wetlands.specs import (
    EnvironmentSpec,
    LocalPackage,
    LocalPackageValidationError,
    PostInstallCommand,
    local_package_content_identity,
)
from wetlands.task import (
    ExecutionEvent,
    ExecutionEventKind,
    ExecutionState,
    ExecutionTask,
    InvalidStateError,
)

try:
    __version__ = version("wetlands")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "DebugEndpoint",
    "EnvironmentManager",
    "EnvironmentGenerationChangedError",
    "EnvironmentInUseError",
    "EnvironmentNotFoundError",
    "EnvironmentRecipeConflictError",
    "EnvironmentNotReadyError",
    "EnvironmentSpec",
    "ExecutionError",
    "ExecutionEvent",
    "ExecutionEventKind",
    "ExecutionFailure",
    "ExecutionFailureCategory",
    "ExecutionState",
    "ExecutionTask",
    "LocalPackage",
    "LocalPackageValidationError",
    "local_package_content_identity",
    "ManagedEnvironment",
    "ManagedEnvironmentInfo",
    "ManagedEnvironmentState",
    "ManagerCloseError",
    "ManagerCloseTimeoutError",
    "InvalidStateError",
    "Operation",
    "OperationCanceled",
    "OperationError",
    "OperationEvent",
    "OperationEventKind",
    "OperationFailure",
    "OperationState",
    "PostInstallCommand",
    "PixiInfo",
    "PreparationError",
    "PreparationOperation",
    "ProvisioningError",
    "ProvisioningOperation",
    "ProvisioningStage",
    "RemoteExceptionInfo",
    "RemovalError",
    "RemovalOperation",
    "RunningWorker",
    "ValueDecodingError",
    "ValueEncodingError",
    "UnmanagedTargetError",
    "WorkerPool",
    "WorkerInfo",
    "WorkerStartError",
    "__version__",
]
