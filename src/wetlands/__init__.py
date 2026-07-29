from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from wetlands._internal.value_codec import ValueDecodingError, ValueEncodingError
from wetlands.environment_manager import EnvironmentManager, EnvironmentNotReadyError
from wetlands.lifecycle import (
    EnvironmentGenerationChangedError,
    EnvironmentInUseError,
    EnvironmentRecipeConflictError,
    ManagerCloseError,
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
)
from wetlands.provisioning import PixiInfo, ProvisioningStage, ProvisioningStep
from wetlands.protocol import CodecCapability, WorkerCapabilities
from wetlands.specs import (
    EnvironmentSpec,
    LocalPackage,
    LocalPackageValidationError,
    PostInstallCommand,
)
from wetlands.task import (
    ExecutionEvent,
    ExecutionEventKind,
    ExecutionFailure,
    ExecutionState,
    ExecutionTask,
    InvalidStateError,
)

try:
    __version__ = version("wetlands")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "CodecCapability",
    "EnvironmentManager",
    "EnvironmentGenerationChangedError",
    "EnvironmentInUseError",
    "EnvironmentRecipeConflictError",
    "EnvironmentNotReadyError",
    "EnvironmentSpec",
    "ExecutionError",
    "ExecutionEvent",
    "ExecutionEventKind",
    "ExecutionFailure",
    "ExecutionState",
    "ExecutionTask",
    "LocalPackage",
    "LocalPackageValidationError",
    "ManagedEnvironment",
    "ManagerCloseError",
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
    "ProvisioningStep",
    "ValueDecodingError",
    "ValueEncodingError",
    "UnmanagedTargetError",
    "WorkerCapabilities",
    "WorkerPool",
    "WorkerStartError",
    "__version__",
]
