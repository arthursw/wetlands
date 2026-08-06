from __future__ import annotations

import inspect
from unittest.mock import patch

import pytest

import wetlands


def test_public_api_exports_only_v2_lifecycle_types() -> None:
    expected = {
        "DebugEndpoint",
        "EnvironmentGenerationChangedError",
        "EnvironmentInUseError",
        "EnvironmentNotFoundError",
        "EnvironmentRecipeConflictError",
        "EnvironmentManager",
        "EnvironmentNotReadyError",
        "EnvironmentSpec",
        "ExecutionError",
        "ExecutionEvent",
        "ExecutionEventKind",
        "ExecutionFailure",
        "ExecutionFailureCategory",
        "ExecutionState",
        "ExecutionTask",
        "InvalidStateError",
        "LocalPackage",
        "LocalPackageValidationError",
        "local_package_content_identity",
        "ManagedEnvironment",
        "ManagedEnvironmentInfo",
        "ManagedEnvironmentState",
        "ManagerCloseError",
        "Operation",
        "OperationCanceled",
        "OperationError",
        "OperationEvent",
        "OperationEventKind",
        "OperationFailure",
        "OperationState",
        "PixiInfo",
        "PostInstallCommand",
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
    }
    assert set(wetlands.__all__) == expected
    assert not hasattr(wetlands, "NDArray")
    assert not hasattr(wetlands, "Task")


def test_manager_constructor_has_no_manager_selection_or_legacy_paths() -> None:
    parameters = inspect.signature(wetlands.EnvironmentManager).parameters
    assert set(parameters) == {
        "root",
        "pixi_executable",
        "network",
        "termination_grace",
    }
    assert not hasattr(wetlands.EnvironmentManager, "create")
    assert not hasattr(wetlands.EnvironmentManager, "install")
    assert not hasattr(wetlands.EnvironmentManager, "load")
    assert not hasattr(wetlands.EnvironmentManager, "execute_commands")
    assert not hasattr(wetlands.EnvironmentManager, "get_process_logger")


def test_provision_exposes_a_pre_mutation_notification() -> None:
    parameters = inspect.signature(wetlands.EnvironmentManager.provision).parameters

    assert parameters["on_mutation_started"].default is None


def test_worker_environment_is_a_public_start_option() -> None:
    parameters = inspect.signature(wetlands.ManagedEnvironment.start).parameters

    assert "worker_environment" in parameters
    assert parameters["worker_environment"].default is None


def test_public_worker_environment_validation_precedes_runtime_reconciliation(tmp_path) -> None:
    manager = wetlands.EnvironmentManager(tmp_path)
    environment = wetlands.ManagedEnvironment._from_ready(
        manager,
        "example",
        tmp_path / "environments" / "example",
        {
            "pixi_version": "0.1.0",
            "pixi_executable": str(tmp_path / "pixi"),
            "generation_id": "generation-1",
            "recipe_hash": "recipe-1",
            "lock_sha256": "lock-1",
        },
    )

    with (
        patch("wetlands.managed_environment.runtime_state.reconcile_persistent_pool") as reconcile,
        pytest.raises(TypeError, match="keys and values must be strings"),
    ):
        environment.start(worker_environment=lambda _index: {"NAME": 1})

    reconcile.assert_not_called()


def test_manager_configuration_is_read_only(tmp_path) -> None:
    manager = wetlands.EnvironmentManager(
        tmp_path,
        network={"https": "https://proxy.invalid"},
    )

    with pytest.raises(AttributeError):
        manager.root = tmp_path / "other"
    assert not hasattr(manager, "wetlands_instance_path")
    with pytest.raises(AttributeError):
        manager.termination_grace = 0
    assert manager.network is not None
    with pytest.raises(TypeError):
        manager.network["https"] = "https://other.invalid"


@pytest.mark.parametrize(
    "termination_grace",
    [float("nan"), float("inf"), float("-inf"), -1, True, "1"],
)
def test_manager_rejects_invalid_termination_grace(
    tmp_path,
    termination_grace,
) -> None:
    with pytest.raises(ValueError, match="finite non-negative"):
        wetlands.EnvironmentManager(tmp_path, termination_grace=termination_grace)


def test_execution_task_does_not_expose_mutable_future() -> None:
    task = wetlands.ExecutionTask()
    assert not hasattr(task, "future")
    assert not hasattr(task, "status")
    assert not hasattr(task, "start")
    assert "CANCELATION" not in wetlands.ExecutionEventKind.__members__


def test_operation_does_not_expose_internal_event_emission() -> None:
    assert not hasattr(wetlands.Operation(), "emit")


def test_environment_handle_cannot_start_or_attach_after_manager_close(
    tmp_path,
) -> None:
    manager = wetlands.EnvironmentManager(tmp_path)
    environment = wetlands.ManagedEnvironment._from_ready(
        manager,
        "example",
        tmp_path / "environments" / "example",
        {
            "pixi_version": "0.1.0",
            "pixi_executable": str(tmp_path / "pixi"),
            "generation_id": "generation-1",
            "recipe_hash": "recipe-1",
            "lock_sha256": "lock-1",
        },
    )
    manager.close()

    with pytest.raises(RuntimeError, match="EnvironmentManager is closed"):
        environment.start()
    with pytest.raises(RuntimeError, match="EnvironmentManager is closed"):
        environment.attach_pool()
