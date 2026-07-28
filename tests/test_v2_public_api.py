from __future__ import annotations

import inspect

import pytest

import wetlands


def test_public_api_exports_only_v2_lifecycle_types() -> None:
    expected = {
        "CodecCapability",
        "EnvironmentGenerationChangedError",
        "EnvironmentInUseError",
        "EnvironmentRecipeConflictError",
        "EnvironmentManager",
        "EnvironmentNotReadyError",
        "EnvironmentSpec",
        "ExecutionError",
        "ExecutionEvent",
        "ExecutionEventKind",
        "ExecutionFailure",
        "ExecutionState",
        "ExecutionTask",
        "LocalPackage",
        "ManagedEnvironment",
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
        "ProvisioningStep",
        "ValueDecodingError",
        "ValueEncodingError",
        "UnmanagedTargetError",
        "WorkerCapabilities",
        "WorkerPool",
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


def test_manager_configuration_is_read_only(tmp_path) -> None:
    manager = wetlands.EnvironmentManager(
        tmp_path,
        network={"https": "https://proxy.invalid"},
    )

    with pytest.raises(AttributeError):
        manager.root = tmp_path / "other"
    with pytest.raises(AttributeError):
        manager.wetlands_instance_path = tmp_path / "other"
    with pytest.raises(AttributeError):
        manager.termination_grace = 0
    assert manager.network is not None
    with pytest.raises(TypeError):
        manager.network["https"] = "https://other.invalid"


def test_execution_task_does_not_expose_mutable_future() -> None:
    task = wetlands.ExecutionTask()
    assert not hasattr(task, "future")
    assert not hasattr(task, "status")
    assert "CANCELATION" not in wetlands.ExecutionEventKind.__members__


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
