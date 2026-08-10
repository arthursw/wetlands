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
        "ManagedProcess",
        "ManagedProcessResult",
        "ManagerCloseError",
        "ManagerCloseTimeoutError",
        "Operation",
        "OperationCanceled",
        "OperationError",
        "OperationEvent",
        "OperationEventKind",
        "OperationFailure",
        "OperationState",
        "OutputEvent",
        "OutputStream",
        "PixiInfo",
        "PostInstallCommand",
        "PreparationError",
        "PreparationOperation",
        "ProvisioningError",
        "ProvisioningOperation",
        "ProvisioningStage",
        "ProcessCleanupError",
        "ProcessError",
        "ProcessEventLagError",
        "ProcessExitError",
        "ProcessLineTimeoutError",
        "ProcessOutputLimitError",
        "ProcessTimeoutError",
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


def test_manager_close_exposes_one_keyword_only_total_timeout() -> None:
    parameters = inspect.signature(wetlands.EnvironmentManager.close).parameters

    assert tuple(parameters) == ("self", "timeout")
    assert parameters["timeout"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["timeout"].default is None
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


def test_managed_environment_exposes_only_argv_based_command_execution() -> None:
    spawn = inspect.signature(wetlands.ManagedEnvironment.spawn).parameters
    assert tuple(spawn) == ("self", "argv", "cwd", "env", "output_limit")
    assert spawn["cwd"].kind is inspect.Parameter.KEYWORD_ONLY
    assert spawn["cwd"].default is None
    assert spawn["env"].kind is inspect.Parameter.KEYWORD_ONLY
    assert spawn["env"].default is None
    assert spawn["output_limit"].kind is inspect.Parameter.KEYWORD_ONLY
    assert spawn["output_limit"].default == 1_048_576

    run = inspect.signature(wetlands.ManagedEnvironment.run).parameters
    assert tuple(run) == ("self", "argv", "cwd", "env", "timeout", "output_limit", "check")
    assert run["cwd"].kind is inspect.Parameter.KEYWORD_ONLY
    assert run["cwd"].default is None
    assert run["env"].kind is inspect.Parameter.KEYWORD_ONLY
    assert run["env"].default is None
    assert run["timeout"].kind is inspect.Parameter.KEYWORD_ONLY
    assert run["timeout"].default is None
    assert run["output_limit"].kind is inspect.Parameter.KEYWORD_ONLY
    assert run["output_limit"].default == 1_048_576
    assert run["check"].kind is inspect.Parameter.KEYWORD_ONLY
    assert run["check"].default is True

    assert not hasattr(wetlands.ManagedEnvironment, "execute_commands")


def test_managed_process_exposes_a_bounded_supervision_surface() -> None:
    for name in ("argv", "environment", "generation_id", "pid", "returncode", "running"):
        assert isinstance(inspect.getattr_static(wetlands.ManagedProcess, name), property)

    wait = inspect.signature(wetlands.ManagedProcess.wait).parameters
    assert tuple(wait) == ("self", "timeout", "check")
    assert wait["timeout"].default is None
    assert wait["check"].kind is inspect.Parameter.KEYWORD_ONLY
    assert wait["check"].default is True

    wait_async = inspect.signature(wetlands.ManagedProcess.wait_async).parameters
    assert tuple(wait_async) == ("self", "timeout", "check")
    assert wait_async["timeout"].default is None
    assert wait_async["check"].kind is inspect.Parameter.KEYWORD_ONLY
    assert wait_async["check"].default is True

    events = inspect.signature(wetlands.ManagedProcess.events).parameters
    assert tuple(events) == ("self", "replay")
    assert events["replay"].kind is inspect.Parameter.KEYWORD_ONLY
    assert events["replay"].default is True

    wait_for_line = inspect.signature(wetlands.ManagedProcess.wait_for_line).parameters
    assert tuple(wait_for_line) == ("self", "predicate", "timeout", "replay")
    assert wait_for_line["timeout"].default is None
    assert wait_for_line["replay"].kind is inspect.Parameter.KEYWORD_ONLY
    assert wait_for_line["replay"].default is True

    terminate = inspect.signature(wetlands.ManagedProcess.terminate).parameters
    assert tuple(terminate) == ("self", "timeout")
    assert terminate["timeout"].default is None
    assert tuple(inspect.signature(wetlands.ManagedProcess.kill).parameters) == ("self",)
    assert tuple(inspect.signature(wetlands.ManagedProcess.close).parameters) == ("self",)

    assert not hasattr(wetlands.ManagedProcess, "popen")
    assert not hasattr(wetlands.ManagedProcess, "process")


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
