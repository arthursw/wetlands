from __future__ import annotations

import threading
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import pytest

import wetlands.environment_manager as manager_module
import wetlands.managed_environment as managed_module
from wetlands.environment_manager import EnvironmentManager
from wetlands.lifecycle import ManagerCloseError, ManagerCloseTimeoutError
from wetlands.managed_environment import ManagedEnvironment
from wetlands.operation import OperationCanceled, OperationEventKind, OperationState
from wetlands.specs import EnvironmentSpec


def _environment(manager: EnvironmentManager, name: str = "example") -> ManagedEnvironment:
    return ManagedEnvironment._from_ready(
        manager,
        name,
        manager.environments_root / name,
        {
            "pixi_version": "0.48.2",
            "pixi_executable": str(manager.root / "pixi"),
            "generation_id": "generation-1",
            "recipe_hash": "recipe-1",
            "lock_sha256": "lock-1",
        },
    )


def _wait_until_closed(manager: EnvironmentManager) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            manager._ensure_open()
        except RuntimeError:
            return
        time.sleep(0.01)
    raise AssertionError("manager did not start closing")


def test_close_cancels_and_joins_active_preparation(monkeypatch, tmp_path: Path) -> None:
    started = threading.Event()
    cleaned = threading.Event()

    def prepare(manager: EnvironmentManager, operation, on_mutation_started=None) -> Any:
        started.set()
        while not operation.cancellation_requested:
            time.sleep(0.01)
        cleaned.set()
        raise OperationCanceled(operation.id)

    monkeypatch.setattr(manager_module, "reconcile_shared_memory_leases", lambda root: None)
    monkeypatch.setattr(manager_module, "prepare_pixi", prepare)
    manager = EnvironmentManager(tmp_path)
    operation = manager.prepare()
    assert started.wait(2)

    manager.close()

    assert cleaned.is_set()
    assert operation.state is OperationState.CANCELED
    with pytest.raises(OperationCanceled):
        operation.wait_for()
    with pytest.raises(RuntimeError, match="EnvironmentManager is closed"):
        manager.prepare()


def test_close_rejects_operation_runner_listener_without_closing_manager(
    monkeypatch,
    tmp_path: Path,
) -> None:
    close_attempted = threading.Event()
    listener_ready = threading.Event()
    emit_event = threading.Event()
    close_errors: list[BaseException] = []
    expected = object()

    def prepare(manager: EnvironmentManager, operation, on_mutation_started=None) -> Any:
        listener_ready.set()
        assert emit_event.wait(2)
        operation._emit(OperationEventKind.STEP, "preparing")
        return expected

    def close_from_listener(event) -> None:
        if event.kind is not OperationEventKind.STEP:
            return
        try:
            manager.close()
        except BaseException as error:
            close_errors.append(error)
        finally:
            close_attempted.set()

    monkeypatch.setattr(manager_module, "reconcile_shared_memory_leases", lambda root: None)
    monkeypatch.setattr(manager_module, "prepare_pixi", prepare)
    manager = EnvironmentManager(tmp_path)
    operation = manager.prepare()
    assert listener_ready.wait(2)
    operation.listen(close_from_listener)
    emit_event.set()

    assert operation.wait_for(2) is expected
    assert close_attempted.is_set()
    assert len(close_errors) == 1
    assert isinstance(close_errors[0], RuntimeError)
    assert "active operation listener" in str(close_errors[0])

    manager.prepare().wait_for(2)
    manager.close()


def test_close_aggregates_active_operation_failure(monkeypatch, tmp_path: Path) -> None:
    started = threading.Event()

    def provision(manager, operation, name, spec, replace_existing, on_mutation_started):
        started.set()
        while not operation.cancellation_requested:
            time.sleep(0.01)
        raise RuntimeError("provision cleanup failed")

    monkeypatch.setattr(manager_module, "provision_environment", provision)
    manager = EnvironmentManager(tmp_path)
    operation = manager.provision("example", EnvironmentSpec())
    assert started.wait(2)

    with pytest.raises(ManagerCloseError) as caught:
        manager.close()

    assert operation.state is OperationState.FAILED
    assert len(caught.value.errors) == 1
    assert str(caught.value.errors[0]) == "provision cleanup failed"
    manager.close()


def test_close_snapshots_environments_after_provisioning_finishes(monkeypatch, tmp_path: Path) -> None:
    started = threading.Event()
    release = threading.Event()
    pool_closed = threading.Event()
    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager)

    class Pool:
        _closed = False

        def close(self) -> None:
            self._closed = True
            pool_closed.set()

    environment._pools.append(Pool())

    def provision(manager, operation, name, spec, replace_existing, on_mutation_started):
        started.set()
        assert release.wait(2)
        return environment

    monkeypatch.setattr(manager_module, "provision_environment", provision)
    operation = manager.provision("example", EnvironmentSpec())
    assert started.wait(2)
    close_thread = threading.Thread(target=manager.close)
    close_thread.start()
    _wait_until_closed(manager)

    release.set()
    close_thread.join(2)

    assert not close_thread.is_alive()
    assert operation.state is OperationState.CANCELED
    assert pool_closed.is_set()


def test_close_attempts_every_pool_and_retries_incomplete_cleanup(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager)
    first_error = RuntimeError("first pool failed")
    second_error = RuntimeError("second pool failed")

    class Pool:
        def __init__(self, error: BaseException | None = None) -> None:
            self._closed = False
            self.error = error
            self.calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.error is not None:
                error, self.error = self.error, None
                raise error
            self._closed = True

    first = Pool(first_error)
    second = Pool(second_error)
    already_clean = Pool()
    already_clean._closed = True
    environment._pools.extend((first, second, already_clean))
    manager._environments["example"] = environment

    with pytest.raises(ManagerCloseError) as caught:
        manager.close()

    assert caught.value.errors == (first_error, second_error)
    assert first.calls == second.calls == 1
    assert already_clean.calls == 0

    manager.close()

    assert first._closed and second._closed
    assert first.calls == second.calls == 2
    assert already_clean.calls == 0
    manager.close()
    assert first.calls == second.calls == 2
    assert already_clean.calls == 0


@pytest.mark.parametrize(
    "timeout",
    [-1, float("nan"), float("inf"), float("-inf"), True, "1"],
)
def test_close_rejects_invalid_timeout(tmp_path: Path, timeout) -> None:
    manager = EnvironmentManager(tmp_path)

    with pytest.raises(ValueError, match="finite non-negative"):
        manager.close(timeout=timeout)

    manager.close()


def test_close_timeout_is_one_deadline_for_active_operations(monkeypatch, tmp_path: Path) -> None:
    started = threading.Event()
    release = threading.Event()

    def prepare(manager: EnvironmentManager, operation, on_mutation_started=None) -> Any:
        started.set()
        assert release.wait(2)
        raise OperationCanceled(operation.id)

    monkeypatch.setattr(manager_module, "reconcile_shared_memory_leases", lambda root: None)
    monkeypatch.setattr(manager_module, "prepare_pixi", prepare)
    manager = EnvironmentManager(tmp_path)
    operation = manager.prepare()
    assert started.wait(2)

    before = time.monotonic()
    with pytest.raises(ManagerCloseError) as caught:
        manager.close(timeout=0.05)
    elapsed = time.monotonic() - before

    assert elapsed < 0.5
    assert operation.cancellation_requested
    timeout_error = next(error for error in caught.value.errors if isinstance(error, ManagerCloseTimeoutError))
    assert timeout_error.phase == "active operations"
    assert timeout_error.timeout == 0.05

    release.set()
    manager.close(timeout=1)


def test_close_timeout_bounds_manager_work(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    manager = EnvironmentManager(tmp_path)

    def hold_manager_work() -> None:
        with manager._manager_work():
            entered.set()
            assert release.wait(2)

    work = threading.Thread(target=hold_manager_work)
    work.start()
    assert entered.wait(2)

    before = time.monotonic()
    with pytest.raises(ManagerCloseError) as caught:
        manager.close(timeout=0.05)
    assert time.monotonic() - before < 0.5
    assert any(
        isinstance(error, ManagerCloseTimeoutError) and error.phase == "manager work" for error in caught.value.errors
    )

    release.set()
    work.join(2)
    manager.close(timeout=1)


def test_close_timeout_does_not_duplicate_pool_cleanup(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager)

    class Pool:
        _closed = False

        def __init__(self) -> None:
            self.calls = 0

        def close(self) -> None:
            self.calls += 1
            entered.set()
            assert release.wait(2)
            self._closed = True

    pool = Pool()
    environment._pools.append(pool)
    manager._environments["example"] = environment

    with pytest.raises(ManagerCloseError) as first:
        manager.close(timeout=0.05)
    assert entered.is_set()
    assert pool.calls == 1
    assert any(isinstance(error, ManagerCloseTimeoutError) for error in first.value.errors)

    with pytest.raises(ManagerCloseError):
        manager.close(timeout=0.05)
    assert pool.calls == 1

    release.set()
    manager.close(timeout=1)
    assert pool.calls == 1
    assert pool._closed


def test_close_passes_only_remaining_deadline_to_reclaimer(monkeypatch, tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path)
    received: list[float | None] = []

    def timed_out_close(timeout: float | None = 0.25) -> bool:
        received.append(timeout)
        return False

    monkeypatch.setattr(manager._environment_reclaimer, "close", timed_out_close)

    with pytest.raises(ManagerCloseError) as caught:
        manager.close(timeout=0.05)

    assert len(received) == 1
    assert received[0] is not None
    assert 0 <= received[0] <= 0.05 + 1e-12
    assert any(
        isinstance(error, ManagerCloseTimeoutError) and error.phase == "environment reclaimer"
        for error in caught.value.errors
    )

    monkeypatch.setattr(manager._environment_reclaimer, "close", lambda timeout=0.25: True)
    manager.close(timeout=1)


def test_close_without_timeout_keeps_bounded_reclaimer_default(monkeypatch, tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path)
    received: list[float | None] = []

    def incomplete_close(timeout: float | None = 0.25) -> bool:
        received.append(timeout)
        return False

    monkeypatch.setattr(manager._environment_reclaimer, "close", incomplete_close)

    manager.close()
    manager.close()

    assert received == [0.25]


def test_close_waits_for_worker_start_registration_and_rejects_new_work(
    monkeypatch,
    tmp_path: Path,
) -> None:
    launch_started = threading.Event()
    release_launch = threading.Event()
    runtime_closed = threading.Event()

    class Runtime:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._workers = [object()]

        def launch(self, **kwargs: Any) -> None:
            launch_started.set()
            assert release_launch.wait(2)

        def _exit(self) -> None:
            runtime_closed.set()

        def _raise_if_failed(self) -> None:
            return

    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager)
    manager._environments["example"] = environment
    monkeypatch.setattr(environment, "_require_current_generation", lambda: None)
    monkeypatch.setattr(managed_module, "environment_lifecycle_gate", lambda manager, name: nullcontext())
    monkeypatch.setattr(managed_module.runtime_state, "reconcile_persistent_pool", lambda *args, **kwargs: None)
    monkeypatch.setattr(managed_module, "ExternalEnvironment", Runtime)

    start_result: list[Any] = []
    start_thread = threading.Thread(target=lambda: start_result.append(environment.start()))
    start_thread.start()
    assert launch_started.wait(2)
    close_thread = threading.Thread(target=manager.close)
    close_thread.start()
    _wait_until_closed(manager)

    with pytest.raises(RuntimeError, match="EnvironmentManager is closed"):
        manager.environment("example")
    with pytest.raises(RuntimeError, match="EnvironmentManager is closed"):
        environment.attach_pool()
    assert close_thread.is_alive()

    release_launch.set()
    start_thread.join(2)
    close_thread.join(2)

    assert len(start_result) == 1
    assert runtime_closed.is_set()
    assert not start_thread.is_alive()
    assert not close_thread.is_alive()


@pytest.mark.parametrize(
    "timeout",
    [0, -1, float("nan"), float("inf"), float("-inf"), True, "1"],
)
def test_attach_pool_rejects_invalid_timeout(tmp_path: Path, timeout) -> None:
    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager)

    with pytest.raises(ValueError, match="positive finite"):
        environment.attach_pool(timeout=timeout)
