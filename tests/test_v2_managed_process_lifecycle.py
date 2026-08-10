from __future__ import annotations

import threading
from pathlib import Path

import pytest

from wetlands.environment_manager import EnvironmentManager
from wetlands.lifecycle import ManagerCloseError, ManagerCloseTimeoutError
from wetlands.managed_environment import ManagedEnvironment


def _environment(manager: EnvironmentManager, name: str) -> ManagedEnvironment:
    environment = ManagedEnvironment._from_ready(
        manager,
        name,
        manager.environments_root / name,
        {
            "pixi_version": "0.48.2",
            "pixi_executable": str(manager.root / "pixi"),
            "generation_id": f"generation-{name}",
            "recipe_hash": f"recipe-{name}",
            "lock_sha256": f"lock-{name}",
        },
    )
    manager._environments[name] = environment
    return environment


def test_manager_close_starts_all_process_cleanup_before_collecting(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path)
    first_environment = _environment(manager, "first")
    second_environment = _environment(manager, "second")
    second_started = threading.Event()

    class Process:
        def __init__(self, environment: ManagedEnvironment, *, waits_for_second: bool) -> None:
            self.environment = environment
            self.waits_for_second = waits_for_second

        def close(self) -> None:
            if self.waits_for_second:
                assert second_started.wait(1)
            else:
                second_started.set()
            self.environment._release_process(self)

    first = Process(first_environment, waits_for_second=True)
    second = Process(second_environment, waits_for_second=False)
    first_environment._register_process(first)
    second_environment._register_process(second)

    manager.close(timeout=1)

    assert second_started.is_set()
    assert not first_environment._has_live_resources()
    assert not second_environment._has_live_resources()


def test_manager_close_retries_process_cleanup_failure(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager, "example")
    cleanup_error = RuntimeError("process cleanup failed")

    class Process:
        calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise cleanup_error
            environment._release_process(self)

    process = Process()
    environment._register_process(process)

    with pytest.raises(ManagerCloseError) as caught:
        manager.close(timeout=1)

    assert caught.value.errors == (cleanup_error,)
    assert environment._has_live_resources()

    manager.close(timeout=1)

    assert process.calls == 2
    assert not environment._has_live_resources()


def test_manager_close_does_not_duplicate_timed_out_process_cleanup(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path)
    environment = _environment(manager, "example")
    entered = threading.Event()
    release = threading.Event()

    class Process:
        calls = 0

        def close(self) -> None:
            self.calls += 1
            entered.set()
            assert release.wait(2)
            environment._release_process(self)

    process = Process()
    environment._register_process(process)

    with pytest.raises(ManagerCloseError) as first:
        manager.close(timeout=0.01)
    assert entered.is_set()
    assert process.calls == 1
    assert any(isinstance(error, ManagerCloseTimeoutError) for error in first.value.errors)

    with pytest.raises(ManagerCloseError):
        manager.close(timeout=0.01)
    assert process.calls == 1

    release.set()
    manager.close(timeout=1)
    assert process.calls == 1
    assert not environment._has_live_resources()
