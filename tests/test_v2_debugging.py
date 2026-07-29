from __future__ import annotations

import os
import sys
import threading
from unittest.mock import MagicMock, patch

import pytest

from wetlands import DebugEndpoint, EnvironmentManager, RunningWorker
from wetlands._internal import management, runtime_state
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION
from wetlands.module_executor import _DebuggerState


def _entry(*, worker_id: str = "worker-1", index: int = 0) -> dict[str, object]:
    return {
        "worker_id": worker_id,
        "env_name": "cellpose",
        "env_path": "/managed/cellpose",
        "worker_index": index,
        "pool_id": "pool-1",
        "pid": os.getpid(),
        "port": 5000 + index,
        "management_port": 5100 + index,
        "persistent": True,
        "generation_id": "generation-1",
        "recipe_hash": "recipe-1",
        "worker_runtime_version": WORKER_RUNTIME_VERSION,
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "debugger": None,
    }


def test_running_workers_returns_public_immutable_values(tmp_path) -> None:
    manager = EnvironmentManager(tmp_path)
    entry = _entry()
    entry["debugger"] = {
        "adapter": "debugpy",
        "host": "127.0.0.1",
        "port": 5678,
    }

    with patch.object(manager, "_running_worker_entries", return_value=[entry]):
        workers = manager.running_workers("cellpose")

    assert workers == (
        RunningWorker(
            id="worker-1",
            environment="cellpose",
            pool_id="pool-1",
            index=0,
            process_id=os.getpid(),
            persistent=True,
            debugger=DebugEndpoint(
                worker_id="worker-1",
                adapter="debugpy",
                host="127.0.0.1",
                port=5678,
            ),
        ),
    )


def test_worker_discovery_requires_current_ready_identity(tmp_path) -> None:
    manager = EnvironmentManager(tmp_path)
    environment = MagicMock()
    environment.name = "cellpose"
    environment.path = tmp_path / "environments" / "cellpose"
    environment.generation_id = "generation-1"
    environment.recipe_hash = "recipe-1"

    with (
        patch.object(manager, "environment", return_value=environment),
        patch.object(runtime_state, "live_workers_for_env", return_value=[]) as live,
    ):
        assert manager.running_workers("cellpose") == ()

    live.assert_called_once_with(
        manager.root,
        "cellpose",
        expected_identity={
            "env_path": str(environment.path),
            "generation_id": "generation-1",
            "recipe_hash": "recipe-1",
            "worker_runtime_version": WORKER_RUNTIME_VERSION,
            "protocol_version": EXECUTION_PROTOCOL_VERSION,
        },
        include_nonpersistent=True,
    )


def test_start_debugger_does_not_claim_execution_controller(tmp_path) -> None:
    manager = EnvironmentManager(tmp_path)
    entry = _entry()
    response = {
        **management._identity(entry),
        "action": "debugger_started",
        "management_protocol_version": 1,
        "adapter": "debugpy",
        "host": "127.0.0.1",
        "port": 5678,
    }

    with (
        patch.object(manager, "_running_worker_entries", return_value=[entry]),
        patch.object(runtime_state, "load_or_create_root_authkey", return_value=b"key"),
        patch.object(management, "start_debugger", return_value=response) as start,
        patch.object(runtime_state, "record_debugger") as record,
        patch.object(runtime_state, "claim_controller") as claim,
    ):
        endpoint = manager.start_debugger("cellpose")

    assert endpoint == DebugEndpoint("worker-1", "debugpy", "127.0.0.1", 5678)
    start.assert_called_once_with(entry, b"key")
    record.assert_called_once_with(
        manager.root,
        worker_id="worker-1",
        adapter="debugpy",
        host="127.0.0.1",
        port=5678,
    )
    claim.assert_not_called()


def test_start_debugger_requires_worker_selection_for_multiple_workers(tmp_path) -> None:
    manager = EnvironmentManager(tmp_path)
    with patch.object(
        manager,
        "_running_worker_entries",
        return_value=[_entry(worker_id="worker-1"), _entry(worker_id="worker-2", index=1)],
    ):
        with pytest.raises(ValueError, match="select one"):
            manager.start_debugger("cellpose")


def test_management_identity_mismatch_fails_closed() -> None:
    entry = _entry()
    response = {
        **management._identity(entry),
        "action": "management_hello",
        "management_protocol_version": 1,
    }
    assert isinstance(entry["pid"], int)
    response["pid"] = entry["pid"] + 1

    with pytest.raises(management.ManagementConnectionError, match="identity mismatch"):
        management._validate_message(response, management._identity(entry), "management_hello")


def test_management_connect_timeout_is_wrapped_as_public_runtime_error() -> None:
    entry = _entry()

    with (
        patch.object(management, "_connect", side_effect=TimeoutError("slow endpoint")),
        pytest.raises(management.ManagementConnectionError, match="slow endpoint"),
    ):
        management.start_debugger(entry, b"auth-key")


def test_debugger_registry_publication_is_bound_to_worker_id(tmp_path) -> None:
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=1,
    )
    runtime_state.record_worker(
        tmp_path,
        env_name="cellpose",
        env_path=tmp_path / "environments" / "cellpose",
        worker_index=0,
        pid=os.getpid(),
        port=5000,
        worker_id="worker-1",
        management_port=5100,
        persistent=True,
        generation_id="generation-1",
        recipe_hash="recipe-1",
        worker_runtime_version=WORKER_RUNTIME_VERSION,
        protocol_version=EXECUTION_PROTOCOL_VERSION,
        pool_id="pool-1",
    )

    runtime_state.record_debugger(
        tmp_path,
        worker_id="worker-1",
        adapter="debugpy",
        host="127.0.0.1",
        port=5678,
    )

    entry = runtime_state.load_workers(tmp_path)["workers"]["cellpose:pool-1:0"]
    assert entry["debugger"] == {
        "adapter": "debugpy",
        "host": "127.0.0.1",
        "port": 5678,
    }
    with pytest.raises(RuntimeError, match="no longer registered"):
        runtime_state.record_debugger(
            tmp_path,
            worker_id="missing",
            adapter="debugpy",
            host="127.0.0.1",
            port=5679,
        )


def test_worker_debugger_start_is_lazy_and_idempotent(monkeypatch) -> None:
    fake_debugpy = MagicMock()
    fake_debugpy.listen.return_value = ("127.0.0.1", 5678)
    monkeypatch.setitem(sys.modules, "debugpy", fake_debugpy)
    debugger = _DebuggerState()

    assert debugger.port is None
    assert debugger.start() == 5678
    assert debugger.start() == 5678

    fake_debugpy.configure.assert_called_once_with(subProcess=False)
    fake_debugpy.listen.assert_called_once_with(("127.0.0.1", 0))


def test_concurrent_worker_debugger_start_reuses_one_adapter(monkeypatch) -> None:
    fake_debugpy = MagicMock()
    fake_debugpy.listen.return_value = ("127.0.0.1", 5678)
    monkeypatch.setitem(sys.modules, "debugpy", fake_debugpy)
    debugger = _DebuggerState()
    results: list[int] = []

    threads = [threading.Thread(target=lambda: results.append(debugger.start())) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [5678] * 4
    fake_debugpy.listen.assert_called_once_with(("127.0.0.1", 0))
