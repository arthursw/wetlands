import contextlib
import json
import os
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import psutil
import pytest

from wetlands._internal import runtime_state
from wetlands._internal.process_termination import ProcessTerminationError
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION


_WORKER_IDENTITY = {
    "generation_id": "generation-1",
    "recipe_hash": "recipe-1",
    "worker_runtime_version": WORKER_RUNTIME_VERSION,
    "protocol_version": EXECUTION_PROTOCOL_VERSION,
}


def test_authkey_created_once_and_reused(tmp_path):
    root = tmp_path / "wetlands"

    first = runtime_state.load_or_create_root_authkey(root)
    second = runtime_state.load_or_create_root_authkey(root)

    assert len(first) == 32
    assert second == first
    key_path = root / "state" / "auth.key"
    assert key_path.read_bytes() == first
    if os.name != "nt":
        assert stat.S_IMODE(key_path.stat().st_mode) & 0o777 == 0o600


def test_missing_worker_registry_is_the_only_empty_registry_case(tmp_path):
    assert runtime_state.load_workers(tmp_path) == {
        "schema_version": runtime_state.SCHEMA_VERSION,
        "workers": {},
        "controllers": {},
        "persistent_pools": {},
    }


@pytest.mark.parametrize(
    "payload, message",
    [
        ("{broken", "invalid JSON"),
        ("[]", "must contain a JSON object"),
        (
            json.dumps(
                {
                    "schema_version": runtime_state.SCHEMA_VERSION - 1,
                    "workers": {},
                    "controllers": {},
                    "persistent_pools": {},
                }
            ),
            "unsupported schema version",
        ),
        (
            json.dumps(
                {
                    "schema_version": runtime_state.SCHEMA_VERSION,
                    "workers": [],
                    "controllers": {},
                    "persistent_pools": {},
                }
            ),
            "field 'workers' must be an object",
        ),
        (
            json.dumps(
                {
                    "schema_version": runtime_state.SCHEMA_VERSION,
                    "workers": {"bad": 42},
                    "controllers": {},
                    "persistent_pools": {},
                }
            ),
            "contains an invalid entry",
        ),
        (
            json.dumps(
                {
                    "schema_version": runtime_state.SCHEMA_VERSION,
                    "workers": {"bad": {}},
                    "controllers": {},
                    "persistent_pools": {},
                }
            ),
            "contains an invalid entry",
        ),
    ],
)
def test_invalid_worker_registry_fails_closed(tmp_path, payload, message):
    registry_path = runtime_state.state_dir(tmp_path) / runtime_state.WORKERS_FILE
    registry_path.write_text(payload, encoding="utf-8")

    with pytest.raises(runtime_state.RuntimeRegistryError, match=message):
        runtime_state.load_workers(tmp_path)

    assert registry_path.read_text(encoding="utf-8") == payload


def test_unreadable_worker_registry_fails_closed(tmp_path):
    registry_path = runtime_state.state_dir(tmp_path) / runtime_state.WORKERS_FILE
    registry_path.write_text("{}", encoding="utf-8")

    with (
        patch.object(Path, "read_text", side_effect=PermissionError("denied")),
        pytest.raises(runtime_state.RuntimeRegistryError, match="Cannot read"),
    ):
        runtime_state.load_workers(tmp_path)


def test_registry_mutator_never_overwrites_corrupt_state(tmp_path):
    registry_path = runtime_state.state_dir(tmp_path) / runtime_state.WORKERS_FILE
    corrupt = "{existing worker state is corrupt"
    registry_path.write_text(corrupt, encoding="utf-8")

    with pytest.raises(runtime_state.RuntimeRegistryError, match="invalid JSON"):
        runtime_state.begin_persistent_pool_attempt(
            tmp_path,
            env_name="cellpose",
            pool_id="pool-new",
            expected_worker_count=1,
        )

    assert registry_path.read_text(encoding="utf-8") == corrupt


def test_worker_registry_does_not_store_authkey(tmp_path):
    root = tmp_path / "wetlands"
    runtime_state.load_or_create_root_authkey(root)
    runtime_state.begin_persistent_pool_attempt(
        root,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=1,
    )

    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=tmp_path / "envs" / "cellpose",
        worker_index=0,
        pid=os.getpid(),
        port=53122,
        worker_id="pool-1-worker-0",
        management_port=53123,
        persistent=True,
        pool_id="pool-1",
        **_WORKER_IDENTITY,
    )

    registry_text = (root / "state" / "workers.json").read_text()
    registry = json.loads(registry_text)
    assert "auth" not in registry_text.lower()
    assert "key" not in registry["workers"]["cellpose:pool-1:0"]


def test_registry_updates_preserve_multiple_workers(tmp_path):
    root = tmp_path / "wetlands"
    runtime_state.begin_persistent_pool_attempt(
        root,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=2,
    )

    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=tmp_path / "envs" / "cellpose",
        worker_index=0,
        pid=os.getpid(),
        port=5001,
        worker_id="pool-1-worker-0",
        management_port=5101,
        persistent=True,
        pool_id="pool-1",
        **_WORKER_IDENTITY,
    )
    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=tmp_path / "envs" / "cellpose",
        worker_index=1,
        pid=os.getpid(),
        port=5002,
        worker_id="pool-1-worker-1",
        management_port=5102,
        persistent=True,
        pool_id="pool-1",
        **_WORKER_IDENTITY,
    )

    registry = runtime_state.load_workers(root)
    assert set(registry["workers"]) == {
        "cellpose:pool-1:0",
        "cellpose:pool-1:1",
    }


def test_remove_worker_deletes_only_matching_entry(tmp_path):
    root = tmp_path / "wetlands"
    for name, count in (("cellpose", 2), ("other", 1)):
        runtime_state.begin_persistent_pool_attempt(
            root,
            env_name=name,
            pool_id=f"{name}-pool",
            expected_worker_count=count,
        )
    for name, index in [("cellpose", 0), ("cellpose", 1), ("other", 0)]:
        runtime_state.record_worker(
            root,
            env_name=name,
            env_path=tmp_path / "envs" / name,
            worker_index=index,
            pid=os.getpid(),
            port=5000 + index,
            worker_id=f"{name}-pool-worker-{index}",
            management_port=5100 + index,
            persistent=True,
            pool_id=f"{name}-pool",
            **_WORKER_IDENTITY,
        )

    runtime_state.remove_worker(root, "cellpose", 0, "cellpose-pool")

    registry = runtime_state.load_workers(root)
    assert set(registry["workers"]) == {
        "cellpose:cellpose-pool:1",
        "other:other-pool:0",
    }


def test_nonpersistent_worker_pools_are_tracked_with_distinct_pool_ids(tmp_path):
    root = tmp_path / "wetlands"
    for pool_id, port in (("pool-a", 5001), ("pool-b", 5002)):
        runtime_state.record_worker(
            root,
            env_name="cellpose",
            env_path=tmp_path / "envs" / "cellpose",
            worker_index=0,
            pid=os.getpid(),
            port=port,
            worker_id=f"{pool_id}-worker-0",
            management_port=port + 100,
            persistent=False,
            pool_id=pool_id,
            **_WORKER_IDENTITY,
        )

    assert runtime_state.live_workers_for_env(root, "cellpose") == []
    live = runtime_state.live_workers_for_env(
        root,
        "cellpose",
        include_nonpersistent=True,
    )
    assert {entry["pool_id"] for entry in live} == {"pool-a", "pool-b"}

    runtime_state.remove_worker(root, "cellpose", 0, "pool-a")
    remaining = runtime_state.live_workers_for_env(
        root,
        "cellpose",
        include_nonpersistent=True,
    )
    assert [entry["pool_id"] for entry in remaining] == ["pool-b"]


def test_access_denied_preserves_worker_record_and_fails_closed(tmp_path):
    runtime_state.record_worker(
        tmp_path,
        env_name="cellpose",
        env_path=tmp_path / "environments" / "cellpose",
        worker_index=0,
        pid=os.getpid(),
        port=5001,
        worker_id="pool-a-worker-0",
        management_port=5101,
        persistent=False,
        pool_id="pool-a",
        **_WORKER_IDENTITY,
    )
    before = runtime_state.load_workers(tmp_path)

    with (
        patch.object(
            runtime_state.psutil,
            "Process",
            side_effect=psutil.AccessDenied(os.getpid()),
        ),
        pytest.raises(
            runtime_state.WorkerIdentityUnavailableError,
            match="Cannot inspect recorded worker",
        ),
    ):
        runtime_state.live_workers_for_env(
            tmp_path,
            "cellpose",
            include_nonpersistent=True,
        )

    assert runtime_state.load_workers(tmp_path) == before


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_reconciliation_kills_descendants_after_worker_leader_exits(tmp_path):
    child_marker = tmp_path / "child.pid"
    release_marker = tmp_path / "release"
    child_code = """
import os
import signal
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], "w", encoding="utf-8") as marker:
    marker.write(str(os.getpid()))
while True:
    time.sleep(1)
"""
    leader_code = """
import pathlib
import subprocess
import sys
import time
subprocess.Popen([sys.executable, "-c", sys.argv[3], sys.argv[1]])
release = pathlib.Path(sys.argv[2])
while not release.exists():
    time.sleep(0.01)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            leader_code,
            str(child_marker),
            str(release_marker),
            child_code,
        ],
        start_new_session=True,
    )
    child_pid = None
    try:
        deadline = time.monotonic() + 5
        while not child_marker.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert child_marker.exists()
        child_pid = int(child_marker.read_text(encoding="utf-8"))
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
            pid=process.pid,
            port=5001,
            worker_id="pool-1-worker-0",
            management_port=5101,
            persistent=True,
            pool_id="pool-1",
            **_WORKER_IDENTITY,
        )

        release_marker.write_text("exit", encoding="utf-8")
        process.wait(timeout=5)
        lingering = runtime_state.live_workers_for_env(
            tmp_path,
            "cellpose",
            include_nonpersistent=True,
        )
        assert [entry["pid"] for entry in lingering] == [process.pid]
        assert runtime_state.load_workers(tmp_path)["workers"]
        assert runtime_state.reconcile_persistent_pool(
            tmp_path,
            "cellpose",
            grace=0.2,
        )

        registry = runtime_state.load_workers(tmp_path)
        assert registry["workers"] == {}
        assert registry["persistent_pools"] == {}
        assert not psutil.pid_exists(child_pid) or psutil.Process(child_pid).status() == psutil.STATUS_ZOMBIE
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        if process.poll() is None:
            process.wait(timeout=5)


def _record_persistent_worker(root, *, index: int, pool_id: str = "pool-1") -> None:
    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=root / "environments" / "cellpose",
        worker_index=index,
        pid=os.getpid(),
        port=5000 + index,
        worker_id=f"{pool_id}-worker-{index}",
        management_port=5100 + index,
        persistent=True,
        pool_id=pool_id,
        **_WORKER_IDENTITY,
    )


def test_persistent_pool_is_hidden_until_complete_set_is_commissioned(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=2,
    )
    _record_persistent_worker(tmp_path, index=0)
    _record_persistent_worker(tmp_path, index=1)

    assert runtime_state.live_workers_for_env(tmp_path, "cellpose") == []

    runtime_state.commission_persistent_pool(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
    )

    workers = runtime_state.live_workers_for_env(tmp_path, "cellpose")
    assert [worker["worker_index"] for worker in workers] == [0, 1]
    registry = runtime_state.load_workers(tmp_path)
    assert registry["persistent_pools"]["cellpose"]["commissioned"] is True
    assert all(worker["commissioned"] for worker in registry["workers"].values())


def test_partial_persistent_pool_cannot_be_commissioned(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=2,
    )
    _record_persistent_worker(tmp_path, index=0)

    with pytest.raises(RuntimeError, match="incomplete"):
        runtime_state.commission_persistent_pool(
            tmp_path,
            env_name="cellpose",
            pool_id="pool-1",
        )

    registry = runtime_state.load_workers(tmp_path)
    assert registry["persistent_pools"]["cellpose"]["commissioned"] is False
    assert registry["workers"]["cellpose:pool-1:0"]["commissioned"] is False


def test_next_operation_reconciles_uncommissioned_survivors_before_clearing(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=2,
    )
    _record_persistent_worker(tmp_path, index=0)
    entry = runtime_state.load_workers(tmp_path)["workers"]["cellpose:pool-1:0"]

    with patch("wetlands._internal.runtime_state.terminate_attached_process_tree") as terminate:
        assert runtime_state.reconcile_persistent_pool(
            tmp_path,
            "cellpose",
            grace=1.25,
        )

    terminate.assert_called_once_with(
        os.getpid(),
        expected_started_at=entry["process_started_at"],
        expected_process_group_id=entry["process_group_id"],
        expected_session_id=entry["session_id"],
        grace=1.25,
    )
    registry = runtime_state.load_workers(tmp_path)
    assert registry["workers"] == {}
    assert registry["persistent_pools"] == {}


def test_reconciliation_failure_preserves_journal_and_worker_identity(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=1,
    )
    _record_persistent_worker(tmp_path, index=0)

    with (
        patch(
            "wetlands._internal.runtime_state.terminate_attached_process_tree",
            side_effect=ProcessTerminationError("survivor"),
        ),
        pytest.raises(ProcessTerminationError, match="survivor"),
    ):
        runtime_state.reconcile_persistent_pool(
            tmp_path,
            "cellpose",
            grace=1.0,
        )

    registry = runtime_state.load_workers(tmp_path)
    assert "cellpose" in registry["persistent_pools"]
    assert "cellpose:pool-1:0" in registry["workers"]


def test_pool_journal_cannot_be_discarded_while_worker_records_remain(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=1,
    )
    _record_persistent_worker(tmp_path, index=0)

    assert not runtime_state.discard_persistent_pool(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
    )

    registry = runtime_state.load_workers(tmp_path)
    assert "cellpose" in registry["persistent_pools"]
    assert "cellpose:pool-1:0" in registry["workers"]


def test_complete_commissioned_pool_is_not_reconciled(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
        expected_worker_count=1,
    )
    _record_persistent_worker(tmp_path, index=0)
    runtime_state.commission_persistent_pool(
        tmp_path,
        env_name="cellpose",
        pool_id="pool-1",
    )

    with patch("wetlands._internal.runtime_state.terminate_attached_process_tree") as terminate:
        assert not runtime_state.reconcile_persistent_pool(
            tmp_path,
            "cellpose",
            grace=1.0,
        )

    terminate.assert_not_called()
