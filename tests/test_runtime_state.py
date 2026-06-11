import json
import os
import stat

from wetlands._internal import runtime_state


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


def test_worker_registry_does_not_store_authkey(tmp_path):
    root = tmp_path / "wetlands"
    runtime_state.load_or_create_root_authkey(root)

    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=tmp_path / "envs" / "cellpose",
        worker_index=0,
        pid=12345,
        port=53122,
        persistent=True,
    )

    registry_text = (root / "state" / "workers.json").read_text()
    registry = json.loads(registry_text)
    assert "auth" not in registry_text.lower()
    assert "key" not in registry["workers"]["cellpose:0"]


def test_registry_updates_preserve_multiple_workers(tmp_path):
    root = tmp_path / "wetlands"

    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=tmp_path / "envs" / "cellpose",
        worker_index=0,
        pid=111,
        port=5001,
        persistent=True,
    )
    runtime_state.record_worker(
        root,
        env_name="cellpose",
        env_path=tmp_path / "envs" / "cellpose",
        worker_index=1,
        pid=222,
        port=5002,
        persistent=True,
    )

    registry = runtime_state.load_workers(root)
    assert set(registry["workers"]) == {"cellpose:0", "cellpose:1"}


def test_remove_worker_deletes_only_matching_entry(tmp_path):
    root = tmp_path / "wetlands"
    for name, index in [("cellpose", 0), ("cellpose", 1), ("other", 0)]:
        runtime_state.record_worker(
            root,
            env_name=name,
            env_path=tmp_path / "envs" / name,
            worker_index=index,
            pid=100 + index,
            port=5000 + index,
            persistent=True,
        )

    runtime_state.remove_worker(root, "cellpose", 0)

    registry = runtime_state.load_workers(root)
    assert set(registry["workers"]) == {"cellpose:1", "other:0"}
