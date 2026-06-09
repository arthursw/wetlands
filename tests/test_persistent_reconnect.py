import subprocess
import sys
import time
from pathlib import Path

from wetlands._internal import runtime_state
from wetlands.environment_manager import EnvironmentManager


def _read_port(process: subprocess.Popen[str]) -> int:
    deadline = time.time() + 10
    while time.time() < deadline:
        line = process.stdout.readline() if process.stdout is not None else ""
        if line.startswith("Listening port "):
            return int(line.replace("Listening port ", ""))
    raise TimeoutError("module_executor did not report its listening port")


def test_attach_reconnects_to_persistent_worker_after_detach(tmp_path, monkeypatch):
    root = tmp_path / "wetlands"
    authkey = runtime_state.load_or_create_root_authkey(root)
    assert authkey

    module_path = tmp_path / "math_mod.py"
    module_path.write_text("def add(a, b):\n    return a + b\n")

    executor_path = Path(__file__).resolve().parents[1] / "src" / "wetlands" / "module_executor.py"
    process = subprocess.Popen(
        [
            sys.executable,
            "-u",
            str(executor_path),
            "cellpose",
            "--wetlands_instance_path",
            str(root),
            "--persistent",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        port = _read_port(process)
        runtime_state.record_worker(
            root,
            env_name="cellpose",
            env_path=tmp_path / "envs" / "cellpose",
            worker_index=0,
            pid=process.pid,
            port=port,
            persistent=True,
        )

        monkeypatch.setattr(EnvironmentManager, "install_conda", lambda self: None)
        manager = EnvironmentManager(wetlands_instance_path=root, conda_path=tmp_path / "conda", manager="micromamba")
        env = manager.attach("cellpose")

        assert env.execute(module_path, "add", args=(2, 3)) == 5
        env.detach()
        assert process.poll() is None
        assert runtime_state.load_workers(root)["workers"]["cellpose:0"]["pid"] == process.pid

        time.sleep(0.2)
        reattached = manager.attach("cellpose")
        assert reattached.execute(module_path, "add", args=(4, 6)) == 10
        reattached.exit()

        process.wait(timeout=5)
        assert runtime_state.load_workers(root)["workers"] == {}
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
