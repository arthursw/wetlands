import json
import os
import secrets
import socket
import subprocess
import sys
import time
from pathlib import Path

from wetlands._internal import runtime_state
from wetlands.environment_manager import EnvironmentManager


def _read_startup_payload(process: subprocess.Popen[str], token: str, startup_socket: socket.socket) -> dict:
    deadline = time.time() + 10
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"module_executor exited early with return code {process.returncode}")
        startup_socket.settimeout(min(0.1, deadline - time.time()))
        try:
            connection, _address = startup_socket.accept()
        except socket.timeout:
            continue
        with connection:
            connection.settimeout(max(0.1, deadline - time.time()))
            data = b""
            while b"\n" not in data:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                data += chunk
        payload = json.loads(data.split(b"\n", 1)[0].decode("utf-8"))
        assert payload["token"] == token
        return payload
    raise TimeoutError("module_executor did not report startup information")


def test_attach_reconnects_to_persistent_worker_after_detach(tmp_path, monkeypatch):
    root = tmp_path / "wetlands"
    authkey = runtime_state.load_or_create_root_authkey(root)
    assert authkey

    module_path = tmp_path / "math_mod.py"
    module_path.write_text("def add(a, b):\n    return a + b\n")

    executor_path = Path(__file__).resolve().parents[1] / "src" / "wetlands" / "module_executor.py"
    startup_token = secrets.token_urlsafe(32)
    startup_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    startup_socket.bind(("127.0.0.1", 0))
    startup_socket.listen(1)
    startup_host, startup_port = startup_socket.getsockname()
    env = os.environ.copy()
    env["WETLANDS_STARTUP_TOKEN"] = startup_token
    process = subprocess.Popen(
        [
            sys.executable,
            "-u",
            str(executor_path),
            "cellpose",
            "--wetlands_instance_path",
            str(root),
            "--persistent",
            "--startup_host",
            startup_host,
            "--startup_port",
            str(startup_port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        startup_payload = _read_startup_payload(process, startup_token, startup_socket)
        port = startup_payload["port"]
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
        startup_socket.close()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
