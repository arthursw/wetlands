"""Cross-platform real-Pixi acceptance coverage for Wetlands 2."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import socket
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Protocol

import numpy as np
import psutil
import pytest

from wetlands import (
    EnvironmentManager,
    EnvironmentSpec,
    ExecutionState,
    LocalPackage,
    OperationCanceled,
    OperationState,
    OutputStream,
    PostInstallCommand,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.agent_integration,
    pytest.mark.slow,
]

SAMPLEPROJECT_COMMIT = "621e4974ca25ce531773def586ba3ed8e736b3fc"


def _load_example_module(name: str) -> ModuleType:
    path = Path(__file__).parent.parent / "examples" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"wetlands_docs_{name}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load documentation example {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


task_cancellation = _load_example_module("task_cancellation")
task_errors = _load_example_module("task_errors")
task_progress = _load_example_module("task_progress")
task_timeout = _load_example_module("task_timeout")


def _wait_until(predicate: Callable[[], bool], timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    pytest.fail(f"Timed out waiting for {description}")


def _observe(operation: Any) -> Any:
    def report(event: Any) -> None:
        if event.kind.value == "progress":
            return
        detail = event.line if event.line is not None else event.message
        print(f"[{event.stage or 'lifecycle'}:{event.kind.value}] {detail}")

    return operation.listen(report)


class _DAPClient:
    """Small stdlib-only client for exercising a real debug adapter."""

    def __init__(self, host: str, port: int, *, timeout: float = 30.0) -> None:
        self._socket = socket.create_connection((host, port), timeout=timeout)
        self._socket.settimeout(timeout)
        self._stream = self._socket.makefile("rwb")
        self._pending: list[dict[str, Any]] = []
        self._sequence = 1

    def close(self) -> None:
        self._stream.close()
        self._socket.close()

    def request(self, command: str, arguments: dict[str, Any] | None = None) -> int:
        sequence = self._sequence
        self._sequence += 1
        message: dict[str, Any] = {
            "seq": sequence,
            "type": "request",
            "command": command,
        }
        if arguments is not None:
            message["arguments"] = arguments
        self._write_message(message)
        return sequence

    def wait_for_response(self, request_sequence: int) -> dict[str, Any]:
        response = self._wait_for(
            lambda message: message.get("type") == "response" and message.get("request_seq") == request_sequence
        )
        assert response.get("success") is True, response
        return response

    def wait_for_event(self, event: str) -> dict[str, Any]:
        return self._wait_for(lambda message: message.get("type") == "event" and message.get("event") == event)

    def _wait_for(self, predicate: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
        for index, message in enumerate(self._pending):
            if predicate(message):
                return self._pending.pop(index)
        while True:
            message = self._read_message()
            if predicate(message):
                return message
            self._pending.append(message)

    def _write_message(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
        self._stream.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii"))
        self._stream.write(payload)
        self._stream.flush()

    def _read_message(self) -> dict[str, Any]:
        content_length: int | None = None
        while True:
            line = self._stream.readline()
            if not line:
                raise EOFError("Debug adapter closed the DAP connection")
            if line == b"\r\n":
                break
            name, separator, value = line.decode("ascii").partition(":")
            if not separator:
                raise ValueError(f"Malformed DAP header: {line!r}")
            if name.lower() == "content-length":
                content_length = int(value.strip())
        if content_length is None:
            raise ValueError("DAP message did not contain Content-Length")
        payload = _read_exactly(self._stream, content_length)
        message = json.loads(payload)
        if not isinstance(message, dict):
            raise ValueError("DAP message was not an object")
        return message


class _Readable(Protocol):
    def read(self, size: int | None = -1, /) -> bytes: ...


def _read_exactly(stream: _Readable, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise EOFError("Debug adapter closed the DAP message body")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _complete_debug_session(host: str, port: int) -> None:
    client = _DAPClient(host, port)
    try:
        initialize = client.request(
            "initialize",
            {
                "adapterID": "python",
                "clientID": "wetlands-integration-test",
                "clientName": "Wetlands integration test",
                "columnsStartAt1": True,
                "linesStartAt1": True,
                "pathFormat": "path",
                "supportsRunInTerminalRequest": False,
            },
        )
        client.wait_for_response(initialize)

        attach = client.request("attach", {"justMyCode": False})
        client.wait_for_event("initialized")

        configured = client.request("configurationDone")
        client.wait_for_response(configured)
        client.wait_for_response(attach)

        threads = client.request("threads")
        threads_response = client.wait_for_response(threads)
        assert threads_response.get("body", {}).get("threads")

        disconnected = client.request(
            "disconnect",
            {
                "restart": False,
                "terminateDebuggee": False,
            },
        )
        client.wait_for_response(disconnected)
    finally:
        client.close()


def _listener_accepts_connections(host: str, port: int) -> bool:
    try:
        connection = socket.create_connection((host, port), timeout=0.2)
    except OSError:
        return False
    connection.close()
    return True


def _create_worker_package(root: Path) -> Path:
    package = root / "worker package"
    module = package / "wetlands_acceptance_worker"
    module.mkdir(parents=True)
    (package / "pyproject.toml").write_text(
        """
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "wetlands-acceptance-worker"
version = "1.0.0"
""".lstrip(),
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        """
import os
import time

import numpy as np


def add(left, right):
    return left + right


def read_environment(name):
    return os.environ.get(name)


def transform_array(array):
    array[...] = -1
    return np.arange(12, dtype=np.int32).reshape(3, 4)


def cooperative(task=None):
    task.update("cooperative task started")
    while not task.cancel_requested:
        time.sleep(0.02)
    task.cancel()
    return "stopped"


def ignore_cancellation(task=None):
    task.update("stubborn task started")
    time.sleep(300)
""".lstrip(),
        encoding="utf-8",
    )
    return package


def test_real_pixi_debugger_can_reconnect_after_normal_startup(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "manager", termination_grace=1.0)
    python_requirement = f"{sys.version_info.major}.{sys.version_info.minor}.*"
    endpoint = None

    try:
        _observe(manager.prepare()).wait_for(300)
        environment = _observe(
            manager.provision(
                "debugger",
                EnvironmentSpec(python=python_requirement),
            )
        ).wait_for(600)

        with environment.start() as pool:
            assert pool.execute_import("builtins:len", args=([20, 22],), timeout=60) == 2

            endpoint = manager.start_debugger("debugger")
            assert endpoint.adapter == "debugpy"
            assert endpoint.host == "127.0.0.1"

            _complete_debug_session(endpoint.host, endpoint.port)
            assert manager.start_debugger("debugger") == endpoint
            _complete_debug_session(endpoint.host, endpoint.port)

            assert pool.execute_import("builtins:sum", args=([20, 22],), timeout=60) == 42

        assert endpoint is not None
        _wait_until(
            lambda: not _listener_accepts_connections(endpoint.host, endpoint.port),
            30,
            "debug adapter listener shutdown",
        )
    finally:
        manager.close()


def test_real_pixi_managed_commands_run_and_spawn(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "manager", termination_grace=0.5)
    python_requirement = f"{sys.version_info.major}.{sys.version_info.minor}.*"

    try:
        environment = _observe(
            manager.provision(
                "commands",
                EnvironmentSpec(python=python_requirement),
            )
        ).wait_for(600)

        result = environment.run(
            [
                "python",
                "-c",
                "import os, sys; print(os.environ['WETLANDS_COMMAND_TEST']); print('err', file=sys.stderr)",
            ],
            env={"WETLANDS_COMMAND_TEST": "configured"},
            timeout=60,
        )
        assert result.returncode == 0
        assert result.stdout == f"configured{os.linesep}"
        assert result.stderr == f"err{os.linesep}"

        process = environment.spawn(
            ["python", "-u", "-c", "import time; print('ready', flush=True); time.sleep(300)"],
        )
        ready = process.wait_for_line(lambda event: event.text == f"ready{os.linesep}", timeout=60)
        assert ready.stream is OutputStream.STDOUT
        process.terminate(timeout=0.5)
        assert process.wait(check=False).returncode != 0
    finally:
        manager.close(timeout=30)


def test_real_pixi_release_acceptance(tmp_path: Path) -> None:
    package = _create_worker_package(tmp_path)
    manager = EnvironmentManager(tmp_path / "manager", termination_grace=1.0)
    python_requirement = f"{sys.version_info.major}.{sys.version_info.minor}.*"

    try:
        pixi = _observe(manager.prepare()).wait_for(300)
        assert pixi.managed
        assert pixi.executable.is_file()
        assert pixi.version
        assert _observe(manager.prepare()).wait_for(30) == pixi

        post_install_marker = tmp_path / "post-install.ok"
        base_spec = EnvironmentSpec(
            python=python_requirement,
            conda=("numpy", "packaging"),
            pypi=(
                "typing-extensions",
                f"sampleproject @ git+https://github.com/pypa/sampleproject.git@{SAMPLEPROJECT_COMMIT}",
            ),
            local=(LocalPackage(package, editable=True),),
            post_install=(
                PostInstallCommand(
                    (
                        "python",
                        "-c",
                        (
                            "from pathlib import Path; "
                            f"Path({str(post_install_marker)!r}).write_text('ok', encoding='utf-8')"
                        ),
                    )
                ),
            ),
        )

        first = _observe(manager.provision("acceptance", base_spec)).wait_for(600)
        generated_lock = first.pixi_lock_path.read_bytes()
        assert generated_lock
        assert post_install_marker.read_text(encoding="utf-8") == "ok"

        locked_spec = EnvironmentSpec(
            python=base_spec.python,
            conda=base_spec.conda,
            pypi=base_spec.pypi,
            local=base_spec.local,
            post_install=base_spec.post_install,
            pixi_lock=generated_lock,
        )
        environment = _observe(
            manager.provision(
                "acceptance",
                locked_spec,
                replace_existing=True,
            )
        ).wait_for(600)
        assert environment.pixi_lock_path.read_bytes() == generated_lock

        with environment.start(
            worker_environment=lambda index: {"CUDA_VISIBLE_DEVICES": str(index)},
        ) as pool:
            assert (
                pool.execute_import(
                    "wetlands_acceptance_worker:read_environment",
                    args=("CUDA_VISIBLE_DEVICES",),
                    timeout=60,
                )
                == "0"
            )
            installed_sample_version = pool.execute_import(
                "importlib.metadata:version",
                args=("sampleproject",),
                timeout=60,
            )
            assert isinstance(installed_sample_version, str)
            assert installed_sample_version

            original = np.arange(20, dtype=np.float64)[::2]
            original_snapshot = original.copy()
            result = pool.execute_import(
                "wetlands_acceptance_worker:transform_array",
                args=(original,),
                timeout=60,
            )
            np.testing.assert_array_equal(original, original_snapshot)
            np.testing.assert_array_equal(result, np.arange(12, dtype=np.int32).reshape(3, 4))
            assert result.flags.owndata

            cooperative = pool.submit_import(
                "wetlands_acceptance_worker:cooperative",
                context_keyword="task",
            )
            _wait_until(
                lambda: cooperative.message == "cooperative task started",
                30,
                "cooperative worker task startup",
            )
            assert cooperative.cancel()
            with pytest.raises(OperationCanceled):
                cooperative.wait_for(30)
            assert cooperative.state is ExecutionState.CANCELED
            assert pool.execute_import("wetlands_acceptance_worker:add", args=(20, 22), timeout=60) == 42

            stubborn = pool.submit_import(
                "wetlands_acceptance_worker:ignore_cancellation",
                context_keyword="task",
            )
            _wait_until(
                lambda: stubborn.message == "stubborn task started",
                30,
                "non-cooperative worker task startup",
            )
            assert stubborn.cancel()
            with pytest.raises(OperationCanceled):
                stubborn.wait_for(60)
            assert stubborn.state is ExecutionState.CANCELED
            assert pool.execute_import("wetlands_acceptance_worker:add", args=(19, 23), timeout=60) == 42
            assert (
                pool.execute_import(
                    "wetlands_acceptance_worker:read_environment",
                    args=("CUDA_VISIBLE_DEVICES",),
                    timeout=60,
                )
                == "0"
            )

        child_pid_file = tmp_path / "post-install-child.pid"
        interrupted_script = (
            "import subprocess, sys, time; "
            "from pathlib import Path; "
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)']); "
            f"Path({str(child_pid_file)!r}).write_text(str(child.pid), encoding='utf-8'); "
            "time.sleep(300)"
        )
        interrupted = _observe(
            manager.provision(
                "interrupted",
                EnvironmentSpec(
                    python=python_requirement,
                    conda=("packaging",),
                    post_install=(PostInstallCommand(("python", "-c", interrupted_script)),),
                ),
            )
        )
        _wait_until(child_pid_file.exists, 300, "long-running post-install process")
        child_pid = int(child_pid_file.read_text(encoding="utf-8"))
        assert psutil.pid_exists(child_pid)

        assert interrupted.cancel()
        with pytest.raises(OperationCanceled):
            interrupted.wait_for(60)
        assert interrupted.state is OperationState.CANCELED
        assert not (manager.environments_root / "interrupted").exists()
        assert not (manager.environments_root / "interrupted" / ".wetlands" / "ready.json").exists()

        with contextlib.suppress(psutil.NoSuchProcess):
            psutil.Process(child_pid).wait(timeout=10)
        assert not psutil.pid_exists(child_pid)

        rebuilt = _observe(
            manager.provision(
                "interrupted",
                EnvironmentSpec(python=python_requirement, conda=("packaging",)),
            )
        ).wait_for(600)
        assert (rebuilt.path / ".wetlands" / "ready.json").is_file()
        with rebuilt.start() as pool:
            assert pool.execute_import("builtins:len", args=([20, 22],), timeout=60) == 2
    finally:
        manager.close()


def test_documented_complex_task_examples(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    root = tmp_path / "documentation-examples"

    with caplog.at_level("INFO", logger="wetlands"):
        progress = task_progress.main(root)
    assert progress == {
        "progress": [(1, 4), (2, 4), (3, 4), (4, 4)],
        "outputs": {"items_processed": 4},
        "result": [2, 4, 6, 8],
    }
    assert "Worker finished processing items" in caplog.text

    cancellation = task_cancellation.main(root)
    assert cancellation == {
        "cooperative_state": "canceled",
        "forced_state": "canceled",
        "worker_replaced": True,
        "follow_up": 42,
    }

    errors = task_errors.main(root)
    assert errors["remote_category"] == "remote_exception"
    assert errors["remote_target"] == "example_module:raise_example_error"
    assert errors["remote_type"] == "ValueError"
    assert errors["remote_message"] == "The worker could not process this input"
    remote_traceback = errors["remote_traceback"]
    assert isinstance(remote_traceback, str)
    assert "ValueError: The worker could not process this input" in remote_traceback
    assert errors["provisioning_stage"] == "post_install"
    assert errors["provisioning_command"]
    assert errors["provisioning_returncode"] == 7
    provisioning_stderr = errors["provisioning_stderr"]
    assert isinstance(provisioning_stderr, tuple)
    assert any("deliberate setup failure" in line for line in provisioning_stderr)
    assert errors["retry_result"] == 42

    timeout = task_timeout.main(root)
    assert timeout["state_after_timeout"] in {"pending", "running"}
    assert timeout["running_after_timeout"] is True
    assert timeout["final_state"] == "canceled"
