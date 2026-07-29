"""Cross-platform real-Pixi acceptance coverage for Wetlands 2."""

from __future__ import annotations

import contextlib
import sys
import time
from pathlib import Path
from typing import Any, Callable

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
    PostInstallCommand,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.agent_integration,
    pytest.mark.slow,
]


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
import time

import numpy as np


def add(left, right):
    return left + right


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
            pypi=("typing-extensions",),
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

        with environment.start() as pool:
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
