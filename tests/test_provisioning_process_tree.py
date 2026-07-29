from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import psutil
import pytest

from wetlands._internal.process_termination import ProcessTerminationError
from wetlands._internal.provisioning import ProcessTreeRunner, ProvisioningStep
from wetlands.operation import (
    OperationCanceled,
    OperationState,
    ProvisioningError,
    ProvisioningOperation,
)
from wetlands.specs import ProvisioningStage


def _wait_for_pid(path: Path, timeout: float = 5.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return int(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for process marker {path}")


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_canceling_runner_kills_term_resistant_child_and_grandchild(tmp_path: Path) -> None:
    child_marker = tmp_path / "child.pid"
    grandchild_marker = tmp_path / "grandchild.pid"
    grandchild_code = """
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
    child_code = """
import os
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
subprocess.Popen([sys.executable, "-c", sys.argv[3], sys.argv[2]])
with open(sys.argv[1], "w", encoding="utf-8") as marker:
    marker.write(str(os.getpid()))
while True:
    time.sleep(1)
"""
    leader_code = """
import signal
import subprocess
import sys
import time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
subprocess.Popen([sys.executable, "-c", sys.argv[3], sys.argv[1], sys.argv[2], sys.argv[4]])
while True:
    time.sleep(1)
"""
    operation: ProvisioningOperation[tuple[str, ...]] = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.2, environment_name="example")
    step = ProvisioningStep(
        "tree",
        ProvisioningStage.POST_INSTALL,
        (
            sys.executable,
            "-c",
            leader_code,
            str(child_marker),
            str(grandchild_marker),
            child_code,
            grandchild_code,
        ),
    )
    operation._start_runner(lambda: runner.run(step), thread_name="test-provisioning-tree")

    child_pid = _wait_for_pid(child_marker)
    grandchild_pid = _wait_for_pid(grandchild_marker)
    assert operation.cancel()
    with pytest.raises(OperationCanceled):
        operation.wait_for(timeout=5)

    assert operation.state is OperationState.CANCELED
    for pid in (child_pid, grandchild_pid):
        assert not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_unverified_cancellation_cleanup_fails_instead_of_canceling(tmp_path: Path) -> None:
    started = tmp_path / "started"
    code = """
import pathlib
import signal
import sys
import time
pathlib.Path(sys.argv[1]).write_text("started", encoding="utf-8")
signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(0.3)
"""
    operation: ProvisioningOperation[tuple[str, ...]] = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1, environment_name="example")
    step = ProvisioningStep(
        "unverified",
        ProvisioningStage.CONDA_INSTALL,
        (sys.executable, "-c", code, str(started)),
    )

    def terminate_but_report_unverified(
        process: subprocess.Popen[str],
        *,
        grace: float,
        close_windows_job,
    ) -> None:
        process.terminate()
        raise ProcessTerminationError("simulated surviving process group")

    with patch(
        "wetlands._internal.provisioning.terminate_launched_process_tree",
        side_effect=terminate_but_report_unverified,
    ):
        operation._start_runner(
            lambda: runner.run(step),
            thread_name="test-provisioning-cleanup-failure",
        )
        _wait_for_pid_like_marker(started)
        assert operation.cancel()
        with pytest.raises(ProvisioningError) as caught:
            operation.wait_for(timeout=5)

    assert operation.state is OperationState.FAILED
    assert caught.value.failure.step_id == "unverified"
    assert caught.value.failure.stage == ProvisioningStage.CONDA_INSTALL.value
    assert "simulated surviving process group" in caught.value.failure.cleanup_error


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_output_reader_failure_makes_cancellation_failed(tmp_path: Path) -> None:
    started = tmp_path / "started"
    code = """
import pathlib
import sys
import time
pathlib.Path(sys.argv[1]).write_text("started", encoding="utf-8")
time.sleep(30)
"""
    operation: ProvisioningOperation[tuple[str, ...]] = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1, environment_name="example")
    step = ProvisioningStep(
        "reader-failure",
        ProvisioningStage.CONDA_INSTALL,
        (sys.executable, "-c", code, str(started)),
    )
    real_popen = subprocess.Popen

    class BrokenReader:
        def readline(self) -> str:
            raise OSError("simulated output drain failure")

    def popen_with_broken_stdout(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        assert process.stdout is not None
        process.stdout.close()
        process.stdout = BrokenReader()
        return process

    with patch(
        "wetlands._internal.provisioning.subprocess.Popen",
        side_effect=popen_with_broken_stdout,
    ):
        operation._start_runner(
            lambda: runner.run(step),
            thread_name="test-provisioning-reader-failure",
        )
        _wait_for_pid_like_marker(started)
        assert operation.cancel()
        with pytest.raises(ProvisioningError) as caught:
            operation.wait_for(timeout=5)

    assert operation.state is OperationState.FAILED
    assert "stdout reader failed: simulated output drain failure" in (caught.value.failure.cleanup_error)


def test_windows_termination_is_graceful_before_job_close_and_force_kill() -> None:
    operation: ProvisioningOperation[tuple[str, ...]] = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1, environment_name="example")
    process = MagicMock()
    process.pid = 42
    process._wetlands_started_at = 1.0
    process.poll.return_value = 1
    parent = MagicMock()
    parent.pid = 42
    parent.create_time.return_value = 1.0
    child = MagicMock()
    child.pid = 43
    parent.children.return_value = [child]
    job = MagicMock()

    with (
        patch("wetlands._internal.provisioning.psutil.Process", return_value=parent),
        patch(
            "wetlands._internal.provisioning.psutil.wait_procs",
            side_effect=[([], [child, parent]), ([], [])],
        ),
    ):
        runner._terminate_windows_process_tree(process, job)

    child.terminate.assert_called_once_with()
    parent.terminate.assert_called_once_with()
    job.close.assert_called_once_with()
    child.kill.assert_called_once_with()
    parent.kill.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=0.1)


def _wait_for_pid_like_marker(path: Path, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not path.exists():
        raise TimeoutError(f"Timed out waiting for marker {path}")
