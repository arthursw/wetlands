from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import psutil
import pytest

from wetlands._internal.process_termination import (
    ProcessIdentityError,
    ProcessTerminationError,
    _terminate_posix_group,
    _terminate_windows_tree,
    _wait_for_posix_group_exit,
    capture_process_identity,
    terminate_attached_process_tree,
    terminate_launched_process_tree,
)


def _wait_for_file(path: Path, timeout: float = 5.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return int(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for process marker {path}")


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_launched_worker_termination_kills_child_and_grandchild(tmp_path: Path) -> None:
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
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            leader_code,
            str(child_marker),
            str(grandchild_marker),
            child_code,
            grandchild_code,
        ],
        start_new_session=True,
    )
    identity = capture_process_identity(process.pid)
    process._wetlands_started_at = identity.started_at  # type: ignore[attr-defined]
    process._wetlands_process_group_id = identity.process_group_id  # type: ignore[attr-defined]
    process._wetlands_session_id = identity.session_id  # type: ignore[attr-defined]
    child_pid = grandchild_pid = None
    try:
        child_pid = _wait_for_file(child_marker)
        grandchild_pid = _wait_for_file(grandchild_marker)

        terminate_launched_process_tree(
            process,
            grace=0.2,
            close_windows_job=lambda _process: None,
        )

        assert process.poll() is not None
        for pid in (child_pid, grandchild_pid):
            assert not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
        with pytest.raises(ProcessLookupError):
            os.killpg(process.pid, 0)
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if process.poll() is None:
            process.wait(timeout=5)


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_attached_worker_pid_reuse_mismatch_never_sends_a_signal() -> None:
    with (
        patch("wetlands._internal.process_termination.os.killpg") as kill_group,
        pytest.raises(ProcessIdentityError, match="start identity changed"),
    ):
        terminate_attached_process_tree(
            os.getpid(),
            expected_started_at=0.0,
            expected_process_group_id=os.getpid(),
            expected_session_id=os.getpid(),
            grace=0.01,
        )

    kill_group.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_attached_worker_without_proven_session_ownership_never_sends_a_signal() -> None:
    started_at = psutil.Process().create_time()
    with (
        patch("wetlands._internal.process_termination.os.killpg") as kill_group,
        pytest.raises(ProcessIdentityError, match="ownership is not proven"),
    ):
        terminate_attached_process_tree(
            os.getpid(),
            expected_started_at=started_at,
            expected_process_group_id=None,
            expected_session_id=None,
            grace=0.01,
        )

    kill_group.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_surviving_process_group_is_reported_after_term_and_kill() -> None:
    with (
        patch(
            "wetlands._internal.process_termination._wait_for_posix_group_exit",
            side_effect=[False, False],
        ),
        patch("wetlands._internal.process_termination._posix_group_exists", return_value=True),
        patch("wetlands._internal.process_termination._posix_group_members", return_value=[42, 43]),
        patch("wetlands._internal.process_termination.os.killpg") as kill_group,
        pytest.raises(ProcessTerminationError, match=r"surviving PIDs: \[42, 43\]"),
    ):
        _terminate_posix_group(42, grace=0.01)

    assert kill_group.call_args_list == [
        call(42, signal.SIGTERM),
        call(42, signal.SIGKILL),
    ]


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_term_error_is_ignored_when_group_disappeared() -> None:
    process = MagicMock()
    with (
        patch(
            "wetlands._internal.process_termination._posix_group_exists",
            side_effect=[True, False],
        ),
        patch(
            "wetlands._internal.process_termination.os.killpg",
            side_effect=OSError("group disappeared"),
        ) as kill_group,
    ):
        _terminate_posix_group(42, grace=0.01, process=process)

    kill_group.assert_called_once_with(42, signal.SIGTERM)
    process.wait.assert_called_once_with(timeout=0)


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_term_error_is_reported_when_group_remains_unverified() -> None:
    with (
        patch(
            "wetlands._internal.process_termination._posix_group_exists",
            return_value=True,
        ),
        patch(
            "wetlands._internal.process_termination._wait_for_posix_group_exit",
            return_value=False,
        ) as wait_for_exit,
        patch(
            "wetlands._internal.process_termination.os.killpg",
            side_effect=OSError("permission denied"),
        ),
        pytest.raises(
            ProcessTerminationError,
            match="Could not terminate worker process group 42",
        ),
    ):
        _terminate_posix_group(42, grace=0.01)

    wait_for_exit.assert_called_once_with(42, process=None, timeout=0)


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_kill_error_is_ignored_when_group_disappeared() -> None:
    process = MagicMock()
    with (
        patch(
            "wetlands._internal.process_termination._posix_group_exists",
            return_value=True,
        ),
        patch(
            "wetlands._internal.process_termination._wait_for_posix_group_exit",
            side_effect=[False, True],
        ) as wait_for_exit,
        patch(
            "wetlands._internal.process_termination.os.killpg",
            side_effect=[None, OSError("group disappeared")],
        ) as kill_group,
    ):
        _terminate_posix_group(42, grace=0.01, process=process)

    assert kill_group.call_args_list == [
        call(42, signal.SIGTERM),
        call(42, signal.SIGKILL),
    ]
    assert wait_for_exit.call_args_list == [
        call(42, process=process, timeout=0.01),
        call(42, process=process, timeout=0),
    ]


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_attached_group_leader_is_reaped_when_owned() -> None:
    with (
        patch("wetlands._internal.process_termination.os.waitpid", return_value=(42, 0)) as waitpid,
        patch("wetlands._internal.process_termination._posix_group_exists", return_value=False),
        patch("wetlands._internal.process_termination.psutil.process_iter") as process_iter,
    ):
        assert _wait_for_posix_group_exit(42, process=None, timeout=0)

    waitpid.assert_called_once_with(42, os.WNOHANG)
    process_iter.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_attached_zombie_only_group_is_terminated_when_not_owned() -> None:
    inaccessible = MagicMock()
    inaccessible.pid = 7
    zombie = MagicMock()
    zombie.pid = 42
    zombie.info = {"status": psutil.STATUS_ZOMBIE}
    with (
        patch("wetlands._internal.process_termination.os.waitpid", side_effect=ChildProcessError) as waitpid,
        patch("wetlands._internal.process_termination._posix_group_exists", return_value=True),
        patch(
            "wetlands._internal.process_termination.psutil.process_iter",
            return_value=[inaccessible, zombie],
        ),
        patch(
            "wetlands._internal.process_termination.os.getpgid",
            side_effect=[PermissionError, 42],
        ),
    ):
        assert _wait_for_posix_group_exit(42, process=None, timeout=0)

    waitpid.assert_called_once_with(42, os.WNOHANG)


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_attached_group_with_unknown_member_status_is_not_reported_terminated() -> None:
    unknown = MagicMock()
    unknown.pid = 42
    unknown.info = {"status": None}
    with (
        patch("wetlands._internal.process_termination.os.waitpid", side_effect=ChildProcessError),
        patch("wetlands._internal.process_termination._posix_group_exists", return_value=True),
        patch("wetlands._internal.process_termination.psutil.process_iter", return_value=[unknown]),
        patch("wetlands._internal.process_termination.os.getpgid", return_value=42),
    ):
        assert not _wait_for_posix_group_exit(42, process=None, timeout=0)


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_attached_group_with_live_member_is_not_reported_terminated() -> None:
    zombie = MagicMock()
    zombie.pid = 42
    zombie.info = {"status": psutil.STATUS_ZOMBIE}
    live_child = MagicMock()
    live_child.pid = 43
    live_child.info = {"status": psutil.STATUS_SLEEPING}
    with (
        patch("wetlands._internal.process_termination.os.waitpid", side_effect=ChildProcessError),
        patch("wetlands._internal.process_termination._posix_group_exists", return_value=True),
        patch(
            "wetlands._internal.process_termination.psutil.process_iter",
            return_value=[zombie, live_child],
        ),
        patch("wetlands._internal.process_termination.os.getpgid", return_value=42),
    ):
        assert not _wait_for_posix_group_exit(42, process=None, timeout=0)


def test_windows_launched_worker_uses_job_aware_tree_termination() -> None:
    process = MagicMock()
    process.pid = 42
    close_job = MagicMock()
    with (
        patch("wetlands._internal.process_termination.os.name", "nt"),
        patch("wetlands._internal.process_termination._terminate_windows_tree") as terminate,
    ):
        terminate_launched_process_tree(
            process,
            grace=1.5,
            close_windows_job=close_job,
        )

    terminate.assert_called_once_with(
        42,
        expected_started_at=process._wetlands_started_at,
        grace=1.5,
        process=process,
        close_windows_job=close_job,
    )


def test_windows_recursive_fallback_terminates_then_kills_survivors() -> None:
    parent = MagicMock()
    parent.pid = 42
    parent.create_time.return_value = 1.0
    child = MagicMock()
    child.pid = 43
    parent.children.return_value = [child]

    with (
        patch("wetlands._internal.process_termination.psutil.Process", return_value=parent),
        patch(
            "wetlands._internal.process_termination.psutil.wait_procs",
            side_effect=[([], [child, parent]), ([], [])],
        ),
    ):
        _terminate_windows_tree(
            42,
            expected_started_at=1.0,
            grace=0.1,
        )

    child.terminate.assert_called_once_with()
    parent.terminate.assert_called_once_with()
    child.kill.assert_called_once_with()
    parent.kill.assert_called_once_with()


def test_windows_job_is_closed_even_when_worker_leader_already_exited() -> None:
    process = MagicMock()
    process.pid = 42
    process._wetlands_job_handle = object()
    close_job = MagicMock()

    with patch(
        "wetlands._internal.process_termination.psutil.Process",
        side_effect=psutil.NoSuchProcess(42),
    ):
        _terminate_windows_tree(
            42,
            expected_started_at=1.0,
            grace=0.1,
            process=process,
            close_windows_job=close_job,
        )

    close_job.assert_called_once_with(process)
