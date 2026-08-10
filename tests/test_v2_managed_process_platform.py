from __future__ import annotations

import os
import signal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import call, patch

import pytest

from wetlands._internal.process_termination import ProcessIdentity
from wetlands.managed_process import ManagedProcess, _LaunchOptions


class _Pipe:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Process:
    def __init__(self, pid: int = 42) -> None:
        self.pid = pid
        self.stdout = _Pipe()
        self.stderr = _Pipe()
        self._handle = 99
        self._returncode: int | None = None
        self.signals: list[int] = []
        self.killed = False
        self.waited = False

    def poll(self) -> int | None:
        return self._returncode

    def send_signal(self, sent_signal: int) -> None:
        self.signals.append(sent_signal)

    def kill(self) -> None:
        self.killed = True
        self._returncode = 1

    def wait(self, timeout: float | None = None) -> int:
        self.waited = True
        if self._returncode is None:
            self._returncode = 1
        return self._returncode


class _Environment:
    def __init__(self, path: Path) -> None:
        self.name = "test"
        self.path = path
        self.generation_id = "generation-1"
        self.pixi_executable_path = path / "pixi.exe"
        self.pixi_manifest_path = path / "pixi.toml"
        self._manager = SimpleNamespace(
            root=path.parent,
            state_root=path.parent / "state",
            network=None,
            termination_grace=0.1,
        )
        self.processes: list[ManagedProcess] = []

    def _register_process(self, process: ManagedProcess) -> None:
        self.processes.append(process)

    def _release_process(self, process: ManagedProcess) -> None:
        self.processes.remove(process)


def _options(environment: _Environment) -> _LaunchOptions:
    return _LaunchOptions(("example",), environment.path, {}, 1024)


def test_windows_launch_assigns_job_before_readers_resume_and_supervision(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()
    events: list[str] = []

    def popen(argv: list[str], **kwargs: Any) -> _Process:
        events.append("popen")
        assert kwargs["creationflags"] & 0x00000200
        assert kwargs["creationflags"] & 0x00000004
        return process

    with (
        patch("wetlands.managed_process.os.name", "nt"),
        patch("wetlands.managed_process.subprocess.CREATE_NEW_PROCESS_GROUP", 0x00000200, create=True),
        patch("wetlands.managed_process.subprocess.Popen", side_effect=popen),
        patch(
            "wetlands.managed_process.capture_process_identity",
            return_value=ProcessIdentity(process.pid, 1.0, None, None),
        ),
        patch("wetlands.managed_process._assign_windows_kill_job", side_effect=lambda _process: events.append("job")),
        patch.object(
            ManagedProcess,
            "_start_readers",
            side_effect=lambda _handle: events.append("readers"),
            autospec=True,
        ),
        patch.object(
            ManagedProcess,
            "_resume_windows_process",
            side_effect=lambda _handle: events.append("resume"),
            autospec=True,
        ),
        patch.object(
            ManagedProcess,
            "_start_supervisor",
            side_effect=lambda _handle: events.append("supervisor"),
            autospec=True,
        ),
    ):
        handle = ManagedProcess._launch_validated(environment=environment, options=_options(environment))  # type: ignore[arg-type]

    assert events == ["popen", "job", "readers", "resume", "supervisor"]
    environment._release_process(handle)


def test_windows_job_assignment_failure_kills_suspended_child_and_releases_ownership(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()

    with (
        patch("wetlands.managed_process.os.name", "nt"),
        patch("wetlands.managed_process.subprocess.CREATE_NEW_PROCESS_GROUP", 0x00000200, create=True),
        patch("wetlands.managed_process.subprocess.Popen", return_value=process),
        patch(
            "wetlands.managed_process.capture_process_identity",
            return_value=ProcessIdentity(process.pid, 1.0, None, None),
        ),
        patch("wetlands.managed_process._assign_windows_kill_job", side_effect=OSError("job assignment failed")),
        pytest.raises(OSError, match="job assignment failed"),
    ):
        ManagedProcess._launch_validated(environment=environment, options=_options(environment))  # type: ignore[arg-type]

    assert process.killed
    assert process.waited
    assert environment.processes == []


def test_windows_resume_failure_cleans_assigned_process_and_releases_ownership(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()

    def assign_job(assigned: _Process) -> None:
        assigned._wetlands_job_handle = 123  # type: ignore[attr-defined]

    def cleaned_tree(handle: ManagedProcess, _grace: float) -> None:
        process.kill()
        process.wait()

    with (
        patch("wetlands.managed_process.os.name", "nt"),
        patch("wetlands.managed_process.subprocess.CREATE_NEW_PROCESS_GROUP", 0x00000200, create=True),
        patch("wetlands.managed_process.subprocess.Popen", return_value=process),
        patch(
            "wetlands.managed_process.capture_process_identity",
            return_value=ProcessIdentity(process.pid, 1.0, None, None),
        ),
        patch("wetlands.managed_process._assign_windows_kill_job", side_effect=assign_job),
        patch.object(ManagedProcess, "_start_readers", autospec=True),
        patch.object(ManagedProcess, "_resume_windows_process", autospec=True, side_effect=OSError("resume failed")),
        patch.object(ManagedProcess, "_terminate_tree", autospec=True, side_effect=cleaned_tree),
        pytest.raises(OSError, match="resume failed"),
    ):
        ManagedProcess._launch_validated(environment=environment, options=_options(environment))  # type: ignore[arg-type]

    assert process.killed
    assert process.waited
    assert environment.processes == []


def test_windows_termination_signals_then_forces_and_verifies_job(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()
    process._wetlands_job_handle = 123  # type: ignore[attr-defined]
    handle = ManagedProcess(
        environment=environment,  # type: ignore[arg-type]
        argv=("example",),
        process=process,  # type: ignore[arg-type]
        output_limit=1024,
        started_at=1.0,
    )
    handle._identity = ProcessIdentity(process.pid, 1.0, None, None)
    events: list[str] = []
    active = iter((1, 1, 0, 0))

    def active_processes(_job: object) -> int:
        events.append("query")
        return next(active)

    with (
        patch("wetlands.managed_process.signal.CTRL_BREAK_EVENT", 999, create=True),
        patch("wetlands.managed_process.identity_matches", return_value=True),
        patch.object(handle, "_windows_job_active_processes", side_effect=active_processes),
        patch.object(handle, "_terminate_windows_job_object", side_effect=lambda _job: events.append("force")),
        patch("wetlands.managed_process._close_windows_job", side_effect=lambda _process: events.append("close")),
    ):
        handle._terminate_windows_job(0)

    assert process.signals == [999]
    assert events == ["query", "query", "force", "query", "query", "close"]
    assert process.waited


def test_windows_force_kill_skips_graceful_signal_and_terminates_job(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()
    process._wetlands_job_handle = 123  # type: ignore[attr-defined]
    handle = ManagedProcess(
        environment=environment,  # type: ignore[arg-type]
        argv=("example",),
        process=process,  # type: ignore[arg-type]
        output_limit=1024,
        started_at=1.0,
    )
    handle._identity = ProcessIdentity(process.pid, 1.0, None, None)
    active = iter((1, 0, 0))

    with (
        patch("wetlands.managed_process.os.name", "nt"),
        patch("wetlands.managed_process.identity_matches", return_value=True),
        patch.object(handle, "_windows_job_active_processes", side_effect=lambda _job: next(active)),
        patch.object(handle, "_terminate_windows_job_object") as terminate_job,
        patch("wetlands.managed_process._close_windows_job"),
    ):
        handle._kill_tree()

    assert process.signals == []
    terminate_job.assert_called_once_with(123)
    assert process.waited


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group signaling only")
def test_posix_terminate_delegates_graceful_escalation_with_requested_grace(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()
    process._wetlands_started_at = 1.0  # type: ignore[attr-defined]
    process._wetlands_process_group_id = process.pid  # type: ignore[attr-defined]
    process._wetlands_session_id = process.pid  # type: ignore[attr-defined]
    handle = ManagedProcess(
        environment=environment,  # type: ignore[arg-type]
        argv=("example",),
        process=process,  # type: ignore[arg-type]
        output_limit=1024,
        started_at=1.0,
    )

    with patch("wetlands.managed_process.terminate_launched_process_tree") as terminate:
        handle._terminate_tree(2.5)

    terminate.assert_called_once()
    assert terminate.call_args.args == (process,)
    assert terminate.call_args.kwargs["grace"] == 2.5
    assert callable(terminate.call_args.kwargs["close_windows_job"])


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group signaling only")
def test_posix_force_kill_never_sends_a_graceful_signal(tmp_path: Path) -> None:
    environment = _Environment(tmp_path)
    process = _Process()
    identity = ProcessIdentity(process.pid, 1.0, process.pid, process.pid)
    handle = ManagedProcess(
        environment=environment,  # type: ignore[arg-type]
        argv=("example",),
        process=process,  # type: ignore[arg-type]
        output_limit=1024,
        started_at=1.0,
    )
    handle._identity = identity

    with (
        patch("wetlands.managed_process.identity_matches", return_value=True),
        patch("wetlands.managed_process.os.getpgid", return_value=process.pid),
        patch("wetlands.managed_process.os.getsid", return_value=process.pid),
        patch("wetlands.managed_process.os.killpg") as kill_group,
        patch("wetlands.managed_process._wait_for_posix_group_exit", return_value=True),
    ):
        handle._kill_tree()

    assert kill_group.call_args_list == [call(process.pid, signal.SIGKILL)]
