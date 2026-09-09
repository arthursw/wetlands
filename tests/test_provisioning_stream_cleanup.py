from __future__ import annotations

import gc
import os
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from wetlands._internal import provisioning
from wetlands._internal.provisioning import ProcessTreeRunner, ProvisioningStep
from wetlands._internal.process_termination import ProcessIdentityError, ProcessTerminationError
from wetlands.operation import (
    OperationCanceled,
    OperationEventKind,
    OperationState,
    ProvisioningError,
    ProvisioningOperation,
)
from wetlands.specs import ProvisioningStage

pytestmark = [
    pytest.mark.filterwarnings("error::ResourceWarning"),
    pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning"),
]


@pytest.fixture
def readers(monkeypatch):
    started = []
    real_start = threading.Thread.start

    def tracked_start(thread):
        if thread._target.__name__ == "drain":
            started.append(thread)
        real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", tracked_start)
    yield started
    for thread in started:
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.fixture
def processes(monkeypatch, readers):
    launched = []
    real_popen = subprocess.Popen

    def tracked_popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        launched.append(process)
        return process

    monkeypatch.setattr(provisioning.subprocess, "Popen", tracked_popen)
    yield launched
    # Keep failed assertions from leaving real child processes or pipes behind.
    for process in launched:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        for stream in (process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
    gc.collect()


def _step(code):
    return ProvisioningStep("streams", ProvisioningStage.CONDA_INSTALL, (sys.executable, "-c", code))


def _assert_clean(runner, processes, readers):
    assert len(processes) == 1
    process = processes[0]
    assert process.returncode is not None
    assert process.stdout.closed
    assert process.stderr.closed
    assert all(not reader.is_alive() for reader in readers)
    assert runner._active is None
    assert runner._active_job is None


def test_success_closes_both_streams_and_preserves_events(processes, readers):
    operation = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1)
    events = []
    operation.listen(events.append)
    output = runner.run(
        ProvisioningStep(
            "streams",
            ProvisioningStage.CONDA_INSTALL,
            (sys.executable, "-c", "import sys; print('stdout-marker'); print('stderr-marker', file=sys.stderr)"),
        )
    )

    assert output == ("stdout-marker",)
    assert {(event.stream, event.line) for event in events if event.kind is OperationEventKind.OUTPUT} == {
        ("stdout", "stdout-marker"),
        ("stderr", "stderr-marker"),
    }
    assert processes[0].returncode == 0
    assert len(readers) == 2
    _assert_clean(runner, processes, readers)


def test_sequential_commands_do_not_emit_resource_warnings(readers):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    for _ in range(3):
        assert runner.run(_step("import sys; print('out'); print('err', file=sys.stderr)")) == ("out",)
    gc.collect()
    assert len(readers) == 6
    assert all(not reader.is_alive() for reader in readers)


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_nonzero_exit_preserves_status_and_tails(processes, readers, monkeypatch, cleanup_fails):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    verify_tree = runner._verify_finished_tree

    def verify(*args):
        verify_tree(*args)
        if cleanup_fails:
            raise ProcessTerminationError("tree verification failed")

    monkeypatch.setattr(runner, "_verify_finished_tree", verify)
    with pytest.raises(ProvisioningError, match="exit code 7") as caught:
        runner.run(_step("import sys; print('stdout-marker'); print('stderr-marker', file=sys.stderr); sys.exit(7)"))
    assert caught.value.failure.returncode == 7
    assert caught.value.failure.stdout_tail == ("stdout-marker",)
    assert caught.value.failure.stderr_tail == ("stderr-marker",)
    assert caught.value.failure.cleanup_error == ("tree verification failed" if cleanup_fails else None)
    _assert_clean(runner, processes, readers)


def test_cancellation_closes_streams_and_reaps_process(processes, readers):
    operation = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1)
    ready = threading.Event()
    operation.listen(lambda event: ready.set() if event.line == "ready" else None)
    operation._start_runner(
        lambda: runner.run(_step("import time; print('ready', flush=True); time.sleep(30)")),
        thread_name="test-stream-cancellation",
    )
    try:
        assert ready.wait(timeout=5)
        assert operation.cancel()
        with pytest.raises(OperationCanceled):
            operation.wait_for(timeout=5)
        assert operation.state is OperationState.CANCELED
        _assert_clean(runner, processes, readers)
    finally:
        if not operation.state.terminal:
            operation.cancel()


def test_timeout_terminates_and_closes_process(processes, readers):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    with pytest.raises(subprocess.TimeoutExpired):
        runner.run(_step("import time; time.sleep(30)"), timeout=0.2)
    _assert_clean(runner, processes, readers)


def test_reader_failure_stops_a_running_command(processes, readers, monkeypatch):
    operation = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1)
    emit = operation._emit

    def fail_output(kind, *args, **kwargs):
        if kind is OperationEventKind.OUTPUT:
            raise OSError("output delivery failed")
        return emit(kind, *args, **kwargs)

    monkeypatch.setattr(operation, "_emit", fail_output)
    operation._start_runner(
        lambda: runner.run(_step("import time; print('ready', flush=True); time.sleep(30)")),
        thread_name="test-failed-reader",
    )
    try:
        with pytest.raises(ProvisioningError) as caught:
            operation.wait_for(timeout=5)
        assert "output delivery failed" in caught.value.failure.cleanup_error
        _assert_clean(runner, processes, readers)
    finally:
        if not operation.state.terminal:
            operation.cancel()


@pytest.mark.parametrize("kill_reports_error", [False, True])
def test_identity_failure_closes_unowned_streams(processes, readers, monkeypatch, kill_reports_error):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    original = ProcessIdentityError("identity unavailable")

    def fail_identity(pid):
        assert pid == processes[0].pid
        if kill_reports_error:
            kill = processes[0].kill

            def kill_then_report_error():
                kill()
                raise OSError("kill diagnostic")

            monkeypatch.setattr(processes[0], "kill", kill_then_report_error)
        raise original

    monkeypatch.setattr(provisioning, "capture_process_identity", fail_identity)
    with pytest.raises(ProvisioningError, match="Could not establish process ownership") as caught:
        runner.run(_step("import time; time.sleep(30)"))
    assert "identity unavailable" in caught.value.failure.cleanup_error
    if kill_reports_error:
        assert "kill diagnostic" in caught.value.failure.cleanup_error
        assert caught.value.__cause__.__cause__ is original
    else:
        assert caught.value.__cause__ is original
    assert readers == []
    _assert_clean(runner, processes, readers)


@pytest.mark.parametrize("failed_reader", [0, 1])
@pytest.mark.parametrize("failure_point", ["construct", "start", "after_start"])
def test_partial_reader_startup_closes_all_streams(processes, readers, monkeypatch, failed_reader, failure_point):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    original = RuntimeError("reader startup failed")
    method = "__init__" if failure_point == "construct" else "start"
    real_method = getattr(threading.Thread, method)
    calls = 0

    def fail_reader(thread, *args, **kwargs):
        nonlocal calls
        index = calls
        calls += 1
        if index == failed_reader:
            if failure_point == "after_start":
                real_method(thread, *args, **kwargs)
            raise original
        return real_method(thread, *args, **kwargs)

    monkeypatch.setattr(threading.Thread, method, fail_reader)
    with pytest.raises(RuntimeError, match="reader startup failed") as caught:
        runner.run(_step("import time; time.sleep(30)"))
    assert caught.value is original
    assert len(readers) == failed_reader + (failure_point == "after_start")
    _assert_clean(runner, processes, readers)


def test_wait_failure_preserves_original_when_cleanup_also_fails(processes, readers, monkeypatch):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    original = RuntimeError("wait failed")
    real_capture = provisioning.capture_process_identity
    verify_tree = runner._verify_finished_tree

    def capture(pid):
        process = processes[0]
        wait = process.wait

        def fail_once(*args, **kwargs):
            monkeypatch.setattr(process, "wait", wait)
            raise original

        monkeypatch.setattr(process, "wait", fail_once)
        return real_capture(pid)

    def verify(*args):
        verify_tree(*args)
        raise ProcessTerminationError("cleanup diagnostic")

    monkeypatch.setattr(provisioning, "capture_process_identity", capture)
    monkeypatch.setattr(runner, "_verify_finished_tree", verify)
    with pytest.raises(ProvisioningError, match="wait failed") as caught:
        runner.run(_step("import time; time.sleep(30)"))
    assert caught.value.__cause__ is original
    assert caught.value.failure.cleanup_error == "cleanup diagnostic"
    _assert_clean(runner, processes, readers)


@pytest.mark.parametrize("failure_point", ["readline", "emit", "close"])
def test_reader_errors_close_streams_and_report_failure(processes, readers, monkeypatch, failure_point):
    operation = ProvisioningOperation(environment="example")
    runner = ProcessTreeRunner(operation, grace=0.1)
    real_capture = provisioning.capture_process_identity
    real_emit = operation._emit

    def capture(pid):
        stdout = processes[0].stdout
        if failure_point != "emit":
            method = "readline" if failure_point == "readline" else "close"
            original = getattr(stdout, method)

            def fail():
                original()
                raise OSError("reader diagnostic")

            monkeypatch.setattr(stdout, method, fail)
        return real_capture(pid)

    def emit(kind, *args, **kwargs):
        if failure_point == "emit" and kind is OperationEventKind.OUTPUT and kwargs["stream"] == "stdout":
            raise RuntimeError("reader diagnostic")
        return real_emit(kind, *args, **kwargs)

    monkeypatch.setattr(provisioning, "capture_process_identity", capture)
    monkeypatch.setattr(operation, "_emit", emit)
    with pytest.raises(ProvisioningError) as caught:
        runner.run(_step("import sys; print('stdout-marker'); print('stderr-marker', file=sys.stderr)"))
    assert "reader diagnostic" in caught.value.failure.cleanup_error
    assert caught.value.failure.stderr_tail == ("stderr-marker",)
    _assert_clean(runner, processes, readers)


def test_windows_job_closes_before_readers_join_after_tree_failure(processes, readers, monkeypatch):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    tracked_popen = provisioning.subprocess.Popen
    closed = threading.Event()

    def popen(*args, **kwargs):
        if os.name != "nt":
            kwargs.pop("creationflags")
            kwargs["start_new_session"] = True
        return tracked_popen(*args, **kwargs)

    class Job:
        def __init__(self, process):
            self.process = process

        def close(self):
            closed.set()

    def fail_tree(process, job):
        assert isinstance(job, Job)
        raise ProcessTerminationError("job tree verification failed")

    real_join = threading.Thread.join

    def join_after_job_close(thread, *args, **kwargs):
        assert closed.is_set()
        return real_join(thread, *args, **kwargs)

    monkeypatch.setattr(provisioning, "os", SimpleNamespace(name="nt", environ=os.environ))
    monkeypatch.setattr(provisioning.subprocess, "CREATE_NEW_PROCESS_GROUP", 0x200, raising=False)
    monkeypatch.setattr(provisioning.subprocess, "Popen", popen)
    monkeypatch.setattr(provisioning, "_WindowsJob", Job)
    monkeypatch.setattr(runner, "_verify_finished_tree", fail_tree)
    monkeypatch.setattr(threading.Thread, "join", join_after_job_close)
    with pytest.raises(ProvisioningError) as caught:
        runner.run(_step("pass"))
    assert caught.value.failure.cleanup_error == "job tree verification failed"
    _assert_clean(runner, processes, readers)


def test_blocked_reader_retains_close_ownership_after_bounded_join(processes, readers, monkeypatch):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    real_capture = provisioning.capture_process_identity
    release = threading.Event()
    entered = threading.Event()
    closed_by = []

    def capture(pid):
        stdout = processes[0].stdout
        readline = stdout.readline
        close = stdout.close

        def blocked_readline():
            entered.set()
            assert release.wait(timeout=10)
            return readline()

        def tracked_close():
            closed_by.append(threading.current_thread())
            close()

        monkeypatch.setattr(stdout, "readline", blocked_readline)
        monkeypatch.setattr(stdout, "close", tracked_close)
        return real_capture(pid)

    monkeypatch.setattr(provisioning, "capture_process_identity", capture)
    try:
        with pytest.raises(ProvisioningError) as caught:
            runner.run(_step("pass"))
        assert entered.is_set()
        assert "output readers did not terminate" in caught.value.failure.cleanup_error
        assert closed_by == []
        assert not processes[0].stdout.closed
        assert processes[0].stderr.closed
    finally:
        release.set()
        for reader in readers:
            reader.join(timeout=5)
    assert closed_by == [readers[0]]
    _assert_clean(runner, processes, readers)


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group behavior")
def test_exited_leader_descendant_pipe_handles_are_released(processes, readers):
    runner = ProcessTreeRunner(ProvisioningOperation(environment="example"), grace=0.1)
    output = runner.run(
        _step(
            "import subprocess, sys; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
            "print('leader-done')"
        )
    )
    assert output == ("leader-done",)
    _assert_clean(runner, processes, readers)
