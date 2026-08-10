from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import psutil

from wetlands.managed_environment import ManagedEnvironment
from wetlands.managed_process import (
    ManagedProcess,
    OutputStream,
    ProcessCleanupError,
    ProcessEventLagError,
    ProcessExitError,
    ProcessLineTimeoutError,
    ProcessOutputLimitError,
    ProcessTimeoutError,
    _validate_launch_options,
)
from wetlands._internal.process_termination import ProcessIdentity, ProcessIdentityError, capture_process_identity
from wetlands.environment_manager import EnvironmentManager


class _Environment:
    def __init__(self, path: Path, pixi: Path) -> None:
        self.name = "test"
        self.path = path
        self.generation_id = "generation-1"
        self.pixi_executable_path = pixi
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


@pytest.fixture
def environment(tmp_path: Path) -> _Environment:
    project = tmp_path / "environment"
    project.mkdir()
    (project / "pixi.toml").write_text("", encoding="utf-8")
    fake_pixi = """#!/usr/bin/env python3
import os
import subprocess
import sys
if os.name == "nt":
    valid_prefix = os.path.basename(sys.argv[0]) == "run" and sys.argv[1:2] == ["--manifest-path"]
else:
    valid_prefix = sys.argv[1:3] == ["run", "--manifest-path"]
if not valid_prefix or "--locked" not in sys.argv or "--" not in sys.argv:
    raise SystemExit(91)
split = sys.argv.index("--")
if os.name == "nt":
    raise SystemExit(subprocess.run(sys.argv[split + 1:], check=False, env=os.environ).returncode)
os.execvpe(sys.argv[split + 1], sys.argv[split + 1:], os.environ)
"""
    if os.name == "nt":
        pixi = Path(sys.executable)
        (project / "run").write_text(fake_pixi, encoding="utf-8")
    else:
        pixi = tmp_path / "pixi"
        pixi.write_text(fake_pixi, encoding="utf-8")
        pixi.chmod(0o755)
    return _Environment(project, pixi)


def _spawn(environment: _Environment, code: str, **kwargs: object) -> ManagedProcess:
    return ManagedProcess._launch(
        environment=environment,  # type: ignore[arg-type]
        argv=[sys.executable, "-c", code],
        **kwargs,
    )


def test_success_captures_stdout_stderr_and_releases(environment: _Environment) -> None:
    process = _spawn(environment, "import sys; print('out'); print('err', file=sys.stderr)")

    result = process.wait()

    assert result.argv[:2] == (sys.executable, "-c")
    assert result.returncode == 0
    assert result.stdout == f"out{os.linesep}"
    assert result.stderr == f"err{os.linesep}"
    assert result.started_at <= result.ended_at
    assert process.returncode == 0
    assert not process.running
    assert environment.processes == []


def test_nonzero_check_modes_share_result(environment: _Environment) -> None:
    process = _spawn(environment, "import sys; print('bad'); raise SystemExit(7)")

    result = process.wait(check=False)
    assert result.returncode == 7
    with pytest.raises(ProcessExitError) as raised:
        process.wait()
    assert raised.value.result is result


def test_command_not_found_is_a_checked_process_exit(environment: _Environment) -> None:
    process = ManagedProcess._launch(
        environment=environment,  # type: ignore[arg-type]
        argv=["wetlands-command-that-does-not-exist"],
    )

    with pytest.raises(ProcessExitError) as raised:
        process.wait()

    assert raised.value.result.returncode != 0
    assert raised.value.result.stderr


def test_timeout_contains_partial_output_and_cleans_up(environment: _Environment) -> None:
    process = _spawn(environment, "import time; print('ready', flush=True); time.sleep(30)")
    process.wait_for_line(lambda event: event.text == f"ready{os.linesep}", timeout=5)

    with pytest.raises(ProcessTimeoutError) as raised:
        process.wait(timeout=0.05)

    assert raised.value.timeout == 0.05
    assert raised.value.result.stdout == f"ready{os.linesep}"
    assert environment.processes == []
    with pytest.raises(ProcessTimeoutError) as repeated:
        process.wait(check=False)
    assert repeated.value is raised.value


def test_output_limit_counts_combined_raw_bytes(environment: _Environment) -> None:
    process = _spawn(environment, "import sys; sys.stdout.write('12345'); sys.stdout.flush()", output_limit=4)

    with pytest.raises(ProcessOutputLimitError) as raised:
        process.wait()

    assert raised.value.limit == 4
    assert raised.value.result.stdout == "1234"
    assert OutputStream.STDOUT in raised.value.truncated_streams
    assert environment.processes == []


def test_zero_output_limit_allows_silent_command(environment: _Environment) -> None:
    assert _spawn(environment, "pass", output_limit=0).wait().returncode == 0

    noisy = _spawn(environment, "print('x')", output_limit=0)
    with pytest.raises(ProcessOutputLimitError):
        noisy.wait()


def test_cwd_and_environment_overlay_and_removal(
    environment: _Environment,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    working = tmp_path / "working"
    working.mkdir()
    if os.name == "nt":
        (working / "run").write_bytes((environment.path / "run").read_bytes())
    monkeypatch.setenv("WETLANDS_REMOVE_ME", "inherited")
    code = (
        "import os; "
        "print(os.getcwd()); "
        "print(os.environ['WETLANDS_SET_ME']); "
        "print('WETLANDS_REMOVE_ME' in os.environ); "
        "print(os.environ['PIXI_HOME'])"
    )
    process = _spawn(
        environment,
        code,
        cwd=working,
        env={"WETLANDS_SET_ME": "value", "WETLANDS_REMOVE_ME": None},
    )

    lines = process.wait().stdout.splitlines()
    assert lines == [str(working), "value", "False", str(environment._manager.state_root / "pixi-home")]


def test_readiness_replays_events_and_timeout_does_not_stop_process(environment: _Environment) -> None:
    process = _spawn(environment, "import time; print('Listening on 42', flush=True); time.sleep(.4)")
    time.sleep(0.05)

    event = process.wait_for_line(lambda item: item.text.startswith("Listening on "), timeout=5)
    assert event.stream is OutputStream.STDOUT
    with pytest.raises(ProcessLineTimeoutError):
        process.wait_for_line(lambda item: item.text.startswith("missing"), timeout=0.02, replay=False)
    assert process.running
    process.close()


def test_trailing_partial_line_is_an_event(environment: _Environment) -> None:
    process = _spawn(environment, "import sys; sys.stderr.write('partial'); sys.stderr.flush()")
    event = process.wait_for_line(lambda item: item.text == "partial", timeout=5)
    assert event.stream is OutputStream.STDERR
    assert process.wait().stderr == "partial"


def test_wait_for_line_raises_eof_after_all_output_is_checked(environment: _Environment) -> None:
    process = _spawn(environment, "print('finished')")
    process.wait(check=False)

    with pytest.raises(EOFError, match="closed its output"):
        process.wait_for_line(lambda event: event.text == "missing\n")


def test_slow_unterminated_output_is_bounded_by_raw_bytes(environment: _Environment) -> None:
    code = (
        "import sys, time; "
        "[(sys.stdout.buffer.write(b'x' * 257), sys.stdout.flush(), time.sleep(.005)) for _ in range(20)]; "
        "time.sleep(30)"
    )
    process = _spawn(environment, code, output_limit=1024)

    with pytest.raises(ProcessOutputLimitError) as raised:
        process.wait()

    assert raised.value.result.stdout == "x" * 1024
    assert raised.value.result.stderr == ""
    assert raised.value.truncated_streams == frozenset({OutputStream.STDOUT})
    assert environment.processes == []


def test_repeated_termination_and_close_are_idempotent(environment: _Environment) -> None:
    process = _spawn(environment, "import time; time.sleep(30)")
    process.terminate(timeout=0)
    process.terminate(timeout=0)
    process.kill()
    process.close()
    process.close()
    assert environment.processes == []


def test_validation_happens_without_launch(environment: _Environment, tmp_path: Path) -> None:
    invalid = ["", b"x", [], ["ok", 1]]
    for argv in invalid:
        with pytest.raises((TypeError, ValueError)):
            _validate_launch_options(
                argv=argv,  # type: ignore[arg-type]
                cwd=None,
                env=None,
                output_limit=1,
                default_cwd=environment.path,
            )
    with pytest.raises(ValueError, match="does not exist"):
        _validate_launch_options(
            argv=["ok"], cwd=tmp_path / "missing", env=None, output_limit=1, default_cwd=environment.path
        )
    with pytest.raises(TypeError, match="output_limit"):
        _validate_launch_options(
            argv=["ok"],
            cwd=None,
            env=None,
            output_limit=True,
            default_cwd=environment.path,  # type: ignore[arg-type]
        )


def test_async_events_preserve_stream_and_await_process(environment: _Environment) -> None:
    async def exercise() -> None:
        process = _spawn(
            environment,
            "import sys; print('one', flush=True); print('two', file=sys.stderr, flush=True)",
        )
        events = [event async for event in process.events()]
        result = await process

        assert {(event.stream, event.text) for event in events} == {
            (OutputStream.STDOUT, f"one{os.linesep}"),
            (OutputStream.STDERR, f"two{os.linesep}"),
        }
        assert result.returncode == 0

    asyncio.run(exercise())


def test_async_events_preserve_deterministic_live_interleaving(environment: _Environment) -> None:
    async def exercise() -> None:
        code = (
            "import sys, time; "
            "print('out-1', flush=True); time.sleep(.03); "
            "print('err-1', file=sys.stderr, flush=True); time.sleep(.03); "
            "print('out-2', flush=True); time.sleep(.03); "
            "print('err-2', file=sys.stderr, flush=True)"
        )
        process = _spawn(environment, code)

        events = [event async for event in process.events(replay=False)]

        assert [(event.stream, event.text) for event in events] == [
            (OutputStream.STDOUT, f"out-1{os.linesep}"),
            (OutputStream.STDERR, f"err-1{os.linesep}"),
            (OutputStream.STDOUT, f"out-2{os.linesep}"),
            (OutputStream.STDERR, f"err-2{os.linesep}"),
        ]
        assert process.wait().returncode == 0

    asyncio.run(exercise())


def test_lagging_async_observer_gets_a_typed_error(environment: _Environment) -> None:
    async def exercise() -> None:
        code = (
            "import time; print('first', flush=True); time.sleep(.2); "
            "[print(f'line-{index}', flush=True) for index in range(1100)]"
        )
        process = _spawn(environment, code)
        events = process.events()
        first = await events.__anext__()
        assert first.text == f"first{os.linesep}"
        await process
        with pytest.raises(ProcessEventLagError) as raised:
            await events.__anext__()
        assert raised.value.first_unavailable_sequence == 1
        assert raised.value.oldest_retained_sequence > 1

    asyncio.run(exercise())


def test_cancelled_event_observer_does_not_stop_process(environment: _Environment) -> None:
    async def exercise() -> None:
        process = _spawn(environment, "import time; time.sleep(30)")

        async def observe() -> None:
            async for _event in process.events(replay=False):
                pass

        observer = asyncio.create_task(observe())
        await asyncio.sleep(0.02)
        observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer
        assert process.running
        process.close()

    asyncio.run(exercise())


def test_cancelled_async_wait_closes_process(environment: _Environment) -> None:
    async def exercise() -> None:
        process = _spawn(environment, "import time; time.sleep(30)")
        waiter = asyncio.create_task(process.wait_async())
        await asyncio.sleep(0.02)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not process.running
        assert environment.processes == []

    asyncio.run(exercise())


def test_partial_launch_failure_kills_and_releases_child(environment: _Environment) -> None:
    with (
        patch("wetlands.managed_process.capture_process_identity", side_effect=OSError("identity failed")),
        pytest.raises(OSError, match="identity failed"),
    ):
        _spawn(environment, "import time; time.sleep(30)")

    assert environment.processes == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-session ownership")
def test_invalid_posix_session_identity_rejects_launch_and_cleans_child(environment: _Environment) -> None:
    def invalid_identity(pid: int) -> ProcessIdentity:
        identity = capture_process_identity(pid)
        return ProcessIdentity(pid, identity.started_at, pid + 1, pid + 1)

    with (
        patch("wetlands.managed_process.capture_process_identity", side_effect=invalid_identity),
        pytest.raises(ProcessIdentityError, match="isolated process-session ownership"),
    ):
        _spawn(environment, "import time; time.sleep(30)")

    assert environment.processes == []


def test_reader_failure_keeps_ownership_until_close_retry(environment: _Environment) -> None:
    real_popen = __import__("subprocess").Popen

    class BrokenPipe:
        def __init__(self, pipe: object) -> None:
            self.pipe = pipe

        def read1(self, _size: int) -> bytes:
            raise OSError("reader failed")

        def close(self) -> None:
            self.pipe.close()  # type: ignore[attr-defined]

    def broken_stdout(*args: object, **kwargs: object) -> object:
        process = real_popen(*args, **kwargs)
        process.stdout = BrokenPipe(process.stdout)
        return process

    with patch("wetlands.managed_process.subprocess.Popen", side_effect=broken_stdout):
        process = _spawn(environment, "pass")

    with pytest.raises(ProcessCleanupError, match="reader failed"):
        process.wait()
    assert environment.processes == [process]

    process.close()
    assert environment.processes == []


def test_partial_reader_start_failure_does_not_strand_registry(environment: _Environment) -> None:
    real_start = __import__("threading").Thread.start

    def fail_stderr_start(thread: object) -> None:
        if str(thread.name).endswith("-stderr"):  # type: ignore[attr-defined]
            raise RuntimeError("stderr reader start failed")
        real_start(thread)  # type: ignore[arg-type]

    with (
        patch("wetlands.managed_process.threading.Thread.start", autospec=True, side_effect=fail_stderr_start),
        pytest.raises(RuntimeError, match="stderr reader start failed"),
    ):
        _spawn(environment, "import time; time.sleep(30)")

    assert environment.processes == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX signal behavior")
def test_kill_sends_sigkill_before_any_graceful_signal(environment: _Environment) -> None:
    process = _spawn(environment, "import time; time.sleep(30)")
    real_killpg = os.killpg
    signals: list[int] = []

    def record_killpg(process_group_id: int, sent_signal: int) -> None:
        signals.append(sent_signal)
        real_killpg(process_group_id, sent_signal)

    with patch("wetlands.managed_process.os.killpg", side_effect=record_killpg):
        process.kill()

    sent_signals = [sent_signal for sent_signal in signals if sent_signal != 0]
    assert sent_signals
    assert set(sent_signals) == {signal.SIGKILL}
    assert environment.processes == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX graceful termination output race")
def test_timeout_cause_is_not_replaced_by_later_output_limit(environment: _Environment) -> None:
    code = (
        "import signal, time; "
        "signal.signal(signal.SIGTERM, lambda *_: print('late output', flush=True)); "
        "print('ready', flush=True); "
        "time.sleep(30)"
    )
    process = _spawn(environment, code, output_limit=len(f"ready{os.linesep}".encode()))
    process.wait_for_line(lambda event: event.text == f"ready{os.linesep}", timeout=5)

    with pytest.raises(ProcessTimeoutError):
        process.wait(timeout=0)


def test_concurrent_waiters_observe_the_same_winning_timeout(environment: _Environment) -> None:
    process = _spawn(environment, "import time; time.sleep(30)")
    timeout_requested = threading.Event()
    real_request = process._request
    errors: list[BaseException] = []

    def request(cause: str, value: float | None) -> None:
        real_request(cause, value)
        if cause == "timeout":
            timeout_requested.set()

    def wait(timeout: float | None) -> None:
        try:
            process.wait(timeout=timeout, check=False)
        except BaseException as error:
            errors.append(error)

    with patch.object(process, "_request", side_effect=request):
        timeout_waiter = threading.Thread(target=wait, args=(0.02,))
        timeout_waiter.start()
        assert timeout_requested.wait(1)
        other_waiter = threading.Thread(target=wait, args=(None,))
        other_waiter.start()
        timeout_waiter.join(2)
        other_waiter.join(2)

    assert not timeout_waiter.is_alive()
    assert not other_waiter.is_alive()
    assert len(errors) == 2
    assert isinstance(errors[0], ProcessTimeoutError)
    assert errors[0] is errors[1]
    assert environment.processes == []


def test_multiple_processes_share_generation_ownership_independently(environment: _Environment) -> None:
    first = _spawn(environment, "import time; print('first', flush=True); time.sleep(.1)")
    second = _spawn(environment, "import time; print('second', flush=True); time.sleep(30)")
    second.wait_for_line(lambda event: event.text == f"second{os.linesep}", timeout=5)
    assert environment.processes == [first, second]

    assert first.wait().stdout == f"first{os.linesep}"
    assert second in environment.processes
    second.close()
    assert second.wait(check=False).stdout == f"second{os.linesep}"
    assert environment.processes == []


def test_real_manager_close_terminates_a_registered_process(tmp_path: Path) -> None:
    manager = EnvironmentManager(tmp_path / "manager", termination_grace=0.05)
    project = manager.environments_root / "managed"
    project.mkdir(parents=True)
    (project / "pixi.toml").write_text("", encoding="utf-8")
    fake_pixi = (
        "#!/usr/bin/env python3\n"
        "import os, subprocess, sys\n"
        "split = sys.argv.index('--')\n"
        "if os.name == 'nt':\n"
        " raise SystemExit(subprocess.run(sys.argv[split + 1:], check=False, env=os.environ).returncode)\n"
        "os.execvpe(sys.argv[split + 1], sys.argv[split + 1:], os.environ)\n"
    )
    if os.name == "nt":
        pixi = Path(sys.executable)
        (project / "run").write_text(fake_pixi, encoding="utf-8")
    else:
        pixi = tmp_path / "pixi"
        pixi.write_text(fake_pixi, encoding="utf-8")
        pixi.chmod(0o755)
    managed = ManagedEnvironment._from_ready(
        manager,
        "managed",
        project,
        {
            "generation_id": "generation-1",
            "recipe_hash": "recipe-1",
            "pixi_executable": str(pixi),
            "pixi_version": "test",
            "lock_sha256": "test",
        },
    )
    manager._environments["managed"] = managed

    with (
        patch("wetlands.managed_environment.environment_lifecycle_gate", return_value=contextlib.nullcontext()),
        patch.object(managed, "_require_current_generation"),
        patch("wetlands.managed_environment.runtime_state.reconcile_persistent_pool"),
    ):
        process = managed.spawn([sys.executable, "-c", "import time; time.sleep(30)"])

    assert managed._has_live_resources()
    manager.close(timeout=2)

    assert not process.running
    assert not managed._has_live_resources()
    manager.close(timeout=2)


def test_cleanup_failure_retains_initiating_timeout_context(environment: _Environment) -> None:
    process = _spawn(environment, "import time; time.sleep(30)")
    terminate_tree = process._terminate_tree
    attempts = 0

    def fail_once(grace: float) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("termination failed")
        terminate_tree(grace)

    with patch.object(process, "_terminate_tree", side_effect=fail_once):
        with pytest.raises(ProcessCleanupError) as raised:
            process.wait(timeout=0)
        assert isinstance(raised.value.initiating_error, ProcessTimeoutError)
        assert raised.value.__cause__ is raised.value.initiating_error
        assert environment.processes == [process]
        process.close()

    assert environment.processes == []


def test_managed_environment_run_uses_core_launch_contract(environment: _Environment) -> None:
    manager = environment._manager
    manager._manager_work = contextlib.nullcontext
    metadata = {
        "generation_id": environment.generation_id,
        "recipe_hash": "recipe-1",
        "pixi_executable": str(environment.pixi_executable_path),
    }
    managed = ManagedEnvironment._from_ready(manager, environment.name, environment.path, metadata)

    with (
        patch("wetlands.managed_environment.environment_lifecycle_gate", return_value=contextlib.nullcontext()),
        patch.object(managed, "_require_current_generation"),
        patch("wetlands.managed_environment.runtime_state.reconcile_persistent_pool"),
    ):
        result = managed.run([sys.executable, "-c", "print('through-public-api')"])

    assert result.stdout == f"through-public-api{os.linesep}"
    assert managed._processes == []


def test_launch_error_retains_native_type_and_adds_context(environment: _Environment) -> None:
    environment.pixi_executable_path = environment.path / "missing-pixi"

    with pytest.raises(FileNotFoundError) as raised:
        _spawn(environment, "pass")

    message = str(raised.value)
    assert "environment 'test'" in message
    assert "generation 'generation-1'" in message
    assert "pass" in message
    assert environment.processes == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-group descendant verification")
def test_natural_wrapper_exit_terminates_in_group_descendant(
    environment: _Environment,
    tmp_path: Path,
) -> None:
    marker = tmp_path / "descendant.pid"
    child_code = (
        "import os, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "open(sys.argv[1], 'w').write(str(os.getpid())); "
        "time.sleep(30)"
    )
    parent_code = (
        "import os, subprocess, sys, time; "
        "subprocess.Popen([sys.executable, '-c', sys.argv[2], sys.argv[1]]); "
        'exec("while not os.path.exists(sys.argv[1]):\\n time.sleep(.01)")'
    )
    process = ManagedProcess._launch(
        environment=environment,  # type: ignore[arg-type]
        argv=[sys.executable, "-c", parent_code, str(marker), child_code],
    )
    deadline = time.monotonic() + 2
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    child_pid = int(marker.read_text(encoding="utf-8"))

    assert process.wait().returncode == 0
    assert not psutil.pid_exists(child_pid) or psutil.Process(child_pid).status() == psutil.STATUS_ZOMBIE
    assert environment.processes == []
