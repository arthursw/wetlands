"""Connection ownership at the reader's terminal boundary."""

from __future__ import annotations

import json
import pickle
import socket
import struct
import subprocess
import sys
import threading
from contextlib import nullcontext
from multiprocessing.connection import Client, Connection, Pipe
from unittest.mock import MagicMock

import pytest
import psutil

from wetlands import external_environment as runtime_module
from wetlands import managed_environment as managed_module
from wetlands.environment_manager import EnvironmentManager
from wetlands.external_environment import ExternalEnvironment, _Worker
from wetlands.lifecycle import ManagerCloseError, WorkerStartError
from wetlands.managed_environment import ManagedEnvironment, WorkerPool
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION


def _runtime(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path / "manager"
    return ExternalEnvironment("example", tmp_path / "pixi.toml", manager)


def _no_journal(monkeypatch):
    monkeypatch.setattr(runtime_module.runtime_state, "remove_worker", lambda *args, **kwargs: None)
    monkeypatch.setattr(runtime_module, "reconcile_shared_memory_leases", lambda *args: None)


def test_split_frame_shutdown_keeps_connection_until_reader_terminal(tmp_path, monkeypatch):
    _no_journal(monkeypatch)
    runtime = _runtime(tmp_path)
    left, right = socket.socketpair()
    receiving, sending = Connection(left.detach()), Connection(right.detach())
    worker = _Worker(0, None, 0, receiving, None)
    runtime._workers.append(worker)
    header_read = threading.Event()
    resume_payload = threading.Event()
    shutdown_started = threading.Event()
    failures = []
    original_recv = receiving._recv

    def recv(size, *args):
        result = original_recv(size, *args)
        if size == 4:
            header_read.set()
            assert resume_payload.wait(3)
        else:
            # A reader handling a received message must be able to use the
            # runtime state lock while close waits for its terminal state.
            with runtime._lock:
                pass
        return result

    monkeypatch.setattr(receiving, "_recv", recv)
    payload = pickle.dumps({"action": "test"})
    sending._send(struct.pack("!i", len(payload)) + payload)
    runtime._start_reader_thread(worker)
    assert header_read.wait(2)

    def close():
        shutdown_started.set()
        try:
            runtime._exit()
        except BaseException as error:
            failures.append(error)

    closer = threading.Thread(target=close)
    closer.start()
    try:
        assert shutdown_started.wait(2)
        assert runtime._shutdown_event.wait(2)
        assert not receiving.closed
        resume_payload.set()
        closer.join(3)
        assert not closer.is_alive()
        assert failures == []
        assert not worker.reader_thread.is_alive()
        assert receiving.closed
        assert runtime.worker_count == 0
    finally:
        resume_payload.set()
        closer.join(3)
        sending.close()
        receiving.close()


def test_pool_close_pending_reader_retains_owner_until_retry(tmp_path, monkeypatch):
    _no_journal(monkeypatch)
    monkeypatch.setattr(runtime_module, "WORKER_READER_JOIN_TIMEOUT", 0.01)
    runtime = _runtime(tmp_path)
    receiving, sending = Pipe()
    worker = _Worker(0, None, 0, receiving, None)
    runtime._workers.append(worker)
    runtime._controller_id = "held-controller"
    runtime._start_reader_thread(worker)
    pool = WorkerPool(MagicMock(), runtime)
    release = MagicMock()
    monkeypatch.setattr(runtime_module.runtime_state, "release_controller", release)
    try:
        with pytest.raises(WorkerStartError, match="cleanup did not complete"):
            pool.close()
        assert not pool._closed
        assert runtime._workers == [worker]
        assert runtime._controller_id == "held-controller"
        assert not receiving.closed
        release.assert_not_called()
        sending.send({"action": "test"})
        worker.reader_thread.join(2)
        assert not worker.reader_thread.is_alive()
        pool.close()
        assert pool._closed
        assert receiving.closed
        assert runtime.worker_count == 0
        assert runtime._controller_id is None
        release.assert_called_once()
    finally:
        sending.close()
        worker.reader_thread.join(2)
        pool.close()


def test_reader_can_retire_its_own_connection(tmp_path, monkeypatch):
    _no_journal(monkeypatch)
    runtime = _runtime(tmp_path)
    receiving, sending = Pipe()
    worker = _Worker(0, None, 0, receiving, None)
    worker.reader_thread = threading.current_thread()
    runtime._workers.append(worker)
    try:
        assert runtime._remove_dead_worker(worker)
        assert receiving.closed
        assert runtime.worker_count == 0
    finally:
        receiving.close()
        sending.close()


def test_persistent_detach_finishes_reader_without_stopping_process(tmp_path, monkeypatch):
    _no_journal(monkeypatch)
    child_code = """
import json
from wetlands import module_executor
module_executor._notify_startup = lambda host, port, token, payload: print(json.dumps(payload), flush=True)
module_executor.launch_listener(authkey=b"reader-test", persistent=True, commissioned=True,
 startup_host="127.0.0.1", startup_port=1, startup_token="owned-test", environment_path="/example",
 generation_id="g", recipe_hash="r", pool_id="pool", worker_index=0, worker_id="worker")
"""
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", child_code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    connection = None
    listener = None
    try:
        assert process.stdout is not None
        startup = json.loads(process.stdout.readline())
        listener = psutil.Process(startup["pid"])
        connection = Client(("127.0.0.1", startup["port"]), authkey=b"reader-test")
        hello = connection.recv()
        assert hello["pid"] == listener.pid
        runtime = _runtime(tmp_path)
        runtime._persistent = True
        worker = _Worker(0, None, startup["port"], connection, None, persistent=True)
        runtime._workers.append(worker)
        runtime._start_reader_thread(worker)
        runtime.detach()
        assert connection.closed
        assert not worker.reader_thread.is_alive()
        assert runtime.worker_count == 0
        assert process.poll() is None
        assert listener.is_running()
        # The same real persistent listener accepts a new controller after detach.
        with Client(("127.0.0.1", startup["port"]), authkey=b"reader-test") as reattached:
            assert reattached.recv()["pid"] == listener.pid
            reattached.send({"action": "exit", "protocol_version": EXECUTION_PROTOCOL_VERSION})
        assert process.wait(timeout=3) == 0
        listener.wait(timeout=3)
        assert not listener.is_running()
    finally:
        if connection is not None:
            connection.close()
        if listener is not None and listener.is_running():
            listener.terminate()
            try:
                listener.wait(timeout=3)
            except psutil.TimeoutExpired:
                listener.kill()
                listener.wait(timeout=3)
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=3)


def test_started_attach_rollback_keeps_public_manager_retry_owner(tmp_path, monkeypatch):
    _no_journal(monkeypatch)
    monkeypatch.setattr(runtime_module, "WORKER_READER_JOIN_TIMEOUT", 0.01)
    manager = EnvironmentManager(tmp_path / "manager")
    environment = ManagedEnvironment._from_ready(
        manager, "example", manager.environments_root / "example", {"generation_id": "g", "recipe_hash": "r"}
    )
    manager._environments["example"] = environment
    monkeypatch.setattr(environment, "_require_current_generation", lambda: None)
    monkeypatch.setattr(managed_module, "environment_lifecycle_gate", lambda *args: nullcontext())
    monkeypatch.setattr(managed_module.runtime_state, "reconcile_persistent_pool", lambda *args, **kwargs: None)
    entries = [{"pool_id": "pool", "worker_index": 0, "pid": 1, "port": 2}]
    monkeypatch.setattr(managed_module.runtime_state, "live_workers_for_env", lambda *args, **kwargs: entries)
    monkeypatch.setattr(runtime_module.runtime_state, "claim_controller", lambda *args: None)
    release = MagicMock()
    monkeypatch.setattr(runtime_module.runtime_state, "release_controller", release)
    receiving, sending = Pipe()
    worker = _Worker(0, None, 0, receiving, None, persistent=True)
    monkeypatch.setattr(ExternalEnvironment, "_attach_worker", lambda *args, **kwargs: worker)
    primary = RuntimeError("reader startup failed after ownership transfer")
    original_start = ExternalEnvironment._start_reader_thread

    def start_then_fail(runtime, held_worker):
        original_start(runtime, held_worker)
        raise primary

    monkeypatch.setattr(ExternalEnvironment, "_start_reader_thread", start_then_fail)
    try:
        with pytest.raises(RuntimeError) as caught:
            environment.attach_pool()
        assert caught.value is primary
        assert len(environment._pools) == 1
        pool = environment._pools[0]
        assert primary._wetlands_cleanup_owner is pool._runtime
        assert pool._runtime._workers == [worker]
        assert not pool._closed
        assert not receiving.closed
        release.assert_not_called()
        with pytest.raises(ManagerCloseError):
            manager.close()
        assert not pool._closed
        sending.send({"action": "test"})
        worker.reader_thread.join(2)
        manager.close()
        assert pool._closed
        assert receiving.closed
        assert pool._runtime.worker_count == 0
        release.assert_called_once()
    finally:
        sending.close()
        if worker.reader_thread is not None:
            worker.reader_thread.join(2)
        manager.close()


@pytest.mark.parametrize("cleanup_path", ["launch", "replacement"])
def test_live_reader_rollback_releases_state_lock_before_handoff(tmp_path, monkeypatch, cleanup_path):
    _no_journal(monkeypatch)
    runtime = _runtime(tmp_path)
    receiving, sending = Pipe()
    worker = _Worker(0, MagicMock(), 0, receiving, None)
    original_recv = receiving.recv

    def recv():
        message = original_recv()
        with runtime._lock:
            pass
        return message

    monkeypatch.setattr(receiving, "recv", recv)
    monkeypatch.setattr(runtime, "_finish_process_output", lambda *args: True)

    def terminate(*args):
        sending.send({"action": "test"})
        return True

    monkeypatch.setattr(runtime, "_terminate_launched_worker", terminate)
    primary = RuntimeError("second worker failed")
    try:
        if cleanup_path == "launch":
            calls = []

            def launch(*args):
                if calls:
                    raise primary
                calls.append(True)
                runtime._start_reader_thread(worker)
                return worker

            monkeypatch.setattr(runtime, "_launch_worker", launch)
            with pytest.raises(WorkerStartError) as caught:
                runtime.launch(max_workers=2)
            assert caught.value.__cause__ is primary
            assert runtime.worker_count == 0
        else:
            runtime._shutdown_event.set()
            runtime._start_reader_thread(worker)
            with runtime._lock:
                assert runtime._cleanup_failed_worker_launch(
                    worker.process, receiving, worker=worker, state_lock_held=True
                )
        assert not worker.reader_thread.is_alive()
        assert receiving.closed
    finally:
        sending.close()
        if worker.reader_thread is not None:
            worker.reader_thread.join(2)
        receiving.close()


def test_reader_waiting_for_lifecycle_transition_remains_retryable(tmp_path, monkeypatch):
    _no_journal(monkeypatch)
    monkeypatch.setattr(runtime_module, "WORKER_READER_JOIN_TIMEOUT", 0.01)
    runtime = _runtime(tmp_path)
    receiving, sending = Pipe()
    worker = _Worker(0, None, 0, receiving, None)
    runtime._workers.append(worker)
    entered = threading.Event()

    def replacement():
        entered.set()
        runtime._try_replace_worker(0)

    worker.reader_thread = threading.Thread(target=replacement)
    try:
        with runtime._lifecycle_lock:
            worker.reader_thread.start()
            assert entered.wait(2)
            with pytest.raises(WorkerStartError, match="cleanup did not complete"):
                runtime._exit()
            assert runtime._workers == [worker]
            assert not receiving.closed
        worker.reader_thread.join(2)
        assert not worker.reader_thread.is_alive()
        runtime._exit()
        assert receiving.closed
        assert runtime.worker_count == 0
    finally:
        sending.close()
        worker.reader_thread.join(2)
        runtime._exit()
