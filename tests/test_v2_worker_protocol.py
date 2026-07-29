from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from wetlands._internal import runtime_state
from wetlands._internal.process_termination import ProcessIdentity, ProcessIdentityError
from wetlands._internal.value_codec import (
    SUPPORTED_CODECS,
    decode_value,
    descriptor_codecs,
    encode_value,
)
from wetlands.external_environment import ExternalEnvironment, _Worker
from wetlands.lifecycle import WorkerStartError
from wetlands import module_executor
from wetlands.protocol import (
    EXECUTION_PROTOCOL_VERSION,
    ProtocolCompatibilityError,
    ProtocolError,
    WORKER_RUNTIME_VERSION,
    execution_envelope,
    import_target,
    path_target,
    validate_task_message,
    validate_worker_task_message,
    validate_worker_capabilities,
    worker_hello,
)
from wetlands.task import RemoteTaskHandle, ExecutionTask, ExecutionState


def _hello(**updates):
    payload = worker_hello(
        codecs=SUPPORTED_CODECS,
        python_version="3.13.2",
        pid=1234,
        environment_path="/managed/example",
        generation_id="generation-1",
        recipe_hash="recipe-1",
    )
    payload.update(updates)
    return payload


def test_worker_handshake_validates_every_runtime_identity_field():
    expected = {
        "pid": 1234,
        "environment_path": "/managed/example",
        "generation_id": "generation-1",
        "recipe_hash": "recipe-1",
    }

    capabilities = validate_worker_capabilities(
        _hello(),
        required_codecs=SUPPORTED_CODECS,
        expected_identity=expected,
    )

    assert capabilities.runtime_version == WORKER_RUNTIME_VERSION
    assert capabilities.pid == 1234
    for field in expected:
        with pytest.raises(ProtocolCompatibilityError, match="identity mismatch"):
            validate_worker_capabilities(
                _hello(**{field: "wrong" if field != "pid" else 9999}),
                required_codecs=SUPPORTED_CODECS,
                expected_identity=expected,
            )


def test_worker_handshake_rejects_runtime_protocol_and_codec_drift():
    with pytest.raises(ProtocolCompatibilityError, match="protocol mismatch"):
        validate_worker_capabilities(
            _hello(protocol_version=EXECUTION_PROTOCOL_VERSION + 1),
            required_codecs=SUPPORTED_CODECS,
        )
    with pytest.raises(ProtocolCompatibilityError, match="runtime mismatch"):
        validate_worker_capabilities(
            _hello(worker_runtime_version="old"),
            required_codecs=SUPPORTED_CODECS,
        )
    with pytest.raises(ProtocolCompatibilityError, match="missing required codecs"):
        validate_worker_capabilities(_hello(codecs=[]), required_codecs=SUPPORTED_CODECS)


def test_protocol_messages_require_version_action_and_task_identity():
    assert validate_task_message(
        {"action": "cancel", "protocol_version": EXECUTION_PROTOCOL_VERSION, "task_id": "task-1"}
    ) == ("cancel", "task-1")
    with pytest.raises(ProtocolCompatibilityError):
        validate_task_message({"action": "cancel", "task_id": "task-1"})
    with pytest.raises(ProtocolError):
        validate_task_message(
            {"action": "unknown", "protocol_version": EXECUTION_PROTOCOL_VERSION, "task_id": "task-1"}
        )


@pytest.mark.parametrize(
    "message",
    [
        None,
        {
            "action": "unknown",
            "protocol_version": EXECUTION_PROTOCOL_VERSION,
            "task_id": "task-1",
        },
        {
            "action": "accepted",
            "protocol_version": EXECUTION_PROTOCOL_VERSION,
            "task_id": "task-1",
            "unexpected": True,
        },
        {
            "action": "log",
            "protocol_version": EXECUTION_PROTOCOL_VERSION,
            "task_id": "task-1",
            "level": "INFO",
            "message": "hello",
        },
        {
            "action": "released",
            "protocol_version": EXECUTION_PROTOCOL_VERSION,
            "task_id": "task-1",
            "names": ["same", "same"],
        },
        {
            "action": "error",
            "protocol_version": EXECUTION_PROTOCOL_VERSION,
            "task_id": "task-1",
            "failure": {},
            "exception": "boom",
            "traceback": "",
        },
    ],
)
def test_worker_task_messages_reject_malformed_payloads(message):
    with pytest.raises(ProtocolError):
        validate_worker_task_message(message, expected_task_id="task-1")


def test_worker_task_messages_validate_complete_payload_shapes():
    assert (
        validate_worker_task_message(
            {
                "action": "update",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "task-1",
                "message": "working",
                "current": 1,
                "maximum": 2,
                "outputs": {"preview": [1, True, None]},
            },
            expected_task_id="task-1",
        )
        == "update"
    )
    assert (
        validate_worker_task_message(
            {
                "action": "log",
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
                "task_id": "task-1",
                "level": 20,
                "message": "working",
            },
            expected_task_id="task-1",
        )
        == "log"
    )


def test_worker_failure_producer_matches_strict_host_schema():
    failure = module_executor._failure_payload(
        ValueError("bad input"),
        task_id="task-1",
        call_target="example:run",
    )
    message = {
        "action": "error",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": "task-1",
        "failure": failure,
        "exception": "bad input",
        "traceback": failure["traceback"],
    }

    assert validate_worker_task_message(message, expected_task_id="task-1") == "error"


def test_worker_failure_producer_truncates_recursive_exception_chain():
    error = ValueError("recursive cause")
    error.__cause__ = error

    failure = module_executor._failure_payload(
        error,
        task_id="task-1",
        call_target="example:run",
    )
    remote_exception = failure["remote_exception"]

    assert remote_exception["cause"] is not None
    assert remote_exception["cause"]["cause"] is None
    message = {
        "action": "error",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": "task-1",
        "failure": failure,
        "exception": "recursive cause",
        "traceback": failure["traceback"],
    }
    assert validate_worker_task_message(message, expected_task_id="task-1") == "error"


def test_execution_worker_reports_self_caused_exception_without_hanging(tmp_path):
    module = tmp_path / "recursive_failure.py"
    module.write_text(
        "def fail():\n    error = ValueError('recursive cause')\n    error.__cause__ = error\n    raise error\n",
        encoding="utf-8",
    )
    encoded_args, _ = encode_value((), path="args")
    encoded_kwargs, _ = encode_value({}, path="kwargs")
    envelope = execution_envelope(
        task_id="task-recursive-failure",
        target=path_target(module, "fail", cache=False),
        args=encoded_args,
        kwargs=encoded_kwargs,
        codecs=descriptor_codecs(encoded_args, encoded_kwargs),
    )
    connection = MagicMock()

    module_executor.execution_worker(threading.Lock(), connection, envelope)

    messages = [call.args[0] for call in connection.send.call_args_list]
    assert [message["action"] for message in messages] == [
        "accepted",
        "input_released",
        "error",
    ]
    assert (
        validate_worker_task_message(
            messages[-1],
            expected_task_id="task-recursive-failure",
        )
        == "error"
    )


def test_worker_failure_rejects_recursive_remote_exception_payload():
    failure = module_executor._failure_payload(
        ValueError("bad input"),
        task_id="task-1",
        call_target="example:run",
    )
    remote_exception = failure["remote_exception"]
    remote_exception["cause"] = remote_exception
    message = {
        "action": "error",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": "task-1",
        "failure": failure,
        "exception": "bad input",
        "traceback": failure["traceback"],
    }

    with pytest.raises(ProtocolError, match="recursive reference"):
        validate_worker_task_message(message, expected_task_id="task-1")


def test_worker_failure_rejects_excessively_deep_remote_exception_payload():
    remote_exception = None
    for index in range(35):
        remote_exception = {
            "module": "builtins",
            "type_name": "ValueError",
            "qualified_name": "ValueError",
            "message": f"level {index}",
            "traceback": "",
            "cause": remote_exception,
            "context": None,
            "suppress_context": False,
        }
    failure = module_executor._failure_payload(
        ValueError("bad input"),
        task_id="task-1",
        call_target="example:run",
    )
    failure["remote_exception"] = remote_exception
    message = {
        "action": "error",
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
        "task_id": "task-1",
        "failure": failure,
        "exception": "bad input",
        "traceback": failure["traceback"],
    }

    with pytest.raises(ProtocolError, match="maximum nesting depth"):
        validate_worker_task_message(message, expected_task_id="task-1")


def test_worker_handshake_rejects_unknown_and_incomplete_fields():
    with pytest.raises(ProtocolCompatibilityError, match="unexpected fields"):
        validate_worker_capabilities(
            _hello(unexpected=True),
            required_codecs=SUPPORTED_CODECS,
        )
    with pytest.raises(ProtocolCompatibilityError, match="incomplete persistent"):
        validate_worker_capabilities(
            _hello(pool_id="pool-1"),
            required_codecs=SUPPORTED_CODECS,
        )


@pytest.mark.parametrize(
    "target",
    [
        "package.module",
        "package.module:",
        ":callable",
        "package.module:callable:extra",
        "package-name.module:callable",
    ],
)
def test_import_target_is_unambiguous(target):
    with pytest.raises((ValueError, ProtocolError)):
        import_target(target)


def test_path_target_requires_a_real_file_and_dotted_identifier(tmp_path):
    module = tmp_path / "worker.py"
    module.write_text("def run():\n    return 1\n", encoding="utf-8")

    assert path_target(module, "run", cache=True)["path"] == str(module.resolve())
    with pytest.raises(ProtocolError):
        path_target(module, "not-valid", cache=True)
    with pytest.raises(FileNotFoundError):
        path_target(tmp_path / "missing.py", "run", cache=True)


def test_path_target_cache_is_collision_free_and_content_addressed(tmp_path):
    first = tmp_path / "first" / "worker.py"
    second = tmp_path / "second" / "worker.py"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_text("def value():\n    return 'first'\n", encoding="utf-8")
    second.write_text("def value():\n    return 'second'\n", encoding="utf-8")
    module_executor._path_modules.clear()
    module_executor._path_module_keys.clear()

    first_callable = module_executor._resolve_protocol_target(path_target(first, "value", cache=True))
    second_callable = module_executor._resolve_protocol_target(path_target(second, "value", cache=True))
    assert first_callable() == "first"
    assert second_callable() == "second"
    assert len(module_executor._path_modules) == 2

    first.write_text("def value():\n    return 'edited'\n", encoding="utf-8")
    edited_callable = module_executor._resolve_protocol_target(path_target(first, "value", cache=True))
    assert edited_callable() == "edited"
    assert len(module_executor._path_modules) == 2


def test_protocol_envelope_executes_qualified_import_and_offers_encoded_result():
    encoded_args, argument_leases = encode_value((2, 3), path="args")
    encoded_kwargs, keyword_leases = encode_value({}, path="kwargs")
    assert not argument_leases
    assert not keyword_leases
    envelope = execution_envelope(
        task_id="task-add",
        target=import_target("operator:add"),
        args=encoded_args,
        kwargs=encoded_kwargs,
        codecs=descriptor_codecs(encoded_args, encoded_kwargs),
        context_keyword=None,
    )
    connection = MagicMock()

    module_executor.execute_protocol_envelope(envelope, threading.Lock(), connection)

    messages = [call.args[0] for call in connection.send.call_args_list]
    assert [message["action"] for message in messages] == [
        "accepted",
        "input_released",
        "result_offer",
    ]
    assert all(message["protocol_version"] == EXECUTION_PROTOCOL_VERSION for message in messages)
    assert decode_value(messages[-1]["result"], copy_arrays=True) == 5
    leases = module_executor._output_leases.pop("task-add")
    module_executor.dispose_leases(leases, unlink=True)


def test_runtime_context_injection_is_explicit(tmp_path):
    module = tmp_path / "context_worker.py"
    module.write_text(
        "def receives(task=None):\n    return task is not None\n",
        encoding="utf-8",
    )
    encoded_args, _ = encode_value((), path="args")
    encoded_kwargs, _ = encode_value({}, path="kwargs")

    def execute(context_keyword):
        connection = MagicMock()
        envelope = execution_envelope(
            task_id=f"task-{context_keyword}",
            target=path_target(module, "receives", cache=False),
            args=encoded_args,
            kwargs=encoded_kwargs,
            codecs=descriptor_codecs(encoded_args, encoded_kwargs),
            context_keyword=context_keyword,
        )
        module_executor.execute_protocol_envelope(envelope, threading.Lock(), connection)
        offer = next(
            call.args[0] for call in connection.send.call_args_list if call.args[0]["action"] == "result_offer"
        )
        leases = module_executor._output_leases.pop(envelope["task_id"])
        module_executor.dispose_leases(leases, unlink=True)
        return decode_value(offer["result"], copy_arrays=True)

    assert execute(None) is False
    assert execute("task") is True


def test_remote_intermediate_values_reject_arrays_and_unsupported_objects():
    np = pytest.importorskip("numpy")
    handle = RemoteTaskHandle("task-1", threading.Lock(), MagicMock())

    handle.set_output("simple", {"nested": [None, True, 2, b"bytes"]})
    with pytest.raises(TypeError, match="unsupported intermediate value"):
        handle.set_output("array", np.arange(3))
    with pytest.raises(TypeError, match="unsupported intermediate value"):
        handle.set_output("object", object())


def test_remote_messages_are_versioned_and_self_cancellation_is_local():
    connection = MagicMock()
    handle = RemoteTaskHandle("task-1", threading.Lock(), connection)

    handle.update("working", current=1, maximum=2)
    handle.log("message")
    handle.cancel()

    messages = [call.args[0] for call in connection.send.call_args_list]
    assert [message["action"] for message in messages] == ["update", "log"]
    assert all(message["protocol_version"] == EXECUTION_PROTOCOL_VERSION for message in messages)
    assert handle.cancel_requested


def test_canceling_awaiter_waits_for_execution_cleanup():
    task = ExecutionTask()
    task._set_running()
    cleanup_finished = threading.Event()

    def cancel() -> None:
        def finish_cleanup() -> None:
            threading.Event().wait(0.05)
            cleanup_finished.set()
            task._set_canceled()

        threading.Thread(target=finish_cleanup).start()

    task._set_cancel_fn(cancel)

    async def run() -> None:
        waiter = asyncio.create_task(task._async_result())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert cleanup_finished.is_set()
        assert task.state is ExecutionState.CANCELED

    asyncio.run(run())


def test_multi_worker_launch_is_failure_atomic(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path
    manager.debug = False
    manager.termination_grace = 1.0
    runtime = ExternalEnvironment("example", tmp_path / "pixi.toml", manager)
    first = MagicMock(spec=_Worker)
    first.index = 0
    first._retired = False
    first.connection = MagicMock()
    first.process = MagicMock()
    first.process_logger = None
    first.persistent = False
    first.pid = 123
    first.reader_thread = None
    first._current_task = None
    first._finished_task_ids = set()
    first._last_activity = 0.0

    with (
        patch.object(runtime, "launched", return_value=False),
        patch.object(runtime_state, "load_or_create_root_authkey", return_value=b"key"),
        patch.object(runtime, "_launch_worker", side_effect=[first, RuntimeError("second failed")]),
        patch.object(runtime, "_remove_dead_worker", return_value=True) as remove,
        pytest.raises(RuntimeError, match="second failed"),
    ):
        runtime.launch(max_workers=2)

    remove.assert_called_once_with(first)
    assert runtime.worker_count == 0


def test_persistent_launch_failure_discards_uncommissioned_attempt(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path
    manager.debug = False
    manager.termination_grace = 1.0
    runtime = ExternalEnvironment("example", tmp_path / "pixi.toml", manager)
    first = MagicMock(spec=_Worker)
    first.index = 0
    first._retired = False
    first.connection = MagicMock()
    first.process = MagicMock()
    first.process_logger = None
    first.persistent = True
    first.pid = 123
    first.reader_thread = None
    first._current_task = None
    first._finished_task_ids = set()
    first._last_activity = 0.0

    with (
        patch.object(runtime, "launched", return_value=False),
        patch.object(runtime_state, "load_or_create_root_authkey", return_value=b"key"),
        patch.object(runtime_state, "claim_controller"),
        patch.object(runtime_state, "reconcile_persistent_pool"),
        patch.object(runtime_state, "live_workers_for_env", return_value=[]),
        patch.object(runtime_state, "begin_persistent_pool_attempt") as begin,
        patch.object(runtime_state, "commission_persistent_pool") as commission,
        patch.object(runtime_state, "discard_persistent_pool") as discard,
        patch.object(runtime_state, "release_controller"),
        patch.object(runtime, "_launch_worker", side_effect=[first, RuntimeError("second failed")]),
        patch.object(runtime, "_remove_dead_worker", return_value=True),
        pytest.raises(RuntimeError, match="second failed"),
    ):
        runtime.launch(max_workers=2, persistent=True)

    begin.assert_called_once()
    commission.assert_not_called()
    discard.assert_called_once()
    assert runtime.worker_count == 0


def test_persistent_launch_reconciliation_failure_releases_controller_claim(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path
    manager.termination_grace = 1.0
    runtime = ExternalEnvironment("example", tmp_path / "pixi.toml", manager)

    with (
        patch.object(runtime, "launched", return_value=False),
        patch.object(runtime_state, "load_or_create_root_authkey", return_value=b"key"),
        patch.object(runtime_state, "claim_controller"),
        patch.object(
            runtime_state,
            "reconcile_persistent_pool",
            side_effect=ProcessIdentityError("cannot validate survivor"),
        ),
        patch.object(runtime_state, "release_controller") as release,
        patch.object(runtime_state, "begin_persistent_pool_attempt") as begin,
        pytest.raises(ProcessIdentityError, match="cannot validate survivor"),
    ):
        runtime.launch(persistent=True)

    release.assert_called_once()
    begin.assert_not_called()
    assert runtime._controller_id is None


def test_partial_attach_rolls_back_connections_and_controller_claim(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path
    runtime = ExternalEnvironment("example", tmp_path / "pixi.toml", manager)
    first = MagicMock(spec=_Worker)
    first.connection = MagicMock()
    first.connection.closed = False
    entries = (
        {"pool_id": "pool-1", "worker_index": 0},
        {"pool_id": "pool-1", "worker_index": 1},
    )

    with (
        patch.object(runtime_state, "claim_controller"),
        patch.object(runtime_state, "release_controller") as release,
        patch.object(runtime_state, "live_workers_for_env", return_value=list(entries)),
        patch.object(runtime, "_attach_worker", side_effect=[first, RuntimeError("handshake failed")]),
        pytest.raises(RuntimeError, match="handshake failed"),
    ):
        runtime.attach_workers(entries, b"key")

    first.connection.send.assert_called_once_with({"action": "detach", "protocol_version": EXECUTION_PROTOCOL_VERSION})
    first.connection.close.assert_called_once()
    release.assert_called_once()
    assert runtime.worker_count == 0


def test_attach_wraps_protocol_mismatch_in_public_start_error(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path
    runtime = ExternalEnvironment("example", tmp_path / "pixi.toml", manager)
    entries = ({"pool_id": "pool-1", "worker_index": 0},)
    mismatch = ProtocolCompatibilityError("protocol mismatch")

    with (
        patch.object(runtime_state, "claim_controller"),
        patch.object(runtime_state, "release_controller"),
        patch.object(runtime_state, "live_workers_for_env", return_value=list(entries)),
        patch.object(runtime, "_attach_worker", side_effect=mismatch),
        pytest.raises(WorkerStartError) as caught,
    ):
        runtime.attach_workers(entries, b"key")

    assert caught.value.environment == "example"
    assert caught.value.phase == "attach"
    assert caught.value.__cause__ is mismatch


def test_attach_claim_failure_does_not_leave_local_controller_identity(tmp_path):
    manager = MagicMock()
    manager.root = tmp_path
    runtime = ExternalEnvironment("example", tmp_path / "pixi.toml", manager)

    with (
        patch.object(
            runtime_state,
            "claim_controller",
            side_effect=RuntimeError("already controlled"),
        ),
        pytest.raises(RuntimeError, match="already controlled"),
    ):
        runtime.attach_workers(
            ({"pool_id": "pool-1", "worker_index": 0},),
            b"key",
        )

    assert runtime._controller_id is None
    assert runtime.worker_count == 0


def test_uncommissioned_worker_watchdog_exits_at_deadline():
    exit_codes = []

    module_executor._watch_launcher_commission(
        threading.Event(),
        0.0,
        exit_process=exit_codes.append,
    )

    assert exit_codes == [module_executor.UNCOMMISSIONED_EXIT_CODE]


def test_commissioned_worker_watchdog_is_disarmed():
    commissioned = threading.Event()
    commissioned.set()
    exit_codes = []

    module_executor._watch_launcher_commission(
        commissioned,
        0.0,
        exit_process=exit_codes.append,
    )

    assert exit_codes == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-session assertion")
def test_worker_spawn_starts_a_new_posix_session():
    runtime = object.__new__(ExternalEnvironment)
    process = MagicMock()
    process.pid = 1234
    identity = ProcessIdentity(1234, 1.0, 1234, 1234)
    with (
        patch("wetlands.external_environment.subprocess.Popen", return_value=process) as popen,
        patch("wetlands.external_environment.capture_process_identity", return_value=identity),
    ):
        assert runtime._spawn_worker_process(["python", "worker.py"], {}, {}) is process
    assert popen.call_args.kwargs["start_new_session"] is True


@pytest.mark.skipif(os.name == "nt", reason="POSIX process-session assertion")
def test_worker_spawn_identity_failure_uses_identity_aware_tree_termination():
    runtime = object.__new__(ExternalEnvironment)
    process = MagicMock()
    process.pid = 1234
    identity = ProcessIdentity(1234, 1.0, 999, 999)
    with (
        patch("wetlands.external_environment.subprocess.Popen", return_value=process),
        patch("wetlands.external_environment.capture_process_identity", return_value=identity),
        patch("wetlands.external_environment.terminate_launched_process_tree") as terminate,
        pytest.raises(RuntimeError, match="own POSIX session"),
    ):
        runtime._spawn_worker_process(["python", "worker.py"], {}, {})

    terminate.assert_called_once()
    process.kill.assert_not_called()


def test_worker_spawn_capture_failure_uses_direct_child_fallback():
    runtime = object.__new__(ExternalEnvironment)
    process = MagicMock()
    process.pid = 1234
    process.poll.return_value = None
    with (
        patch("wetlands.external_environment.subprocess.Popen", return_value=process),
        patch(
            "wetlands.external_environment.capture_process_identity",
            side_effect=ProcessIdentityError("cannot inspect"),
        ),
        patch("wetlands.external_environment.terminate_launched_process_tree") as terminate,
        pytest.raises(ProcessIdentityError, match="cannot inspect"),
    ):
        runtime._spawn_worker_process(["python", "worker.py"], {}, {})

    terminate.assert_not_called()
    process.kill.assert_called_once()
    process.wait.assert_called_once()


@pytest.mark.parametrize(
    ("persistent", "expects_kill_job"),
    [(False, True), (True, False)],
)
def test_windows_worker_job_policy_preserves_detached_persistent_workers(
    persistent,
    expects_kill_job,
):
    runtime = object.__new__(ExternalEnvironment)
    runtime._persistent = persistent
    process = MagicMock()
    process.pid = 1234
    identity = ProcessIdentity(1234, 1.0, None, None)

    with (
        patch("wetlands.external_environment.os.name", "nt"),
        patch.object(
            subprocess,
            "CREATE_NEW_PROCESS_GROUP",
            0x200,
            create=True,
        ),
        patch(
            "wetlands.external_environment.subprocess.Popen",
            return_value=process,
        ) as popen,
        patch(
            "wetlands.external_environment.capture_process_identity",
            return_value=identity,
        ),
        patch("wetlands.external_environment._assign_windows_kill_job") as assign_job,
    ):
        assert runtime._spawn_worker_process(["python", "worker.py"], {}, {}) is process

    assert popen.call_args.kwargs["creationflags"] == 0x200
    assert assign_job.called is expects_kill_job


def test_persistent_controller_claim_is_exclusive_and_recoverable(tmp_path):
    runtime_state.claim_controller(tmp_path, "example", "controller-1")
    with pytest.raises(RuntimeError, match="another live process"):
        runtime_state.claim_controller(tmp_path, "example", "controller-2")
    runtime_state.release_controller(tmp_path, "example", "controller-1")
    runtime_state.claim_controller(tmp_path, "example", "controller-2")
    runtime_state.release_controller(tmp_path, "example", "controller-2")


def test_persistent_registry_requires_exact_generation_and_protocol_identity(tmp_path):
    runtime_state.begin_persistent_pool_attempt(
        tmp_path,
        env_name="example",
        pool_id="pool-1",
        expected_worker_count=1,
    )
    runtime_state.record_worker(
        tmp_path,
        env_name="example",
        env_path=tmp_path / "environment",
        worker_index=0,
        pid=os.getpid(),
        port=54321,
        worker_id="pool-1-worker-0",
        management_port=54322,
        persistent=True,
        pool_id="pool-1",
        generation_id="generation-1",
        recipe_hash="recipe-1",
        worker_runtime_version=WORKER_RUNTIME_VERSION,
        protocol_version=EXECUTION_PROTOCOL_VERSION,
    )
    runtime_state.commission_persistent_pool(
        tmp_path,
        env_name="example",
        pool_id="pool-1",
    )
    identity = {
        "generation_id": "generation-1",
        "recipe_hash": "recipe-1",
        "worker_runtime_version": WORKER_RUNTIME_VERSION,
        "protocol_version": EXECUTION_PROTOCOL_VERSION,
    }

    assert (
        runtime_state.live_workers_for_env(
            tmp_path,
            "example",
            expected_identity=identity,
        )[0]["generation_id"]
        == "generation-1"
    )
    with pytest.raises(RuntimeError, match="identity mismatch"):
        runtime_state.live_workers_for_env(
            tmp_path,
            "example",
            expected_identity={**identity, "generation_id": "generation-2"},
        )


def test_worker_bootstrap_loads_in_isolated_python_without_wetlands_installed():
    script = Path(module_executor.__file__).resolve()
    completed = subprocess.run(
        [sys.executable, "-I", str(script), "--help"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "Module executor" in completed.stdout
