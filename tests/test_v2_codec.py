from __future__ import annotations

import inspect
import os
import threading
from multiprocessing import shared_memory
from unittest.mock import MagicMock, patch

import pytest

import wetlands._internal.value_codec as value_codec
from wetlands._internal.value_codec import (
    CODEC_MARKER,
    CORE_CODEC_ID,
    CORE_CODEC_VERSION,
    MAX_ARRAY_NBYTES,
    NUMPY_CODEC_ID,
    NUMPY_CODEC_VERSION,
    ValueDecodingError,
    decode_value,
    descriptor_codecs,
    dispose_leases,
    encode_value,
    load_shared_memory_lease_ledger,
    reconcile_shared_memory_leases,
)


def _lease_context(tmp_path, *, direction: str = "input", started_at: float | None = None):
    import psutil

    return {
        "root": str(tmp_path),
        "creator_pid": os.getpid(),
        "creator_started_at": (psutil.Process(os.getpid()).create_time() if started_at is None else started_at),
        "environment_name": "example",
        "generation_id": "generation-1",
        "pool_id": "pool-1",
        "task_id": "task-1",
        "direction": direction,
    }


def test_nested_core_values_round_trip() -> None:
    value = {
        "none": None,
        "values": [True, 3, 4.5, "text", b"bytes", (1, 2)],
    }
    descriptor, leases = encode_value(value)
    try:
        assert decode_value(descriptor, copy_arrays=True) == value
    finally:
        dispose_leases(leases, unlink=True)


def test_descriptor_capabilities_include_only_codecs_actually_used() -> None:
    numpy = pytest.importorskip("numpy")
    core, core_leases = encode_value({"value": 1})
    array, array_leases = encode_value([numpy.arange(2)])
    try:
        assert descriptor_codecs(core) == ((CORE_CODEC_ID, CORE_CODEC_VERSION),)
        assert descriptor_codecs(array) == (
            (CORE_CODEC_ID, CORE_CODEC_VERSION),
            (NUMPY_CODEC_ID, NUMPY_CODEC_VERSION),
        )
    finally:
        dispose_leases(core_leases, unlink=True)
        dispose_leases(array_leases, unlink=True)


def test_unsupported_values_and_cycles_are_rejected() -> None:
    with pytest.raises(TypeError, match=r"\$: unsupported"):
        encode_value(object())
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(TypeError, match="cyclic"):
        encode_value(cyclic)


def test_numpy_array_round_trip_is_independently_owned() -> None:
    numpy = pytest.importorskip("numpy")
    source = numpy.arange(12, dtype=numpy.int32).reshape(3, 4)[:, ::2]
    expected = source.copy()
    descriptor, owner_leases = encode_value(source)
    source[...] = 99
    attachments = []
    try:
        result = decode_value(descriptor, copy_arrays=True, attachments=attachments)
        result[0, 0] = -1
        assert source[0, 0] == 99
        assert expected[0, 0] == 0
        assert result[0, 1] == expected[0, 1]
        assert result.flags.c_contiguous
        assert result.flags.owndata
    finally:
        dispose_leases(attachments, unlink=False)
        dispose_leases(owner_leases, unlink=True)


def test_object_dtype_is_rejected() -> None:
    numpy = pytest.importorskip("numpy")
    with pytest.raises(TypeError, match="object-dtype"):
        encode_value(numpy.array([object()], dtype=object))


def test_numpy_scalar_empty_structured_and_endian_arrays_round_trip() -> None:
    numpy = pytest.importorskip("numpy")
    padded = numpy.dtype(
        {
            "names": ["label", "score"],
            "formats": ["i1", ">f8"],
            "offsets": [0, 8],
            "titles": ["Label", None],
            "itemsize": 24,
        },
        align=True,
    )
    values = [
        numpy.array(7, dtype=numpy.int16),
        numpy.empty((2, 0, 4), dtype=numpy.float32),
        numpy.array([(1, 2.5)], dtype=[("label", "<i4"), ("score", ">f8")]),
        numpy.array([(1, 2.5)], dtype=padded),
        numpy.zeros(2, dtype=[("matrix", "<i2", (2, 3))]),
        numpy.arange(5, dtype=">i4"),
    ]
    for source in values:
        descriptor, owner_leases = encode_value(source)
        attachments = []
        try:
            result = decode_value(descriptor, copy_arrays=True, attachments=attachments)
            assert result.shape == source.shape
            assert result.dtype == source.dtype
            assert result.strides == source.copy(order="C").strides
            numpy.testing.assert_array_equal(result, source)
        finally:
            dispose_leases(attachments, unlink=False)
            dispose_leases(owner_leases, unlink=True)


def test_forged_numpy_descriptor_size_is_rejected() -> None:
    pytest.importorskip("numpy")
    descriptor = {
        CODEC_MARKER: {
            "id": NUMPY_CODEC_ID,
            "version": NUMPY_CODEC_VERSION,
            "kind": "ndarray",
            "name": "not-opened-because-size-is-invalid",
            "shape": (4,),
            "dtype": {"kind": "scalar", "value": "<i4"},
            "nbytes": 8,
            "segment_size": 8,
        }
    }
    with pytest.raises(ValueDecodingError, match="byte count"):
        decode_value(descriptor, copy_arrays=True)


def test_invalid_dictionary_key_is_rejected_with_path() -> None:
    with pytest.raises(TypeError, match="unsupported dictionary key"):
        encode_value({("tuple",): 1})


def test_dtype_metadata_is_rejected() -> None:
    numpy = pytest.importorskip("numpy")
    source = numpy.zeros(1, dtype=numpy.dtype("i4", metadata={"application": object()}))

    with pytest.raises(TypeError, match="dtype metadata"):
        encode_value(source)


@pytest.mark.parametrize(
    ("descriptor", "message"),
    [
        ({}, "malformed value descriptor"),
        (
            {CODEC_MARKER: {"id": CORE_CODEC_ID, "version": True, "kind": "none"}},
            "malformed codec identity",
        ),
        (
            {
                CODEC_MARKER: {
                    "id": CORE_CODEC_ID,
                    "version": CORE_CODEC_VERSION,
                    "kind": "none",
                    "unexpected": None,
                }
            },
            "invalid none payload",
        ),
        (
            {
                CODEC_MARKER: {
                    "id": CORE_CODEC_ID,
                    "version": CORE_CODEC_VERSION,
                    "kind": "dict",
                    "items": [
                        (
                            {
                                CODEC_MARKER: {
                                    "id": CORE_CODEC_ID,
                                    "version": CORE_CODEC_VERSION,
                                    "kind": "str",
                                    "value": "duplicate",
                                }
                            },
                            {
                                CODEC_MARKER: {
                                    "id": CORE_CODEC_ID,
                                    "version": CORE_CODEC_VERSION,
                                    "kind": "none",
                                }
                            },
                        ),
                        (
                            {
                                CODEC_MARKER: {
                                    "id": CORE_CODEC_ID,
                                    "version": CORE_CODEC_VERSION,
                                    "kind": "str",
                                    "value": "duplicate",
                                }
                            },
                            {
                                CODEC_MARKER: {
                                    "id": CORE_CODEC_ID,
                                    "version": CORE_CODEC_VERSION,
                                    "kind": "none",
                                }
                            },
                        ),
                    ],
                }
            },
            "duplicate decoded dictionary key",
        ),
    ],
)
def test_malformed_core_descriptors_are_rejected(descriptor, message: str) -> None:
    with pytest.raises(ValueDecodingError, match=message):
        decode_value(descriptor, copy_arrays=True)


def test_cyclic_descriptor_is_rejected() -> None:
    descriptor = {
        CODEC_MARKER: {
            "id": CORE_CODEC_ID,
            "version": CORE_CODEC_VERSION,
            "kind": "list",
            "items": [],
        }
    }
    descriptor[CODEC_MARKER]["items"].append(descriptor)

    with pytest.raises(ValueDecodingError, match="cyclic value descriptors"):
        decode_value(descriptor, copy_arrays=True)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"shape": [1]}, "invalid array shape"),
        ({"shape": (MAX_ARRAY_NBYTES, 2)}, "array shape overflows"),
        ({"shape": (MAX_ARRAY_NBYTES,), "dtype": {"kind": "scalar", "value": "<i8"}}, "byte count overflows"),
        ({"segment_size": 0}, "invalid shared-memory segment size"),
        ({"unexpected": True}, "invalid NumPy payload"),
    ],
)
def test_forged_numpy_descriptor_layout_is_rejected(updates: dict, message: str) -> None:
    pytest.importorskip("numpy")
    payload = {
        "id": NUMPY_CODEC_ID,
        "version": NUMPY_CODEC_VERSION,
        "kind": "ndarray",
        "name": "must-not-be-opened",
        "shape": (1,),
        "dtype": {"kind": "scalar", "value": "|u1"},
        "nbytes": 1,
        "segment_size": 1,
    }
    payload.update(updates)

    with pytest.raises(ValueDecodingError, match=message):
        decode_value({CODEC_MARKER: payload}, copy_arrays=True)


def test_partial_nested_encode_unlinks_already_created_segments(monkeypatch: pytest.MonkeyPatch) -> None:
    numpy = pytest.importorskip("numpy")
    original = shared_memory.SharedMemory
    created_names: list[str] = []

    def recording_shared_memory(*args, **kwargs):
        memory = original(*args, **kwargs)
        if kwargs.get("create"):
            created_names.append(memory.name)
        return memory

    monkeypatch.setattr(value_codec.shared_memory, "SharedMemory", recording_shared_memory)
    with pytest.raises(TypeError, match=r"\$\[1\]: unsupported"):
        encode_value([numpy.arange(3), object()])

    assert created_names
    for name in created_names:
        with pytest.raises(FileNotFoundError):
            original(name=name, create=False)


def test_partial_nested_decode_closes_attachments() -> None:
    numpy = pytest.importorskip("numpy")
    array_descriptor, owners = encode_value(numpy.arange(3))
    invalid = {CODEC_MARKER: {"id": CORE_CODEC_ID, "version": CORE_CODEC_VERSION, "kind": "invalid", "items": []}}
    descriptor = {
        CODEC_MARKER: {
            "id": CORE_CODEC_ID,
            "version": CORE_CODEC_VERSION,
            "kind": "list",
            "items": [array_descriptor, invalid],
        }
    }
    attachments = []
    try:
        with pytest.raises(ValueDecodingError, match="unsupported core kind"):
            decode_value(descriptor, copy_arrays=True, attachments=attachments)
        assert attachments == []
    finally:
        dispose_leases(owners, unlink=True)


def test_zero_copy_decode_requires_caller_owned_attachment_list() -> None:
    numpy = pytest.importorskip("numpy")
    descriptor, owners = encode_value(numpy.arange(3))
    try:
        with pytest.raises(ValueDecodingError, match="requires an attachment lease list"):
            decode_value(descriptor, copy_arrays=False)
    finally:
        dispose_leases(owners, unlink=True)


@pytest.mark.skipif(os.name == "nt", reason="Windows shared memory does not use POSIX resource_tracker registration")
def test_non_owner_tracking_is_disabled_without_unregistering_local_creator() -> None:
    fake_memory = MagicMock()
    fake_memory._name = "/foreign"
    constructor = MagicMock(return_value=fake_memory)
    no_track_signature = inspect.Signature([inspect.Parameter("name", inspect.Parameter.POSITIONAL_OR_KEYWORD)])

    with (
        patch.object(value_codec.shared_memory, "SharedMemory", constructor),
        patch.object(value_codec.inspect, "signature", return_value=no_track_signature),
        patch("multiprocessing.resource_tracker.unregister") as unregister,
    ):
        value_codec._open_non_owner("foreign")
        unregister.assert_called_once_with("/foreign", "shared_memory")
        unregister.reset_mock()
        with value_codec._created_names_lock:
            value_codec._created_names.add("foreign")
        try:
            value_codec._open_non_owner("foreign")
            unregister.assert_not_called()
        finally:
            with value_codec._created_names_lock:
                value_codec._created_names.discard("foreign")


def test_python_313_non_owner_tracking_uses_track_false() -> None:
    fake_memory = MagicMock()
    constructor = MagicMock(return_value=fake_memory)
    track_signature = inspect.Signature(
        [
            inspect.Parameter("name", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter("track", inspect.Parameter.KEYWORD_ONLY),
        ]
    )

    with (
        patch.object(value_codec.shared_memory, "SharedMemory", constructor),
        patch.object(value_codec.inspect, "signature", return_value=track_signature),
    ):
        assert value_codec._open_non_owner("foreign") is fake_memory

    constructor.assert_called_once_with(name="foreign", create=False, track=False)


def test_creator_lease_is_recorded_before_return_and_removed_idempotently(tmp_path) -> None:
    numpy = pytest.importorskip("numpy")
    descriptor, leases = encode_value(
        numpy.arange(4),
        lease_context=_lease_context(tmp_path),
    )
    name = descriptor[CODEC_MARKER]["name"]

    entry = load_shared_memory_lease_ledger(tmp_path)["leases"][name]
    assert entry["creator_pid"] == os.getpid()
    assert entry["environment_name"] == "example"
    assert entry["generation_id"] == "generation-1"
    assert entry["pool_id"] == "pool-1"
    assert entry["task_id"] == "task-1"
    assert entry["direction"] == "input"

    dispose_leases(leases, unlink=True)
    dispose_leases(leases, unlink=True)
    assert load_shared_memory_lease_ledger(tmp_path)["leases"] == {}


def test_segment_is_not_created_when_durable_lease_recording_fails(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    numpy = pytest.importorskip("numpy")
    constructor = MagicMock()
    monkeypatch.setattr(value_codec.shared_memory, "SharedMemory", constructor)
    monkeypatch.setattr(
        value_codec,
        "_record_lease",
        MagicMock(side_effect=OSError("ledger unavailable")),
    )

    with pytest.raises(OSError, match="ledger unavailable"):
        encode_value(
            numpy.arange(4),
            lease_context=_lease_context(tmp_path),
        )

    constructor.assert_not_called()


def test_creator_lease_is_durable_before_explicit_segment_creation(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    numpy = pytest.importorskip("numpy")
    original = shared_memory.SharedMemory
    creation: dict[str, object] = {}

    def inspect_ledger_before_creation(*args, **kwargs):
        name = kwargs["name"]
        creation.update(kwargs)
        assert name in load_shared_memory_lease_ledger(tmp_path)["leases"]
        return original(*args, **kwargs)

    monkeypatch.setattr(
        value_codec.shared_memory,
        "SharedMemory",
        inspect_ledger_before_creation,
    )
    descriptor, leases = encode_value(
        numpy.arange(4, dtype=numpy.int64),
        lease_context=_lease_context(tmp_path),
    )
    name = descriptor[CODEC_MARKER]["name"]
    try:
        assert creation == {
            "name": name,
            "create": True,
            "size": 4 * numpy.dtype(numpy.int64).itemsize,
        }
        assert name.startswith("wls_")
        assert len(name.removeprefix("wls_")) == 24
        int(name.removeprefix("wls_"), 16)
    finally:
        dispose_leases(leases, unlink=True)


def test_segment_creation_failure_removes_write_ahead_intent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    numpy = pytest.importorskip("numpy")
    attempted_names: list[str] = []

    def fail_after_inspecting_intent(*args, **kwargs):
        name = kwargs["name"]
        attempted_names.append(name)
        assert name in load_shared_memory_lease_ledger(tmp_path)["leases"]
        raise OSError("shared-memory creation failed")

    monkeypatch.setattr(
        value_codec.shared_memory,
        "SharedMemory",
        fail_after_inspecting_intent,
    )

    with pytest.raises(OSError, match="shared-memory creation failed"):
        encode_value(
            numpy.arange(4),
            lease_context=_lease_context(tmp_path),
        )

    assert len(attempted_names) == 1
    assert load_shared_memory_lease_ledger(tmp_path)["leases"] == {}


def test_reconciliation_removes_stale_intent_when_segment_never_existed(
    tmp_path,
) -> None:
    name = value_codec._new_shared_memory_name()
    value_codec._record_lease(
        name,
        _lease_context(tmp_path, started_at=0.0),
    )

    assert reconcile_shared_memory_leases(tmp_path) == (name,)
    assert load_shared_memory_lease_ledger(tmp_path)["leases"] == {}


def test_reconciliation_never_unlinks_a_live_creator(tmp_path) -> None:
    numpy = pytest.importorskip("numpy")
    descriptor, leases = encode_value(
        numpy.arange(4),
        lease_context=_lease_context(tmp_path, direction="output"),
    )
    name = descriptor[CODEC_MARKER]["name"]
    try:
        with patch.object(value_codec, "_open_for_unlink") as open_memory:
            assert reconcile_shared_memory_leases(tmp_path) == ()
        open_memory.assert_not_called()
        assert name in load_shared_memory_lease_ledger(tmp_path)["leases"]
    finally:
        dispose_leases(leases, unlink=True)


@pytest.mark.parametrize("direction", ["input", "output"])
def test_reconciliation_unlinks_host_or_worker_lease_after_creator_identity_dies(
    tmp_path,
    direction: str,
) -> None:
    numpy = pytest.importorskip("numpy")
    context = _lease_context(tmp_path, direction=direction, started_at=0.0)
    descriptor, leases = encode_value(numpy.arange(4), lease_context=context)
    name = descriptor[CODEC_MARKER]["name"]
    dispose_leases(leases, unlink=False)
    leases.clear()

    assert reconcile_shared_memory_leases(tmp_path) == (name,)
    assert load_shared_memory_lease_ledger(tmp_path)["leases"] == {}
    with pytest.raises(FileNotFoundError):
        value_codec._open_for_unlink(name)

    dispose_leases(leases, unlink=True)


def test_reconciliation_preserves_unknown_access_denied_creator(tmp_path) -> None:
    numpy = pytest.importorskip("numpy")
    import psutil

    descriptor, leases = encode_value(
        numpy.arange(4),
        lease_context=_lease_context(tmp_path),
    )
    name = descriptor[CODEC_MARKER]["name"]
    try:
        with (
            patch.object(psutil, "Process", side_effect=psutil.AccessDenied(os.getpid())),
            patch.object(value_codec, "_open_for_unlink") as open_memory,
        ):
            assert reconcile_shared_memory_leases(tmp_path) == ()
        open_memory.assert_not_called()
        assert name in load_shared_memory_lease_ledger(tmp_path)["leases"]
    finally:
        dispose_leases(leases, unlink=True)


def test_shared_memory_lease_cleanup_is_thread_safe_and_idempotent() -> None:
    memory = MagicMock()
    lease = value_codec.SharedMemoryLease("segment", memory, creator=True)
    threads = [threading.Thread(target=lease.dispose) for _ in range(8)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    memory.close.assert_called_once()
    memory.unlink.assert_called_once()
    assert lease.released


@pytest.mark.parametrize("supports_track", [False, True])
def test_recovery_open_respects_pre_and_post_python_313_tracking(
    supports_track: bool,
) -> None:
    fake_memory = MagicMock()
    constructor = MagicMock(return_value=fake_memory)
    parameters = [inspect.Parameter("name", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    if supports_track:
        parameters.append(inspect.Parameter("track", inspect.Parameter.KEYWORD_ONLY))

    with (
        patch.object(value_codec.shared_memory, "SharedMemory", constructor),
        patch.object(
            value_codec.inspect,
            "signature",
            return_value=inspect.Signature(parameters),
        ),
    ):
        assert value_codec._open_for_unlink("stale") is fake_memory

    expected = (
        {"name": "stale", "create": False, "track": False} if supports_track else {"name": "stale", "create": False}
    )
    assert constructor.call_args.kwargs == expected
