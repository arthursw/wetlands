from __future__ import annotations

import inspect
import os
import sys
import time
from multiprocessing import shared_memory
from pathlib import Path

import pytest

from wetlands import EnvironmentManager, EnvironmentSpec, OperationCanceled
from wetlands._internal.value_codec import load_shared_memory_lease_ledger


def _fake_pixi(tmp_path: Path, *, numpy_site: Path | None = None) -> Path:
    implementation = tmp_path / "fake_pixi.py"
    implementation.write_text(
        f"""
from __future__ import annotations

import pathlib
import shlex
import subprocess
import sys

numpy_site = {str(numpy_site) if numpy_site is not None else None!r}
arguments = sys.argv[1:]
if arguments == ["--version"]:
    print("pixi 0.48.2")
elif arguments and arguments[0] == "install":
    manifest = pathlib.Path(arguments[arguments.index("--manifest-path") + 1])
    (manifest.parent / "pixi.lock").write_text("version: 6\\n", encoding="utf-8")
    if numpy_site is not None and sys.platform != "win32":
        python = manifest.parent / ".pixi" / "envs" / "default" / "bin" / "python"
        python.parent.mkdir(parents=True)
        python.write_text(
            "#!/bin/sh\\nPYTHONPATH="
            + shlex.quote(numpy_site)
            + " exec "
            + shlex.quote(sys.executable)
            + ' "$@"\\n',
            encoding="utf-8",
        )
        python.chmod(0o755)
elif arguments and arguments[0] == "run":
    manifest_index = arguments.index("--manifest-path")
    command = arguments[manifest_index + 2:]
    if (
        len(command) == 3
        and command[:2] == ["python", "-c"]
        and "importlib.metadata.version('debugpy')" in command[2]
    ):
        print(sys.executable)
        raise SystemExit(0)
    if command and command[0] == "python":
        command[0] = sys.executable
    raise SystemExit(subprocess.run(command, check=False).returncode)
else:
    raise SystemExit("unexpected fake Pixi arguments: " + repr(arguments))
""".lstrip(),
        encoding="utf-8",
    )
    if os.name == "nt":
        executable = tmp_path / "pixi.cmd"
        executable.write_text(
            f'@"{sys.executable}" "{implementation}" %*\n',
            encoding="utf-8",
        )
    else:
        executable = tmp_path / "pixi"
        executable.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" "{implementation}" "$@"\n',
            encoding="utf-8",
        )
        executable.chmod(0o755)
    return executable


def _worker_module(tmp_path: Path) -> Path:
    module = tmp_path / "transport_worker.py"
    module.write_text(
        """
from __future__ import annotations

import os
import sys
import time


def mutate_and_return(image):
    original = int(image[0, 0])
    image[...] = -100
    return {"mask": image[:, ::-1], "original": original}


def wait_until_canceled(image, *, context):
    context.update("worker-started")
    while not context.cancel_requested:
        time.sleep(0.01)
    return image


def exit_process(image):
    os._exit(23)


def runtime_info():
    import sys
    try:
        import numpy
    except ImportError:
        numpy_version = None
    else:
        numpy_version = numpy.__version__
    return {"executable": sys.executable, "path": sys.path, "numpy": numpy_version}
""".lstrip(),
        encoding="utf-8",
    )
    return module


def _assert_segments_unlinked(names: list[str]) -> None:
    supports_track = "track" in inspect.signature(shared_memory.SharedMemory).parameters
    for name in names:
        arguments = {"name": name, "create": False}
        if supports_track:
            arguments["track"] = False
        with pytest.raises(FileNotFoundError):
            shared_memory.SharedMemory(**arguments)


def _descriptor_segment_names(descriptor) -> list[str]:
    names: list[str] = []
    if isinstance(descriptor, dict):
        payload = descriptor.get("__wetlands_codec__")
        if isinstance(payload, dict) and isinstance(payload.get("name"), str):
            names.append(payload["name"])
        for value in descriptor.values():
            names.extend(_descriptor_segment_names(value))
    elif isinstance(descriptor, (list, tuple)):
        for value in descriptor:
            names.extend(_descriptor_segment_names(value))
    return names


def _wait_for_message(task, message: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while task.message != message and time.monotonic() < deadline:
        time.sleep(0.01)
    assert task.message == message


@pytest.fixture
def transport_environment(tmp_path: Path):
    manager = EnvironmentManager(tmp_path / "state", pixi_executable=_fake_pixi(tmp_path))
    environment = manager.provision("transport", EnvironmentSpec(python="3.11")).wait_for()
    try:
        yield environment, _worker_module(tmp_path)
    finally:
        manager.close()


@pytest.fixture
def numpy_transport_environment(tmp_path: Path):
    if os.name == "nt":
        pytest.skip("The lightweight fake-Pixi NumPy worker wrapper is POSIX-only")
    numpy = pytest.importorskip("numpy")
    numpy_site = Path(numpy.__file__).resolve().parent.parent
    manager = EnvironmentManager(
        tmp_path / "state",
        pixi_executable=_fake_pixi(tmp_path, numpy_site=numpy_site),
    )
    environment = manager.provision("transport", EnvironmentSpec(python="3.11")).wait_for()
    try:
        yield environment, _worker_module(tmp_path)
    finally:
        manager.close()


@pytest.mark.integration
@pytest.mark.agent_integration
def test_scalar_task_runs_on_worker_without_numpy(transport_environment) -> None:
    environment, module = transport_environment

    with environment.start() as pool:
        result = pool.execute_path(module, "runtime_info", timeout=10)
        capabilities = pool._runtime._workers[0].capabilities

    assert result["numpy"] is None
    assert {(codec.id, codec.version) for codec in capabilities.codecs} == {("wetlands.core", 1)}


@pytest.mark.integration
@pytest.mark.agent_integration
def test_numpy_task_fails_before_dispatch_to_worker_without_numpy(transport_environment) -> None:
    numpy = pytest.importorskip("numpy")
    environment, module = transport_environment

    with environment.start() as pool:
        task = pool.submit_path(module, "mutate_and_return", args=(numpy.arange(3),))
        input_names = _descriptor_segment_names(task._payload["args"])
        with pytest.raises(Exception, match="missing required task codecs"):
            task.wait_for(timeout=10)
        assert pool.execute_path(module, "runtime_info", timeout=10)["numpy"] is None

    assert task._input_leases is None
    _assert_segments_unlinked(input_names)


@pytest.mark.integration
@pytest.mark.agent_integration
def test_worker_pool_copies_numpy_inputs_and_acknowledges_output_release(numpy_transport_environment) -> None:
    numpy = pytest.importorskip("numpy")
    environment, module = numpy_transport_environment
    source = numpy.arange(12, dtype=numpy.int32).reshape(3, 4)[:, ::2]
    expected = source.copy()

    with environment.start() as pool:
        task = pool.submit_path(module, "mutate_and_return", args=(source,))
        input_names = [lease.name for lease in task._input_leases]
        result = task.wait_for(timeout=10)
        output_names = list(task._offered_names)

    assert result["original"] == 0
    numpy.testing.assert_array_equal(source, expected)
    numpy.testing.assert_array_equal(result["mask"], numpy.full(source.shape, -100)[:, ::-1])
    assert result["mask"].flags.c_contiguous
    assert result["mask"].flags.owndata
    assert task._input_leases is None
    assert output_names
    assert load_shared_memory_lease_ledger(environment._manager.root)["leases"] == {}
    _assert_segments_unlinked(input_names + output_names)


@pytest.mark.integration
def test_worker_pool_cancellation_unlinks_input_segments_after_cleanup(numpy_transport_environment) -> None:
    numpy = pytest.importorskip("numpy")
    environment, module = numpy_transport_environment

    with environment.start() as pool:
        task = pool.submit_path(
            module,
            "wait_until_canceled",
            args=(numpy.arange(8),),
            context_keyword="context",
        )
        input_names = [lease.name for lease in task._input_leases]
        _wait_for_message(task, "worker-started")
        assert task.cancel()
        with pytest.raises(OperationCanceled):
            task.wait_for(timeout=10)

    assert task._input_leases is None
    assert load_shared_memory_lease_ledger(environment._manager.root)["leases"] == {}
    _assert_segments_unlinked(input_names)


@pytest.mark.integration
def test_worker_death_unlinks_host_owned_input_segments(numpy_transport_environment) -> None:
    numpy = pytest.importorskip("numpy")
    environment, module = numpy_transport_environment

    with environment.start() as pool:
        task = pool.submit_path(module, "exit_process", args=(numpy.arange(8),))
        input_names = [lease.name for lease in task._input_leases]
        with pytest.raises(Exception, match="Worker"):
            task.wait_for(timeout=10)

    assert task._input_leases is None
    assert load_shared_memory_lease_ledger(environment._manager.root)["leases"] == {}
    _assert_segments_unlinked(input_names)
