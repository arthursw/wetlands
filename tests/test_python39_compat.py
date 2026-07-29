"""Python 3.9 compatibility regression tests for the Wetlands 2 runtime."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.compat

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _require_python39() -> str:
    if sys.version_info[:2] != (3, 9):
        pytest.skip("This compatibility suite runs in the Python 3.9 CI environment")
    return sys.executable


def test_python39_imports_v2_runtime_modules() -> None:
    python = _require_python39()
    code = """
import importlib
for name in (
    "wetlands",
    "wetlands.environment_manager",
    "wetlands.external_environment",
    "wetlands.managed_environment",
    "wetlands.module_executor",
    "wetlands.operation",
    "wetlands.protocol",
    "wetlands.specs",
    "wetlands.task",
    "wetlands._internal.provisioning",
    "wetlands._internal.value_codec",
):
    importlib.import_module(name)
"""
    result = subprocess.run(
        [python, "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_python39_core_codec_round_trip() -> None:
    _require_python39()
    from wetlands._internal.value_codec import decode_value, encode_value

    value = {"items": [None, True, 3, 4.5, "text", b"bytes", (1, 2)]}
    descriptor, leases = encode_value(value)

    assert leases == []
    assert decode_value(descriptor, copy_arrays=True) == value


def test_python39_compiles_worker_bootstrap() -> None:
    python = _require_python39()
    code = """
from pathlib import Path
for name in (
    "src/wetlands/module_executor.py",
    "src/wetlands/protocol.py",
    "src/wetlands/task.py",
    "src/wetlands/_internal/value_codec.py",
):
    path = Path(name)
    compile(path.read_bytes(), str(path), "exec")
"""
    result = subprocess.run(
        [python, "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
