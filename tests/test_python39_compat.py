"""Python 3.9 compatibility regression tests."""

from __future__ import annotations

import shutil
import subprocess
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.9/3.10
    import tomli as tomllib  # type: ignore[no-redef]

from wetlands import module_executor

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_python39_imports_runtime_core_modules():
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    modules = [
        "wetlands.environment",
        "wetlands.environment_manager",
        "wetlands.external_environment",
        "wetlands.internal_environment",
        "wetlands.main",
        "wetlands.module_executor",
        "wetlands.task",
        "wetlands._internal.command_executor",
        "wetlands._internal.dependency_manager",
        "wetlands._internal.settings_manager",
    ]
    code = "import importlib\nfor name in %r:\n    importlib.import_module(name)\n" % modules
    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_task_injection_uses_parameter_name_with_postponed_annotations(tmp_path):
    module_path = tmp_path / "user_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            from __future__ import annotations

            from wetlands.task import RemoteTaskHandle

            def accepts_task(value: int, task: RemoteTaskHandle | None = None) -> RemoteTaskHandle | None:
                return task
            """
        ),
        encoding="utf-8",
    )

    message = {
        "module_path": str(module_path),
        "function": "accepts_task",
        "args": [1],
        "kwargs": {},
        "task_id": "task-py39-annotations",
    }

    result = module_executor.execute_function(message, MagicMock(), MagicMock())

    assert module_executor.RemoteTaskHandle is not None
    assert isinstance(result, module_executor.RemoteTaskHandle)
    assert result._task_id == "task-py39-annotations"


def test_package_metadata_declares_python39_support():
    with (PROJECT_ROOT / "pyproject.toml").open("rb") as f:
        project = tomllib.load(f)["project"]

    assert project["requires-python"] == ">=3.9"
    assert "Programming Language :: Python :: 3.9" in project["classifiers"]
