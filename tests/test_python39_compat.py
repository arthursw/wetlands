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


def test_python39_execute_function_imports_user_module_with_pep604_annotations(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_pep604_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            value: Marker | None = Marker()

            def has_value():
                return value is not None
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        result = module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "has_value",
        }})
        assert result is True
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_python39_execute_function_imports_nested_import_time_pep604_annotations(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_nested_pep604_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            if True:
                value: Marker | None = Marker()

            def has_value():
                return value is not None
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        result = module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "has_value",
        }})
        assert result is True
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_python39_execute_function_preserves_local_only_annotation_semantics(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_local_annotation_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            def has_runtime_annotations(value: int) -> int:
                local: Marker | None = None
                annotations = has_runtime_annotations.__annotations__
                return annotations["value"] is int and annotations["return"] is int
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        result = module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "has_runtime_annotations",
            "args": [1],
        }})
        assert result is True
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_python39_execute_function_preserves_skipped_branch_annotation_semantics(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_skipped_annotation_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            if False:
                value: Marker | None = None

            def has_runtime_annotations(value: int) -> int:
                annotations = has_runtime_annotations.__annotations__
                return annotations["value"] is int and annotations["return"] is int
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        result = module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "has_runtime_annotations",
            "args": [1],
        }})
        assert result is True
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_python39_execute_function_preserves_caught_annotation_type_error(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_caught_annotation_error_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            try:
                value: Marker | None = None
            except TypeError:
                value = "caught"

            def get_value():
                return value
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        result = module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "get_value",
        }})
        assert result == "caught"
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_python39_execute_function_serializes_pep604_module_import(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_slow_import_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            import time

            class Marker:
                pass

            value: Marker | None = Marker()
            time.sleep(0.2)

            def has_value():
                return value is not None
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from concurrent.futures import ThreadPoolExecutor
        from wetlands import module_executor

        message = {{
            "module_path": {str(module_path)!r},
            "function": "has_value",
        }}

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: module_executor.execute_function(message), range(2)))
        assert results == [True, True]
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_package_metadata_declares_python39_support():
    with (PROJECT_ROOT / "pyproject.toml").open("rb") as f:
        project = tomllib.load(f)["project"]

    assert project["requires-python"] == ">=3.9"
    assert "Programming Language :: Python :: 3.9" in project["classifiers"]
