"""Python 3.9 compatibility regression tests."""

from __future__ import annotations

import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from wetlands import module_executor

pytestmark = pytest.mark.compat

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


def test_execute_function_imports_user_module_with_pep604_annotations_on_python310_plus(tmp_path):
    if module_executor.sys.version_info < (3, 10):
        pytest.skip("PEP 604 annotation evaluation requires Python 3.10+")

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

    result = module_executor.execute_function({"module_path": str(module_path), "function": "has_value"})

    assert result is True


def test_python39_execute_function_reports_modern_annotation_error(tmp_path):
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

        module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "has_value",
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Python 3.10 union annotation syntax" in result.stderr
    assert "from __future__ import annotations" in result.stderr


def test_python39_execute_function_accepts_future_annotations(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_future_annotations_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            from __future__ import annotations

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


def test_python39_execute_function_reports_pipe_type_error_as_likely_modern_annotation_error(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_bad_pipe_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            value = 1 | "x"

            def get_value():
                return value
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "get_value",
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "unsupported operand type(s) for |" in result.stderr
    assert "with a `|` type error" in result.stderr
    assert "often caused by Python 3.10 union annotation syntax" in result.stderr


def test_python39_execute_function_reports_future_annotation_rhs_pipe_type_error_as_likely_modern_annotation_error(
    tmp_path,
):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_future_annotation_bad_rhs_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            from __future__ import annotations

            class Marker:
                pass

            value: Marker | None = 1 | "x"

            def get_value():
                return value
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "get_value",
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "unsupported operand type(s) for |" in result.stderr
    assert "with a `|` type error" in result.stderr
    assert "often caused by Python 3.10 union annotation syntax" in result.stderr


def test_python39_execute_function_reports_modern_annotation_error_in_function_signature(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_function_signature_annotation_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            def run(value: Marker | None = None):
                return value
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "run",
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Python 3.10 union annotation syntax" in result.stderr
    assert "from __future__ import annotations" in result.stderr


def test_python39_execute_function_reports_modern_annotation_error_in_multiline_signature(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    module_path = tmp_path / "user_multiline_signature_annotation_module.py"
    module_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            def run(
                value: Marker | None = None,
            ):
                return value
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        module_executor.execute_function({{
            "module_path": {str(module_path)!r},
            "function": "run",
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Python 3.10 union annotation syntax" in result.stderr
    assert "from __future__ import annotations" in result.stderr


def test_python39_execute_function_reports_nested_modern_annotation_error(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    tool_path = tmp_path / "tool_module.py"
    tool_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            value: Marker | None = Marker()
            """
        ),
        encoding="utf-8",
    )
    worker_path = tmp_path / "worker_module.py"
    worker_path.write_text(
        textwrap.dedent(
            f"""
            import importlib.util

            def run():
                spec = importlib.util.spec_from_file_location("tool_module", {str(tool_path)!r})
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod.value
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        module_executor.execute_function({{
            "module_path": {str(worker_path)!r},
            "function": "run",
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert str(worker_path) in result.stderr
    assert "Python 3.10 union annotation syntax" in result.stderr
    assert "from __future__ import annotations" in result.stderr


def test_python39_run_script_reports_modern_annotation_error(tmp_path):
    python39 = shutil.which("python3.9")
    if python39 is None:
        pytest.skip("python3.9 is not available")
    if shutil.which("uv") is None:
        pytest.skip("uv is not available")

    script_path = tmp_path / "user_pep604_script.py"
    script_path.write_text(
        textwrap.dedent(
            """
            class Marker:
                pass

            value: Marker | None = Marker()
            """
        ),
        encoding="utf-8",
    )
    code = textwrap.dedent(
        f"""
        from wetlands import module_executor

        module_executor.run_script({{
            "script_path": {str(script_path)!r},
        }})
        """
    )

    result = subprocess.run(
        ["uv", "run", "--python", "3.9", "python", "-c", code],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Python 3.10 union annotation syntax" in result.stderr
    assert "from __future__ import annotations" in result.stderr


def test_package_metadata_declares_python39_support():
    with (PROJECT_ROOT / "pyproject.toml").open("rb") as f:
        project = tomllib.load(f)["project"]

    assert project["requires-python"] == ">=3.9"
    assert "Programming Language :: Python :: 3.9" in project["classifiers"]
