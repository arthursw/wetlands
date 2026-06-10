# Python 3.9 Compatibility Progress Log

## Initial Plan
- Add deterministic tests for Python 3.9 import compatibility, task-handle injection with postponed annotations, and package metadata.
- Fix Python 3.9 runtime incompatibilities without changing public behavior.
- Lower package metadata to Python 3.9, update CI, refresh `uv.lock`, then verify in the feature worktree and after merge.

## Iteration 1

### Planned
- Add failing compatibility coverage before runtime edits.
- Keep the implementation focused on import/runtime compatibility and metadata.

### Implemented
- Added this progress log.
- Added Python 3.9 compatibility regression tests.
- Added postponed annotations to runtime modules that used evaluated PEP 604 annotations.
- Replaced `TaskEvent`'s Python 3.10-only dataclass slots option with a Python 3.9-compatible frozen dataclass.
- Added compatibility for Python 3.9's older `multiprocessing.connection` auth challenge helpers in persistent worker attach.
- Lowered package metadata and added Python 3.9 to CI.

### Learned
- `wetlands.task` already uses postponed annotations, but `TaskEvent` uses `@dataclass(..., slots=True)`, which is unsupported on Python 3.9.
- Several runtime modules use PEP 604 union annotations without `from __future__ import annotations`, which raises during Python 3.9 imports.
- The Python 3.9 suite also exercises `module_executor.py` as a Python 3.9 subprocess; persistent attach needed to handle Python 3.9's MD5-only multiprocessing auth protocol.
- `uv lock` resolved successfully after lowering `requires-python`, but the existing lockfile already represented the resulting graph and did not have a tracked diff.

### Plan Changes
- Add postponed annotations to every runtime module that currently evaluates PEP 604 annotations on import.
- Use plain `@dataclass(frozen=True)` for `TaskEvent`.

### Next
- Move the feature worktree to Trash.

### Verification
- `python3.9 -m compileall -q src/wetlands`: passed.
- `uv run --python 3.9 pytest tests --ignore=tests/test_wetlands.py --ignore=tests/test_installer.py`: 373 passed.
- `uv run --python 3.13 pytest tests --ignore=tests/test_wetlands.py --ignore=tests/test_installer.py`: 373 passed.
- `uv run ruff check`: passed.

## Post-Merge

### Implemented
- Merged `python39-compat` into `main`.
- Review agent reported no findings.

### Verification
- `python3.9 -m compileall -q src/wetlands`: passed.
- `uv run --python 3.9 pytest tests --ignore=tests/test_wetlands.py --ignore=tests/test_installer.py`: 373 passed.
- `uv run pytest tests --ignore=tests/test_wetlands.py --ignore=tests/test_installer.py`: 373 passed under Python 3.9 because `uv` reused the active environment.
- `uv run --python 3.13 pytest tests --ignore=tests/test_wetlands.py --ignore=tests/test_installer.py`: 373 passed.
- `uv run ruff check`: passed.

## Follow-Up: Python 3.9 User Module Annotations

### Planned
- Fix Python 3.9 worker imports for user modules that use PEP 604 annotations without `from __future__ import annotations`.
- Preserve normal import semantics where Python 3.9 already imports the module successfully.

### Implemented
- Replaced the source-transforming Python 3.9 import fallback with a targeted compatibility error for user modules or scripts that use Python 3.10 union annotations in Python 3.9 environments.
- Limited the targeted compatibility error to traceback lines whose AST node is actually an annotation using PEP 604 syntax, so ordinary invalid `|` expressions keep their original `TypeError`.
- Covered multi-line function signatures and modules that already use postponed annotations so the diagnostic avoids common false negatives and false positives.
- Kept normal import and script execution semantics for user code; Wetlands no longer compiles user modules with postponed annotations on their behalf.
- Updated the shared-memory integration test so Python 3.9 environments assert the compatibility error via direct remote execution and Python 3.10+ environments keep the existing NDArray roundtrip behavior.
- Added regression tests that Python 3.9 reports the compatibility error for modern annotations, preserves non-annotation `|` errors, accepts `from __future__ import annotations`, handles multi-line signatures, and that Python 3.10+ imports modern annotations normally.

### Learned
- The prior compatibility pass fixed Wetlands package imports, but user-provided modules executed inside Python 3.9 workers can also contain PEP 604 annotations.
- The full integration failure was triggered by `shared_memory_module.py` declaring `ndarray: NDArray | None = None` in a Python 3.9 worker.
- Transforming user modules to postpone annotations preserved the reported integration path but added too much semantic complexity around skipped branches, caught annotation errors, and import concurrency.
- The final policy is simpler: user code must be valid for the target environment Python version, or it must opt into postponed annotations itself.

### Verification
- `python3.9 -m compileall -q src/wetlands`: passed.
- `uv run --python 3.9 pytest tests/test_python39_compat.py tests/test_module_executor.py tests/test_task.py`: 92 passed, 1 skipped.
- `uv run --python 3.13 pytest tests/test_python39_compat.py tests/test_module_executor.py tests/test_task.py`: 93 passed.
- `uv run ruff check`: passed.
- Full integration tests, including `tests/test_wetlands.py::test_shared_memory_ndarray`, were not run in this pass by request.
