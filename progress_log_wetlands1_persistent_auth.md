# Wetlands 1 Persistent Auth Progress Log

## Initial Plan
- Add trusted-local persistent workers with `multiprocessing.connection` auth keys stored under `wetlands/state/auth.key`.
- Track persistent workers in `wetlands/state/workers.json` so later managers can attach.
- Preserve the existing arbitrary `execute()` and `run_script()` APIs.
- Keep default `launch()` behavior non-persistent.

## Iteration 1
- Implemented root-local runtime state helpers for private state directory creation, auth-key creation/reuse, locked registry updates, and atomic JSON writes.
- Added deterministic unit tests for auth-key reuse, registry secrecy, registry preservation, and narrow worker removal.

## Learned
- Current worker lifecycle assumes each worker has a `Popen` handle and unauthenticated client connection.
- Worker EOF is currently treated as an error, so persistent detach must change executor-side connection handling.
- `CommandExecutor.kill_process(None)` is safe, but health/liveness paths still need explicit attached-worker branches.

## Revised Plan
- Add tests for authenticated launch wiring, persistent registry recording, attach cleanup, detach semantics, and attached-worker liveness.
- Wire auth keys through `ExternalEnvironment._launch_worker()` and `module_executor.launch_listener()`.
- Extend `_Worker` to support attached workers with `pid`, no `Popen`, and no `ProcessLogger`.
- Add `EnvironmentManager.attach()` and detach APIs, then verify focused suites before broader tests.

## Iteration 2
- Added authenticated TCP wiring for workers using the root-local auth key and `Listener`/`Client` auth support.
- Added persistent worker registry writes on launch and cleanup on worker exit, failed attach, and dead-worker removal.
- Added `EnvironmentManager.attach(name)`, `ExternalEnvironment.detach()`, and `EnvironmentManager.detach()`.
- Updated persistent executor behavior so idle client disconnects return to `accept()` and unauthenticated clients are rejected without killing persistent workers.
- Added attached-worker support with PID-based liveness and cleanup when no local `Popen` handle exists.
- Added reconnect coverage with a real persistent worker process, detach, manager attach, and post-reconnect execution.
- Added a local import fallback for `Environment.import_module()` so module functions can still be discovered when local optional dependencies are missing but the remote environment has them.

## Verification
- `uv run pytest tests/test_runtime_state.py tests/test_external_environment.py tests/test_module_executor.py tests/test_environment_manager_lifecycle.py::TestPersistentAttach tests/test_environment.py tests/test_persistent_reconnect.py` passed with 90 tests.
- `uv run ruff check` passed.
- A full `uv run pytest` run before the import fallback reached 396 passing tests and two shared-memory integration failures because the local dev environment does not have `numpy` installed.
- After the fallback, the targeted shared-memory pixi test got past local function discovery and executed the remote function, but the run was stopped after hanging in the optional shared-memory path; `uv run python -c "import importlib.util; print(importlib.util.find_spec('numpy'))"` confirmed local `numpy` is absent.

## Next
- Run the final diff through a review agent.
- Address any review findings, then decide whether to merge the dedicated worktree branch back into the main worktree.

## Review Iteration
- A review agent found four lifecycle risks: persistent stdout/stderr dependence after startup, reconnect while an abandoned task thread was still running, unbounded attach when an existing client was still connected, and duplicate persistent launches overwriting live worker registry records.
- Updated persistent workers to print the listener port once for startup discovery, then remove console logging and redirect standard streams to `os.devnull`.
- Updated persistent disconnect handling to join active task threads before returning to `accept()`.
- Added a bounded authenticated connect path for attach and restored the previous socket default timeout afterward.
- Updated persistent launch to refuse live existing workers for the same environment and direct callers to attach or exit first.
- Added regression tests for these review findings plus active-task failure during detach.

## Review Verification
- `uv run pytest tests/test_runtime_state.py tests/test_external_environment.py tests/test_module_executor.py tests/test_environment_manager_lifecycle.py::TestPersistentAttach tests/test_environment.py tests/test_persistent_reconnect.py` passed with 94 tests.
- `uv run ruff check` passed.

## Second Review Iteration
- A second review found that `socket.setdefaulttimeout()` did not actually bound `multiprocessing.connection.Client()` because the stdlib socket client resets blocking mode.
- Replaced timeout-bound attach with a manual socket `Connection` plus the same multiprocessing challenge/response protocol guarded by `multiprocessing.connection.wait()` before each auth read.
- Updated attach cleanup so timeout means the worker may be busy and the registry entry is preserved; refused connections and auth/protocol failures still remove stale entries.
- Added an occupied-listener test that keeps one authenticated connection open and verifies a second attach attempt times out instead of blocking.
- Updated mocked attach tests to patch `_connect_worker()` rather than direct `Client()` calls.

## Second Review Verification
- `uv run pytest tests/test_runtime_state.py tests/test_external_environment.py tests/test_module_executor.py tests/test_environment_manager_lifecycle.py::TestPersistentAttach tests/test_environment.py tests/test_persistent_reconnect.py` passed with 96 tests.
- `uv run ruff check` passed.

## Final Review Fix
- A final review found that a timeout-bound attach can leave a closed TCP connection queued in the listener backlog; when the worker later accepts it, stdlib auth may raise `EOFError`.
- Updated persistent workers to treat `EOFError` from `listener.accept()` like an abandoned unauthenticated client and continue accepting.
- Added a regression test for abandoned auth connections in persistent listener mode.

## Final Verification
- `uv run pytest tests/test_runtime_state.py tests/test_external_environment.py tests/test_module_executor.py tests/test_environment_manager_lifecycle.py::TestPersistentAttach tests/test_environment.py tests/test_persistent_reconnect.py` passed with 97 tests.
- `uv run ruff check` passed.
