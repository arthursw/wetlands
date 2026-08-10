"""Managed environments and worker-pool public APIs."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

from wetlands._internal import runtime_state
from wetlands._internal.provisioning import _read_ready, environment_lifecycle_gate
from wetlands.external_environment import ExternalEnvironment, _validate_worker_environments
from wetlands.lifecycle import EnvironmentGenerationChangedError, ManagerCloseError, ManagerCloseTimeoutError
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION
from wetlands.task import ExecutionTask

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager
    from wetlands.managed_process import ManagedProcess, ManagedProcessResult


@dataclass
class _PoolCloseAttempt:
    done: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


@dataclass
class _ProcessCloseAttempt:
    done: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


class ManagedEnvironment:
    """A verified, ready Pixi environment managed by one manager root.

    Instances are returned by :meth:`EnvironmentManager.provision` or
    :meth:`EnvironmentManager.environment`; applications do not construct them directly.
    """

    def __init__(
        self,
        manager: EnvironmentManager,
        name: str,
        path: Path,
        metadata: dict[str, Any],
    ) -> None:
        self._manager = manager
        self._name = name
        self._path = path.resolve()
        self._metadata = dict(metadata)
        self._pools: list[WorkerPool] = []
        self._pool_close_attempts: dict[int, _PoolCloseAttempt] = {}
        self._processes: list[ManagedProcess] = []
        self._process_close_attempts: dict[int, _ProcessCloseAttempt] = {}
        self._lock = threading.RLock()

    @classmethod
    def _from_ready(
        cls,
        manager: EnvironmentManager,
        name: str,
        path: Path,
        metadata: dict[str, Any],
    ) -> ManagedEnvironment:
        return cls(manager, name, path, metadata)

    @property
    def name(self) -> str:
        """Return the managed environment name."""
        return self._name

    @property
    def path(self) -> Path:
        """Return the canonical Pixi project directory."""
        return self._path

    @property
    def pixi_manifest_path(self) -> Path:
        """Return the generated ``pixi.toml`` path."""
        return self._path / "pixi.toml"

    @property
    def pixi_lock_path(self) -> Path:
        """Return the resolved or supplied ``pixi.lock`` path."""
        return self._path / "pixi.lock"

    @property
    def pixi_version(self) -> str:
        """Return the Pixi version recorded when this generation was provisioned."""
        return str(self._metadata["pixi_version"])

    @property
    def pixi_executable_path(self) -> Path:
        """Return the Pixi executable used to provision this generation."""
        return Path(self._metadata["pixi_executable"])

    @property
    def generation_id(self) -> str:
        """Return the unique identifier for this published environment generation."""
        return str(self._metadata["generation_id"])

    @property
    def recipe_hash(self) -> str:
        """Return the hash of the complete normalized environment recipe."""
        return str(self._metadata["recipe_hash"])

    @property
    def lockfile_hash(self) -> str:
        """Return the SHA-256 hash of this generation's lockfile."""
        return str(self._metadata["lock_sha256"])

    def spawn(
        self,
        argv: Sequence[str],
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str | None] | None = None,
        output_limit: int = 1_048_576,
    ) -> ManagedProcess:
        """Launch an independently supervised command in this generation."""
        from wetlands.managed_process import ManagedProcess, _validate_launch_options

        options = _validate_launch_options(
            argv=argv,
            cwd=cwd,
            env=env,
            output_limit=output_limit,
            default_cwd=self.path,
        )
        with self._manager._manager_work():
            with environment_lifecycle_gate(self._manager, self.name):
                self._require_current_generation()
                runtime_state.reconcile_persistent_pool(
                    self._manager.root,
                    self.name,
                    grace=self._manager.termination_grace,
                )
                return ManagedProcess._launch_validated(
                    environment=self,
                    options=options,
                )

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str | None] | None = None,
        timeout: float | None = None,
        output_limit: int = 1_048_576,
        check: bool = True,
    ) -> ManagedProcessResult:
        """Run a command to completion and return its Wetlands-owned result."""
        from wetlands.managed_process import _validate_check, _validate_timeout

        normalized_timeout = _validate_timeout(timeout)
        normalized_check = _validate_check(check)
        process = self.spawn(argv, cwd=cwd, env=env, output_limit=output_limit)
        try:
            return process.wait(timeout=normalized_timeout, check=normalized_check)
        finally:
            process.close()

    def start(
        self,
        *,
        workers: int = 1,
        persistent: bool = False,
        worker_environment: Callable[[int], Mapping[str, str]] | None = None,
        worker_timeout: float | None = None,
    ) -> WorkerPool:
        """Start a new warm worker pool for this environment generation.

        Args:
            workers: Number of worker processes in the pool.
            persistent: Keep workers alive when the controller deliberately detaches.
            worker_environment: Optional callable receiving each zero-based worker
                index and returning environment variables for that worker. Wetlands
                snapshots the mappings before launch and reuses the mapping for the
                same index when replacing a worker. This cannot be combined with
                ``persistent=True``.
            worker_timeout: Optional worker inactivity timeout in seconds.
                Each IPC message resets the timer, so this is a health check rather
                than a maximum task execution time.
        """
        with self._manager._manager_work():
            if workers < 1:
                raise ValueError("workers must be at least one")
            if persistent and worker_environment is not None:
                raise ValueError("worker_environment cannot be combined with persistent=True")
            worker_environments = _validate_worker_environments(workers, worker_environment)
            snapshotted_worker_environment = worker_environments.__getitem__ if worker_environment is not None else None
            with self._lock:
                existing_pools = tuple(self._pools)
            for existing_pool in existing_pools:
                if not existing_pool._closed:
                    existing_pool._runtime._raise_if_failed()
            with environment_lifecycle_gate(self._manager, self.name):
                self._require_current_generation()
                runtime_state.reconcile_persistent_pool(
                    self._manager.root,
                    self.name,
                    grace=self._manager.termination_grace,
                )
                runtime = ExternalEnvironment(
                    self.name,
                    self.pixi_manifest_path,
                    self._manager,
                    expected_generation_id=self.generation_id,
                    expected_recipe_hash=self.recipe_hash,
                )
                pool = WorkerPool(self, runtime)
                with self._lock:
                    self._pools.append(pool)
                try:
                    runtime.launch(
                        max_workers=workers,
                        persistent=persistent,
                        worker_environment=snapshotted_worker_environment,
                        worker_timeout=worker_timeout,
                    )
                except BaseException:
                    if not runtime._workers:
                        with self._lock:
                            self._pools.remove(pool)
                        pool._closed = True
                    raise
            return pool

    def close_pools(self) -> None:
        """Close every worker pool started through this environment handle."""
        errors = self._close_pools()
        if errors:
            raise ManagerCloseError(errors)

    def _register_process(self, process: ManagedProcess) -> None:
        """Retain generation ownership until process cleanup is proven."""
        with self._lock:
            if not any(existing is process for existing in self._processes):
                self._processes.append(process)

    def _release_process(self, process: ManagedProcess) -> None:
        """Release a process after its supervisor proves the owned tree clean."""
        with self._lock:
            self._processes[:] = [existing for existing in self._processes if existing is not process]
            self._process_close_attempts.pop(id(process), None)

    def _close_pool(self, pool: WorkerPool, attempt: _PoolCloseAttempt) -> None:
        try:
            pool.close()
        except BaseException as error:
            attempt.error = error
        finally:
            attempt.done.set()

    def _close_pools(
        self,
        *,
        deadline: float | None = None,
        timeout: float | None = None,
    ) -> tuple[BaseException, ...]:
        attempts = self._start_pool_close_attempts()
        return self._collect_pool_close_attempts(attempts, deadline=deadline, timeout=timeout)

    def _start_pool_close_attempts(self) -> tuple[tuple[WorkerPool, _PoolCloseAttempt], ...]:
        with self._lock:
            attempts: list[tuple[WorkerPool, _PoolCloseAttempt]] = []
            for pool in self._pools:
                if pool._closed is True:
                    continue
                key = id(pool)
                attempt = self._pool_close_attempts.get(key)
                if attempt is None:
                    attempt = _PoolCloseAttempt()
                    self._pool_close_attempts[key] = attempt
                    threading.Thread(
                        target=self._close_pool,
                        args=(pool, attempt),
                        name=f"wetlands-pool-close-{self.name}",
                        daemon=True,
                    ).start()
                attempts.append((pool, attempt))
        return tuple(attempts)

    def _collect_pool_close_attempts(
        self,
        attempts: tuple[tuple[WorkerPool, _PoolCloseAttempt], ...],
        *,
        deadline: float | None,
        timeout: float | None,
    ) -> tuple[BaseException, ...]:
        errors: list[BaseException] = []
        for pool, attempt in attempts:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            if not attempt.done.wait(remaining):
                assert timeout is not None
                errors.append(ManagerCloseTimeoutError(f"worker pools for {self.name}", timeout))
                continue
            with self._lock:
                if self._pool_close_attempts.get(id(pool)) is attempt:
                    self._pool_close_attempts.pop(id(pool), None)
            if attempt.error is not None:
                errors.append(attempt.error)
        return tuple(errors)

    def _close_process(self, process: ManagedProcess, attempt: _ProcessCloseAttempt) -> None:
        try:
            process.close()
        except BaseException as error:
            attempt.error = error
        finally:
            attempt.done.set()

    def _start_process_close_attempts(self) -> tuple[tuple[ManagedProcess, _ProcessCloseAttempt], ...]:
        with self._lock:
            attempts: list[tuple[ManagedProcess, _ProcessCloseAttempt]] = []
            for process in self._processes:
                key = id(process)
                attempt = self._process_close_attempts.get(key)
                if attempt is None:
                    attempt = _ProcessCloseAttempt()
                    self._process_close_attempts[key] = attempt
                    threading.Thread(
                        target=self._close_process,
                        args=(process, attempt),
                        name=f"wetlands-process-close-{self.name}",
                        daemon=True,
                    ).start()
                attempts.append((process, attempt))
        return tuple(attempts)

    def _collect_process_close_attempts(
        self,
        attempts: tuple[tuple[ManagedProcess, _ProcessCloseAttempt], ...],
        *,
        deadline: float | None,
        timeout: float | None,
    ) -> tuple[BaseException, ...]:
        errors: list[BaseException] = []
        for process, attempt in attempts:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            if not attempt.done.wait(remaining):
                assert timeout is not None
                errors.append(ManagerCloseTimeoutError(f"managed processes for {self.name}", timeout))
                continue
            with self._lock:
                if self._process_close_attempts.get(id(process)) is attempt:
                    self._process_close_attempts.pop(id(process), None)
            if attempt.error is not None:
                errors.append(attempt.error)
        return tuple(errors)

    def _has_open_pools(self) -> bool:
        with self._lock:
            return any(not pool._closed for pool in self._pools)

    def _has_live_resources(self) -> bool:
        with self._lock:
            return bool(self._processes) or any(not pool._closed for pool in self._pools)

    def attach_pool(self, *, timeout: float = 5.0) -> WorkerPool:
        """Exclusively attach to this generation's detached persistent pool."""
        with self._manager._manager_work():
            if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("timeout must be a positive finite number")
            with environment_lifecycle_gate(self._manager, self.name):
                self._require_current_generation()
                runtime_state.reconcile_persistent_pool(
                    self._manager.root,
                    self.name,
                    grace=self._manager.termination_grace,
                )
                entries = runtime_state.live_workers_for_env(
                    self._manager.root,
                    self.name,
                    expected_identity={
                        "env_path": str(self.path),
                        "generation_id": self.generation_id,
                        "recipe_hash": self.recipe_hash,
                        "worker_runtime_version": WORKER_RUNTIME_VERSION,
                        "protocol_version": EXECUTION_PROTOCOL_VERSION,
                    },
                )
                if not entries:
                    raise RuntimeError(f"No detached persistent pool exists for environment {self.name!r}")
                runtime = ExternalEnvironment(
                    self.name,
                    self.pixi_manifest_path,
                    self._manager,
                    expected_generation_id=self.generation_id,
                    expected_recipe_hash=self.recipe_hash,
                )
                pool = WorkerPool(self, runtime)
                authkey = runtime_state.load_or_create_root_authkey(self._manager.root)
                runtime.attach_workers(entries, authkey, timeout=timeout)
            with self._lock:
                self._pools.append(pool)
            return pool

    def _require_current_generation(self) -> None:
        ready = _read_ready(self.path)
        actual_generation_id = str(ready.get("generation_id")) if ready is not None else None
        actual_recipe_hash = str(ready.get("recipe_hash")) if ready is not None else None
        if actual_generation_id != self.generation_id or actual_recipe_hash != self.recipe_hash:
            raise EnvironmentGenerationChangedError(
                self.name,
                expected_generation_id=self.generation_id,
                expected_recipe_hash=self.recipe_hash,
                actual_generation_id=actual_generation_id,
                actual_recipe_hash=actual_recipe_hash,
            )


class WorkerPool:
    """A group of warm worker processes for one managed environment generation."""

    def __init__(self, environment: ManagedEnvironment, runtime: ExternalEnvironment) -> None:
        self.environment = environment
        self._runtime = runtime
        self._closed = False

    @property
    def worker_count(self) -> int:
        """Return the number of workers in the pool."""
        return self._runtime.worker_count

    def submit_import(
        self,
        target: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        context_keyword: str | None = None,
    ) -> ExecutionTask[Any]:
        """Submit an installed ``module:qualified.callable`` target for execution."""
        self._ensure_open()
        return self._runtime.submit_import(
            target,
            args=args,
            kwargs=kwargs,
            context_keyword=context_keyword,
        )

    def submit_path(
        self,
        path: str | Path,
        qualname: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        cache: bool = True,
        context_keyword: str | None = None,
    ) -> ExecutionTask[Any]:
        """Submit a callable from an explicit local source path.

        Path execution is intended for local development; installed packages should use
        :meth:`submit_import`.
        """
        self._ensure_open()
        return self._runtime.submit_path(
            path,
            qualname,
            args=args,
            kwargs=kwargs,
            cache=cache,
            context_keyword=context_keyword,
        )

    def execute_import(
        self,
        target: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        timeout: float | None = None,
        context_keyword: str | None = None,
    ) -> Any:
        """Execute an installed target and block until it finishes."""
        return self.submit_import(
            target,
            args=args,
            kwargs=kwargs,
            context_keyword=context_keyword,
        ).wait_for(timeout)

    def execute_path(
        self,
        path: str | Path,
        qualname: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        cache: bool = True,
        timeout: float | None = None,
        context_keyword: str | None = None,
    ) -> Any:
        """Execute a local path target and block until it finishes."""
        return self.submit_path(
            path,
            qualname,
            args=args,
            kwargs=kwargs,
            cache=cache,
            context_keyword=context_keyword,
        ).wait_for(timeout)

    def detach(self) -> None:
        """Release control of a persistent pool without stopping its workers."""
        self._ensure_open()
        self._runtime.detach()
        self._closed = True

    def close(self) -> None:
        """Stop all workers using the manager's bounded termination grace.

        If cleanup raises, the pool remains open so the caller can retry.
        """
        if self._closed:
            return
        self._runtime._exit()
        self._closed = True

    def _ensure_open(self) -> None:
        self.environment._manager._ensure_open()
        if self._closed:
            raise RuntimeError("WorkerPool is closed")
        self._runtime._raise_if_failed()

    def __enter__(self) -> WorkerPool:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
