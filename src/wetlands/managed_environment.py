"""Managed environments and worker-pool public APIs."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, TYPE_CHECKING

from wetlands._internal import runtime_state
from wetlands._internal.provisioning import _read_ready, environment_lifecycle_gate
from wetlands.external_environment import ExternalEnvironment
from wetlands.lifecycle import EnvironmentGenerationChangedError
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION
from wetlands.task import ExecutionTask

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager


class ManagedEnvironment:
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
        return self._name

    @property
    def path(self) -> Path:
        return self._path

    @property
    def pixi_manifest_path(self) -> Path:
        return self._path / "pixi.toml"

    @property
    def pixi_lock_path(self) -> Path:
        return self._path / "pixi.lock"

    @property
    def pixi_version(self) -> str:
        return str(self._metadata["pixi_version"])

    @property
    def pixi_executable_path(self) -> Path:
        return Path(self._metadata["pixi_executable"])

    @property
    def generation_id(self) -> str:
        return str(self._metadata["generation_id"])

    @property
    def recipe_hash(self) -> str:
        return str(self._metadata["recipe_hash"])

    @property
    def lockfile_hash(self) -> str:
        return str(self._metadata["lock_sha256"])

    def start(
        self,
        *,
        workers: int = 1,
        persistent: bool = False,
        worker_timeout: float | None = None,
    ) -> WorkerPool:
        self._manager._ensure_open()
        if workers < 1:
            raise ValueError("workers must be at least one")
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
        with self._lock:
            pools = tuple(self._pools)
        for pool in pools:
            pool.close()

    def attach_pool(self, *, timeout: float = 5.0) -> WorkerPool:
        """Exclusively attach to this generation's detached persistent pool."""
        self._manager._ensure_open()
        if timeout <= 0:
            raise ValueError("timeout must be positive")
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
    def __init__(self, environment: ManagedEnvironment, runtime: ExternalEnvironment) -> None:
        self.environment = environment
        self._runtime = runtime
        self._closed = False

    @property
    def worker_count(self) -> int:
        return self._runtime.worker_count

    def submit_import(
        self,
        target: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        context_keyword: str | None = None,
    ) -> ExecutionTask[Any]:
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
        return self.submit_path(
            path,
            qualname,
            args=args,
            kwargs=kwargs,
            cache=cache,
            context_keyword=context_keyword,
        ).wait_for(timeout)

    def detach(self) -> None:
        self._ensure_open()
        self._runtime.detach()
        self._closed = True

    def close(self) -> None:
        if self._closed:
            return
        self._runtime._exit()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("WorkerPool is closed")
        self._runtime._raise_if_failed()

    def __enter__(self) -> WorkerPool:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
