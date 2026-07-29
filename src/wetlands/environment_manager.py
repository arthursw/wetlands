"""Wetlands 2.0 Pixi environment manager."""

from __future__ import annotations

import math
import threading
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterator

from wetlands._internal.provisioning import _read_ready, prepare_pixi, provision_environment
from wetlands._internal import management, runtime_state
from wetlands._internal.value_codec import reconcile_shared_memory_leases
from wetlands.debugging import DebugEndpoint, RunningWorker
from wetlands.lifecycle import ManagerCloseError
from wetlands.managed_environment import ManagedEnvironment
from wetlands.operation import (
    Operation,
    OperationCanceled,
    OperationEvent,
    PreparationOperation,
    ProvisioningOperation,
)
from wetlands.specs import EnvironmentSpec, PixiInfo, environment_name_key, validate_environment_name
from wetlands.protocol import EXECUTION_PROTOCOL_VERSION, WORKER_RUNTIME_VERSION

_NETWORK_KEYS = frozenset({"http", "https", "no_proxy"})


class EnvironmentManager:
    """Manage isolated Pixi environments without construction-time side effects."""

    def __init__(
        self,
        root: str | Path = Path("wetlands"),
        *,
        pixi_executable: str | Path | None = None,
        network: Mapping[str, str] | None = None,
        termination_grace: float = 5.0,
    ) -> None:
        if type(termination_grace) not in {int, float} or not math.isfinite(termination_grace) or termination_grace < 0:
            raise ValueError("termination_grace must be a finite non-negative number")
        self._root = Path(root).expanduser().resolve(strict=False)
        self._pixi_executable = (
            Path(pixi_executable).expanduser().resolve(strict=False) if pixi_executable is not None else None
        )
        normalized_network: dict[str, str] = {}
        for raw_key, raw_value in (network or {}).items():
            key = str(raw_key).lower()
            if key != "no_proxy" and key.endswith("_proxy"):
                key = key.removesuffix("_proxy")
            if key not in _NETWORK_KEYS:
                raise ValueError("network keys must be http, https, or no_proxy (optionally with a _proxy suffix)")
            normalized_network[key] = str(raw_value)
        self._network = MappingProxyType(normalized_network) if normalized_network else None
        self._termination_grace = float(termination_grace)
        self._environments_root = self.root / "environments"
        self._state_root = self.root / "state"

        self._prepare_condition = threading.Condition()
        self._preparing = False
        self._prepared: PixiInfo | None = None
        self._environment_lock = threading.RLock()
        self._environments: dict[str, ManagedEnvironment] = {}
        self._lifecycle_condition = threading.Condition(threading.RLock())
        self._close_lock = threading.Lock()
        self._active_operations: set[Operation[Any]] = set()
        self._active_manager_work = 0
        self._closed = False
        self._close_complete = False

    @property
    def root(self) -> Path:
        return self._root

    @property
    def wetlands_instance_path(self) -> Path:
        """Canonical root used by worker-runtime state."""

        return self._root

    @property
    def pixi_executable(self) -> Path | None:
        return self._pixi_executable

    @property
    def network(self) -> Mapping[str, str] | None:
        return self._network

    @property
    def termination_grace(self) -> float:
        return self._termination_grace

    @property
    def environments_root(self) -> Path:
        return self._environments_root

    @property
    def state_root(self) -> Path:
        return self._state_root

    def prepare(self) -> PreparationOperation[PixiInfo]:
        operation: PreparationOperation[PixiInfo] = PreparationOperation()
        self._start_operation(
            operation,
            lambda: self._prepare_sync(operation),
            thread_name=f"wetlands-prepare-{operation.id[:8]}",
        )
        return operation

    def _prepare_sync(self, operation: Operation[Any]) -> PixiInfo:
        reconcile_shared_memory_leases(self.root)
        with self._prepare_condition:
            while self._preparing:
                if operation.cancellation_requested:
                    raise OperationCanceled(operation.id)
                self._prepare_condition.wait(0.1)
            if self._prepared is not None:
                return self._prepared
            self._preparing = True
        try:
            pixi = prepare_pixi(self, operation)
        except BaseException:
            with self._prepare_condition:
                self._preparing = False
                self._prepare_condition.notify_all()
            raise
        with self._prepare_condition:
            self._prepared = pixi
            self._preparing = False
            self._prepare_condition.notify_all()
            return pixi

    def provision(
        self,
        name: str,
        spec: EnvironmentSpec,
        *,
        replace_existing: bool = False,
    ) -> ProvisioningOperation[ManagedEnvironment]:
        normalized_name = validate_environment_name(name)
        if not isinstance(spec, EnvironmentSpec):
            raise TypeError("spec must be an EnvironmentSpec")
        operation: ProvisioningOperation[ManagedEnvironment] = ProvisioningOperation(environment=normalized_name)

        def run() -> ManagedEnvironment:
            environment = provision_environment(self, operation, normalized_name, spec, replace_existing)
            key = environment_name_key(normalized_name)
            with self._environment_lock:
                existing = self._environments.get(key)
                if (
                    existing is not None
                    and existing.name == environment.name
                    and existing.generation_id == environment.generation_id
                    and existing.path == environment.path
                ):
                    return existing
                self._environments[key] = environment
            return environment

        self._start_operation(
            operation,
            run,
            thread_name=f"wetlands-provision-{normalized_name}-{operation.id[:8]}",
        )
        return operation

    def environment(self, name: str) -> ManagedEnvironment:
        with self._manager_work():
            normalized_name = validate_environment_name(name)
            key = environment_name_key(normalized_name)
            with self._environment_lock:
                existing = self._environments.get(key)
            if existing is not None:
                if existing.name != normalized_name:
                    raise EnvironmentNotReadyError(
                        f"Environment name {normalized_name!r} aliases managed name {existing.name!r}"
                    )
            target = self.environments_root / normalized_name
            metadata = _read_ready(target)
            if metadata is None:
                with self._environment_lock:
                    self._environments.pop(key, None)
                raise EnvironmentNotReadyError(f"Environment {normalized_name!r} is not ready")
            if existing is not None and existing.generation_id == metadata.get("generation_id"):
                return existing
            environment = ManagedEnvironment._from_ready(self, normalized_name, target, metadata)
            with self._environment_lock:
                self._environments[key] = environment
            return environment

    def _running_worker_entries(self, name: str) -> list[dict[str, Any]]:
        environment = self.environment(name)
        return runtime_state.live_workers_for_env(
            self.root,
            environment.name,
            expected_identity={
                "env_path": str(environment.path),
                "generation_id": environment.generation_id,
                "recipe_hash": environment.recipe_hash,
                "worker_runtime_version": WORKER_RUNTIME_VERSION,
                "protocol_version": EXECUTION_PROTOCOL_VERSION,
            },
            include_nonpersistent=True,
        )

    @staticmethod
    def _public_worker(entry: dict[str, Any]) -> RunningWorker:
        raw_debugger = entry.get("debugger")
        debugger = (
            None
            if not isinstance(raw_debugger, dict)
            else DebugEndpoint(
                worker_id=str(entry["worker_id"]),
                adapter="debugpy",
                host=str(raw_debugger["host"]),
                port=int(raw_debugger["port"]),
            )
        )
        return RunningWorker(
            id=str(entry["worker_id"]),
            environment=str(entry["env_name"]),
            pool_id=str(entry["pool_id"]) if entry.get("pool_id") is not None else None,
            index=int(entry["worker_index"]),
            process_id=int(entry["pid"]),
            persistent=bool(entry["persistent"]),
            debugger=debugger,
        )

    def running_workers(self, environment: str) -> tuple[RunningWorker, ...]:
        """Return live workers belonging to the environment's current generation."""
        with self._manager_work():
            normalized_name = validate_environment_name(environment)
            return tuple(self._public_worker(entry) for entry in self._running_worker_entries(normalized_name))

    def start_debugger(
        self,
        environment: str,
        *,
        worker: str | None = None,
    ) -> DebugEndpoint:
        """Lazily start debugpy in a live worker without claiming its task controller."""
        with self._manager_work():
            normalized_name = validate_environment_name(environment)
            entries = self._running_worker_entries(normalized_name)
            if worker is None:
                if not entries:
                    raise RuntimeError(f"Environment {normalized_name!r} has no running workers")
                if len(entries) != 1:
                    raise ValueError(
                        f"Environment {normalized_name!r} has {len(entries)} running workers; select one by ID"
                    )
                entry = entries[0]
            else:
                if not isinstance(worker, str) or not worker:
                    raise ValueError("worker must be a nonempty worker ID")
                matches = [entry for entry in entries if entry.get("worker_id") == worker]
                if not matches:
                    raise ValueError(f"Worker {worker!r} is not running in environment {normalized_name!r}")
                entry = matches[0]

            authkey = runtime_state.load_or_create_root_authkey(self.root)
            response = management.start_debugger(entry, authkey)
            adapter = response.get("adapter")
            host = response.get("host")
            port = response.get("port")
            if adapter != "debugpy" or host != "127.0.0.1" or type(port) is not int or not (0 < port <= 65535):
                raise management.ManagementConnectionError("Worker returned an invalid debugger endpoint")
            endpoint = DebugEndpoint(
                worker_id=str(entry["worker_id"]),
                adapter="debugpy",
                host=host,
                port=port,
            )
            runtime_state.record_debugger(
                self.root,
                worker_id=endpoint.worker_id,
                adapter=endpoint.adapter,
                host=endpoint.host,
                port=endpoint.port,
            )
            return endpoint

    def close(self) -> None:
        with self._close_lock:
            with self._lifecycle_condition:
                if self._close_complete:
                    return
                operations = tuple(self._active_operations)
                if any(operation._runs_on_current_thread() for operation in operations):
                    raise RuntimeError(
                        "EnvironmentManager.close() cannot run from an active operation listener; "
                        "schedule shutdown on another thread"
                    )
                self._closed = True

            errors: list[BaseException] = []
            for operation in operations:
                operation.cancel()
            for operation in operations:
                try:
                    operation.wait_for()
                except OperationCanceled:
                    pass
                except BaseException as error:
                    errors.append(error)

            with self._lifecycle_condition:
                while self._active_manager_work:
                    self._lifecycle_condition.wait()

            # Provisioning publishes its ManagedEnvironment before it reaches a
            # terminal state, so taking this snapshot after joining operations
            # cannot miss an environment that completed concurrently with close.
            with self._environment_lock:
                environments = tuple(self._environments.values())
            for environment in environments:
                errors.extend(environment._close_pools())

            with self._lifecycle_condition:
                remaining_pool = any(environment._has_open_pools() for environment in environments)
                self._close_complete = not remaining_pool
            if errors:
                raise ManagerCloseError(tuple(errors))

    def _ensure_open(self) -> None:
        with self._lifecycle_condition:
            self._ensure_open_locked()

    def _ensure_open_locked(self) -> None:
        if self._closed:
            raise RuntimeError("EnvironmentManager is closed")

    def _start_operation(
        self,
        operation: Operation[Any],
        runner: Callable[[], Any],
        *,
        thread_name: str,
    ) -> None:
        with self._lifecycle_condition:
            self._ensure_open_locked()
            self._active_operations.add(operation)

            def unregister(event: OperationEvent) -> None:
                if event.state.terminal:
                    operation.remove_listener(unregister)
                    self._unregister_operation(operation)

            operation.listen(unregister, replay=False)
            try:
                operation._start_runner(runner, thread_name=thread_name)
            except BaseException:
                operation.remove_listener(unregister)
                self._active_operations.discard(operation)
                self._lifecycle_condition.notify_all()
                raise

    def _unregister_operation(self, operation: Operation[Any]) -> None:
        with self._lifecycle_condition:
            self._active_operations.discard(operation)
            self._lifecycle_condition.notify_all()

    @contextmanager
    def _manager_work(self) -> Iterator[None]:
        """Keep shutdown from overtaking synchronous manager-owned work."""

        with self._lifecycle_condition:
            self._ensure_open_locked()
            self._active_manager_work += 1
        try:
            yield
        finally:
            with self._lifecycle_condition:
                self._active_manager_work -= 1
                self._lifecycle_condition.notify_all()

    def __enter__(self) -> EnvironmentManager:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class EnvironmentNotReadyError(RuntimeError):
    pass
