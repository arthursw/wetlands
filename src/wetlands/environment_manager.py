"""Wetlands 2.0 Pixi environment manager."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from wetlands._internal.provisioning import _read_ready, prepare_pixi, provision_environment
from wetlands._internal.value_codec import reconcile_shared_memory_leases
from wetlands.managed_environment import ManagedEnvironment
from wetlands.operation import Operation, OperationCanceled, PreparationOperation, ProvisioningOperation
from wetlands.specs import EnvironmentSpec, PixiInfo, environment_name_key, validate_environment_name

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
        if termination_grace < 0:
            raise ValueError("termination_grace must be non-negative")
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
        self._closed = False

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
        self._ensure_open()
        operation: PreparationOperation[PixiInfo] = PreparationOperation()
        operation._start_runner(
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
        self._ensure_open()
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

        operation._start_runner(run, thread_name=f"wetlands-provision-{normalized_name}-{operation.id[:8]}")
        return operation

    def environment(self, name: str) -> ManagedEnvironment:
        self._ensure_open()
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

    def close(self) -> None:
        if self._closed:
            return
        with self._environment_lock:
            environments = tuple(self._environments.values())
        for environment in environments:
            environment.close_pools()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("EnvironmentManager is closed")

    def __enter__(self) -> EnvironmentManager:
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class EnvironmentNotReadyError(RuntimeError):
    pass
