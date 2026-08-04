"""Discovery and removal of managed environment targets."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from wetlands._internal import runtime_state
from wetlands._internal.provisioning import (
    _discard_matching_journals,
    _find_name_alias,
    _is_link_or_reparse,
    _path_identity,
    _read_ready,
    _target_has_valid_owner_marker,
    environment_lifecycle_gate,
)
from wetlands.environment_info import ManagedEnvironmentInfo, ManagedEnvironmentState
from wetlands.lifecycle import (
    EnvironmentInUseError,
    EnvironmentNotFoundError,
    UnmanagedTargetError,
)
from wetlands.operation import (
    OperationCanceled,
    OperationEventKind,
    OperationFailure,
    RemovalError,
    RemovalOperation,
)
from wetlands.specs import environment_name_key, validate_environment_name

if TYPE_CHECKING:
    from wetlands.environment_manager import EnvironmentManager

_INSPECTION_STAGE = "target_inspection"
_REMOVAL_STAGE = "environment_removal"
_CLEANUP_STAGE = "cleanup"


def _seal_removal(operation: RemovalOperation[Any]) -> None:
    if not operation._seal_cancellation():
        raise OperationCanceled(operation.id)


def _info_from_target(name: str, target: Path, ready: dict[str, Any] | None) -> ManagedEnvironmentInfo:
    if ready is None:
        return ManagedEnvironmentInfo(
            name=name,
            path=target.resolve(strict=False),
            state=ManagedEnvironmentState.INCOMPLETE,
        )
    return ManagedEnvironmentInfo(
        name=name,
        path=target.resolve(strict=False),
        state=ManagedEnvironmentState.READY,
        generation_id=str(ready["generation_id"]),
        recipe_hash=str(ready["recipe_hash"]),
        pixi_version=str(ready["pixi_version"]),
    )


def discover_managed_environments(manager: EnvironmentManager) -> tuple[ManagedEnvironmentInfo, ...]:
    """Return snapshots of direct targets whose Wetlands ownership is proven."""

    root = manager.environments_root
    try:
        if not os.path.lexists(root):
            return ()
        if _is_link_or_reparse(root) or not root.is_dir():
            raise RuntimeError(f"Managed environment root is linked or not a directory: {root}")
        entries = tuple(root.iterdir())
    except OSError as error:
        raise RuntimeError(f"Cannot inspect managed environment root {root}") from error

    discovered: list[ManagedEnvironmentInfo] = []
    names_by_key: dict[str, str] = {}
    for target in entries:
        try:
            name = validate_environment_name(target.name)
        except ValueError:
            continue
        if not _target_has_valid_owner_marker(root, target):
            continue
        key = environment_name_key(name)
        existing = names_by_key.get(key)
        if existing is not None and existing != name:
            raise RuntimeError(f"Managed environment root contains ambiguous portable names {existing!r} and {name!r}")
        names_by_key[key] = name
        discovered.append(_info_from_target(name, target, _read_ready(target)))
    discovered.sort(key=lambda item: (environment_name_key(item.name), item.name))
    return tuple(discovered)


def _cached_environment_in_use(manager: EnvironmentManager, name: str) -> str | None:
    key = environment_name_key(name)
    with manager._environment_lock:
        environment = manager._environments.get(key)
    if environment is None or environment.name != name or not environment._has_open_pools():
        return None
    return environment.generation_id


def remove_managed_environment(
    manager: EnvironmentManager,
    operation: RemovalOperation[ManagedEnvironmentInfo],
    name: str,
) -> ManagedEnvironmentInfo:
    """Remove one proven managed target while holding its cross-process gate."""

    stage = _INSPECTION_STAGE
    with environment_lifecycle_gate(
        manager,
        name,
        operation=operation,
        error_type=RemovalError,
    ):
        try:
            if operation.cancellation_requested:
                raise OperationCanceled(operation.id)
            operation._emit(
                OperationEventKind.STEP,
                f"Inspecting managed environment {name!r}",
                stage=_INSPECTION_STAGE,
                environment=name,
            )
            alias = _find_name_alias(manager.environments_root, name)
            target = manager.environments_root / name
            if alias is not None:
                raise EnvironmentNotFoundError(name, alias=alias)
            if not os.path.lexists(target):
                raise EnvironmentNotFoundError(name)
            if not _target_has_valid_owner_marker(manager.environments_root, target):
                raise UnmanagedTargetError(name, target)

            ready = _read_ready(target)
            info = _info_from_target(name, target, ready)
            cached_generation = _cached_environment_in_use(manager, name)
            if cached_generation is not None:
                raise EnvironmentInUseError(name, cached_generation)

            runtime_state.reconcile_persistent_pool(
                manager.root,
                name,
                grace=manager.termination_grace,
            )
            live_workers = runtime_state.live_workers_for_env(
                manager.root,
                name,
                include_nonpersistent=True,
            )
            if live_workers:
                generation_id = info.generation_id or str(live_workers[0].get("generation_id") or "") or None
                raise EnvironmentInUseError(name, generation_id)

            stage = _CLEANUP_STAGE
            operation._emit(
                OperationEventKind.CLEANUP,
                f"Cleaning runtime state for environment {name!r}",
                stage=_CLEANUP_STAGE,
                environment=name,
            )
            runtime_state.remove_workers_for_env(manager.root, name)
            _discard_matching_journals(manager, target)
            stage = _REMOVAL_STAGE
            operation._emit(
                OperationEventKind.CLEANUP,
                f"Detaching managed environment {name!r}",
                stage=_REMOVAL_STAGE,
                environment=name,
            )
            target_identity = _path_identity(target)
            try:
                manager._quarantine_environment(
                    target,
                    operation_id=operation.id,
                    expected_identity=target_identity,
                    before_commit=lambda: _seal_removal(operation),
                    lock_operation=operation,
                )
            except OperationCanceled:
                raise
            except BaseException as error:
                raise RemovalError(
                    OperationFailure(
                        operation_id=operation.id,
                        stage=stage,
                        environment=name,
                        message=f"Could not remove managed environment {name!r}: {error}",
                    )
                ) from error

            key = environment_name_key(name)
            with manager._environment_lock:
                manager._environment_epochs[key] = manager._environment_epochs.get(key, 0) + 1
                manager._environments.pop(key, None)
            operation._emit(
                OperationEventKind.CLEANUP,
                f"Queued removed environment {name!r} for background disk reclamation",
                stage=_REMOVAL_STAGE,
                environment=name,
            )
            return info
        except (OperationCanceled, RemovalError, EnvironmentInUseError, EnvironmentNotFoundError, UnmanagedTargetError):
            raise
        except BaseException as error:
            raise RemovalError(
                OperationFailure(
                    operation_id=operation.id,
                    stage=stage,
                    environment=name,
                    message=f"Could not remove managed environment {name!r}: {error}",
                    cleanup_error=(str(error) or type(error).__name__) if stage == _CLEANUP_STAGE else None,
                )
            ) from error


__all__ = ["discover_managed_environments", "remove_managed_environment"]
