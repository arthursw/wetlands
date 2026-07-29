"""Public environment and worker-pool lifecycle errors."""

from __future__ import annotations

from pathlib import Path


class ManagerCloseError(RuntimeError):
    """One or more resources could not be cleaned up during manager shutdown."""

    def __init__(self, errors: tuple[BaseException, ...]) -> None:
        if not errors:
            raise ValueError("errors must not be empty")
        self.errors = errors
        details = "; ".join(f"{type(error).__name__}: {error}" for error in errors)
        super().__init__(f"EnvironmentManager cleanup did not complete: {details}")


class EnvironmentInUseError(RuntimeError):
    """A managed environment cannot be replaced while its workers are alive."""

    def __init__(self, environment: str, generation_id: str | None = None) -> None:
        self.environment = environment
        self.generation_id = generation_id
        generation = f" generation {generation_id!r}" if generation_id is not None else ""
        super().__init__(f"Environment {environment!r}{generation} is in use by a live worker pool")


class EnvironmentGenerationChangedError(RuntimeError):
    """A managed handle or pool no longer matches the ready environment."""

    def __init__(
        self,
        environment: str,
        *,
        expected_generation_id: str,
        expected_recipe_hash: str,
        actual_generation_id: str | None,
        actual_recipe_hash: str | None,
    ) -> None:
        self.environment = environment
        self.expected_generation_id = expected_generation_id
        self.expected_recipe_hash = expected_recipe_hash
        self.actual_generation_id = actual_generation_id
        self.actual_recipe_hash = actual_recipe_hash
        super().__init__(
            f"Environment {environment!r} changed generation or recipe "
            f"(expected generation {expected_generation_id!r}, recipe {expected_recipe_hash!r}; "
            f"found generation {actual_generation_id!r}, recipe {actual_recipe_hash!r})"
        )


class EnvironmentRecipeConflictError(RuntimeError):
    """A ready environment has a different recipe and replacement was not requested."""

    def __init__(
        self,
        environment: str,
        *,
        existing_recipe_hash: str,
        requested_recipe_hash: str,
    ) -> None:
        self.environment = environment
        self.existing_recipe_hash = existing_recipe_hash
        self.requested_recipe_hash = requested_recipe_hash
        super().__init__(
            f"Environment {environment!r} has recipe {existing_recipe_hash!r}, "
            f"not requested recipe {requested_recipe_hash!r}; "
            "pass replace_existing=True to rebuild it"
        )


class UnmanagedTargetError(RuntimeError):
    """An existing target is not proven to be owned by Wetlands."""

    def __init__(self, environment: str, path: str | Path) -> None:
        self.environment = environment
        self.path = Path(path)
        super().__init__(f"Environment target {str(self.path)!r} is unmanaged; Wetlands will not modify or remove it")


class WorkerStartError(RuntimeError):
    """A worker pool could not be launched or attached cleanly."""

    def __init__(
        self,
        environment: str,
        message: str,
        *,
        worker_index: int | None = None,
        phase: str = "launch",
        cleanup_errors: tuple[str, ...] = (),
    ) -> None:
        self.environment = environment
        self.worker_index = worker_index
        self.phase = phase
        self.cleanup_errors = cleanup_errors
        worker = f" worker {worker_index}" if worker_index is not None else " worker pool"
        cleanup = f"; cleanup also failed: {'; '.join(cleanup_errors)}" if cleanup_errors else ""
        super().__init__(f"Could not {phase}{worker} for environment {environment!r}: {message}{cleanup}")


__all__ = [
    "EnvironmentGenerationChangedError",
    "EnvironmentInUseError",
    "EnvironmentRecipeConflictError",
    "ManagerCloseError",
    "UnmanagedTargetError",
    "WorkerStartError",
]
