"""Immutable descriptions of environments owned by Wetlands."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from pathlib import Path


class ManagedEnvironmentState(enum.Enum):
    """The publication state of a discovered managed environment."""

    READY = "ready"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True)
class ManagedEnvironmentInfo:
    """A side-effect-free snapshot of one environment managed by a root."""

    name: str
    path: Path
    state: ManagedEnvironmentState
    generation_id: str | None = None
    recipe_hash: str | None = None
    pixi_version: str | None = None

    @property
    def ready(self) -> bool:
        """Return whether complete managed metadata has been published."""

        return self.state is ManagedEnvironmentState.READY


__all__ = ["ManagedEnvironmentInfo", "ManagedEnvironmentState"]
