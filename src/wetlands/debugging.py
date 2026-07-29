"""Public worker-discovery and debugger endpoint value types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class DebugEndpoint:
    """A debug adapter listening for an IDE connection."""

    worker_id: str
    adapter: Literal["debugpy"]
    host: str
    port: int


@dataclass(frozen=True)
class RunningWorker:
    """A live worker published by a Wetlands manager root."""

    id: str
    environment: str
    pool_id: str | None
    index: int
    process_id: int
    persistent: bool
    debugger: DebugEndpoint | None
