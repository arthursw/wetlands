from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from wetlands.exceptions import EnvironmentReuseError

try:
    __version__ = version("wetlands")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = ["EnvironmentReuseError", "__version__"]
