from __future__ import annotations

from importlib.metadata import version

import wetlands
from wetlands.protocol import WORKER_RUNTIME_VERSION


def test_package_exposes_installed_version():
    assert wetlands.__version__ == version("wetlands")


def test_package_and_worker_runtime_versions_are_released_together():
    assert version("wetlands") == WORKER_RUNTIME_VERSION
