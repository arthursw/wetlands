from __future__ import annotations

from importlib.metadata import version

import wetlands


def test_package_exposes_installed_version():
    assert wetlands.__version__ == version("wetlands")
