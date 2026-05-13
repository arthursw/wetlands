"""Shell command helpers for Wetlands-generated scripts."""

from __future__ import annotations

import platform
import shlex
from typing import Any


def shell_quote(value: Any) -> str:
    """Quote a single shell argument for the script shell used by CommandExecutor."""
    text = str(value)
    if platform.system() == "Windows":
        return "'" + text.replace("'", "''") + "'"
    return shlex.quote(text)
