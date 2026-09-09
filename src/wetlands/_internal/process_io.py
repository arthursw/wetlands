"""Ownership handoff between subprocess launchers and their output readers."""

from __future__ import annotations

import threading
import subprocess
from typing import Any


def start_process_reaper(process: subprocess.Popen, *, name: str) -> threading.Thread:
    """Retain and wait for a deliberately detached child without blocking exit.

    The caller retains ownership if starting the waiter raises.
    """
    reaper = threading.Thread(target=process.wait, name=name, daemon=True)
    reaper.start()
    return reaper


class PipeOwnership:
    """Keep unclaimed pipes separate from pipes owned by running readers.

    Cleanup closes only unclaimed pipes. A reader claims its pipe before reading
    and closes it in its own finally block. This also handles Thread.start()
    raising after the operating-system thread has already started.
    """

    def __init__(self, **streams: Any) -> None:
        self._lock = threading.Lock()
        self._unclaimed = {name: stream for name, stream in streams.items() if stream is not None}
        self._closing = False

    def claim(self, name: str) -> Any:
        with self._lock:
            if self._closing:
                return None
            return self._unclaimed.pop(name, None)

    def close_claimed(self, name: str, stream: Any) -> None:
        try:
            stream.close()
        except BaseException:
            # Reading has ended, so subsequent owner cleanup may retry safely.
            with self._lock:
                self._unclaimed[name] = stream
            raise

    def close_unclaimed(self) -> tuple[BaseException, ...]:
        errors: list[BaseException] = []
        with self._lock:
            self._closing = True
            for name, stream in tuple(self._unclaimed.items()):
                try:
                    stream.close()
                except BaseException as error:
                    errors.append(error)
                else:
                    del self._unclaimed[name]
        return tuple(errors)
