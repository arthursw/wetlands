"""Windows kill-on-close Job Objects shared by subprocess owners."""

from __future__ import annotations

import subprocess
from typing import Any


def create_windows_kill_job(process: subprocess.Popen) -> Any:
    """Create and assign a Job, closing its handle if setup fails."""
    import ctypes
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    kernel32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        error = getattr(ctypes, "get_last_error")()
        raise OSError(error, getattr(ctypes, "FormatError")(error))
    try:
        information = EXTENDED_LIMIT_INFORMATION()
        information.BasicLimitInformation.LimitFlags = 0x00002000
        if not kernel32.SetInformationJobObject(
            handle,
            9,
            ctypes.byref(information),
            ctypes.sizeof(information),
        ):
            error = getattr(ctypes, "get_last_error")()
            raise OSError(error, getattr(ctypes, "FormatError")(error))
        process_handle = wintypes.HANDLE(int(getattr(process, "_handle")))
        if not kernel32.AssignProcessToJobObject(handle, process_handle):
            error = getattr(ctypes, "get_last_error")()
            raise OSError(error, getattr(ctypes, "FormatError")(error))
    except BaseException as error:
        try:
            close_windows_job_handle(handle)
        except BaseException as cleanup_error:
            raise OSError(f"Windows Job setup failed: {error}; handle cleanup failed: {cleanup_error}") from error
        raise
    return handle


def close_windows_job_handle(handle: Any) -> None:
    """Close a Job handle without truncating pointers on 64-bit Windows."""
    import ctypes
    from ctypes import wintypes

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    if not kernel32.CloseHandle(handle):
        error = getattr(ctypes, "get_last_error")()
        raise OSError(error, getattr(ctypes, "FormatError")(error))
