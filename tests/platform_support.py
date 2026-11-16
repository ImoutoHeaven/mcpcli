"""Native process and pipe checks shared by the black-box test suites."""

import ctypes
import os
from pathlib import Path
import queue
import signal
import socket
import time
import subprocess
import sys
import threading

WINDOWS = os.name == "nt"
LINUX = sys.platform.startswith("linux")


def running(pid):
    """Treat zombies as exited; a failed inspection must never pass a leak test."""
    if WINDOWS:
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x00100000, False, int(pid))  # SYNCHRONIZE
        if not handle:
            error = ctypes.get_last_error()
            if error == 87:  # ERROR_INVALID_PARAMETER: PID no longer exists.
                return False
            raise OSError(error, f"OpenProcess({pid})")
        try:
            status = kernel.WaitForSingleObject(handle, 0)
            if status == 0:
                return False
            if status == 258:  # WAIT_TIMEOUT
                return True
            raise OSError(ctypes.get_last_error(), f"WaitForSingleObject({pid})")
        finally:
            kernel.CloseHandle(handle)
    if LINUX:
        try:
            return Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[0] != "Z"
        except FileNotFoundError:
            return False
    result = subprocess.run(["ps", "-o", "stat=", "-p", str(int(pid))],
                            text=True, capture_output=True, timeout=5)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"ps failed while checking PID {pid}: {result.stderr}")
    state = result.stdout.strip()
    return bool(state) and not state.startswith("Z")


def kill_owned(pid, marker):
    """Emergency cleanup only when the live command still carries our unique path."""
    pid = int(pid)
    if not running(pid):
        return
    if WINDOWS:
        command = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
             f"(Get-CimInstance Win32_Process -Filter 'ProcessId = {pid}').CommandLine"],
            text=True, encoding="utf-8", errors="replace", capture_output=True, timeout=10,
        )
    else:
        command = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], text=True,
                                 capture_output=True, timeout=5)
    if command.returncode == 0 and marker in command.stdout:
        os.kill(pid, signal.SIGTERM if WINDOWS else signal.SIGKILL)


def readline_with_timeout(stream, seconds):
    """select() cannot wait on anonymous pipes on Windows."""
    result = queue.Queue()

    def read():
        try:
            result.put(stream.readline())
        except BaseException as error:
            result.put(error)

    threading.Thread(target=read, daemon=True).start()
    try:
        value = result.get(timeout=seconds)
    except queue.Empty as error:
        raise AssertionError("child did not emit its readiness line") from error
    if isinstance(value, BaseException):
        raise value
    if not value:
        raise AssertionError("child exited before becoming ready")
    return value


class IdleConnection:
    """Hold one authenticated local endpoint open without submitting a frame."""

    def __init__(self, endpoint):
        if not WINDOWS:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.socket.settimeout(5)
            self.socket.connect(endpoint)
            return
        from ctypes import wintypes
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                           ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD,
                                           wintypes.HANDLE]
        self.kernel.CreateFileW.restype = wintypes.HANDLE
        self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        deadline = time.monotonic() + 5
        while True:
            self.handle = self.kernel.CreateFileW(endpoint, 0xC0000000, 0, None, 3, 0, None)
            if self.handle != ctypes.c_void_p(-1).value:
                break
            error = ctypes.get_last_error()
            if error not in (2, 231) or time.monotonic() >= deadline:
                raise OSError(error, f"CreateFileW({endpoint})")
            time.sleep(0.01)

    def close(self):
        if WINDOWS:
            self.kernel.CloseHandle(self.handle)
        else:
            self.socket.close()


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--running":
        raise SystemExit("usage: platform_support.py --running PID")
    raise SystemExit(0 if running(int(sys.argv[2])) else 1)
