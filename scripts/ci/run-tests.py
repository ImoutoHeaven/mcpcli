#!/usr/bin/env python3
"""Run every Rust test executable and the portable CLI suite with bounded deadlines.

Run executables directly because Cargo places its Windows test children in a Job.
The Windows workflow starts this script through Task Scheduler outside that Job.
"""

import argparse
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
from platform_support import test_temp_base


def outside_job():
    if os.name != "nt":
        return
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    kernel.IsProcessInJob.argtypes = [ctypes.c_void_p, ctypes.c_void_p,
                                     ctypes.POINTER(ctypes.c_int)]
    inside = ctypes.c_int()
    if not kernel.IsProcessInJob(kernel.GetCurrentProcess(), None, ctypes.byref(inside)):
        raise OSError(ctypes.get_last_error(), "IsProcessInJob")
    if inside.value:
        raise RuntimeError("Windows daemon tests must run outside the runner's Job")


def run(command, timeout, log, env, capture=False):
    print("$ " + subprocess.list2cmdline(command), flush=True)
    started = time.monotonic()
    with log.open("w", encoding="utf-8") as output, log.open("r", encoding="utf-8", errors="replace") as progress:
        child = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.STDOUT,
                                 start_new_session=os.name != "nt")
        try:
            while True:
                remaining = max(0, started + timeout - time.monotonic())
                try:
                    code = child.wait(timeout=min(1, remaining))
                except subprocess.TimeoutExpired:
                    if not capture:
                        print(progress.read(), end="", flush=True)
                    if time.monotonic() >= started + timeout:
                        raise
                else:
                    if not capture:
                        print(progress.read(), end="", flush=True)
                    break
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(child.pid)],
                               capture_output=True, timeout=30)
            else:
                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=30)
            raise AssertionError(f"command exceeded {timeout}s: {command}")
    text = log.read_text(encoding="utf-8", errors="replace")
    if capture and code:
        print(text, end="", flush=True)
    print(f"exit {code} after {time.monotonic() - started:.1f}s", flush=True)
    if code:
        raise AssertionError(f"exit {code}: {command}")
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--assert-outside-job", action="store_true")
    options = parser.parse_args()
    if options.assert_outside_job:
        outside_job()
    base = test_temp_base()
    env = dict(os.environ, MCPCLI_TEST_PYTHON=sys.executable, PYTHONUTF8="1")
    if os.name == "nt":
        env.update(TEMP=str(base), TMP=str(base))
    print(f"platform={sys.platform}, python={sys.executable}", flush=True)
    build = run(["cargo", "test", "--locked", "--all-targets", "--no-run",
                 "--message-format=json"], options.timeout, Path("ci-build-tests.log"), env, True)
    executables = []
    for line in build.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (item.get("reason") == "compiler-artifact" and item.get("profile", {}).get("test")
                and item.get("executable") and item["executable"] not in executables):
            executables.append(item["executable"])
    if not executables:
        raise AssertionError("Cargo emitted no test executables")
    failures = []
    for index, executable in enumerate(executables):
        try:
            output = run([executable, "--nocapture"], options.timeout,
                         Path(f"ci-rust-{index}.log"), env)
            if "... ignored" in output:
                raise AssertionError("ignored tests must have an explicit CI gate, not a silent pass")
        except Exception as error:
            failures.append(str(error))
    release = run(["cargo", "build", "--release", "--locked", "--message-format=json"],
                  options.timeout, Path("ci-build-release.log"), env, True)
    binaries = []
    for line in release.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (item.get("reason") == "compiler-artifact" and item.get("executable")
                and item.get("target", {}).get("name") == "mcpcli"):
            binaries.append(Path(item["executable"]))
    assert len(binaries) == 1, f"Cargo did not identify the release binary: {binaries}"
    binary = binaries[0]
    with tempfile.TemporaryDirectory(prefix="mc-ci-", dir=base) as directory:
        try:
            run([sys.executable, "tests/cli_e2e.py", "--binary", str(binary.resolve()),
                 "--artifacts", directory, "--report", "ci-cli-report.json"],
                options.timeout, Path("ci-cli.log"), env)
        except Exception as error:
            failures.append(str(error))
            with zipfile.ZipFile("ci-cli-artifacts.zip", "w", zipfile.ZIP_DEFLATED) as archive:
                for path in Path(directory).rglob("*"):
                    if path.is_file() and path.name not in ("mcpcli", "mcpcli.exe"):
                        archive.write(path, path.relative_to(directory))
    if failures:
        raise AssertionError("\n".join(failures))
    print("PASS: Rust unit/protocol tests and portable CLI E2E", flush=True)


if __name__ == "__main__":
    main()
