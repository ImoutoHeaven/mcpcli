#!/usr/bin/env python3
"""Black-box CLI acceptance tests. Python stdlib only; no network or browser required."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback

from platform_support import IdleConnection, LINUX, WINDOWS, interrupt, kill_owned, readline_with_timeout, running, test_temp_base


class CaseSkipped(Exception):
    """An OS-specific check is recorded separately from a passing test."""


def wait_for(predicate, message, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(message)


def error_value(result):
    for line in reversed(result.stderr.splitlines()):
        try:
            return json.loads(line)["error"]
        except (ValueError, KeyError, TypeError):
            continue
    raise AssertionError(f"no JSON error: {result.stderr!r}")


def connected_data_sockets(pid, socket_path):
    inodes = set()
    for fd in Path(f"/proc/{pid}/fd").iterdir():
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        if target.startswith("socket:["):
            inodes.add(target[8:-1])
    count = 0
    for line in Path("/proc/net/unix").read_text().splitlines()[1:]:
        fields = line.split()
        if (len(fields) >= 8 and fields[6] in inodes and fields[7] == str(socket_path)
                and fields[5] == "03"):
            count += 1
    return count


class Fixture:
    def __init__(self, binary, artifacts, name):
        self.binary = binary
        self.root = Path(tempfile.mkdtemp(prefix=f"{name[:12]}-", dir=artifacts))
        self.config = self.root / "mcpcli.json"
        self.runtime = self.root / "r"
        self.log = self.root / "wire.jsonl"
        self.sessions = set()
        self.processes = []
        self.http_servers = []
        self.env = dict(os.environ, MCPCLI_RUNTIME_DIR=str(self.runtime), TMPDIR=str(self.root),
                        PYTHONUTF8="1")
        self.env.pop("MCPCLI_CONFIG", None)
        self.env.pop("MCPCLI_SESSION", None)
        self.script = Path(__file__).resolve().parent / "fixtures" / "cli_server.py"
        self.skill = self.root / "skills" / "ops"
        (self.skill / "references").mkdir(parents=True)
        (self.skill / "SKILL.md").write_text(
            "---\nname: ops\ndescription: >-\n  Inspect stateful services\n  and preserve sessions.\n---\n"
            "# Operations\nRead references/guide.md before calling tools.\n", encoding="utf-8")
        (self.skill / "references" / "guide.md").write_bytes(b"Keep the session open.\n")
        self.settings = {
            "version": 1,
            "servers": {"state": {"transport": "stdio", "command": sys.executable,
                "args": [str(self.script), "--log", str(self.log)],
                "description": "Stateful acceptance fixture"}},
            "skills": ["skills"],
        }
        self.save_config()

    def save_config(self):
        self.config.write_text(json.dumps(self.settings), encoding="utf-8")

    def use_http(self, mode):
        script = self.script.with_name("protocol_server.py")
        log = self.root / f"http-{mode}.log"
        process = subprocess.Popen([sys.executable, str(script), "--http", "--mode", mode,
                                    "--log", str(log)], env=self.env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.http_servers.append(process)
        port = int(readline_with_timeout(process.stdout, 5))
        self.settings["servers"] = {"state": {
            "transport": "http", "url": f"http://127.0.0.1:{port}/mcp",
        }}
        self.save_config()
        return log

    def command(self, *args, session="default", timeout=10):
        self.sessions.add(session)
        return [str(self.binary), "--config", str(self.config), "--session", session,
                "--timeout", str(timeout), *args]

    def invoke(self, *args, code=0, session="default", timeout=10, input_text=None, wall_timeout=40):
        command = self.command(*args, session=session, timeout=timeout)
        process = subprocess.Popen(command, env=self.env, text=True, encoding="utf-8",
            stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(input=input_text, timeout=wall_timeout)
        except subprocess.TimeoutExpired:
            # A detached daemon retaining these pipes must fail without trapping the test runner.
            self.processes.append(process)
            if process.poll() is None:
                process.kill()
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                pass  # cleanup stops the daemon before draining the retained pipes again.
            raise
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        assert result.returncode == code, (
            f"{command!r}: expected exit {code}, got {result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}")
        return result

    def value(self, *args, **options):
        result = self.invoke(*args, **options)
        try:
            return json.loads(result.stdout)
        except ValueError as error:
            raise AssertionError(f"stdout is not one JSON value: {result.stdout!r}") from error

    def fail(self, *args, code=2, **options):
        result = self.invoke(*args, code=code, **options)
        assert not result.stdout, f"failure polluted stdout: {result.stdout!r}"
        return error_value(result)

    def call(self, tool, arguments=None, **options):
        args = ["call", "state", tool]
        if arguments is not None:
            args.extend(["--args", json.dumps(arguments)])
        return self.value(*args, **options)

    def start(self, *args, session="default", timeout=10, open_stdin=False, signal_console=False):
        startup = {}
        if WINDOWS and signal_console:
            info = subprocess.STARTUPINFO()
            info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            info.wShowWindow = subprocess.SW_HIDE
            startup = {"creationflags": subprocess.CREATE_NEW_CONSOLE, "startupinfo": info}
        process = subprocess.Popen(self.command(*args, session=session, timeout=timeout), env=self.env,
            stdin=subprocess.PIPE if open_stdin else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8",
            **startup)
        self.processes.append(process)
        return process

    def events(self, event=None, **fields):
        try:
            lines = self.log.read_text().splitlines()
        except FileNotFoundError:
            return []
        values = []
        for line in lines:
            try:
                value = json.loads(line)
            except ValueError:
                continue  # Another process may be appending the current final line.
            if (event is None or value["event"] == event) and all(value.get(k) == v for k, v in fields.items()):
                values.append(value)
        return values

    def cleanup(self):
        failures = []
        for process in self.processes:
            if process.poll() is None:
                process.kill()
        if self.runtime.exists():
            for session in sorted(self.sessions):
                try:
                    self.invoke("daemon", "stop", session=session, wall_timeout=25)
                except Exception as error:
                    failures.append(f"cleanup for session {session}: {error}")
        # Cleanup only processes carrying this test's unique config/log path.
        # Oneshot children can exist without any runtime directory.
        owned = []
        for path in self.runtime.glob("*/state.json"):
            try:
                owned.append((json.loads(path.read_text())["pid"], str(self.config)))
            except (OSError, ValueError, KeyError):
                pass
        owned.extend((entry["pid"], str(self.log)) for entry in self.events("start"))
        owned.extend((entry["child_pid"], str(self.log)) for entry in self.events("child"))
        for pid, marker in owned:
            try:
                kill_owned(pid, marker)
            except (OSError, subprocess.SubprocessError) as error:
                failures.append(f"emergency cleanup PID {pid}: {error}")
        for process in self.processes:
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                failures.append(f"captured pipes still open after daemon cleanup for PID {process.pid}")
        for process in self.http_servers:
            if process.poll() is None:
                process.terminate()
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=3)
        return failures


def offline_skills(f):
    for option in ("--help", "--version"):
        process = subprocess.run([str(f.binary), "--config", str(f.root / "missing.json"), option],
                                 env=f.env, text=True, encoding="utf-8", capture_output=True, timeout=3)
        assert process.returncode == 0
    catalog = f.value("catalog")
    assert [s["name"] for s in catalog["servers"]] == ["state"]
    assert [s["name"] for s in catalog["skills"]] == ["ops"]
    assert "content" not in catalog["skills"][0]
    assert f.value("search", "ops")["servers"] == []
    assert len(f.value("search", "ops")["skills"]) == 1
    assert f.value("search", "absent")["skills"] == []
    assert f.value("skills", "list")["skills"][0]["description"].endswith("preserve sessions.")
    shown = f.value("skills", "show", "ops")
    assert Path(shown["root"]).samefile(f.skill) and "# Operations" in shown["content"]
    assert f.value("skills", "files", "ops")["files"] == ["references/guide.md"]
    assert f.value("skills", "read", "ops", "references/guide.md")["content"] == "Keep the session open.\n"
    assert f.fail("skills", "read", "ops", "../mcpcli.json")["kind"] == "skill_read"
    (f.skill / "escape").symlink_to(f.config)
    assert f.fail("skills", "read", "ops", "escape")["kind"] == "skill_read"
    assert f.fail("skills", "files", "ops")["kind"] == "skill_files"
    (f.skill / "escape").unlink()
    bad = f.root / "skills" / "bad"
    bad.mkdir()
    (bad / "SKILL.md").write_text("---\nname: wrong\ndescription: Invalid directory name\n---\n")
    assert len(f.value("skills", "list")["warnings"]) == 1
    assert not f.runtime.exists(), "offline discovery started a runtime"
    assert not f.log.exists(), "offline discovery spawned an MCP server"


def state_and_discovery(f):
    tools = f.value("tools", "state")
    assert all(set(tool) == {"name", "description"} for tool in tools)
    assert "admin" not in [tool["name"] for tool in tools]
    assert f.value("inspect", "state", "set")["inputSchema"]["type"] == "object"
    first = f.call("set", {"stage": "canary"})["structuredContent"]
    second = f.call("get")["structuredContent"]
    assert first["pid"] == second["pid"] and second["state"]["stage"] == "canary"
    f.call("unlock")
    assert "admin" in [tool["name"] for tool in f.value("tools", "state")]
    assert f.call("admin")["structuredContent"]["state"]["stage"] == "canary"
    one = f.value("--oneshot", "call", "state", "get")["structuredContent"]
    assert one["state"] == {} and one["pid"] != second["pid"]
    assert f.call("get")["structuredContent"]["state"]["stage"] == "canary"
    f.value("reset", "state")
    reset = f.call("get")["structuredContent"]
    assert reset["state"] == {} and reset["pid"] != second["pid"]
    assert len(f.events("start")) == 3


def json_inputs_and_outputs(f):
    payload = {"message": 'line one\n"quoted" \\ $()',
               "nested": [True, None, {"name": "中文"}],
               "id": 18446744073709551615,
               "private": {"$serde_json::private::Number": "123",
                           "$serde_json::private::RawValue": "[1,2]"}}
    encoded = json.dumps(payload)
    assert f.call("echo", payload)["structuredContent"] == payload
    path = f.root / "arguments.json"
    path.write_text(encoded)
    assert f.value("call", "state", "echo", "--args-file", str(path))["structuredContent"] == payload
    assert f.value("call", "state", "echo", "--stdin", input_text=encoded)["structuredContent"] == payload
    assert f.call("echo")["structuredContent"] == {}
    before = len(f.events("call"))
    for invalid in ('[]', '{"a":1,"a":2}', '{"n":18446744073709551616}',
                    '{"n":0.12345678901234567890123456789}'):
        assert f.fail("call", "state", "echo", "--args", invalid)["kind"] == "arguments"
    assert len(f.events("call")) == before, "invalid arguments reached the MCP server"
    source = '{"number":1.917236612140147400e-154}'
    precise = f.invoke("call", "state", "echo", "--args", source)
    received = json.loads(precise.stdout, parse_float=Decimal)
    assert received["structuredContent"]["number"] == Decimal("1.917236612140147400e-154")
    failure = f.value("call", "state", "error", "--args", '{"failure":"expected"}', code=3)
    assert failure["isError"] is True and failure["structuredContent"]["failure"] == "expected"
    mixed = f.call("mixed", {"ok": True})
    assert mixed["content"][1]["data"] == "AQID"
    assert mixed["_meta"]["fixture"] == "cli-acceptance"
    text = f.invoke("--text", "call", "state", "mixed", "--args", '{"ok":true}')
    assert "non-text" in text.stderr and "structuredContent" in text.stderr
    assert json.loads(text.stdout) == {"ok": True}


def session_and_restart(f):
    a = f.call("set", {"value": "a"})["structuredContent"]
    b = f.call("set", {"value": "b"}, session="other")["structuredContent"]
    assert a["pid"] != b["pid"]
    assert f.call("get")["structuredContent"]["state"] == {"value": "a"}
    assert f.call("get", session="other")["structuredContent"]["state"] == {"value": "b"}
    assert f.value("daemon", "status")["namespace"] != f.value("daemon", "status", session="other")["namespace"]
    f.value("daemon", "restart")
    assert f.call("get")["structuredContent"]["state"] == {}
    assert f.call("get", session="other")["structuredContent"]["state"] == {"value": "b"}


def changed_and_deleted_config(f):
    f.call("set", {"value": "before"})
    previous = f.value("daemon", "status")
    f.settings["servers"]["state"]["description"] = "Changed configuration"
    f.save_config()
    assert f.fail("tools", "state", code=6)["kind"] == "daemon_mismatch"
    assert f.fail("daemon", "start", code=6)["kind"] == "daemon_mismatch"
    assert f.value("daemon", "status")["pid"] == previous["pid"]
    f.value("daemon", "restart")
    assert f.call("get")["structuredContent"]["state"] == {}
    f.config.unlink()
    assert f.value("daemon", "status")["running"] is True
    assert f.value("daemon", "stop")["stopped"] is True
    assert f.value("daemon", "status")["running"] is False


def simultaneous_startup(f):
    barrier = threading.Barrier(16)

    def increment(_):
        barrier.wait()
        return f.call("increment", timeout=20)["structuredContent"]["state"]["counter"]

    with ThreadPoolExecutor(max_workers=16) as pool:
        counters = list(pool.map(increment, range(16)))
    assert sorted(counters) == list(range(1, 17))
    assert len(f.events("start")) == 1, "concurrent callers started multiple MCP processes"

    # Eight requests queued on one server must not exhaust all global work permits.
    settings = f.settings["servers"]["state"]
    f.settings["servers"] = {"slow": dict(settings), "fast": dict(settings)}
    f.save_config()
    f.value("daemon", "restart")
    f.value("call", "slow", "get")
    fast = f.value("call", "fast", "get")["structuredContent"]
    status = f.value("daemon", "status")
    socket_path = f.runtime / status["namespace"] / "sock"
    queued = [f.start("call", "slow", "wait", "--args", '{"seconds":30}', timeout=40)
              for _ in range(8)]
    wait_for(lambda: f.events("waiting"), "slow server did not start waiting", seconds=8)
    if LINUX:
        wait_for(lambda: connected_data_sockets(status["pid"], socket_path) >= 8,
                 "slow server queue did not fill", seconds=8)
    else:
        time.sleep(1)
        assert all(process.poll() is None for process in queued), "queued client exited early"
    started = time.monotonic()
    result = f.value("call", "fast", "get", timeout=2, wall_timeout=4)["structuredContent"]
    assert result["pid"] == fast["pid"]
    assert time.monotonic() - started < 2, "one busy server starved an independent server"
    for process in queued:
        process.kill()
        process.communicate(timeout=3)


def timeout_and_disconnect(f):
    f.call("get")
    failed = f.fail("call", "state", "wait", "--args", '{"seconds":30}', code=5, timeout=1)
    assert failed["outcome_unknown"] is True
    wait_for(lambda: f.events("cancel", matched=True), "timeout did not cancel the MCP request")
    assert len(f.events("call", tool="wait")) == 1, "timed-out call was replayed"
    before = len(f.events("waiting"))
    process = f.start("call", "state", "wait", "--args", '{"seconds":30}', timeout=30)
    wait_for(lambda: len(f.events("waiting")) > before, "wait tool did not start")
    request_id = f.events("waiting")[-1]["request_id"]
    process.kill()
    process.wait(timeout=3)
    wait_for(lambda: f.events("cancel", request_id=request_id, matched=True),
             "client disconnect did not cancel the MCP request")
    assert len(f.events("call", tool="wait")) == 2
    assert f.call("get")["structuredContent"]["state"] == {}
    assert len(f.events("start")) == 1


def interrupted_stdin(f):
    process = f.start("call", "state", "echo", "--stdin", open_stdin=True, signal_console=True)
    process.stdin.write('{"unfinished":')
    process.stdin.flush()

    def listening_for_sigint():
        try:
            status = Path(f"/proc/{process.pid}/status").read_text()
            flags = next(line.split()[1] for line in status.splitlines() if line.startswith("SigCgt:"))
            return int(flags, 16) & (1 << (signal.SIGINT - 1))
        except (OSError, StopIteration):
            return False

    if LINUX:
        wait_for(listening_for_sigint, "CLI did not register interruption handling")
    else:
        time.sleep(1)
        assert process.poll() is None, "stdin reader exited before interruption"
    interrupt(process, signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)
    assert process.wait(timeout=5) == (143 if WINDOWS else 130), "stdin ignored interruption"
    stdout, stderr = process.communicate()
    parsed_error = json.loads(stderr)["error"]
    assert not stdout and parsed_error["kind"] == "cancelled"
    assert not parsed_error.get("outcome_unknown", False), "pre-dispatch Ctrl-C falsely marked outcome_unknown"
    assert not f.events("call"), "incomplete input was dispatched"


def stdin_deadline(f):
    started = time.monotonic()
    process = f.start("call", "state", "echo", "--stdin", timeout=1, open_stdin=True)
    process.stdin.write('{"unfinished":')
    process.stdin.flush()
    assert process.wait(timeout=5.5) == 5, "request timeout did not bound incomplete stdin"
    assert time.monotonic() - started < 5.5
    stdout, stderr = process.communicate()
    assert not stdout and json.loads(stderr)["error"]["kind"] == "timeout"
    assert not f.events("call")


def configuration_errors_redact_values(f):
    sentinel = "SECRET_SENTINEL_DO_NOT_ECHO"
    f.settings["servers"] = {"bad": {
        "transport": "http", "url": "http://127.0.0.1:9/mcp",
        "headers": "Bearer " + sentinel,
    }}
    f.save_config()
    result = f.invoke("catalog", code=2)
    assert not result.stdout
    assert sentinel not in result.stderr, "configuration error disclosed a credential value"
    assert error_value(result)["kind"] == "configuration"
    assert not f.runtime.exists()


def oneshot_signals_clean_process_group(f):
    signals = [(signal.CTRL_BREAK_EVENT, 143)] if WINDOWS else [(signal.SIGINT, 130), (signal.SIGTERM, 143)]
    for signum, expected in signals:
        previous = len(f.events("child"))
        process = f.start("--oneshot", "call", "state", "spawn_wait", "--args",
                          '{"seconds":30}', timeout=40, signal_console=True)
        wait_for(lambda: len(f.events("child")) > previous, "oneshot did not spawn descendant")
        child = f.events("child")[-1]
        wait_for(lambda: f.events("waiting", pid=child["pid"]), "oneshot call did not start waiting")
        interrupt(process, signum)
        assert process.wait(timeout=6) == expected
        process.communicate()
        wait_for(lambda: not running(child["pid"]), "oneshot cancellation leaked MCP server")
        wait_for(lambda: not running(child["child_pid"]), "oneshot cancellation leaked same-group child")
    assert not f.runtime.exists(), "oneshot started a daemon runtime"


def saturated_control_channel(f):
    f.call("get")
    status = f.value("daemon", "status")
    socket_path = f.runtime / status["namespace"] / "sock"
    processes = [f.start("call", "state", "wait", "--args", '{"seconds":30}', timeout=40)
                 for _ in range(32)]
    wait_for(lambda: f.events("waiting"), "server never started the pending call", seconds=12)
    if LINUX:
        wait_for(lambda: connected_data_sockets(status["pid"], socket_path) >= 32,
                 "32 outstanding CLI calls never filled data admission", seconds=12)
    else:
        time.sleep(1)
        assert all(process.poll() is None for process in processes), "saturated client exited early"
    started = time.monotonic()
    assert f.value("daemon", "status", wall_timeout=4)["running"] is True
    assert time.monotonic() - started < 3, "status was blocked by data saturation"
    assert f.value("daemon", "stop", wall_timeout=12)["stopped"] is True
    for process in processes:
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode != 0, f"stopped long call unexpectedly succeeded: {stdout} {stderr}"
    assert f.value("daemon", "status")["running"] is False
    for entry in f.events("start"):
        wait_for(lambda pid=entry["pid"]: not running(pid), "stopped daemon leaked an MCP process")


def unclean_stop_requires_explicit_recovery(f):
    status = f.value("daemon", "start")
    assert not f.events("start"), "daemon start eagerly spawned the server"
    os.kill(status["pid"], signal.SIGTERM if WINDOWS else signal.SIGKILL)
    wait_for(lambda: not running(status["pid"]), "daemon did not exit after SIGKILL")
    assert f.fail("call", "state", "get", code=6)["kind"] == "state_lost"
    assert not f.events("start"), "implicit recovery spawned an MCP process"
    restarted = f.value("daemon", "start")
    assert restarted["pid"] != status["pid"]
    assert f.call("get")["structuredContent"]["state"] == {}


def lost_server_requires_reset(f):
    f.call("set", {"value": "before"})
    failed = f.fail("call", "state", "die", code=4)
    assert failed["outcome_unknown"] is True
    wait_for(lambda: f.value("daemon", "status")["servers"]["state"]["state"] == "lost",
             "server disconnection was not recorded")
    assert f.fail("call", "state", "get", code=6)["kind"] == "session_lost"
    assert len(f.events("start")) == 1
    f.value("reset", "state")
    assert f.call("get")["structuredContent"]["state"] == {}
    assert len(f.events("start")) == 2


def http_cleanup_failure(f, mode):
    log = f.use_http(mode)
    f.settings["servers"]["local_peer"] = {
        "transport": "stdio", "command": sys.executable,
        "args": [str(f.script), "--log", str(f.log)],
        "description": "Healthy sibling server",
    }
    f.save_config()
    result = f.invoke("--oneshot", "call", "state", "get", code=1)
    assert json.loads(result.stdout)["structuredContent"] == {"name": "get"}
    assert error_value(result)["kind"] == "cleanup"
    assert "do not replay" in error_value(result)["message"]
    assert log.read_text().splitlines().count("tools/call") == 1
    peer = f.value("call", "local_peer", "set", "--args", '{"keep":"alive"}')["structuredContent"]
    f.call("get")
    assert f.fail("reset", "state", code=1)["kind"] == "cleanup"
    assert f.value("daemon", "status")["servers"]["state"]["state"] == "lost"
    assert f.fail("call", "state", "get", code=6)["kind"] == "session_lost"
    assert f.fail("reset", "state", code=1)["kind"] == "cleanup", "cleanup failure was forgotten"
    assert log.read_text().splitlines().count("tools/call") == 2, "cleanup error replayed a tool call"
    # reset --force clears the unconfirmed remote HTTP slot while preserving sibling stdio state.
    forced = f.invoke("reset", "--force", "state")
    forced_json = json.loads(forced.stdout)
    assert forced_json["reset"] is True and "cleanup_warning" in forced_json
    assert "unconfirmed remote cleanup" in forced.stderr
    assert f.value("call", "local_peer", "get")["structuredContent"]["pid"] == peer["pid"]
    assert f.value("call", "local_peer", "get")["structuredContent"]["state"] == {"keep": "alive"}
    assert f.call("get")["structuredContent"] == {"name": "get"}
    # Active HTTP cleanup failure during daemon restart still brings up the replacement daemon.
    old_pid = f.value("daemon", "status")["pid"]
    restarted = f.invoke("daemon", "restart")
    restarted_status = json.loads(restarted.stdout)
    assert restarted_status["running"] is True and restarted_status["pid"] != old_pid
    assert "cleanup" in restarted.stderr
    assert f.call("get")["structuredContent"] == {"name": "get"}
    assert f.fail("daemon", "stop", code=1)["kind"] == "runtime"
    assert f.value("daemon", "status")["running"] is False
    states = list(f.runtime.glob("*/state.json"))
    assert len(states) == 1
    state = json.loads(states[0].read_text())
    assert state["clean_stop"] is False and state["cleanup_errors"]
    assert f.fail("call", "state", "get", code=6)["kind"] == "state_lost"


def http_delete_error_is_reported(f):
    http_cleanup_failure(f, "delete_error")


def http_delete_timeout_is_reported(f):
    http_cleanup_failure(f, "delete_hang")




def oneshot_unknown_inspect_closes_session(f):
    for mode in ("normal", "delete_hang"):
        log = f.use_http(mode)
        started = time.monotonic()
        result = f.invoke("--oneshot", "inspect", "state", "nonexistent", code=2, wall_timeout=6)
        elapsed = time.monotonic() - started
        failure = error_value(result)
        assert not result.stdout and failure["kind"] == "unknown_tool", (result.stdout, failure)
        assert not failure.get("outcome_unknown", False), failure
        assert log.read_text().splitlines().count("DELETE") == 1, "inspect exited before cleanup"
        assert log.read_text().splitlines().count("tools/call") == 0
        if mode == "delete_hang":
            assert 1 <= elapsed < 5, f"cleanup did not respect its bounded wait: {elapsed}"
            assert "cleanup" in failure["message"].lower(), failure


def oneshot_unknown_inspect_signal_preserves_error(f):
    log = f.use_http("delete_hang")
    process = f.start("--oneshot", "inspect", "state", "nonexistent", timeout=20,
                      signal_console=True)
    wait_for(lambda: log.exists() and "DELETE" in log.read_text().splitlines(),
             "unknown inspect did not enter cleanup", seconds=8)
    interrupt(process, signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)
    stdout, stderr = process.communicate(timeout=6)
    assert process.returncode == 2, (process.returncode, stdout, stderr)
    failure = json.loads(stderr)["error"]
    assert not stdout and failure["kind"] == "unknown_tool", (stdout, failure)
    assert not failure.get("outcome_unknown", False), failure
    assert "cleanup" in failure["message"].lower(), failure
    assert log.read_text().splitlines().count("DELETE") == 1
    assert log.read_text().splitlines().count("tools/call") == 0


def interrupt_after_result_preserves_known_outcome(f):
    log = f.use_http("delete_hang")
    process = f.start("--oneshot", "call", "state", "get", timeout=20, signal_console=True)
    wait_for(lambda: log.exists() and "DELETE" in log.read_text().splitlines(),
             "tool result did not enter remote cleanup", seconds=8)
    interrupt(process, signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)
    stdout, stderr = process.communicate(timeout=6)
    assert process.returncode == (143 if WINDOWS else 130), (process.returncode, stderr)
    assert json.loads(stdout)["structuredContent"] == {"name": "get"}, stdout
    failure = json.loads(stderr)["error"]
    assert failure["kind"] == "cancelled", failure
    assert not failure.get("outcome_unknown", False), "known result became an uncertain call"
    assert log.read_text().splitlines().count("tools/call") == 1


def http_optional_session_delete(f):
    for mode in ("delete_expired", "delete_unsupported"):
        log = f.use_http(mode)
        result = f.invoke("--oneshot", "call", "state", "get")
        assert json.loads(result.stdout)["structuredContent"] == {"name": "get"}
        assert not result.stderr
        assert log.read_text().splitlines().count("DELETE") == 1
    assert not f.runtime.exists()


def burst_admission(f):
    f.call("get")
    barrier = threading.Barrier(40)

    def increment(_):
        barrier.wait()
        return f.call("increment", timeout=20)["structuredContent"]["state"]["counter"]

    with ThreadPoolExecutor(max_workers=40) as pool:
        counters = list(pool.map(increment, range(40)))
    assert sorted(counters) == list(range(1, 41)), "connections past 32 were dropped instead of queued"




def admission_uses_original_deadline(f):
    f.call("get")
    status = f.value("daemon", "status")
    endpoint = status.get("endpoint", str(f.runtime / status["namespace"] / "sock"))
    held = []
    try:
        for _ in range(32):
            held.append(IdleConnection(endpoint))
        started = time.monotonic()
        failed = f.fail("call", "state", "increment", timeout=1, code=5, wall_timeout=3)
        assert failed["kind"] == "timeout", failed
        assert time.monotonic() - started < 2.5, "admission renewed the expired request budget"
        assert not f.events("call", tool="increment"), "queued request executed after its deadline"
    finally:
        for connection in held:
            connection.close()
    # Let the daemon accept any queued connection; a stale frame must still never execute.
    assert f.call("get")["structuredContent"]["state"] == {}
    assert not f.events("call", tool="increment"), "expired request executed after admission reopened"


def insecure_unix_endpoint_rejected_before_write(f):
    if WINDOWS:
        raise CaseSkipped("Unix socket modes; Windows named-pipe DACL checks run in platform unit tests")
    f.call("set", {"counter": 40})
    status = f.value("daemon", "status")
    socket_path = f.runtime / status["namespace"] / "sock"
    socket_path.chmod(0o666)
    try:
        failed = f.fail("call", "state", "increment", code=4)
        assert failed["kind"] == "daemon_transport"
        assert not failed.get("outcome_unknown", False), "pre-write failure falsely set outcome_unknown"
    finally:
        socket_path.chmod(0o600)
    assert f.call("get")["structuredContent"]["state"]["counter"] == 40


def text_mode_crlf_and_empty_outputs(f):
    assert f.invoke("--text", "search", "no-such-capability").stdout == ""
    crlf_skill = f.root / "skills" / "crlf-doc"
    crlf_skill.mkdir(parents=True)
    (crlf_skill / "SKILL.md").write_bytes(
        b"---\r\nname: crlf-doc\r\ndescription: CRLF skill\r\n---\r\nLine 1\r\nSpoof\rCheck\r\n"
    )
    assert f.invoke("--text", "skills", "files", "crlf-doc").stdout == ""
    shown = f.invoke("--text", "skills", "show", "crlf-doc").stdout
    assert "\\r\n" not in shown and "\r" not in shown
    assert "Line 1\nSpoof\\rCheck\n" in shown
    assert not shown.endswith("\n\n")
    read_guide = f.invoke("--text", "skills", "read", "ops", "references/guide.md").stdout
    assert read_guide == "Keep the session open.\n"


def unprivileged_execute_only_ancestors(f):
    if WINDOWS:
        raise CaseSkipped("execute-only Unix directory permissions; Windows reparse/DACL tests run separately")
    if os.geteuid() != 0:
        if os.environ.get("MCPCLI_REQUIRE_ROOT_TEST") == "1":
            raise AssertionError("root permission test was required but this process is not root")
        raise CaseSkipped("requires root; enforced by Linux container and FreeBSD CI lanes")
    outer = Path(tempfile.mkdtemp(prefix="mcpcli-0711-", dir="/var/tmp"))
    try:
        outer.chmod(0o711)
        restricted = outer / "home"
        restricted.mkdir()
        restricted.chmod(0o711)
        user_dir = restricted / "user"
        (user_dir / "skills" / "ops" / "references").mkdir(parents=True)
        user_bin = user_dir / "mcpcli"
        shutil.copy2(f.binary, user_bin)
        user_bin.chmod(0o755)
        (user_dir / "skills" / "ops" / "SKILL.md").write_text(
            "---\nname: ops\ndescription: Traverse test\n---\n# Body\n", encoding="utf-8"
        )
        (user_dir / "skills" / "ops" / "references" / "doc.md").write_text("ok\n", encoding="utf-8")
        cfg = user_dir / "mcpcli.json"
        cfg.write_text(json.dumps({"version": 1, "servers": {}, "skills": ["skills"]}), encoding="utf-8")
        for cur, dirs, files_list in os.walk(user_dir):
            os.chown(cur, 65534, 65534)
            for name in dirs:
                os.chown(Path(cur) / name, 65534, 65534)
            for name in files_list:
                os.chown(Path(cur) / name, 65534, 65534)
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "MCPCLI_RUNTIME_DIR": str(user_dir / "rt")}
        for cmd in (["catalog"], ["skills", "list"], ["skills", "show", "ops"],
                    ["skills", "files", "ops"], ["skills", "read", "ops", "references/doc.md"]):
            res = subprocess.run(
                [str(user_bin), "--config", str(cfg), *cmd],
                env=env, cwd=user_dir, text=True, encoding="utf-8", capture_output=True, timeout=10,
                user=65534, group=65534,
            )
            assert res.returncode == 0, f"{cmd} failed under 0711 ancestor: {res.stderr}"
    finally:
        shutil.rmtree(outer, ignore_errors=True)


CASES = [
    offline_skills, state_and_discovery, json_inputs_and_outputs, session_and_restart,
    changed_and_deleted_config, simultaneous_startup, timeout_and_disconnect,
    interrupted_stdin, stdin_deadline, configuration_errors_redact_values,
    oneshot_signals_clean_process_group, saturated_control_channel,
    unclean_stop_requires_explicit_recovery, lost_server_requires_reset,
    http_delete_error_is_reported, http_delete_timeout_is_reported, http_optional_session_delete,
    interrupt_after_result_preserves_known_outcome, oneshot_unknown_inspect_closes_session,
    oneshot_unknown_inspect_signal_preserves_error,
    burst_admission, admission_uses_original_deadline, insecure_unix_endpoint_rejected_before_write, text_mode_crlf_and_empty_outputs,
    unprivileged_execute_only_ancestors,
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--artifacts", type=Path, default=test_temp_base() / "mcpcli-e2e")
    parser.add_argument("--case", action="append", choices=[case.__name__ for case in CASES])
    parser.add_argument("--report", type=Path)
    options = parser.parse_args()
    source_binary = options.binary.resolve(strict=True)
    options.artifacts.mkdir(parents=True, exist_ok=True)
    snapshot = Path(tempfile.mkdtemp(prefix="binary-", dir=options.artifacts))
    binary = snapshot / ("mcpcli.exe" if WINDOWS else "mcpcli")
    shutil.copy2(source_binary, binary)
    binary_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
    outcomes = []
    for case in CASES:
        if options.case and case.__name__ not in options.case:
            continue
        fixture = Fixture(binary, options.artifacts, case.__name__)
        started = time.monotonic()
        outcome = {"case": case.__name__, "artifacts": str(fixture.root)}
        try:
            print(f"START {case.__name__}", flush=True)
            case(fixture)
            outcome["passed"] = True
        except CaseSkipped as error:
            outcome["passed"] = False
            outcome["skipped"] = str(error)
        except Exception:
            outcome["passed"] = False
            outcome["error"] = traceback.format_exc()
        finally:
            cleanup_failures = fixture.cleanup()
        if cleanup_failures:
            outcome["passed"] = False
            outcome["error"] = outcome.get("error", "") + "\n".join(cleanup_failures)
        if outcome.get("skipped"):
            print(f"SKIP {case.__name__}: {outcome['skipped']}", flush=True)
        elif outcome["passed"]:
            print(f"PASS {case.__name__}", flush=True)
        else:
            print(f"FAIL {case.__name__}\n{outcome['error']}", flush=True)
        outcome["seconds"] = round(time.monotonic() - started, 3)
        outcomes.append(outcome)
    report = {"passed": sum(case["passed"] for case in outcomes), "total": len(outcomes),
              "source_binary": str(source_binary), "binary": str(binary),
              "sha256": binary_hash, "cases": outcomes,
              "skipped": sum("skipped" in case for case in outcomes)}
    if options.report:
        options.report.parent.mkdir(parents=True, exist_ok=True)
        options.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "skipped": report["skipped"],
                      "total": report["total"]}), flush=True)
    return 0 if report["passed"] + report["skipped"] == report["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
