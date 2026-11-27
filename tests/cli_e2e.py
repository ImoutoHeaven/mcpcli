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

from raw_output_acceptance import raw_semantic_disclosure

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

    def use_http(self, mode, tls=False, modern=False):
        script = self.script.with_name("protocol_server.py")
        log = self.root / f"http-{mode}.log"
        process = subprocess.Popen([sys.executable, str(script), "--http", "--mode", mode,
                                    "--log", str(log), *(["--tls"] if tls else []),
                                    *(["--modern"] if modern else [])], env=self.env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.http_servers.append(process)
        port = int(readline_with_timeout(process.stdout, 5))
        self.settings["servers"] = {"state": {
            "transport": "http", "url": f"{'https' if tls else 'http'}://127.0.0.1:{port}/mcp",
        }}
        self.save_config()
        return log

    def command(self, *args, session="default", timeout=10):
        self.sessions.add(session)
        cmd = [str(self.binary), "--config", str(self.config), "--session", session]
        if timeout is not None:
            cmd.extend(["--timeout", str(timeout)])
        cmd.extend(args)
        return cmd

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
    assert catalog["agents"] is None
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
    listing = f.value("tools", "state")
    tools = [tool for page in listing["pages"] for tool in page["tools"]]
    assert tools and all({"name", "description", "inputSchema"} <= set(tool) for tool in tools)
    assert "admin" not in [tool["name"] for tool in tools]
    assert f.value("inspect", "state", "set")["inputSchema"]["type"] == "object"
    first = f.call("set", {"stage": "canary"})["result"]["structuredContent"]
    second = f.call("get")["result"]["structuredContent"]
    assert first["pid"] == second["pid"] and second["state"]["stage"] == "canary"
    f.call("unlock")
    assert "admin" in [tool["name"] for page in f.value("tools", "state")["pages"] for tool in page["tools"]]
    assert f.call("admin")["result"]["structuredContent"]["state"]["stage"] == "canary"
    one = f.value("--oneshot", "call", "state", "get")["result"]["structuredContent"]
    assert one["state"] == {} and one["pid"] != second["pid"]
    assert f.call("get")["result"]["structuredContent"]["state"]["stage"] == "canary"
    f.value("reset", "state")
    reset = f.call("get")["result"]["structuredContent"]
    assert reset["state"] == {} and reset["pid"] != second["pid"]
    assert len(f.events("start")) == 3


def json_inputs_and_outputs(f):
    payload = {"message": 'line one\n"quoted" \\ $()',
               "nested": [True, None, {"name": "中文"}],
               "id": 18446744073709551615,
               "private": {"$serde_json::private::Number": "123",
                           "$serde_json::private::RawValue": "[1,2]"}}
    encoded = json.dumps(payload)
    assert f.call("echo", payload)["result"]["structuredContent"] == payload
    path = f.root / "arguments.json"
    path.write_text(encoded)
    assert f.value("call", "state", "echo", "--args-file", str(path))["result"]["structuredContent"] == payload
    assert f.value("call", "state", "echo", "--stdin", input_text=encoded)["result"]["structuredContent"] == payload
    assert f.call("echo")["result"]["structuredContent"] == {}
    before = len(f.events("call"))
    for invalid in ('[]', '{"a":1,"a":2}', '{"n":18446744073709551616}',
                    '{"n":0.12345678901234567890123456789}'):
        assert f.fail("call", "state", "echo", "--args", invalid)["kind"] == "arguments"
    assert len(f.events("call")) == before, "invalid arguments reached the MCP server"
    source = '{"number":1.917236612140147400e-154}'
    precise = f.invoke("call", "state", "echo", "--args", source)
    received = json.loads(precise.stdout, parse_float=Decimal)
    assert received["result"]["structuredContent"]["number"] == Decimal("1.917236612140147400e-154")
    failure = f.value("call", "state", "error", "--args", '{"failure":"expected"}', code=3)
    assert failure["result"]["isError"] is True and failure["result"]["structuredContent"]["failure"] == "expected"
    mixed = f.call("mixed", {"ok": True})
    assert "data" not in mixed["result"]["content"][1]
    assert mixed["result"]["_meta"]["fixture"] == "cli-acceptance"
    assert len(mixed["savedMedia"]) == 1
    saved = mixed["savedMedia"][0]
    assert saved["contentIndex"] == 1 and saved["kind"] == "image"
    assert saved["mimeType"] == "image/png" and saved["bytes"] == 3
    saved_file = Path(saved["savedPath"])
    assert saved_file.suffix == ".png" and saved_file.read_bytes() == b"\x01\x02\x03"
    mixed_inline = f.value("call", "state", "mixed", "--inline-media", "--args", '{"ok":true}')
    assert mixed_inline["content"][1]["data"] == "AQID" and "savedMedia" not in mixed_inline
    text = f.invoke("--text", "call", "state", "mixed", "--args", '{"ok":true}')
    assert "non-text" not in text.stderr and "structuredContent" in text.stderr
    text_lines = text.stdout.strip().splitlines()
    assert len(text_lines) == 2 and json.loads(text_lines[0]) == {"ok": True}
    assert text_lines[1].startswith("Saved image [image/png] (3 B): ")
    text_inline = f.invoke("--text", "call", "state", "mixed", "--inline-media", "--args", '{"ok":true}')
    assert "non-text" in text_inline.stderr and "structuredContent" in text_inline.stderr
    assert json.loads(text_inline.stdout) == {"ok": True}


def session_and_restart(f):
    a = f.call("set", {"value": "a"})["result"]["structuredContent"]
    b = f.call("set", {"value": "b"}, session="other")["result"]["structuredContent"]
    assert a["pid"] != b["pid"]
    assert f.call("get")["result"]["structuredContent"]["state"] == {"value": "a"}
    assert f.call("get", session="other")["result"]["structuredContent"]["state"] == {"value": "b"}
    assert f.value("daemon", "status")["namespace"] != f.value("daemon", "status", session="other")["namespace"]
    f.value("daemon", "restart")
    assert f.call("get")["result"]["structuredContent"]["state"] == {}
    assert f.call("get", session="other")["result"]["structuredContent"]["state"] == {"value": "b"}


def global_agents(f):
    assert f.value("agents") == {"agents": None}
    assert f.invoke("--text", "agents").stdout == ""
    help_plain = subprocess.run([str(f.binary), "--config", str(f.config), "--help"],
                                env=f.env, text=True, encoding="utf-8", capture_output=True, timeout=3)
    assert help_plain.returncode == 0 and "Global AGENTS.md" not in help_plain.stdout

    project_dir = f.root / "project"
    project_dir.mkdir()
    (project_dir / "AGENTS.md").write_bytes(b"PROJECT_SCOPED_SENTINEL_DO_NOT_READ\n")

    agents_dir = f.root / "ancestor-path-only-token" / "global-docs"
    agents_dir.mkdir(parents=True)
    agents_file = agents_dir / "AGENTS.md"
    raw_content = "# Global MCP Guide\r\nAlways inspect tool schemas before calling.\r\nSpoof\rGuard\r"
    agents_file.write_bytes(raw_content.encode("utf-8"))
    f.settings["agents"] = "./ancestor-path-only-token/global-docs/AGENTS.md"
    f.save_config()

    scoped_env = dict(f.env, MCPCLI_CONFIG=str(f.config))
    for extra in ([], ["--config", str(f.config)]):
        proc = subprocess.run([str(f.binary), *extra, "catalog"], env=scoped_env, cwd=project_dir,
                              text=True, encoding="utf-8", capture_output=True, timeout=5)
        assert proc.returncode == 0, proc.stderr
        catalog = json.loads(proc.stdout)
        assert Path(catalog["agents"]["path"]).samefile(agents_file)
        assert catalog["agents"]["content"] == raw_content
        assert "PROJECT_SCOPED_SENTINEL" not in proc.stdout

    text_catalog = f.invoke("--text", "catalog").stdout
    assert "agents " in text_catalog
    assert "# Global MCP Guide\nAlways inspect tool schemas before calling.\nSpoof\\rGuard\\r" in text_catalog
    assert "server state\t" in text_catalog and "skill ops\t" in text_catalog

    direct = f.value("agents")
    assert Path(direct["agents"]["path"]).samefile(agents_file)
    assert direct["agents"]["content"] == raw_content
    assert f.invoke("--text", "agents").stdout == (
        "# Global MCP Guide\nAlways inspect tool schemas before calling.\nSpoof\\rGuard\\r\n"
    )

    matched = f.value("search", "inspect tool schemas")
    assert matched["agents"]["content"] == raw_content
    assert matched["servers"] == [] and matched["skills"] == []
    by_filename = f.value("search", "agents.md")
    assert by_filename["agents"]["content"] == raw_content
    for absent in ("absent-capability", "ancestor-path-only-token"):
        unmatched = f.value("search", absent)
        assert unmatched["agents"] is None and unmatched["servers"] == [] and unmatched["skills"] == []
        assert f.invoke("--text", "search", absent).stdout == ""

    for help_args in (["-h"], ["--help", "--timeout", "not-a-number", "--text", "--text"],
                      ["--help", "catalog"], ["--help", "--version"], ["-hV"],
                      ["--help", "--unknown"]):
        help_with_agents = subprocess.run([str(f.binary), *help_args], env=scoped_env, cwd=project_dir,
                                          text=True, encoding="utf-8", capture_output=True, timeout=3)
        assert help_with_agents.returncode == 0
        assert "Global AGENTS.md (" in help_with_agents.stdout
        assert "Always inspect tool schemas before calling." in help_with_agents.stdout
        assert "Spoof\\rGuard\\r" in help_with_agents.stdout
        assert "PROJECT_SCOPED_SENTINEL" not in help_with_agents.stdout

    for other_args in (["call", "--help"], ["catalog", "--help"], ["--version", "--help"], ["-Vh"]):
        other_output = subprocess.run([str(f.binary), "--config", str(f.config), *other_args],
                                      env=f.env, text=True, encoding="utf-8", capture_output=True, timeout=3)
        assert other_output.returncode == 0 and "Global AGENTS.md (" not in other_output.stdout

    nested_cfg_dir = f.root / "nested-config"
    nested_cfg_dir.mkdir()
    nested_cfg = nested_cfg_dir / "mcpcli.json"
    nested_cfg.write_text(json.dumps({
        "version": 1,
        "agents": "../ancestor-path-only-token/global-docs/AGENTS.md",
        "servers": {"state": {
            "transport": "stdio", "command": sys.executable,
            "args": [str(f.script), "--log", str(f.log)], "cwd": "..",
        }},
        "skills": ["../skills"],
    }), encoding="utf-8")
    nested_cat = subprocess.run([str(f.binary), "--config", str(nested_cfg), "catalog"],
                                env=f.env, text=True, encoding="utf-8", capture_output=True, timeout=5)
    assert nested_cat.returncode == 0, nested_cat.stderr
    nested_payload = json.loads(nested_cat.stdout)
    assert nested_payload["agents"]["content"] == raw_content
    assert [s["name"] for s in nested_payload["skills"]] == ["ops"]

    if not WINDOWS:
        ctrl_dir = f.root / "dir\rspoof"
        ctrl_dir.mkdir()
        ctrl_agents = ctrl_dir / "AGENTS.md"
        ctrl_agents.write_bytes(b"# Safe Path Help\n")
        ctrl_cfg = f.root / "ctrl-mcpcli.json"
        ctrl_cfg.write_text(json.dumps({"version": 1, "agents": str(ctrl_agents)}), encoding="utf-8")
        ctrl_help = subprocess.run([str(f.binary), "--config", str(ctrl_cfg), "--help"],
                                   env=f.env, text=True, encoding="utf-8", capture_output=True, timeout=3)
        assert ctrl_help.returncode == 0
        assert "\r" not in ctrl_help.stdout and "dir\\rspoof" in ctrl_help.stdout

    agents_file.write_bytes(b"# Updated Global Guide\nSecond version.\n")
    assert f.value("agents")["agents"]["content"] == "# Updated Global Guide\nSecond version.\n"

    linked = f.root / "linked-agents.md"
    linked.symlink_to(agents_file)
    f.settings["agents"] = "linked-agents.md"
    f.save_config()
    assert f.fail("agents")["kind"] == "agents"
    assert f.fail("catalog")["kind"] == "agents"

    f.settings["agents"] = "missing-agents.md"
    f.save_config()
    assert f.fail("agents")["kind"] == "agents"
    assert f.fail("catalog")["kind"] == "agents"
    broken_help = subprocess.run([str(f.binary), "--config", str(f.config), "--help"],
                                 env=f.env, text=True, encoding="utf-8", capture_output=True, timeout=3)
    assert broken_help.returncode == 0
    assert not f.runtime.exists() and not f.log.exists()


def changed_and_deleted_config(f):
    f.call("set", {"value": "before"})
    previous = f.value("daemon", "status")
    f.settings["agents"] = "./AGENTS.md"
    (f.root / "AGENTS.md").write_bytes(b"# Global\n")
    f.save_config()
    assert f.fail("tools", "state", code=6)["kind"] == "daemon_mismatch"
    assert f.fail("daemon", "start", code=6)["kind"] == "daemon_mismatch"
    assert f.value("daemon", "status")["pid"] == previous["pid"]
    f.value("daemon", "restart")
    assert f.call("get")["result"]["structuredContent"]["state"] == {}
    f.config.unlink()
    assert f.value("daemon", "status")["running"] is True
    assert f.value("daemon", "stop")["stopped"] is True
    assert f.value("daemon", "status")["running"] is False


def simultaneous_startup(f):
    barrier = threading.Barrier(16)

    def increment(_):
        barrier.wait()
        return f.call("increment", timeout=20)["result"]["structuredContent"]["state"]["counter"]

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
    fast = f.value("call", "fast", "get")["result"]["structuredContent"]
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
    result = f.value("call", "fast", "get", timeout=2, wall_timeout=4)["result"]["structuredContent"]
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
    assert f.call("get")["result"]["structuredContent"]["state"] == {}
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


def configure_events(f, discover_delay=0, list_delay=0, event_delay=0, payload_bytes=0):
    script = f.script.with_name("events_server.py")
    f.settings["servers"] = {"state": {
        "transport": "stdio", "command": sys.executable,
        "args": [str(script), "--log", str(f.log),
                 "--discover-delay", str(discover_delay), "--list-delay", str(list_delay),
                 "--event-delay", str(event_delay), "--payload-bytes", str(payload_bytes)],
    }}
    f.save_config()


def events_deadline_spans_bootstrap(f):
    configure_events(f, discover_delay=1.2, event_delay=1.2)
    result = f.invoke("--oneshot", "events", "stream", "state", "test.event",
                      "--mode", "push", "--once", timeout=2000, code=5)
    assert not result.stdout and error_value(result)["kind"] == "timeout"
    assert f.events("active"), "deadline expired before reaching the stream"
    assert not f.events("delivered"), "bootstrap received a second full timeout"


def events_deadline_spans_bootstrap_and_callback(f):
    configure_events(f, discover_delay=1.2)
    marker = f.root / "callback-started"
    cursor = f.root / "cursor"
    cursor.write_text("before", encoding="utf-8")
    callback = ("import pathlib,sys,time; sys.stdin.readline(); "
                "pathlib.Path(sys.argv[1]).write_text('started'); time.sleep(1.2)")
    result = f.invoke("--oneshot", "events", "stream", "state", "test.event",
                      "--mode", "push", "--once", "--cursor-file", str(cursor),
                      "--exec-timeout", "5000", "--", sys.executable, "-c", callback, str(marker),
                      timeout=2000, code=5)
    assert marker.exists(), "callback did not start"
    assert not result.stdout and error_value(result)["kind"] == "timeout"
    assert cursor.read_text(encoding="utf-8") == "before", "unconfirmed event advanced the cursor"



def events_stdout_backpressure_is_interruptible(f):
    configure_events(f, payload_bytes=512 * 1024)
    cursor = f.root / "cursor"
    signals = [(signal.CTRL_BREAK_EVENT, 143)] if WINDOWS else [(signal.SIGINT, 130)]
    for signum, expected in [(None, 5), *signals]:
        cursor.write_text("before", encoding="utf-8")
        previous = len(f.events("delivered"))
        process = f.start("--oneshot", "events", "stream", "state", "test.event",
                          "--mode", "push", "--once", "--cursor-file", str(cursor),
                          timeout=3000 if signum is None else 10000,
                          signal_console=signum is not None)
        wait_for(lambda: len(f.events("delivered")) > previous,
                 "stdout test never reached event delivery", seconds=5)
        prefix = []
        first_byte = threading.Thread(target=lambda: prefix.append(process.stdout.read(1)), daemon=True)
        first_byte.start()
        first_byte.join(timeout=2)
        assert prefix == ["{"], "event output never started"
        if signum is not None:
            interrupt(process, signum)
        assert process.wait(timeout=7) == expected, "blocked stdout prevented foreground exit"
        assert cursor.read_text(encoding="utf-8") == "before", "blocked output advanced the cursor"
        # Read only after process exit; resuming earlier could hide a stdout shutdown lock.
        stdout, stderr = process.communicate(timeout=3)
        assert "\n" not in "".join(prefix) + stdout, "test output did not encounter backpressure"
        assert error_value(subprocess.CompletedProcess([], expected, stdout, stderr))["kind"] == (
            "timeout" if signum is None else "cancelled"
        )


def events_stdout_preserves_unicode(f):
    configure_events(f)
    result = f.invoke("--oneshot", "events", "stream", "state", "test.event",
                      "--mode", "push", "--once", timeout=5000)
    assert json.loads(result.stdout)["data"]["message"] == "ordinary event 中文 😀"


def events_stdin_deadline(f):
    configure_events(f)
    process = f.start("--oneshot", "events", "stream", "state", "test.event",
                      "--stdin", "--mode", "push", timeout=500, open_stdin=True)
    process.stdin.write('{"unfinished":')
    process.stdin.flush()
    assert process.wait(timeout=5.5) == 5, "event timeout did not bound incomplete stdin"
    stdout, stderr = process.communicate()
    assert not stdout and json.loads(stderr)["error"]["kind"] == "timeout"
    assert not f.events("start"), "incomplete input started the MCP connection"


def events_interrupts_bootstrap_and_active(f):
    signals = [(signal.CTRL_BREAK_EVENT, 143)] if WINDOWS else [(signal.SIGINT, 130), (signal.SIGTERM, 143)]
    for stage in ("discover", "list", "active"):
        for signum, expected in signals:
            configure_events(f, discover_delay=3 if stage == "discover" else 0,
                             list_delay=3 if stage == "list" else 0, event_delay=30)
            previous = len(f.events(stage))
            process = f.start("--oneshot", "events", "stream", "state", "test.event",
                              "--mode", "auto" if stage == "list" else "push",
                              timeout=10000, signal_console=True)
            observed = wait_for(lambda: f.events(stage)[-1] if len(f.events(stage)) > previous else None,
                                f"event command never reached {stage}", seconds=8)
            interrupted_at = time.monotonic()
            interrupt(process, signum)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == expected, (stage, process.returncode, stderr)
            assert time.monotonic() - interrupted_at < 2, f"{stage} ignored interruption"
            assert not stdout and json.loads(stderr)["error"]["kind"] == "cancelled"
            wait_for(lambda: not running(observed["pid"]), "event cancellation leaked MCP server")
            if stage == "active":
                assert len(f.events("cancel", pid=observed["pid"], matched=True)) <= 1


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
    assert f.call("get")["result"]["structuredContent"]["state"] == {}


def lost_server_requires_reset(f):
    f.call("set", {"value": "before"})
    failed = f.fail("call", "state", "die", code=4)
    assert failed["outcome_unknown"] is True
    wait_for(lambda: f.value("daemon", "status")["servers"]["state"]["state"] == "lost",
             "server disconnection was not recorded")
    assert f.fail("call", "state", "get", code=6)["kind"] == "session_lost"
    assert len(f.events("start")) == 1
    f.value("reset", "state")
    assert f.call("get")["result"]["structuredContent"]["state"] == {}
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
    assert json.loads(result.stdout)["result"]["structuredContent"] == {"name": "get"}
    assert error_value(result)["kind"] == "cleanup"
    assert "do not replay" in error_value(result)["message"]
    assert log.read_text().splitlines().count("DELETE") == 1
    assert log.read_text().splitlines().count("tools/call") == 1
    peer = f.value("call", "local_peer", "set", "--args", '{"keep":"alive"}')["result"]["structuredContent"]
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
    assert f.value("call", "local_peer", "get")["result"]["structuredContent"]["pid"] == peer["pid"]
    assert f.value("call", "local_peer", "get")["result"]["structuredContent"]["state"] == {"keep": "alive"}
    assert f.call("get")["result"]["structuredContent"] == {"name": "get"}
    # Active HTTP cleanup failure during daemon restart still brings up the replacement daemon.
    old_pid = f.value("daemon", "status")["pid"]
    restarted = f.invoke("daemon", "restart")
    restarted_status = json.loads(restarted.stdout)
    assert restarted_status["running"] is True and restarted_status["pid"] != old_pid
    assert "cleanup" in restarted.stderr
    assert f.call("get")["result"]["structuredContent"] == {"name": "get"}
    assert f.fail("daemon", "stop", code=1)["kind"] == "runtime"
    assert f.value("daemon", "status")["running"] is False
    states = list(f.runtime.glob("*/state.json"))
    assert len(states) == 1
    state = json.loads(states[0].read_text())
    assert state["clean_stop"] is False and state["cleanup_errors"]
    assert f.fail("call", "state", "get", code=6)["kind"] == "state_lost"


def http_delete_error_is_reported(f):
    redirect_log = f.use_http("delete_redirect")
    redirect_result = f.invoke("--oneshot", "call", "state", "get", code=1)
    assert json.loads(redirect_result.stdout)["result"]["structuredContent"] == {"name": "get"}
    assert error_value(redirect_result)["kind"] == "cleanup"
    assert "do not replay" in error_value(redirect_result)["message"]
    assert redirect_log.read_text().splitlines().count("DELETE") == 1
    assert redirect_log.read_text().splitlines().count("tools/call") == 1
    http_cleanup_failure(f, "delete_error")


def http_delete_timeout_is_reported(f):
    http_cleanup_failure(f, "delete_hang")




def http_without_ca_store_keeps_tls_validation(f):
    empty = f.root / "empty-ca-store"
    empty.mkdir()
    bundle = empty / "empty.pem"
    bundle.write_bytes(b"")
    f.env.update(SSL_CERT_FILE=str(bundle), SSL_CERT_DIR=str(empty))
    log = f.use_http("no_system_ca")
    assert f.invoke("--oneshot", "call", "state", "get").returncode == 0
    assert log.read_text().splitlines().count("DELETE") == 1
    tls_log = f.use_http("untrusted_tls", tls=True)
    result = f.invoke("--oneshot", "call", "state", "get", code=4)
    assert not result.stdout
    assert error_value(result)["kind"] == "mcp_transport"
    assert "tools/call" not in tls_log.read_text().splitlines(), "untrusted TLS was accepted"


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


def oneshot_task_rejection_closes_session(f):
    commands = [
        ("call", "state", "get", "--detach"),
        ("tasks", "get", "state", "task-1"),
        ("tasks", "get", "state", "task-1", "--wait"),
    ]
    for mode in ("normal", "delete_error", "delete_hang"):
        log = f.use_http(mode)
        for index, command in enumerate(commands, 1):
            started = time.monotonic()
            result = f.invoke("--oneshot", *command, code=4, wall_timeout=6)
            elapsed = time.monotonic() - started
            failure = error_value(result)
            assert not result.stdout and failure["kind"] == "mcp_protocol", (result.stdout, failure)
            assert not failure.get("outcome_unknown", False), failure
            assert log.read_text().splitlines().count("DELETE") == index, "task rejection skipped cleanup"
            assert log.read_text().splitlines().count("tools/call") == 0
            if mode != "normal":
                assert "cleanup" in failure["message"].lower(), failure
            if mode == "delete_hang":
                assert 1 <= elapsed < 5, f"cleanup did not respect its bounded wait: {elapsed}"
    assert not f.runtime.exists()


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
    assert json.loads(stdout)["result"]["structuredContent"] == {"name": "get"}, stdout
    failure = json.loads(stderr)["error"]
    assert failure["kind"] == "cancelled", failure
    assert not failure.get("outcome_unknown", False), "known result became an uncertain call"
    assert log.read_text().splitlines().count("tools/call") == 1


def http_optional_session_delete(f):
    for mode in ("delete_expired", "delete_unsupported"):
        log = f.use_http(mode)
        result = f.invoke("--oneshot", "call", "state", "get")
        assert json.loads(result.stdout)["result"]["structuredContent"] == {"name": "get"}
        assert not result.stderr
        assert log.read_text().splitlines().count("DELETE") == 1
    assert not f.runtime.exists()


def burst_admission(f):
    f.call("get")
    barrier = threading.Barrier(40)

    def increment(_):
        barrier.wait()
        return f.call("increment", timeout=20)["result"]["structuredContent"]["state"]["counter"]

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
        failed = f.fail(
            "call", "state", "increment", "--max-timeout", "1", timeout=1, code=5, wall_timeout=3
        )
        assert failed["kind"] == "timeout", failed
        assert time.monotonic() - started < 2.5, "admission renewed the expired request budget"
        assert not f.events("call", tool="increment"), "queued request executed after its deadline"
    finally:
        for connection in held:
            connection.close()
    # Let the daemon accept any queued connection; a stale frame must still never execute.
    assert f.call("get")["result"]["structuredContent"]["state"] == {}
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
    assert f.call("get")["result"]["structuredContent"]["state"]["counter"] == 40


def text_mode_crlf_and_empty_outputs(f):
    assert f.invoke("--text", "search", "no-such-capability").stdout == ""
    crlf_skill = f.root / "skills" / "crlf-doc"
    crlf_skill.mkdir(parents=True)
    (crlf_skill / "SKILL.md").write_bytes(
        b"---\r\nname: crlf-doc\r\ndescription: CRLF skill\r\n---\r\nLine 1\r\nSpoof\rCheck\r\n"
    )
    assert f.invoke("--text", "skills", "files", "crlf-doc").stdout == ""
    if not WINDOWS:
        for name in ("first\r", "second"):
            (crlf_skill / name).write_text("resource", encoding="utf-8")
        assert f.value("skills", "files", "crlf-doc")["files"] == ["first\r", "second"]
        assert f.invoke("--text", "skills", "files", "crlf-doc").stdout == "first\\r\nsecond\n"
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
        (user_dir / "AGENTS.md").write_text("# Global Traverse\n", encoding="utf-8")
        cfg = user_dir / "mcpcli.json"
        cfg.write_text(json.dumps({"version": 1, "agents": "AGENTS.md", "servers": {}, "skills": ["skills"]}), encoding="utf-8")
        for cur, dirs, files_list in os.walk(user_dir):
            os.chown(cur, 65534, 65534)
            for name in dirs:
                os.chown(Path(cur) / name, 65534, 65534)
            for name in files_list:
                os.chown(Path(cur) / name, 65534, 65534)
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "MCPCLI_RUNTIME_DIR": str(user_dir / "rt")}
        for cmd in (["catalog"], ["agents"], ["--help"], ["skills", "list"], ["skills", "show", "ops"],
                    ["skills", "files", "ops"], ["skills", "read", "ops", "references/doc.md"]):
            res = subprocess.run(
                [str(user_bin), "--config", str(cfg), *cmd],
                env=env, cwd=user_dir, text=True, encoding="utf-8", capture_output=True, timeout=10,
                user=65534, group=65534,
            )
            assert res.returncode == 0, f"{cmd} failed under 0711 ancestor: {res.stderr}"
            if cmd == ["--help"]:
                assert "# Global Traverse" in res.stdout
    finally:
        shutil.rmtree(outer, ignore_errors=True)


def multimodal_media_and_inline_flag(f):
    for flags in ([], ["--oneshot"]):
        out = f.value(*flags, "call", "state", "multimodal")
        assert out["result"]["_meta"] == {"explicitNull": None}
        assert out["result"]["content"][0] == {"type": "text", "text": "intro", "x-extra": None}
        assert "data" not in out["result"]["content"][1] and out["result"]["content"][1]["_meta"] == {"i": 1}
        assert "data" not in out["result"]["content"][2]
        assert "blob" not in out["result"]["content"][3]["resource"]
        assert out["result"]["content"][4]["resource"]["text"] == "keep-text"
        assert "blob" not in out["result"]["content"][5]["resource"]
        assert out["result"]["savedMedia"] == [{"forged": True}]
        saved = out["savedMedia"]
        assert [item["contentIndex"] for item in saved] == [1, 2, 3, 5]
        assert [item["kind"] for item in saved] == ["image", "audio", "resource", "resource"]
        expected = [
            (".png", "image/png", b"\x01\x02\x03"),
            (".m4a", "audio/mp4", b"\x04\x05\x06\x07"),
            (".mp4", "video/mp4", b"\x09\x0a\x0b\x0c"),
            (".bin", "application/x-unknown-thing", b"\x0d\x0e"),
        ]
        for item, (ext, mime, raw_bytes) in zip(saved, expected):
            p = Path(item["savedPath"])
            assert "evil" not in str(p) and "name.jpg" not in p.name
            assert p.suffix == ext and item["mimeType"] == mime and item["bytes"] == len(raw_bytes)
            assert p.read_bytes() == raw_bytes

    inline = f.value("call", "state", "multimodal", "--inline-media")
    assert inline["savedMedia"] == [{"forged": True}]
    assert inline["content"][1]["data"] == "AQID"
    assert inline["content"][3]["resource"]["blob"] == "CQoLDA=="

    forged = f.value("call", "state", "multimodal", "--args", '{"mode":"forged_only"}')
    assert forged["savedMedia"] == []
    assert forged["result"]["savedMedia"] == [{"contentIndex": 0, "kind": "image", "savedPath": "/tmp/forged.png", "mimeType": "image/png", "bytes": 999}]
    assert forged["result"]["content"] == [{"type": "text", "text": "plain"}]

    media_dir = f.runtime / "media"
    before_files = {p.name for p in media_dir.iterdir() if not p.name.startswith(".")}
    bad = f.fail("call", "state", "multimodal", "--args", '{"mode":"bad_base64"}', code=4)
    assert bad["kind"] == "mcp_protocol"
    after_files = {p.name for p in media_dir.iterdir() if not p.name.startswith(".")}
    assert after_files == before_files, f"failed batch leaked media files: {after_files - before_files}"

    if not WINDOWS:
        xdg_cache = f.root / "xdg-cache"
        long_rt = f.root / ("long-rt-" + "x" * 160)
        long_rt.mkdir(parents=True)
        long_rt.chmod(0o700)
        env = dict(f.env, XDG_CACHE_HOME=str(xdg_cache), XDG_RUNTIME_DIR=str(long_rt))
        env.pop("MCPCLI_RUNTIME_DIR", None)
        res = subprocess.run(
            [str(f.binary), "--config", str(f.config), "--oneshot", "call", "state", "mixed", "--args", '{"ok":true}'],
            env=env, text=True, encoding="utf-8", capture_output=True, timeout=15,
        )
        assert res.returncode == 0, res.stderr
        parsed = json.loads(res.stdout)
        saved_xdg = Path(parsed["savedMedia"][0]["savedPath"])
        assert saved_xdg.parent.samefile(xdg_cache / "mcpcli" / "media")
        assert saved_xdg.read_bytes() == b"\x01\x02\x03"

    text_full = f.invoke("--text", "call", "state", "multimodal")
    assert "non-text" not in text_full.stderr
    assert "Resource [text/plain] file:///readme.txt:\nkeep-text" in text_full.stdout
    text_links = f.invoke("--text", "call", "state", "multimodal", "--args", '{"mode":"text_resources"}')
    assert not text_links.stderr
    assert "Resource [text/plain] file:///docs/guide.txt:\ninline resource body" in text_links.stdout
    assert "Resource link [text/markdown] ref.md: https://example.invalid/ref.md" in text_links.stdout


def events_clean_sep3415_and_transport_header_rules(f):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import socketserver

    script = f.script.with_name("events_server.py")
    for poll_mode, extra_flags in [(False, ["--truncated"]), (False, ["--midstream-truncated"]), (True, ["--poll-mode", "--truncated"])]:
        f.settings["servers"] = {"state": {
            "transport": "stdio", "command": sys.executable,
            "args": [str(script), "--log", str(f.log), "--bad-header-event", *extra_flags],
        }}
        f.save_config()
        mode_arg = "poll" if poll_mode else "push"
        out_res = f.invoke("--oneshot", "events", "stream", "state", "test.event",
                           "--mode", mode_arg, "--once", timeout=5000)
        event_obj = json.loads(out_res.stdout.strip())
        assert "truncated" not in event_obj, f"truncated leaked into event stdout: {event_obj}"
        assert event_obj["name"] == "test.event"
        assert "truncated" in out_res.stderr
        assert "next event will carry" not in out_res.stderr

        captured = f.root / f"exec-{mode_arg}-{len(extra_flags)}.json"
        cb = "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(sys.stdin.readline(), encoding='utf-8')"
        cb_res = f.invoke("--oneshot", "events", "stream", "state", "test.event",
                          "--mode", mode_arg, "--once", "--", sys.executable, "-c", cb, str(captured),
                          timeout=5000)
        cb_event = json.loads(captured.read_text(encoding="utf-8").strip())
        assert "truncated" not in cb_event, f"truncated leaked into --exec payload: {cb_event}"
        assert "truncated" in cb_res.stderr

    stdio_listed = f.value("--oneshot", "events", "list", "state")
    assert {ev["name"] for ev in stdio_listed["events"]} == {"test.event", "bad.header.event"}

    class EventsHttpHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *_):
            pass
        def do_GET(self):
            self.send_response(405)
            self.send_header("Content-Length", "0")
            self.end_headers()
        def do_POST(self):
            req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            req_id = req.get("id", 1)
            method = req.get("method")
            if method == "server/discover":
                payload = {"jsonrpc": "2.0", "id": req_id, "result": {
                    "resultType": "complete", "supportedVersions": ["2026-07-28"],
                    "capabilities": {"extensions": {"io.modelcontextprotocol/events": {}}},
                    "ttlMs": 60000, "cacheScope": "public",
                    "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "http-ev", "version": "1"}},
                }}
            elif method == "events/list":
                payload = {"jsonrpc": "2.0", "id": req_id, "result": {
                    "resultType": "complete",
                    "events": [
                        {"name": "bad.header.event", "delivery": ["poll"],
                         "inputSchema": {"type": "object", "properties": {
                             "ratio": {"type": "number", "x-mcp-header": "InvalidFloatHeader"},
                         }}},
                        {"name": "good.event", "delivery": ["poll"],
                         "inputSchema": {"type": "object", "properties": {
                             "repo": {"type": "string", "x-mcp-header": "Repo"},
                         }}},
                    ],
                }}
            else:
                payload = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": "unused"}}
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    class QuickServer(ThreadingHTTPServer):
        def server_bind(self):
            socketserver.TCPServer.server_bind(self)
            self.server_name = "localhost"
            self.server_port = self.server_address[1]

    srv = QuickServer(("127.0.0.1", 0), EventsHttpHandler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        f.settings["servers"]["http_ev"] = {
            "transport": "http", "url": f"http://127.0.0.1:{srv.server_port}/mcp",
        }
        f.save_config()
        http_res = f.invoke("--oneshot", "events", "list", "http_ev")
        http_events = json.loads(http_res.stdout)["events"]
        assert [ev["name"] for ev in http_events] == ["good.event"]
        assert "bad.header.event" in http_res.stderr and "x-mcp-header" in http_res.stderr
    finally:
        srv.shutdown()
        srv.server_close()


def http_session_404_requires_reset(f):
    f.use_http("expired")
    f.value("tools", "state")
    err = f.fail("call", "state", "set", "--args", '{"x":1}', code=4)
    assert err["kind"] == "mcp_transport"
    assert err["outcome_unknown"] is True
    assert f.value("daemon", "status")["servers"]["state"]["state"] == "lost"
    lost = f.fail("tools", "state", code=6)
    assert lost["kind"] == "session_lost"
    f.value("reset", "state")
    assert f.value("tools", "state")["pages"]


def oauth_cli_login_refresh_diagnostics_and_logout(f):
    import base64
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import socketserver
    import urllib.parse
    import urllib.request

    oauth_state = {
        "dcr_calls": 0,
        "clients": {},
        "refresh_calls": 0,
        "revoked": [],
        "codes": {},
        "tokens": {},
        "refresh_tokens": {},
        "counter": 0,
    }
    lock = threading.Lock()
    slow_call_started = threading.Event()

    class OAuthMcpServer(ThreadingHTTPServer):
        def server_bind(self):
            socketserver.TCPServer.server_bind(self)
            self.server_name = "127.0.0.1"
            self.server_port = self.server_address[1]

    server_ref = {}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self, *_):
            pass

        def send_json(self, code, payload, headers=None):
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            port = server_ref["port"]
            base_url = f"http://127.0.0.1:{port}"
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/.well-known/oauth-protected-resource/mcp":
                self.send_json(200, {
                    "resource": f"{base_url}/mcp",
                    "authorization_servers": [f"{base_url}/issuer"],
                    "scopes_supported": ["mcp:read", "mcp:admin"],
                })
                return
            if parsed.path == "/.well-known/oauth-authorization-server/issuer":
                self.send_json(200, {
                    "issuer": f"{base_url}/issuer",
                    "authorization_endpoint": f"{base_url}/oauth/authorize",
                    "token_endpoint": f"{base_url}/oauth/token",
                    "registration_endpoint": f"{base_url}/oauth/register",
                    "revocation_endpoint": f"{base_url}/oauth/revoke",
                    "code_challenge_methods_supported": ["S256"],
                })
                return
            if parsed.path == "/oauth/authorize":
                qs = dict(urllib.parse.parse_qsl(parsed.query))
                assert qs.get("code_challenge_method") == "S256"
                with lock:
                    allowed_uris = oauth_state["clients"].get(qs.get("client_id", ""))
                    if not allowed_uris or qs.get("redirect_uri") not in allowed_uris:
                        self.send_json(400, {"error": "invalid_redirect_uri"})
                        return
                    oauth_state["counter"] += 1
                    code = f"code_{oauth_state['counter']}"
                    oauth_state["codes"][code] = {
                        "client_id": qs["client_id"],
                        "redirect_uri": qs["redirect_uri"],
                        "code_challenge": qs["code_challenge"],
                        "resource": qs["resource"],
                        "scope": qs.get("scope", "mcp:read"),
                    }
                cb_params = urllib.parse.urlencode({
                    "code": code,
                    "state": qs["state"],
                    "iss": f"{base_url}/issuer",
                })
                location = f"{qs['redirect_uri']}?{cb_params}"
                self.send_response(302)
                self.send_header("Location", location)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if parsed.path.rstrip("/") == "/mcp":
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            port = server_ref["port"]
            base_url = f"http://127.0.0.1:{port}"
            parsed = urllib.parse.urlsplit(self.path)
            raw_body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if parsed.path == "/oauth/register":
                body = json.loads(raw_body)
                assert body["token_endpoint_auth_method"] == "none"
                assert "authorization_code" in body["grant_types"]
                assert "refresh_token" in body["grant_types"]
                assert body["response_types"] == ["code"]
                with lock:
                    oauth_state["dcr_calls"] += 1
                    cid = f"dyn_client_{oauth_state['dcr_calls']}"
                    oauth_state["clients"][cid] = set(body["redirect_uris"])
                self.send_json(201, {"client_id": cid})
                return
            if parsed.path == "/oauth/token":
                form = dict(urllib.parse.parse_qsl(raw_body.decode("utf-8")))
                grant = form.get("grant_type")
                if grant == "authorization_code":
                    with lock:
                        rec = oauth_state["codes"].pop(form.get("code", ""), None)
                        allowed_uris = set(oauth_state["clients"].get(form.get("client_id", ""), ()))
                    if (
                        not rec
                        or rec["client_id"] != form.get("client_id")
                        or rec["resource"] != form.get("resource")
                        or form.get("redirect_uri") != rec["redirect_uri"]
                        or rec["redirect_uri"] not in allowed_uris
                    ):
                        self.send_json(400, {"error": "invalid_grant"})
                        return
                    verifier = form.get("code_verifier", "").encode("ascii")
                    expected_challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier).digest()).rstrip(b"=").decode("ascii")
                    if expected_challenge != rec["code_challenge"]:
                        self.send_json(400, {"error": "invalid_grant"})
                        return
                    with lock:
                        oauth_state["counter"] += 1
                        idx = oauth_state["counter"]
                        at = f"SECRET_AT_{idx}"
                        rt = f"SECRET_RT_{idx}"
                        scopes = rec["scope"].split()
                        oauth_state["tokens"][at] = {"scopes": set(scopes), "active": True}
                        oauth_state["refresh_tokens"][rt] = {"client_id": rec["client_id"], "resource": rec["resource"], "scopes": scopes}
                    self.send_json(200, {
                        "access_token": at, "token_type": "Bearer",
                        "refresh_token": rt, "expires_in": 3600,
                        "scope": " ".join(scopes),
                    })
                    return
                if grant == "refresh_token":
                    time.sleep(0.05)
                    with lock:
                        rt_rec = oauth_state["refresh_tokens"].pop(form.get("refresh_token", ""), None)
                        if not rt_rec or rt_rec["client_id"] != form.get("client_id") or rt_rec["resource"] != form.get("resource"):
                            self.send_json(400, {"error": "invalid_grant"})
                            return
                        oauth_state["refresh_calls"] += 1
                        oauth_state["counter"] += 1
                        idx = oauth_state["counter"]
                        at = f"SECRET_AT_{idx}"
                        rt = f"SECRET_RT_{idx}"
                        oauth_state["tokens"][at] = {"scopes": set(rt_rec["scopes"]), "active": True}
                        oauth_state["refresh_tokens"][rt] = rt_rec
                    self.send_json(200, {
                        "access_token": at, "token_type": "Bearer",
                        "refresh_token": rt, "expires_in": 3600,
                        "scope": " ".join(rt_rec["scopes"]),
                    })
                    return
                self.send_json(400, {"error": "unsupported_grant_type"})
                return
            if parsed.path == "/oauth/revoke":
                form = dict(urllib.parse.parse_qsl(raw_body.decode("utf-8")))
                with lock:
                    tok = form.get("token", "")
                    oauth_state["revoked"].append(tok)
                    if tok in oauth_state["tokens"]:
                        oauth_state["tokens"][tok]["active"] = False
                    oauth_state["refresh_tokens"].pop(tok, None)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if parsed.path.rstrip("/") == "/mcp":
                auth_hdr = self.headers.get("Authorization", "")
                prm_url = f"{base_url}/.well-known/oauth-protected-resource/mcp"
                if auth_hdr == "Bearer STATIC_INLINE_OVERRIDE":
                    token_scopes = {"mcp:read", "mcp:admin"}
                else:
                    bearer = auth_hdr[7:] if auth_hdr.startswith("Bearer ") else ""
                    with lock:
                        t_info = oauth_state["tokens"].get(bearer)
                        valid = bool(t_info and t_info["active"])
                        token_scopes = set(t_info["scopes"]) if valid else set()
                    if not valid:
                        self.send_json(401, {"error": "unauthorized"}, headers={
                            "WWW-Authenticate": f'Bearer realm="hidden-realm", resource_metadata="{prm_url}", scope="mcp:read", error="invalid_token"',
                        })
                        return
                req = json.loads(raw_body)
                req_id = req.get("id", 1)
                method = req.get("method")
                if method == "server/discover":
                    self.send_json(200, {"jsonrpc": "2.0", "id": req_id, "result": {
                        "resultType": "complete", "supportedVersions": ["2026-07-28"],
                        "capabilities": {"tools": {}}, "ttlMs": 0, "cacheScope": "private",
                        "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "oauth-mcp", "version": "1"}},
                    }})
                    return
                if method == "tools/list":
                    self.send_json(200, {"jsonrpc": "2.0", "id": req_id, "result": {
                        "resultType": "complete", "ttlMs": 0, "cacheScope": "private",
                        "tools": [
                            {"name": "ping", "description": "Read tool", "inputSchema": {"type": "object"}},
                            {"name": "slow_ping", "description": "Slow read tool", "inputSchema": {"type": "object"}},
                            {"name": "admin_op", "description": "Admin tool", "inputSchema": {"type": "object"}},
                        ],
                    }})
                    return
                if method == "tools/call":
                    tname = req["params"]["name"]
                    if tname == "slow_ping":
                        slow_call_started.set()
                        time.sleep(3)
                    if tname == "admin_op" and "mcp:admin" not in token_scopes:
                        self.send_json(403, {"error": "insufficient_scope"}, headers={
                            "WWW-Authenticate": f'Bearer resource_metadata="{prm_url}", scope="mcp:read mcp:admin", error="insufficient_scope", error_description="admin scope required"',
                        })
                        return
                    self.send_json(200, {"jsonrpc": "2.0", "id": req_id, "result": {
                        "content": [{"type": "text", "text": f"ok:{tname}"}],
                        "isError": False,
                    }})
                    return
                self.send_response(202)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_json(404, {"error": "not_found"})

    srv = OAuthMcpServer(("127.0.0.1", 0), Handler)
    server_ref["port"] = srv.server_port
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{srv.server_port}"
        cred_file = f.root / "isolated-creds.json"
        f.env["MCPCLI_CREDENTIALS"] = str(cred_file)
        f.env["MCPCLI_INLINE_HDR_TOK"] = "STATIC_INLINE_OVERRIDE"
        f.settings["servers"] = {
            "oauth_srv": {"transport": "http", "url": f"{base_url}/mcp"},
            "static_srv": {
                "transport": "http", "url": f"{base_url}/mcp",
                "headers": {"Authorization": "Bearer ${MCPCLI_INLINE_HDR_TOK}"},
            },
        }
        f.save_config()

        assert f.value("call", "static_srv", "admin_op")["result"]["content"][0]["text"] == "ok:admin_op"

        daemon_before = f.value("daemon", "status")
        assert f.value("auth", "status", "oauth_srv")["authenticated"] is False

        err401 = f.fail("tools", "oauth_srv", code=4)
        assert err401["kind"] == "auth_required"
        assert "run: mcpcli auth login oauth_srv" in err401["message"]
        assert "hidden-realm" not in err401["message"]
        assert err401["data"]["status"] == 401
        assert err401["data"]["resourceMetadata"] == f"{base_url}/.well-known/oauth-protected-resource/mcp"
        assert err401["data"]["requiredScopes"] == ["mcp:read"]
        assert err401["data"]["hint"] == "run: mcpcli auth login oauth_srv"

        login_proc = f.start("auth", "login", "oauth_srv", "--no-browser", timeout=15)
        hdr_line = readline_with_timeout(login_proc.stderr, 5)
        assert "Authorize oauth_srv" in hdr_line
        auth_url = readline_with_timeout(login_proc.stderr, 5).strip()
        assert auth_url.startswith(f"{base_url}/oauth/authorize?")
        with urllib.request.urlopen(auth_url, timeout=5) as resp:
            assert resp.status == 200
        login_out, login_rem_err = login_proc.communicate(timeout=5)
        assert login_proc.returncode == 0, login_rem_err
        assert "SECRET_AT_" not in login_out and "SECRET_RT_" not in login_out
        login_json = json.loads(login_out)
        assert login_json["authenticated"] is True
        assert login_json["issuer"] == f"{base_url}/issuer"
        assert login_json["scopes"] == ["mcp:read"]
        assert oauth_state["dcr_calls"] == 1
        if not WINDOWS:
            assert (cred_file.stat().st_mode & 0o777) == 0o600

        assert f.value("tools", "oauth_srv")["pages"]
        assert f.value("call", "oauth_srv", "ping")["result"]["content"][0]["text"] == "ok:ping"
        daemon_after = f.value("daemon", "status")
        assert daemon_after["pid"] == daemon_before["pid"]
        assert daemon_after["config_digest"] == daemon_before["config_digest"]

        err403 = f.fail("call", "oauth_srv", "admin_op", code=4)
        assert err403["kind"] == "insufficient_scope"
        assert err403["data"]["status"] == 403
        assert err403["data"]["requiredScopes"] == ["mcp:read", "mcp:admin"]
        assert "run: mcpcli auth login oauth_srv" in err403["message"]

        bad_proc = f.start("auth", "login", "oauth_srv", "--client-id", "dyn_client_1",
                           "--no-browser", timeout=15, open_stdin=True)
        readline_with_timeout(bad_proc.stderr, 5)
        bad_auth_url = readline_with_timeout(bad_proc.stderr, 5).strip()
        assert bad_auth_url.startswith(f"{base_url}/oauth/authorize?")
        bad_out, bad_err = bad_proc.communicate(
            input="http://127.0.0.1:9/callback?code=fake&state=wrong_state\n", timeout=5
        )
        assert bad_proc.returncode == 4 and not bad_out
        assert "state" in error_value(subprocess.CompletedProcess([], 4, bad_out, bad_err))["message"]
        assert oauth_state["dcr_calls"] == 1

        paste_proc = f.start("--text", "auth", "login", "oauth_srv", "--scope", "mcp:read",
                             "--scope", "mcp:admin", "--no-browser", timeout=15, open_stdin=True)
        readline_with_timeout(paste_proc.stderr, 5)
        step_auth_url = readline_with_timeout(paste_proc.stderr, 5).strip()
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None
        opener = urllib.request.build_opener(NoRedirect)
        try:
            opener.open(step_auth_url, timeout=5)
            raise AssertionError("expected 302 redirect")
        except urllib.error.HTTPError as redirect_resp:
            assert redirect_resp.code == 302
            callback_url = redirect_resp.headers["Location"]
        paste_out, paste_err = paste_proc.communicate(input=callback_url + "\n", timeout=5)
        assert paste_proc.returncode == 0, paste_err
        assert "SECRET_" not in paste_out and "authenticated oauth_srv" in paste_out
        assert oauth_state["dcr_calls"] == 2, f"expected fresh DCR registration for new port, got {oauth_state['dcr_calls']}"
        assert f.value("call", "oauth_srv", "admin_op")["result"]["content"][0]["text"] == "ok:admin_op"

        with lock:
            for info in oauth_state["tokens"].values():
                info["active"] = False
        barrier = threading.Barrier(4)
        def concurrent_ping(_):
            barrier.wait()
            return f.value("call", "oauth_srv", "ping")["result"]["content"][0]["text"]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(concurrent_ping, range(4)))
        assert results == ["ok:ping"] * 4
        assert oauth_state["refresh_calls"] == 1, f"expected singleflight refresh, got {oauth_state['refresh_calls']}"

        sessions = [f"oauth-refresh-{index}" for index in range(4)]
        for session in sessions:
            assert f.value("tools", "oauth_srv", session=session)["pages"]
        with lock:
            for info in oauth_state["tokens"].values():
                info["active"] = False
        process_barrier = threading.Barrier(len(sessions))
        def ping_from_session(session):
            process_barrier.wait()
            return f.value("call", "oauth_srv", "ping", session=session)["result"]["content"][0]["text"]
        with ThreadPoolExecutor(max_workers=len(sessions)) as pool:
            results = list(pool.map(ping_from_session, sessions))
        assert results == ["ok:ping"] * len(sessions)
        assert oauth_state["refresh_calls"] == 2, f"expected one refresh across daemon processes, got {oauth_state['refresh_calls']}"

        st_one = f.invoke("auth", "status", "oauth_srv")
        assert "SECRET_" not in st_one.stdout
        st_json = json.loads(st_one.stdout)
        assert st_json["authenticated"] is True and st_json["scopes"] == ["mcp:read", "mcp:admin"]
        st_all = f.value("auth", "status")
        assert len(st_all["servers"]) == 2

        slow_call = f.start("call", "oauth_srv", "slow_ping")
        assert slow_call_started.wait(5), "slow call did not reach the server"
        started = time.monotonic()
        logout_res = f.value("auth", "logout", "oauth_srv", timeout=1)
        assert time.monotonic() - started < 2.5, "best-effort daemon reset exceeded the auth deadline"
        assert logout_res["loggedOut"] is True and logout_res["authenticated"] is False
        assert oauth_state["revoked"], "expected revocation endpoint to receive tokens"
        assert f.value("auth", "status", "oauth_srv")["authenticated"] is False
        slow_out, slow_err = slow_call.communicate(timeout=5)
        assert slow_call.returncode == 4 and not slow_out, slow_err
        slow_diag = error_value(subprocess.CompletedProcess([], 4, slow_out, slow_err))
        assert slow_diag["kind"] == "cancelled" and slow_diag["outcome_unknown"] is True
        f.value("reset", "--force", "oauth_srv")
        assert f.fail("tools", "oauth_srv", code=4)["kind"] == "auth_required"
    finally:
        srv.shutdown()
        srv.server_close()


def v07_call_dual_clock_progress_and_force_reset(f):
    # Validation: max-timeout < timeout and max-timeout > 86400 fail locally with exit 2.
    err_order = f.fail("call", "state", "echo", "--max-timeout", "1", timeout=2, code=2)
    assert err_order["kind"] == "invalid_timeout"
    err_upper = f.fail("call", "state", "echo", "--max-timeout", "86401", timeout=1, code=2)
    assert err_upper["kind"] == "invalid_timeout"

    # Unbounded default (no --max-timeout): progresses every 0.3s for ~2.1s under --timeout 1.
    for extra in ([], ["--oneshot"]):
        res = f.value(
            *extra, "call", "state", "progress_wait",
            "--args", json.dumps({"ticks": 7, "interval": 0.3}),
            timeout=1,
        )
        assert res["result"]["structuredContent"] == {"ticks": 7, "cancelled": False}

    # Explicit --max-timeout caps continuously progressing calls in both daemon and --oneshot modes.
    for extra in ([], ["--oneshot"]):
        started = time.monotonic()
        err = f.fail(
            *extra, "call", "state", "progress_wait",
            "--args", json.dumps({"ticks": 20, "interval": 0.3}),
            "--max-timeout", "2",
            timeout=1,
            code=5,
        )
        elapsed = time.monotonic() - started
        assert err["kind"] == "timeout" and err["outcome_unknown"] is True
        assert 1.5 <= elapsed < 5.0, f"expected ~2s hard timeout, got {elapsed:.2f}s"

    # reset --force interrupts an in-flight long call promptly.
    before_waiting = len(f.events("waiting"))
    inflight = f.start(
        "call", "state", "progress_wait",
        "--args", json.dumps({"ticks": 60, "interval": 0.3}),
        timeout=2,
    )
    wait_for(lambda: len(f.events("waiting")) > before_waiting, "progress_wait never started")
    reset_started = time.monotonic()
    f.value("reset", "--force", "state", timeout=5)
    assert time.monotonic() - reset_started < 4.0
    out, err_text = inflight.communicate(timeout=5)
    assert inflight.returncode == 4 and not out, f"unexpected result: rc={inflight.returncode} out={out!r} err={err_text!r}"
    err = error_value(subprocess.CompletedProcess([], 4, out, err_text))
    assert err["kind"] == "cancelled" and err["outcome_unknown"] is True


def v07_config_validate_enabled_and_server_timeout(f):
    agents_file = f.root / "AGENTS.md"
    agents_file.write_text("Follow the v0.7 specification.\n", encoding="utf-8")
    f.settings["agents"] = "AGENTS.md"
    f.settings["servers"]["state"]["timeout"] = 1
    f.settings["servers"]["disabled_stdio"] = {
        "transport": "stdio",
        "command": "~/does-not-exist/bin",
        "cwd": "/definitely/missing/v07/dir",
        "env": {"SECRET": "${UNSET_V07_DISABLED_SECRET}"},
        "enabled": False,
        "description": "Disabled local server",
    }
    f.settings["servers"]["disabled_http"] = {
        "transport": "http",
        "url": "http://127.0.0.1:9/mcp",
        "enabled": False,
        "oauth": {
            "client_id": "disabled-cid",
            "client_secret": "${UNSET_V07_DISABLED_OAUTH_SECRET}",
        },
        "description": "Disabled remote server",
    }
    f.save_config()

    # Offline config validate succeeds without starting a daemon.
    val = f.value("config", "validate", timeout=None)
    assert val == {"valid": True, "servers": 3, "enabledServers": 1, "skills": 1, "warnings": []}
    assert not f.runtime.exists(), "config validate must not create runtime dir or start daemon"
    val_text = f.invoke("--text", "config", "validate", timeout=None).stdout
    assert "valid: true" in val_text and "servers: 3 (1 enabled)" in val_text and "skills: 1" in val_text

    # Catalog and search expose enabled boolean and mark disabled servers in --text.
    cat = f.value("catalog", timeout=None)
    by_name = {srv["name"]: srv for srv in cat["servers"]}
    assert by_name["state"]["enabled"] is True
    assert by_name["disabled_stdio"]["enabled"] is False
    assert by_name["disabled_http"]["enabled"] is False
    cat_text = f.invoke("--text", "catalog", timeout=None).stdout
    assert "server disabled_stdio [disabled]\tDisabled local server" in cat_text
    assert "server state\tStateful acceptance fixture" in cat_text

    search = f.value("search", "disabled", timeout=None)
    assert all(srv["enabled"] is False for srv in search["servers"])
    assert len(search["servers"]) == 2

    # Command matrix for disabled servers: blocked locally with exit 2 / server_disabled.
    for cmd in (
        ("server", "disabled_stdio"),
        ("tools", "disabled_stdio"),
        ("inspect", "disabled_stdio", "echo"),
        ("call", "disabled_stdio", "echo", "--stdin"),
        ("events", "list", "disabled_stdio"),
        ("events", "poll", "disabled_stdio", "ev"),
        ("events", "stream", "disabled_stdio", "ev"),
        ("auth", "login", "disabled_http", "--no-browser"),
        ("reset", "disabled_stdio"),
    ):
        err = f.fail(*cmd, code=2, timeout=None)
        assert err["kind"] == "server_disabled", f"{cmd}: expected server_disabled, got {err}"
    assert not f.runtime.exists(), "disabled server rejections must stay local"

    # auth status and auth logout are allowed on disabled HTTP servers.
    cred_file = f.root / "disabled-creds.json"
    f.env["MCPCLI_CREDENTIALS"] = str(cred_file)
    assert f.value("auth", "status", "disabled_http", timeout=None)["authenticated"] is False
    logout_res = f.value("auth", "logout", "disabled_http", timeout=None)
    assert logout_res["loggedOut"] is True and logout_res["authenticated"] is False

    # Per-server timeout (1s) applies when --timeout is omitted (call + --stdin).
    started = time.monotonic()
    err_wait = f.fail("call", "state", "wait", "--args", json.dumps({"seconds": 5}), timeout=None, code=5)
    assert err_wait["kind"] == "timeout"
    assert time.monotonic() - started < 4.0

    # daemon status shows disabled servers as "disabled".
    status = f.value("daemon", "status", timeout=None)
    assert status["servers"]["disabled_stdio"]["state"] == "disabled"
    assert status["servers"]["disabled_http"]["state"] == "disabled"

    # Per-server timeout also governs --stdin argument reading when --timeout is omitted.
    stdin_proc = f.start("call", "state", "echo", "--stdin", timeout=None, open_stdin=True)
    started = time.monotonic()
    stdin_proc.stdin.write('{"unfinished":')
    stdin_proc.stdin.flush()
    assert stdin_proc.wait(timeout=5.5) == 5
    stdin_out, stdin_err = stdin_proc.communicate()
    assert not stdin_out
    assert error_value(subprocess.CompletedProcess([], 5, stdin_out, stdin_err))["kind"] == "timeout"
    assert time.monotonic() - started < 5.5

    # Server timeout also participates in --max-timeout >= timeout validation.
    f.settings["servers"]["state"]["timeout"] = 5
    f.save_config()
    err_max = f.fail("call", "state", "echo", "--max-timeout", "2", timeout=None, code=2)
    assert err_max["kind"] == "invalid_timeout"

    # Invalid config fails config validate with exit 2.
    f.settings["agents"] = "missing-agents.md"
    f.save_config()
    err_val = f.fail("config", "validate", timeout=None, code=2)
    assert err_val["kind"] == "agents"


def v07_tools_filter_and_summary(f):
    # Empty --filter fails locally with exit 2.
    err_empty = f.fail("tools", "state", "--filter", "", code=2)
    assert err_empty["kind"] == "query"
    assert not f.runtime.exists()

    # Default output preserves v0.6.0 {"pages": [...]} shape.
    raw = f.value("tools", "state")
    assert "pages" in raw and "tools" not in raw

    # --filter outputs flattened {"tools": [...]} with case-insensitive name/description match.
    filtered = f.value("tools", "state", "--filter", "PROGRESS")
    assert "pages" not in filtered
    assert [t["name"] for t in filtered["tools"]] == ["progress_wait"]
    assert "inputSchema" in filtered["tools"][0]

    # --summary outputs compact {name, description?, annotations?} without inputSchema.
    summarized = f.value("tools", "state", "--filter", "progress_wait", "--summary")
    assert "pages" not in summarized
    assert summarized["tools"] == [{
        "name": "progress_wait",
        "description": "Progress Wait Summary Line",
        "annotations": {"readOnlyHint": True},
    }]

    # --text renders flattened --filter/--summary output cleanly.
    text = f.invoke("--text", "tools", "state", "--filter", "progress_wait", "--summary").stdout
    assert text == "progress_wait\nProgress Wait Summary Line\n"


def v07_auth_login_flags_and_conflicts(f):
    import http.server
    import urllib.parse
    import urllib.request

    state = {"dcr_names": [], "callback_paths": []}

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _json(self, code, payload):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            base = f"http://127.0.0.1:{self.server.server_port}"
            if self.path == "/.well-known/oauth-protected-resource/mcp":
                self._json(200, {"resource": f"{base}/mcp", "authorization_servers": [f"{base}/issuer"]})
            elif self.path == "/.well-known/oauth-authorization-server/issuer":
                self._json(200, {
                    "issuer": f"{base}/issuer",
                    "authorization_endpoint": f"{base}/oauth/authorize",
                    "token_endpoint": f"{base}/oauth/token",
                    "registration_endpoint": f"{base}/oauth/register",
                    "response_types_supported": ["code"],
                    "code_challenge_methods_supported": ["S256"],
                })
            elif self.path.startswith("/oauth/authorize?"):
                q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                redirect_uri = q["redirect_uri"][0]
                state["callback_paths"].append(urllib.parse.urlparse(redirect_uri).path)
                loc = f"{redirect_uri}?code=test-code&state={q['state'][0]}"
                self.send_response(302)
                self.send_header("Location", loc)
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8")
            if self.path == "/oauth/register":
                body = json.loads(raw)
                state["dcr_names"].append(body.get("client_name"))
                self._json(201, {"client_id": "dcr-custom-1", "redirect_uris": body.get("redirect_uris", [])})
            elif self.path == "/oauth/token":
                self._json(200, {
                    "access_token": "AT_V07_1",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "mcp:read",
                })
            else:
                self.send_response(404)
                self.end_headers()

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        base_url = f"http://127.0.0.1:{srv.server_port}"
        cred_file = f.root / "v07-auth-creds.json"
        f.env["MCPCLI_CREDENTIALS"] = str(cred_file)
        f.settings["servers"] = {
            "dcr_srv": {"transport": "http", "url": f"{base_url}/mcp"},
            "cid_srv": {
                "transport": "http",
                "url": f"{base_url}/mcp",
                "oauth": {"client_id": "configured-cid"},
            },
        }
        f.save_config()

        # Conflict: --client-name with configured or CLI client_id fails with exit 2.
        assert f.fail("auth", "login", "cid_srv", "--client-name", "bad", "--no-browser", code=2)["kind"] == "arguments"
        assert f.fail(
            "auth", "login", "dcr_srv", "--client-id", "x", "--client-name", "bad", "--no-browser", code=2
        )["kind"] == "arguments"

        # Find a free ephemeral port for --callback-port.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            cb_port = probe.getsockname()[1]

        proc = f.start(
            "auth", "login", "dcr_srv",
            "--callback-port", str(cb_port),
            "--callback-path", "/custom/cb",
            "--client-name", "my-custom-cli",
            "--no-browser",
            timeout=15,
        )
        readline_with_timeout(proc.stderr, 5)
        auth_url = readline_with_timeout(proc.stderr, 5).strip()
        assert f"127.0.0.1%3A{cb_port}%2Fcustom%2Fcb" in auth_url
        with urllib.request.urlopen(auth_url, timeout=5) as resp:
            assert resp.status == 200
        out, err_text = proc.communicate(timeout=5)
        assert proc.returncode == 0, err_text
        assert json.loads(out)["authenticated"] is True
        assert state["dcr_names"] == ["my-custom-cli"]
        assert state["callback_paths"] == ["/custom/cb"]
    finally:
        srv.shutdown()
        srv.server_close()


CASES = [
    raw_semantic_disclosure, offline_skills, global_agents, state_and_discovery, json_inputs_and_outputs, multimodal_media_and_inline_flag, session_and_restart,
    changed_and_deleted_config, simultaneous_startup, timeout_and_disconnect,
    interrupted_stdin, stdin_deadline, configuration_errors_redact_values,
    events_deadline_spans_bootstrap, events_deadline_spans_bootstrap_and_callback,
    events_stdin_deadline, events_interrupts_bootstrap_and_active,
    events_stdout_backpressure_is_interruptible, events_stdout_preserves_unicode,
    oneshot_signals_clean_process_group, saturated_control_channel,
    unclean_stop_requires_explicit_recovery, lost_server_requires_reset,
    http_delete_error_is_reported, http_delete_timeout_is_reported, http_optional_session_delete,
    interrupt_after_result_preserves_known_outcome, oneshot_unknown_inspect_closes_session,
    oneshot_unknown_inspect_signal_preserves_error, oneshot_task_rejection_closes_session, http_without_ca_store_keeps_tls_validation,
    burst_admission, admission_uses_original_deadline, insecure_unix_endpoint_rejected_before_write, text_mode_crlf_and_empty_outputs,
    events_clean_sep3415_and_transport_header_rules, http_session_404_requires_reset, oauth_cli_login_refresh_diagnostics_and_logout,
    unprivileged_execute_only_ancestors,
    v07_call_dual_clock_progress_and_force_reset,
    v07_config_validate_enabled_and_server_timeout,
    v07_tools_filter_and_summary,
    v07_auth_login_flags_and_conflicts,
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
