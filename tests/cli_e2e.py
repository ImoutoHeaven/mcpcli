#!/usr/bin/env python3
"""Black-box CLI acceptance tests. Python stdlib only; no network or browser required."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback


def wait_for(predicate, message, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(message)


def running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[0] != "Z"
    except (OSError, IndexError):
        return False


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
        self.env = dict(os.environ, MCPCLI_RUNTIME_DIR=str(self.runtime), TMPDIR=str(self.root))
        self.script = Path(__file__).resolve().parent / "fixtures" / "cli_server.py"
        self.skill = self.root / "skills" / "ops"
        (self.skill / "references").mkdir(parents=True)
        (self.skill / "SKILL.md").write_text(
            "---\nname: ops\ndescription: >-\n  Inspect stateful services\n  and preserve sessions.\n---\n"
            "# Operations\nRead references/guide.md before calling tools.\n", encoding="utf-8")
        (self.skill / "references" / "guide.md").write_text("Keep the session open.\n", encoding="utf-8")
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
        assert select.select([process.stdout], [], [], 5)[0], "HTTP fixture did not become ready"
        port = int(process.stdout.readline())
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
        options = {"input": input_text} if input_text is not None else {"stdin": subprocess.DEVNULL}
        result = subprocess.run(command, env=self.env, text=True, capture_output=True,
                                timeout=wall_timeout, **options)
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

    def start(self, *args, session="default", timeout=10, open_stdin=False):
        process = subprocess.Popen(self.command(*args, session=session, timeout=timeout), env=self.env,
            stdin=subprocess.PIPE if open_stdin else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                pass
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
                command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
                if marker.encode() not in command:
                    continue
                os.kill(pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
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
                                 env=f.env, text=True, capture_output=True, timeout=3)
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
    assert shown["root"] == str(f.skill) and "# Operations" in shown["content"]
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
    wait_for(lambda: connected_data_sockets(status["pid"], socket_path) >= 8,
             "slow server queue did not fill", seconds=8)
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
    process = f.start("call", "state", "echo", "--stdin", open_stdin=True)
    process.stdin.write('{"unfinished":')
    process.stdin.flush()

    def listening_for_sigint():
        try:
            status = Path(f"/proc/{process.pid}/status").read_text()
            flags = next(line.split()[1] for line in status.splitlines() if line.startswith("SigCgt:"))
            return int(flags, 16) & (1 << (signal.SIGINT - 1))
        except (OSError, StopIteration):
            return False

    wait_for(listening_for_sigint, "CLI did not register interruption handling")
    process.send_signal(signal.SIGINT)
    assert process.wait(timeout=5) == 130, "reading incomplete stdin ignored Ctrl-C"
    stdout, stderr = process.communicate()
    assert not stdout and json.loads(stderr)["error"]["kind"] == "cancelled"
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
    for signum, expected in ((signal.SIGINT, 130), (signal.SIGTERM, 143)):
        previous = len(f.events("child"))
        process = f.start("--oneshot", "call", "state", "spawn_wait", "--args",
                          '{"seconds":30}', timeout=40)
        wait_for(lambda: len(f.events("child")) > previous, "oneshot did not spawn descendant")
        child = f.events("child")[-1]
        wait_for(lambda: f.events("waiting", pid=child["pid"]), "oneshot call did not start waiting")
        process.send_signal(signum)
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
    wait_for(lambda: connected_data_sockets(status["pid"], socket_path) >= 32,
             "32 outstanding CLI calls never filled data admission", seconds=12)
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
    os.kill(status["pid"], signal.SIGKILL)
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
    result = f.invoke("--oneshot", "call", "state", "get", code=1)
    assert json.loads(result.stdout)["structuredContent"] == {"name": "get"}
    assert error_value(result)["kind"] == "cleanup"
    assert "do not replay" in error_value(result)["message"]
    assert log.read_text().splitlines().count("tools/call") == 1
    f.call("get")
    assert f.fail("reset", "state", code=1)["kind"] == "cleanup"
    assert f.value("daemon", "status")["servers"]["state"]["state"] == "lost"
    assert f.fail("call", "state", "get", code=6)["kind"] == "session_lost"
    assert f.fail("reset", "state", code=1)["kind"] == "cleanup", "cleanup failure was forgotten"
    assert log.read_text().splitlines().count("tools/call") == 2, "cleanup error replayed a tool call"
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


def http_optional_session_delete(f):
    for mode in ("delete_expired", "delete_unsupported"):
        log = f.use_http(mode)
        result = f.invoke("--oneshot", "call", "state", "get")
        assert json.loads(result.stdout)["structuredContent"] == {"name": "get"}
        assert not result.stderr
        assert log.read_text().splitlines().count("DELETE") == 1
    assert not f.runtime.exists()


CASES = [
    offline_skills, state_and_discovery, json_inputs_and_outputs, session_and_restart,
    changed_and_deleted_config, simultaneous_startup, timeout_and_disconnect,
    interrupted_stdin, stdin_deadline, configuration_errors_redact_values,
    oneshot_signals_clean_process_group, saturated_control_channel,
    unclean_stop_requires_explicit_recovery, lost_server_requires_reset,
    http_delete_error_is_reported, http_delete_timeout_is_reported, http_optional_session_delete,
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--artifacts", type=Path, default=Path(tempfile.gettempdir()) / "mcpcli-e2e")
    parser.add_argument("--case", action="append", choices=[case.__name__ for case in CASES])
    parser.add_argument("--report", type=Path)
    options = parser.parse_args()
    source_binary = options.binary.resolve(strict=True)
    options.artifacts.mkdir(parents=True, exist_ok=True)
    snapshot = Path(tempfile.mkdtemp(prefix="binary-", dir=options.artifacts))
    binary = snapshot / "mcpcli"
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
            case(fixture)
            outcome["passed"] = True
        except Exception:
            outcome["passed"] = False
            outcome["error"] = traceback.format_exc()
        finally:
            cleanup_failures = fixture.cleanup()
        if cleanup_failures:
            outcome["passed"] = False
            outcome["error"] = outcome.get("error", "") + "\n".join(cleanup_failures)
        if outcome["passed"]:
            print(f"PASS {case.__name__}", flush=True)
        else:
            print(f"FAIL {case.__name__}\n{outcome['error']}", flush=True)
        outcome["seconds"] = round(time.monotonic() - started, 3)
        outcomes.append(outcome)
    report = {"passed": sum(case["passed"] for case in outcomes), "total": len(outcomes),
              "source_binary": str(source_binary), "binary": str(binary),
              "sha256": binary_hash, "cases": outcomes}
    if options.report:
        options.report.parent.mkdir(parents=True, exist_ok=True)
        options.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "total": report["total"]}), flush=True)
    return 0 if report["passed"] == report["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
