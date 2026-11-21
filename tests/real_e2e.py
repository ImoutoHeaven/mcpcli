#!/usr/bin/env python3
"""Real-service acceptance tests; run explicitly, not during cargo test.

Requires Linux, Python 3.11+, installed fastexec, @playwright/mcp, and Chromium.
No packages are downloaded. Every CLI invocation is a separate process. The
binary is copied before testing so concurrent builds cannot change the daemon's
build identity. Reports retain complete JSON responses, including image data.

Example:
  python3 tests/real_e2e.py --binary /path/to/mcpcli \
    --fastexec /usr/local/bin/fastexec \
    --playwright /path/to/node_modules/@playwright/mcp/cli.js \
    --chromium /path/to/chrome --report-dir /path/to/report
"""

import argparse
import base64
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import select
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone


PAGE = b"""<!doctype html><html lang="en"><meta charset="utf-8">
<title>mcpcli acceptance fixture</title>
<h1>mcpcli acceptance fixture</h1><button id="increment">Increment</button>
<p id="count"></p><script>
let count = Number(localStorage.getItem("count") || "0");
function render() { document.querySelector("#count").textContent = "Count: " + count; }
document.querySelector("#increment").onclick = () => {
  count++; localStorage.setItem("count", String(count)); render();
}; render();
</script></html>"""


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def file_sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def text_content(result):
    return "\n".join(block["text"] for block in result["result"].get("content", [])
                     if block.get("type") == "text")


def process_stat(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        return {"state": fields[0], "parent": int(fields[1]),
                "group": int(fields[2]), "start": int(fields[19])}
    except (OSError, ValueError, IndexError):
        return None


class OwnedProcesses:
    """Track only this run's inherited marker, using pidfds against PID reuse."""

    def __init__(self, marker):
        self.marker = marker.encode()
        self.processes = {}

    def scan(self):
        snapshot = {}
        for path in Path("/proc").iterdir():
            if path.name.isdigit():
                stat = process_stat(int(path.name))
                if stat is not None:
                    snapshot[int(path.name)] = stat
        owned = {pid for (pid, start), (fd, _) in self.processes.items()
                 if snapshot.get(pid, {}).get("start") == start
                 and not select.select([fd], [], [], 0)[0]}
        for pid in snapshot:
            try:
                if self.marker in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
                    owned.add(pid)
            except OSError:
                pass
        # Chromium may clear environment variables or make /proc/environ
        # unreadable. Follow parents and groups with a currently live owned
        # leader as well; a remembered numeric PGID alone is never sufficient.
        while True:
            groups = {pid for pid in owned if snapshot[pid]["group"] == pid}
            expanded = owned | {pid for pid, stat in snapshot.items()
                                if stat["parent"] in owned or stat["group"] in groups}
            if expanded == owned:
                break
            owned = expanded
        for pid in owned:
            before = snapshot[pid]
            if (pid, before["start"]) in self.processes:
                continue
            fd = None
            try:
                fd = os.pidfd_open(pid)
                after = process_stat(pid)
                if after is None or before["start"] != after["start"]:
                    os.close(fd)
                    continue
                try:
                    executable = os.readlink(f"/proc/{pid}/exe")
                except OSError:
                    executable = None
                record = {"pid": pid, **after, "executable": executable,
                          "name": Path(f"/proc/{pid}/comm").read_text().strip()}
                self.processes[(pid, after["start"])] = (fd, record)
            except OSError:
                if fd is not None:
                    os.close(fd)

    def alive(self, exclude=()):
        self.scan()
        return [record for fd, record in self.processes.values()
                if record["pid"] not in exclude and not select.select([fd], [], [], 0)[0]]

    def wait_empty(self, seconds, exclude=()):
        deadline = time.monotonic() + seconds
        while True:
            alive = self.alive(exclude)
            if not alive or time.monotonic() >= deadline:
                return alive
            time.sleep(0.1)

    def records(self):
        return [record for _, record in self.processes.values()]

    def emergency_cleanup(self):
        # Only pidfds captured from this run's marker are signalled.
        self.scan()
        for fd, _ in self.processes.values():
            if not select.select([fd], [], [], 0)[0]:
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        self.wait_empty(5)

    def close(self):
        for fd, _ in self.processes.values():
            os.close(fd)


class FixtureHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(PAGE)))
        self.end_headers()
        self.wfile.write(PAGE)

    def log_message(self, *_):
        pass


class Acceptance:
    def __init__(self, args, root):
        self.args = args
        self.root = root
        self.report_dir = Path(args.report_dir).resolve()
        self.report_dir.mkdir(parents=True, exist_ok=False)
        self.report = {"started_utc": datetime.now(timezone.utc).isoformat(),
                       "passed": [], "commands": [], "success": False}
        self.sequence = 0
        self.sessions = set()
        self.http_process = None
        self.http_log = None
        self.fixture = None
        self.fixture_thread = None
        self.marker = "MCPCLI_E2E_RUN=" + uuid.uuid4().hex
        self.processes = OwnedProcesses(self.marker)
        self.env = dict(os.environ)
        self.env[self.marker.split("=", 1)[0]] = self.marker.split("=", 1)[1]
        for name in ("runtime", "tmp", "artifacts", "skills/check-browser/references"):
            (root / name).mkdir(parents=True, mode=0o700)
        self.env["TMPDIR"] = str(root / "tmp")
        self.env["MCPCLI_RUNTIME_DIR"] = str(root / "runtime")
        self.env.pop("MCPCLI_CONFIG", None)
        self.env.pop("MCPCLI_SESSION", None)
        self.binary = root / "mcpcli"
        shutil.copy2(Path(args.binary).resolve(), self.binary)
        self.binary.chmod(0o500)
        self.report["binary"] = {
            "source": str(Path(args.binary).resolve()),
            "sha256": file_sha256(self.binary),
            "version": subprocess.check_output([str(self.binary), "--version"],
                                               text=True, env=self.env).strip(),
        }
        self.config_path = root / "mcpcli.json"

    def passed(self, name):
        self.report["passed"].append(name)
        print("PASS " + name, flush=True)

    def command(self, *arguments, session="primary", input_json=None, expected=0):
        self.sequence += 1
        argv = [str(self.binary), "--config", str(self.config_path),
                "--session", session, "--timeout", "60", *arguments]
        started = time.monotonic()
        result = subprocess.run(argv, input=None if input_json is None else json.dumps(input_json),
                                text=True, capture_output=True, env=self.env,
                                cwd=self.root, timeout=90, check=False)
        prefix = f"{self.sequence:03d}"
        (self.report_dir / (prefix + ".stdout")).write_text(result.stdout)
        (self.report_dir / (prefix + ".stderr")).write_text(result.stderr)
        self.report["commands"].append({
            "id": prefix, "session": session, "arguments": list(arguments),
            "exit_code": result.returncode,
            "seconds": round(time.monotonic() - started, 3),
            "stdout": prefix + ".stdout", "stderr": prefix + ".stderr",
        })
        self.processes.scan()
        require(result.returncode == expected,
                f"{arguments} exited {result.returncode}, expected {expected}: "
                f"{result.stderr[:2000]} {result.stdout[:1000]}")
        require(not result.stderr.strip(), f"unexpected stderr: {result.stderr[:2000]}")
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise AssertionError(f"{arguments} did not produce one JSON document") from error

    def call(self, server, tool, arguments=None, session="primary", expected=0):
        result = self.command("call", server, tool, "--stdin", session=session,
                              input_json=arguments or {}, expected=expected)
        require(isinstance(result["result"].get("content"), list), f"{tool}: missing MCP content")
        require(result["result"].get("isError", False) == (expected == 3),
                f"{tool}: wrong isError value")
        return result

    def configure(self):
        (self.root / "AGENTS.md").write_text(
            "# Global Agent Guidance\nInspect schemas and skill references before mutating state.\n")
        (self.root / "skills/check-browser/SKILL.md").write_text(
            "---\nname: check-browser\ndescription: Verify a local browser counter.\n---\n"
            "# Browser check\nRead references/check.md, then inspect browser_snapshot.\n")
        (self.root / "skills/check-browser/references/check.md").write_text(
            "Use the current page snapshot to find Increment. Preserve its exact reference.\n")
        self.fixture = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        self.fixture_thread = threading.Thread(target=self.fixture.serve_forever, daemon=True)
        self.fixture_thread.start()
        self.page_url = f"http://127.0.0.1:{self.fixture.server_port}/counter"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        browser_args = [
            str(Path(self.args.playwright).resolve()), "--headless", "--no-sandbox",
            "--isolated", "--executable-path", str(Path(self.args.chromium).resolve()),
            "--output-dir", str(self.root / "artifacts"), "--image-responses", "allow",
        ]
        self.http_log = (self.report_dir / "playwright-http.log").open("w")
        self.http_process = subprocess.Popen(
            [self.args.node, *browser_args, "--host", "127.0.0.1", "--port", str(port)],
            stdin=subprocess.DEVNULL, stdout=self.http_log, stderr=subprocess.STDOUT,
            env=self.env, cwd=self.root, start_new_session=True,
        )
        deadline = time.monotonic() + 15
        while True:
            self.processes.scan()
            require(self.http_process.poll() is None, "Playwright HTTP server exited during startup")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                require(time.monotonic() < deadline, "Playwright HTTP server did not become ready")
                time.sleep(0.1)
        config = {
            "version": 1,
            "agents": str(self.root / "AGENTS.md"),
            "servers": {
                "fastexec": {
                    "transport": "stdio", "command": str(Path(self.args.fastexec).resolve()),
                    "env": {"FASTEXEC_DAEMON": "off", "TMPDIR": self.env["TMPDIR"]},
                    "cwd": str(self.root), "description": "Task execution and file operations",
                },
                "browser": {
                    "transport": "stdio", "command": self.args.node, "args": browser_args,
                    "cwd": str(self.root), "description": "Microsoft Playwright over stdio",
                },
                "browser-http": {
                    "transport": "http", "url": f"http://127.0.0.1:{port}/mcp",
                    "description": "Microsoft Playwright over Streamable HTTP",
                },
            },
            "skills": [str(self.root / "skills")],
        }
        self.config_path.write_text(json.dumps(config, indent=2))
        self.report["dependencies"] = {
            "fastexec": {"path": str(Path(self.args.fastexec).resolve()),
                        "sha256": file_sha256(self.args.fastexec)},
            "playwright_mcp": subprocess.check_output(
                [self.args.node, self.args.playwright, "--version"], text=True, timeout=10).strip(),
            "chromium": subprocess.check_output(
                [self.args.chromium, "--version"], text=True, timeout=10).strip(),
        }
        self.report["transports"] = ["stdio", "streamable-http"]
        self.report["fixture_url"] = self.page_url

    def check_discovery_and_skills(self):
        catalog = self.command("catalog")
        require("# Global Agent Guidance" in (catalog.get("agents") or {}).get("content", ""),
                "catalog omits full global AGENTS.md content")
        agents_doc = self.command("agents")
        require(agents_doc["agents"] == catalog["agents"],
                "agents command disagrees with catalog global AGENTS.md")
        require({entry["name"] for entry in catalog["servers"]} ==
                {"fastexec", "browser", "browser-http"}, "catalog omits configured servers")
        require([entry["name"] for entry in catalog["skills"]] == ["check-browser"],
                "catalog omits skill metadata")
        search = self.command("search", "browser")
        require(len(search["servers"]) == 2 and len(search["skills"]) == 1,
                "offline capability search failed")
        require(not self.command("daemon", "status")["running"],
                "offline discovery started the daemon")
        require(self.command("skills", "list")["skills"][0]["name"] == "check-browser",
                "skills list failed")
        require("# Browser check" in self.command("skills", "show", "check-browser")["content"],
                "skill body not preserved")
        files = self.command("skills", "files", "check-browser")["files"]
        require("references/check.md" in files, "skill references not listed")
        resource = self.command("skills", "read", "check-browser", "references/check.md")
        require("exact reference" in resource["content"], "skill resource not preserved")
        self.passed("offline catalog/search and progressive skill reading")

    def start_session(self, name):
        self.sessions.add(name)
        status = self.command("daemon", "start", session=name)
        require(status["running"] and status["session"] == name, "daemon start failed")
        require(all(server["state"] == "lazy" for server in status["servers"].values()),
                "daemon eagerly started a server")
        return status

    def check_fastexec(self):
        tools = [tool for page in self.command("tools", "fastexec")["pages"]
                 for tool in page["tools"]]
        require({"exec", "read", "write", "edit"} <= {tool["name"] for tool in tools},
                "fastexec tool catalog is incomplete")
        schema = self.command("inspect", "fastexec", "exec")
        require(schema["inputSchema"]["type"] == "object", "inspect lost inputSchema")
        path = str(self.root / "sample.txt")
        original = "first line\nquoted 'single' and \"double\"; dollar $HOME; 你好\n"
        self.call("fastexec", "write", {"path": path, "content": original})
        require(Path(path).read_text() == original, "JSON stdin altered file content")
        self.call("fastexec", "edit", {
            "path": path, "edits": [{"oldText": "first line", "newText": "edited line"}]})
        read = self.call("fastexec", "read", {"path": path})
        require("edited line" in text_content(read) and "你好" in text_content(read),
                "cross-CLI file edit/read failed")
        self.passed("real fastexec write/edit/read and complete tool schema")

        started = self.call("fastexec", "exec", {
            "action": "start", "command":
                "printf 'ready\\n'; IFS= read -r value; printf 'received:%s\\n' \"$value\"",
            "waitMs": 20, "maxBytes": 4096,
        })
        structured = started["result"].get("structuredContent")
        require(isinstance(structured, dict) and structured.get("taskId"),
                "structuredContent/taskId was not preserved")
        task_id = structured["taskId"]
        require(structured["state"] != "exited", "background task exited before second CLI call")
        polled = self.call("fastexec", "exec", {
            "action": "poll", "taskId": task_id, "input": "cross-cli\n",
            "eof": True, "waitMs": 5000, "maxBytes": 4096,
        })
        require("received:cross-cli" in text_content(polled), "background task state was lost")
        require(polled["result"]["structuredContent"]["exitCode"] == 0, "background command failed")
        self.passed("real fastexec background stdin/poll across CLI processes")

        missing = self.call("fastexec", "read",
                            {"path": str(self.root / "does-not-exist.txt")}, expected=3)
        require(missing["result"]["isError"] is True and text_content(missing),
                "tool isError result was not retained at exit 3")
        self.passed("real tool isError uses exit 3 and retains MCP result")

    def browser_counter(self, server, session):
        names = {item["name"] for page in self.command("tools", server, session=session)["pages"]
                 for item in page["tools"]}
        require({"browser_navigate", "browser_snapshot", "browser_click",
                 "browser_take_screenshot"} <= names, "Playwright catalog incomplete")
        self.call(server, "browser_navigate", {"url": self.page_url}, session=session)
        snapshot = text_content(self.call(server, "browser_snapshot", session=session))
        require("Count: 0" in snapshot, "fresh browser has unexpected storage/state")
        match = re.search(r'button "Increment"[^\n]*\[ref=([^\]]+)\]', snapshot)
        require(match is not None, "snapshot did not expose Increment's DOM reference")
        schema = self.command("inspect", server, "browser_click", session=session)
        properties = schema["inputSchema"]["properties"]
        target_key = "target" if "target" in properties else "ref"
        require(target_key in properties, "unknown Playwright click reference schema")
        self.call(server, "browser_click",
                  {"element": "Increment", target_key: match.group(1)}, session=session)
        after = text_content(self.call(server, "browser_snapshot", session=session))
        require("Count: 1" in after, "DOM reference/state did not survive separate CLI calls")
        return after

    def check_browsers(self):
        self.browser_counter("browser", "primary")
        self.passed("real Playwright stdio navigate/snapshot/reference/click across CLI processes")
        self.start_session("isolated")
        self.call("browser", "browser_navigate", {"url": self.page_url}, session="isolated")
        isolated = text_content(self.call("browser", "browser_snapshot", session="isolated"))
        primary = text_content(self.call("browser", "browser_snapshot"))
        require("Count: 0" in isolated and "Count: 1" in primary,
                "named sessions share browser state")
        self.passed("two named daemon sessions isolate live browser state")

        screenshot = self.call("browser", "browser_take_screenshot", {"type": "png"})
        images = [block for block in screenshot["result"]["content"] if block["type"] == "image"]
        require(images and images[0]["mimeType"] == "image/png" and "data" not in images[0],
                "default screenshot call did not materialize image out of inline JSON")
        saved_media = screenshot.get("savedMedia") or []
        require(saved_media and saved_media[0]["mimeType"] == "image/png",
                "savedMedia PNG entry missing")
        data = Path(saved_media[0]["savedPath"]).read_bytes()
        require(data.startswith(b"\x89PNG\r\n\x1a\n") and data.endswith(b"IEND\xaeB\x60\x82"),
                "saved screenshot PNG was truncated or malformed")
        inline_shot = self.command(
            "call", "browser", "browser_take_screenshot", "--inline-media", "--args", '{"type":"png"}'
        )
        inline_images = [block for block in inline_shot["content"] if block["type"] == "image"]
        inline_bytes = base64.b64decode(inline_images[0]["data"], validate=True)
        require(inline_bytes.startswith(b"\x89PNG\r\n\x1a\n") and inline_bytes.endswith(b"IEND\xaeB\x60\x82"),
                "inline screenshot PNG was truncated or malformed")
        (self.report_dir / "browser.png").write_bytes(data)
        self.report["screenshot_bytes"] = len(data)
        self.passed("complete Playwright screenshot image saved to media cache and preserved with --inline-media")

        self.browser_counter("browser-http", "primary")
        self.passed("real Playwright Streamable HTTP /mcp stateful tool calls")
        # The HTTP server is independently managed. Close its browser explicitly;
        # daemon stop owns local transports, not arbitrary remote browser processes.
        self.call("browser-http", "browser_close")

        self.processes.scan()
        records = self.processes.records()
        require(any("chrome" in record["name"] for record in records),
                "cleanup observer did not capture real browser descendants")

    def cleanup(self):
        errors = []
        for session in sorted(self.sessions):
            try:
                self.command("daemon", "stop", session=session)
                require(not self.command("daemon", "status", session=session)["running"],
                        f"{session} daemon still running")
            except Exception as error:
                errors.append(f"stop {session}: {error}")
        # Check stdio server/browser descendants while both this test process and
        # the independently managed HTTP server still exist. The outer executor
        # cannot mask leaked children by cleaning its own task tree.
        excluded = () if self.http_process is None else (self.http_process.pid,)
        survivors = self.processes.wait_empty(8, exclude=excluded)
        if survivors:
            errors.append("processes survived daemon stop: " + json.dumps(survivors))
        if self.http_process is not None:
            self.http_process.terminate()
            try:
                self.http_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.http_process.kill()
                self.http_process.wait(timeout=5)
                errors.append("Playwright HTTP server required SIGKILL")
        survivors = self.processes.wait_empty(5)
        if survivors:
            errors.append("processes survived complete cleanup: " + json.dumps(survivors))
        self.report["observed_processes"] = self.processes.records()
        self.report["cleanup_errors"] = errors
        if errors:
            self.processes.emergency_cleanup()
        self.processes.close()
        if self.http_log is not None:
            self.http_log.close()
        if self.fixture is not None:
            self.fixture.shutdown()
            self.fixture.server_close()
            self.fixture_thread.join(timeout=2)
        if not errors:
            self.passed("daemon/server/browser cleanup observed before outer task exit")
        return errors

    def run(self):
        failure = None
        try:
            self.configure()
            self.check_discovery_and_skills()
            self.start_session("primary")
            self.check_fastexec()
            self.check_browsers()
        except Exception:
            failure = traceback.format_exc()
            print(failure, flush=True)
        finally:
            cleanup_errors = self.cleanup()
            self.report["success"] = failure is None and not cleanup_errors
            self.report["failure"] = failure
            self.report["finished_utc"] = datetime.now(timezone.utc).isoformat()
            (self.report_dir / "report.json").write_text(json.dumps(self.report, indent=2))
        print(json.dumps({"success": self.report["success"],
                          "checks": len(self.report["passed"]),
                          "commands": len(self.report["commands"]),
                          "report": str(self.report_dir / "report.json")}), flush=True)
        return 0 if self.report["success"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--fastexec", default="/usr/local/bin/fastexec")
    parser.add_argument("--playwright", required=True, help="Installed @playwright/mcp/cli.js")
    parser.add_argument("--chromium", required=True, help="Installed Chromium executable")
    parser.add_argument("--node", default="node")
    parser.add_argument("--report-dir", required=True, help="New directory for retained evidence")
    parser.add_argument("--work-root", default=tempfile.gettempdir(),
                        help="Temporary directory; choose disk-backed storage when the system temp directory is RAM-backed")
    args = parser.parse_args()
    root = Path(args.work_root).resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix="e2e-", dir=root) as directory:
        return Acceptance(args, Path(directory)).run()


if __name__ == "__main__":
    raise SystemExit(main())
