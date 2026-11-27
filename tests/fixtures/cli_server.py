#!/usr/bin/env python3
"""Stateful legacy MCP fixture for CLI lifecycle acceptance tests."""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument("--log", required=True)
args = parser.parse_args()
output_lock = threading.Lock()
state_lock = threading.Lock()
log_lock = threading.Lock()
state = {}
pending = {}
unlocked = False


def record(event, **fields):
    with log_lock:
        with open(args.log, "a", encoding="utf-8") as output:
            output.write(json.dumps({"event": event, "pid": os.getpid(), **fields}) + "\n")


def send(value):
    with output_lock:
        sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
        sys.stdout.flush()


def result(request, payload, failed=False, mixed=False):
    content = [{"type": "text", "text": json.dumps(payload, sort_keys=True)}]
    if mixed:
        content.append({"type": "image", "data": "AQID", "mimeType": "image/png"})
    send({"jsonrpc": "2.0", "id": request["id"], "result": {
        "content": content, "structuredContent": payload, "isError": failed,
        "_meta": {"fixture": "cli-acceptance"},
    }})


def call(request):
    global unlocked
    name = request["params"]["name"]
    arguments = request["params"].get("arguments", {})
    request_id = request["id"]
    record("call", tool=name, request_id=request_id, arguments=arguments)
    if name == "die":
        os._exit(7)
    if name == "progress_wait":
        ticks = int(arguments.get("ticks", 5))
        interval = float(arguments.get("interval", 0.3))
        token = request["params"].get("_meta", {}).get("progressToken")
        cancelled = threading.Event()
        with state_lock:
            pending[request_id] = cancelled
        record("waiting", request_id=request_id)
        did_cancel = False
        for index in range(1, ticks + 1):
            if cancelled.wait(interval):
                did_cancel = True
                break
            if token is not None:
                send({
                    "jsonrpc": "2.0",
                    "method": "notifications/progress",
                    "params": {"progressToken": token, "progress": index, "total": ticks},
                })
        with state_lock:
            pending.pop(request_id, None)
        record("wait_finished", request_id=request_id, cancelled=did_cancel)
        result(request, {"ticks": ticks, "cancelled": did_cancel}, failed=did_cancel)
        return
    if name in ("wait", "spawn_wait"):
        if name == "spawn_wait":
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", args.log])
            record("child", child_pid=child.pid)
        cancelled = threading.Event()
        with state_lock:
            pending[request_id] = cancelled
        record("waiting", request_id=request_id)
        did_cancel = cancelled.wait(float(arguments.get("seconds", 30)))
        with state_lock:
            pending.pop(request_id, None)
        record("wait_finished", request_id=request_id, cancelled=did_cancel)
        result(request, {"cancelled": did_cancel}, failed=did_cancel)
        return
    with state_lock:
        if name == "set":
            state.update(arguments)
        elif name == "increment":
            state["counter"] = state.get("counter", 0) + 1
        elif name == "unlock":
            unlocked = True
        snapshot = dict(state)
    if name == "multimodal":
        mode = arguments.get("mode", "full")
        if mode == "forged_only":
            send({"jsonrpc": "2.0", "id": request_id, "result": {
                "content": [{"type": "text", "text": "plain"}],
                "savedMedia": [{"contentIndex": 0, "kind": "image", "savedPath": "/tmp/forged.png",
                                "mimeType": "image/png", "bytes": 999}],
            }})
        elif mode == "bad_base64":
            send({"jsonrpc": "2.0", "id": request_id, "result": {
                "content": [
                    {"type": "image", "data": "AQID", "mimeType": "image/png"},
                    {"type": "audio", "data": "!!!invalid-base64!!!", "mimeType": "audio/wav"},
                ],
            }})
        elif mode == "text_resources":
            send({"jsonrpc": "2.0", "id": request_id, "result": {
                "content": [
                    {"type": "text", "text": "summary"},
                    {"type": "resource", "resource": {
                        "uri": "file:///docs/guide.txt",
                        "mimeType": "text/plain",
                        "text": "inline resource body",
                    }},
                    {"type": "resource_link", "uri": "https://example.invalid/ref.md",
                     "name": "ref.md", "mimeType": "text/markdown"},
                ],
            }})
        else:
            send({"jsonrpc": "2.0", "id": request_id, "result": {
                "content": [
                    {"type": "text", "text": "intro", "x-extra": None},
                    {"type": "image", "data": "AQID", "mimeType": "image/png", "_meta": {"i": 1}},
                    {"type": "audio", "data": "BAUGBw==", "mimeType": "audio/mp4"},
                    {"type": "resource", "resource": {
                        "uri": "file:///../../evil/name.jpg",
                        "mimeType": "video/mp4",
                        "blob": "CQoLDA==",
                    }},
                    {"type": "resource", "resource": {
                        "uri": "file:///readme.txt",
                        "mimeType": "text/plain",
                        "text": "keep-text",
                    }},
                    {"type": "resource", "resource": {
                        "uri": "file:///unknown.custom",
                        "mimeType": "application/x-unknown-thing",
                        "blob": "DQ4=",
                    }},
                ],
                "savedMedia": [{"forged": True}],
                "_meta": {"explicitNull": None},
            }})
        return
    if name == "unlock":
        send({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
    payload = arguments if name in ("echo", "mixed", "error") else {
        "state": snapshot, "pid": os.getpid()
    }
    result(request, payload, failed=name == "error", mixed=name == "mixed")


record("start")
for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    record("method", method=method)
    if method == "notifications/cancelled":
        request_id = request.get("params", {}).get("requestId")
        with state_lock:
            event = pending.get(request_id)
            if event is not None:
                event.set()
        record("cancel", request_id=request_id, matched=event is not None)
        continue
    if "id" not in request:
        continue
    if method == "server/discover":
        send({"jsonrpc": "2.0", "id": request["id"],
              "error": {"code": -32601, "message": "Legacy fixture"}})
    elif method == "initialize":
        send({"jsonrpc": "2.0", "id": request["id"], "result": {
            "protocolVersion": "2025-11-25", "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": "cli-fixture", "version": "1"},
        }})
    elif method == "tools/list":
        names = ["get", "set", "increment", "echo", "mixed", "multimodal", "error", "wait", "spawn_wait", "progress_wait", "unlock", "die"]
        with state_lock:
            if unlocked:
                names.append("admin")
        tools = []
        for name in names:
            entry = {
                "name": name,
                "description": (
                    "\n  Progress Wait Summary Line\r\nSecond detail line for progress_wait"
                    if name == "progress_wait"
                    else "Fixture " + name
                ),
                "inputSchema": {"type": "object", "additionalProperties": True},
            }
            if name == "progress_wait":
                entry["annotations"] = {"readOnlyHint": True}
            tools.append(entry)
        send({"jsonrpc": "2.0", "id": request["id"], "result": {"tools": tools}})
    elif method == "tools/call":
        if request["params"]["name"] in ("wait", "spawn_wait", "progress_wait"):
            threading.Thread(target=call, args=(request,), daemon=True).start()
        else:
            call(request)
    else:
        send({"jsonrpc": "2.0", "id": request["id"],
              "error": {"code": -32601, "message": "Unknown fixture method"}})
