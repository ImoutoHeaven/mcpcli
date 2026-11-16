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
        names = ["get", "set", "increment", "echo", "mixed", "error", "wait", "spawn_wait", "unlock", "die"]
        with state_lock:
            if unlocked:
                names.append("admin")
        send({"jsonrpc": "2.0", "id": request["id"], "result": {
            "tools": [{"name": name, "description": "Fixture " + name,
                       "inputSchema": {"type": "object", "additionalProperties": True}}
                      for name in names]
        }})
    elif method == "tools/call":
        if request["params"]["name"] in ("wait", "spawn_wait"):
            threading.Thread(target=call, args=(request,), daemon=True).start()
        else:
            call(request)
    else:
        send({"jsonrpc": "2.0", "id": request["id"],
              "error": {"code": -32601, "message": "Unknown fixture method"}})
