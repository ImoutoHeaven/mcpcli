#!/usr/bin/env python3
"""Normal MCP Events server exits coordinated by the regression tests."""
import json
from pathlib import Path
import sys
import time

phase, stop_path, log_path = sys.argv[1:]
stop_path = Path(stop_path)
log_path = Path(log_path)


def send(message):
    print(json.dumps(message), flush=True)


def response(request, result):
    send({"jsonrpc": "2.0", "id": request["id"], "result": result})


def stop():
    print("events fixture stopped: " + phase, file=sys.stderr, flush=True)
    sys.exit(1)


def wait_then_stop():
    while not stop_path.exists():
        time.sleep(0.005)
    stop()


for line in sys.stdin:
    request = json.loads(line)
    if "id" not in request:
        continue
    method = request["method"]
    with log_path.open("a", encoding="utf-8") as log:
        log.write(method + "\n")
    if method == phase:
        stop()
    if method == "server/discover":
        response(request, {
            "resultType": "complete",
            "supportedVersions": ["2026-07-28"],
            "capabilities": {"extensions": {"io.modelcontextprotocol/events": {}}},
            "ttlMs": 60000,
            "cacheScope": "public",
            "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "events-exit", "version": "1"}},
        })
    elif method == "events/list":
        response(request, {"resultType": "complete", "events": [{
            "name": "pr.merged", "delivery": ["poll", "push"],
            "inputSchema": {"type": "object"}, "payloadSchema": {"type": "object"},
        }]})
    elif method == "events/poll":
        response(request, {
            "resultType": "complete", "events": [], "hasMore": False, "nextPollMs": 1000,
        })
    elif method == "events/stream":
        send({
            "jsonrpc": "2.0",
            "method": "notifications/events/active",
            "params": {
                "heartbeatMs": 30000,
                "_meta": {"io.modelcontextprotocol/subscriptionId": request["id"]},
            },
        })
        if phase == "after-active":
            wait_then_stop()
