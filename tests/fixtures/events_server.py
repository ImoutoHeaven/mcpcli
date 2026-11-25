#!/usr/bin/env python3
"""MCP Events fixture with observable waits for deadline and cancellation tests."""
import argparse
import json
import os
import sys
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument("--log", required=True)
parser.add_argument("--discover-delay", type=float, default=0)
parser.add_argument("--list-delay", type=float, default=0)
parser.add_argument("--event-delay", type=float, default=0)
parser.add_argument("--payload-bytes", type=int, default=0)
parser.add_argument("--truncated", action="store_true")
parser.add_argument("--midstream-truncated", action="store_true")
parser.add_argument("--poll-mode", action="store_true")
parser.add_argument("--bad-header-event", action="store_true")
args = parser.parse_args()
output_lock = threading.Lock()
log_lock = threading.Lock()
pending_lock = threading.Lock()
pending = {}


def record(event, **fields):
    with log_lock:
        with open(args.log, "a", encoding="utf-8") as output:
            output.write(json.dumps({"event": event, "pid": os.getpid(), **fields}) + "\n")


def send(message):
    with output_lock:
        print(json.dumps(message), flush=True)


def response(request, result):
    send({"jsonrpc": "2.0", "id": request["id"], "result": result})


def handle(request):
    method = request["method"]
    if method == "server/discover":
        record("discover")
        time.sleep(args.discover_delay)
        response(request, {
            "resultType": "complete", "supportedVersions": ["2026-07-28"],
            "capabilities": {"extensions": {"io.modelcontextprotocol/events": {}}},
            "ttlMs": 60000, "cacheScope": "public",
            "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "events-fixture", "version": "1"}},
        })
    elif method == "events/list":
        record("list")
        time.sleep(args.list_delay)
        events = [{
            "name": "test.event", "delivery": ["poll"] if args.poll_mode else ["push"],
            "inputSchema": {"type": "object"}, "payloadSchema": {"type": "object"},
        }]
        if args.bad_header_event:
            events.append({
                "name": "bad.header.event", "delivery": ["push"],
                "inputSchema": {"type": "object", "properties": {
                    "ratio": {"type": "number", "x-mcp-header": "InvalidFloatHeader"},
                }},
                "payloadSchema": {"type": "object"},
            })
        response(request, {"resultType": "complete", "events": events})
    elif method == "events/poll":
        record("poll")
        response(request, {
            "resultType": "complete",
            "events": [{
                "name": "test.event", "eventId": "poll-one",
                "timestamp": "2026-11-22T00:00:00Z",
                "data": {"message": "ordinary event 中文 😀", "padding": "x" * args.payload_bytes},
            }],
            "cursor": "poll-after",
            "truncated": args.truncated,
            "hasMore": False,
            "nextPollMs": 1000,
        })
    elif method == "events/stream":
        cancelled = threading.Event()
        with pending_lock:
            pending[request["id"]] = cancelled
        meta = {"io.modelcontextprotocol/subscriptionId": request["id"]}
        send({"jsonrpc": "2.0", "method": "notifications/events/active",
              "params": {"cursor": "before", "truncated": args.truncated,
                         "heartbeatMs": 30000, "_meta": meta}})
        if args.midstream_truncated:
            send({"jsonrpc": "2.0", "method": "notifications/events/active",
                  "params": {"cursor": "mid", "truncated": True,
                             "heartbeatMs": 30000, "_meta": meta}})
        record("active", request_id=request["id"])
        if not cancelled.wait(args.event_delay):
            send({"jsonrpc": "2.0", "method": "notifications/events/event",
                  "params": {"name": "test.event", "eventId": "one",
                             "timestamp": "2026-11-22T00:00:00Z",
                             "data": {"message": "ordinary event 中文 😀", "padding": "x" * args.payload_bytes}, "cursor": "after", "_meta": meta}})
            record("delivered", request_id=request["id"])
    else:
        send({"jsonrpc": "2.0", "id": request["id"],
              "error": {"code": -32601, "message": "not implemented"}})


record("start")
for line in sys.stdin:
    request = json.loads(line)
    if request.get("method") == "notifications/cancelled":
        request_id = request.get("params", {}).get("requestId")
        with pending_lock:
            cancelled = pending.pop(request_id, None)
        if cancelled is not None:
            cancelled.set()
        record("cancel", request_id=request_id, matched=cancelled is not None)
    elif "id" in request:
        threading.Thread(target=handle, args=(request,), daemon=True).start()
record("eof")
