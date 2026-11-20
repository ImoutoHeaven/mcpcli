#!/usr/bin/env python3
"""Deterministic MCP wire fixture. No third-party packages or external network."""
import argparse
import json
import os
import queue
import socketserver
import ssl
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import raw_semantics

parser = argparse.ArgumentParser()
parser.add_argument("--http", action="store_true")
parser.add_argument("--tls", action="store_true")
parser.add_argument("--modern", action="store_true")
parser.add_argument("--mode", default="normal")
parser.add_argument("--oversize-bytes", type=int, default=65537)
parser.add_argument("--log", required=True)
args = parser.parse_args()
write_lock = threading.Lock()
state = {}
unlocked = False
get_replies = queue.Queue()
session_replies = {}
session_lock = threading.Lock()


def record(value):
    with write_lock:
        with open(args.log, "a") as out:
            out.write(value + "\n")


record("pid:" + str(os.getpid()))


def result(request, value):
    return {"jsonrpc": "2.0", "id": request["id"], "result": value}


def error(request, code, message):
    return {"jsonrpc": "2.0", "id": request.get("id"), "error": {"code": code, "message": message}}


def tool(name):
    properties = {}
    if name == "get" and args.mode in ("headers", "invalid_headers"):
        properties = {"region": {"type": "object" if args.mode == "invalid_headers" else "string",
                                 "x-mcp-header": "Region"}}
    return {"name": name, "description": "Fixture " + name,
            "inputSchema": {"type": "object", "properties": properties}}


def dispatch(request):
    global unlocked
    method = request.get("method", "")
    record(method)
    if args.mode in ("raw_semantics", "raw_post_sse", "raw_get_sse", "raw_crlf"):
        if "id" not in request:
            return None
        if method == "server/discover":
            return (result(request, raw_semantics.server_result(True)) if args.modern
                    else error(request, -32601, "Legacy fixture"))
        if method == "initialize":
            return result(request, raw_semantics.server_result(False))
        if method == "tools/list":
            return result(request, raw_semantics.list_result(request.get("params", {}).get("cursor")))
        if method == "tools/call":
            name = request["params"]["name"]
            if name.startswith("raw_error_"):
                return {"jsonrpc": "2.0", "id": request["id"],
                        "error": raw_semantics.rpc_error(name)}
            return result(request, raw_semantics.call_result(name))
        return error(request, -32601, "Method not found")
    if method == "server/discover":
        if args.mode == "hang_initialize":
            time.sleep(30)
        if not args.modern:
            return error(request, -32601, "Method not found")
        return result(request, {"resultType": "complete", "supportedVersions": ["2026-07-28"],
                               "capabilities": {"tools": {"listChanged": True}},
                               "ttlMs": 0, "cacheScope": "private"})
    if method == "initialize":
        return result(request, {"protocolVersion": "2025-11-25",
                               "capabilities": {"tools": {"listChanged": True}},
                               "serverInfo": {"name": "fixture", "version": "1"}})
    if "id" not in request:
        return None
    if method == "tools/list":
        if args.mode == "slow_list":
            time.sleep(0.15)
        cursor = request.get("params", {}).get("cursor")
        if cursor is None:
            return result(request, {"tools": [tool("get")], "nextCursor": "p2",
                                    "resultType": "complete", "ttlMs": 0, "cacheScope": "private"})
        names = ["get" if args.mode == "duplicate" else "set",
                 "unlock", "slow", "die", "invalid", "input_required", "spawn_child", "tool_error"]
        if unlocked:
            names.append("admin")
        page = {"tools": [tool(n) for n in names], "resultType": "complete",
                "ttlMs": 0, "cacheScope": "private"}
        if args.mode == "repeat_cursor":
            page["nextCursor"] = "p2"
        return result(request, page)
    if method == "tools/call":
        name = request["params"]["name"]
        values = request["params"].get("arguments", {})
        if name == "spawn_child":
            descendant = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            record("desc:" + str(descendant.pid))
        if name == "die":
            os._exit(7)
        if name == "slow":
            time.sleep(1)
        if name == "set":
            state.update(values)
        if name == "unlock":
            unlocked = True
            if not args.http:
                send({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
        if name == "invalid":
            return error(request, -32602, "Bad fixture arguments")
        if name == "input_required":
            return result(request, {"resultType": "input_required", "inputRequests": {},
                                    "requestState": "opaque"})
        text = json.dumps(state) if name == "get" else "ok"
        if args.mode in ("oversize", "oversize_chunked", "oversize_sse"):
            text = "x" * args.oversize_bytes
        return result(request, {"content": [{"type": "text", "text": text}],
                               "structuredContent": {"name": name},
                               "isError": name == "tool_error"})
    return error(request, -32601, "Method not found")


def send(message):
    with write_lock:
        body = json.dumps(message, separators=(",", ":")).encode("utf-8")
        sys.stdout.buffer.write(body + (b"\r\n" if args.mode == "raw_crlf" else b"\n"))
        sys.stdout.buffer.flush()


if args.http:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def empty(self, status):
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            record("GET")
            if args.mode in ("get_auth_leak", "get_auth_401", "get_auth_403"):
                self.send_response(403 if args.mode == "get_auth_403" else 401)
                self.send_header("WWW-Authenticate", 'Bearer realm="secret-www-auth-do-not-print"')
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if args.mode in ("get_sse", "get_sse_mixed", "get_sse_invalid", "get_sse_oversize", "raw_get_sse"):
                replies = get_replies
                session = self.headers.get("Mcp-Session-Id")
                if args.mode == "raw_get_sse":
                    with session_lock:
                        replies = session_replies.get(session)
                    if replies is None:
                        self.empty(404)
                        return
                self.send_response(200)
                content_type = {
                    "get_sse_mixed": "TEXT/EVENT-STREAM; charset=UTF-8",
                    "get_sse_invalid": "text/event-stream-INVALID; secret=do-not-print",
                }.get(args.mode, "text/event-stream")
                self.send_header("Content-Type", content_type)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                try:
                    while True:
                        response = replies.get(timeout=10)
                        if response is None:
                            self.wfile.write(b"0\r\n\r\n")
                            self.wfile.flush()
                            return
                        body = b"data: " + json.dumps(response, separators=(",", ":")).encode() + b"\n\n"
                        if args.mode == "get_sse_oversize":
                            body = b"data: " + b"x" * args.oversize_bytes + b"\n\n"
                        for offset in range(0, len(body), 16384):
                            chunk = body[offset:offset + 16384]
                            self.wfile.write(("%x\r\n" % len(chunk)).encode() + chunk + b"\r\n")
                            self.wfile.flush()
                        if args.mode == "raw_get_sse":
                            record("session:delivered " + session)
                except (queue.Empty, BrokenPipeError, ConnectionResetError):
                    return
            self.empty(405)

        def do_DELETE(self):
            record("DELETE")
            if args.mode == "raw_get_sse":
                session = self.headers.get("Mcp-Session-Id")
                with session_lock:
                    replies = session_replies.pop(session, None)
                if replies is not None:
                    replies.put(None)
                    record("session:closed " + session)
            if args.mode == "delete_hang":
                time.sleep(30)
            if args.mode == "delete_redirect":
                self.send_response(307)
                self.send_header("Location", "/redirect")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.empty({"delete_error": 500, "delete_expired": 404,
                        "delete_unsupported": 405}.get(args.mode, 200))

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if args.mode in ("auth", "redirect"):
                record(request.get("method", ""))
                self.send_response(401 if args.mode == "auth" else 307)
                self.send_header("Location", "/redirect")
                self.send_header("WWW-Authenticate", 'Bearer realm="secret-www-auth-do-not-print"')
                body = b"secret-body-do-not-print"
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if args.mode == "expired" and request.get("method") == "tools/call":
                record("tools/call")
                self.empty(404)
                return
            if args.mode == "headers" and request.get("method") == "tools/call":
                record("param-region:" + self.headers.get("Mcp-Param-Region", "MISSING"))
                record("method-header:" + self.headers.get("Mcp-Method", "MISSING"))
            response = dispatch(request)
            if request.get("method") == "tools/call" and args.mode in (
                    "get_sse", "get_sse_mixed", "get_sse_invalid", "get_sse_oversize",
                    "get_auth_401", "get_auth_403", "raw_get_sse"):
                replies = get_replies
                if args.mode == "raw_get_sse":
                    with session_lock:
                        replies = session_replies.get(self.headers.get("Mcp-Session-Id"))
                    if replies is None:
                        self.empty(404)
                        return
                replies.put(response)
                self.empty(202)
                return
            if response is None:
                self.empty(202)
                return
            body = json.dumps(response, separators=(",", ":")).encode()
            content_type = "application/json"
            if args.mode == "mixed_case_ct":
                content_type = "Application/JSON; charset=UTF-8"
            elif args.mode == "jsonp_ct":
                content_type = "application/jsonp; charset=utf-8"
            elif args.mode in ("sse", "oversize_sse", "raw_post_sse"):
                body = b"data: " + body + b"\n\n"
                content_type = "text/event-stream"
            elif args.mode == "mixed_case_sse":
                body = b"data: " + body + b"\n\n"
                content_type = "TEXT/EVENT-STREAM; charset=UTF-8"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            if args.mode == "oversize_chunked":
                self.send_header("Transfer-Encoding", "chunked")
            else:
                self.send_header("Content-Length", str(len(body)))
            if request.get("method") == "initialize":
                session = "fixture-session"
                if args.mode == "raw_get_sse":
                    session = uuid.uuid4().hex
                    with session_lock:
                        session_replies[session] = queue.Queue()
                    record("session:opened " + session)
                self.send_header("Mcp-Session-Id", session)
            self.end_headers()
            try:
                if args.mode == "oversize_chunked":
                    for offset in range(0, len(body), 65536):
                        chunk = body[offset:offset + 65536]
                        self.wfile.write(("%x\r\n" % len(chunk)).encode() + chunk + b"\r\n")
                    self.wfile.write(b"0\r\n\r\n")
                else:
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    class LoopbackServer(ThreadingHTTPServer):
        def server_bind(self):
            # HTTPServer's reverse DNS lookup can stall macOS CI before the readiness line.
            socketserver.TCPServer.server_bind(self)
            self.server_name = "localhost"
            self.server_port = self.server_address[1]

    server = LoopbackServer(("127.0.0.1", 0), Handler)
    if args.tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        fixture = Path(__file__).resolve().parent
        context.load_cert_chain(fixture / "untrusted-test-cert.pem", fixture / "untrusted-test-key.pem")
        server.socket = context.wrap_socket(server.socket, server_side=True)
    print(server.server_port, flush=True)
    server.serve_forever()
else:
    for line in sys.stdin:
        request = json.loads(line)
        if request.get("params", {}).get("name") == "slow":
            def delayed(request=request):
                response = dispatch(request)
                if response is not None:
                    send(response)
            threading.Thread(target=delayed, daemon=True).start()
        else:
            response = dispatch(request)
            if response is not None:
                send(response)
