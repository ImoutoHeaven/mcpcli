"""Synthetic protocol values whose optional and extension fields must survive CLI output."""

INSTRUCTIONS = "  Keep indentation.\n\n中文 instructions\nFinal line.\n"
NAMES = ["raw_object", "raw_null", "raw_tool_error", "raw_error_object", "raw_error_null"]


def server_result(modern):
    body = {
        "capabilities": {"tools": {"listChanged": True, "x-tool-capability": None}},
        "instructions": INSTRUCTIONS,
        "_meta": {"fixture": {"nested": [True, None, "metadata"]}},
        "x-server-extension": {"kept": None},
    }
    info = {"name": "raw-fixture", "version": "1", "title": "Raw fixture",
            "websiteUrl": "https://example.invalid/fixture", "x-info": [None, 7]}
    if modern:
        body.update(resultType="complete", supportedVersions=["2026-07-28"],
                    ttlMs=0, cacheScope="private")
        body["_meta"]["io.modelcontextprotocol/serverInfo"] = info
    else:
        body.update(protocolVersion="2025-11-25", serverInfo=info)
    return body


def tool(name):
    return {
        "name": name, "title": "Full tool", "description": "First line.\n  Second line.\n",
        "inputSchema": {"type": "object", "properties": {"value": {
            "anyOf": [{"type": "string"}, {"type": "null"}], "default": None}},
            "additionalProperties": False, "x-schema": {"nested": [None, True]}},
        "outputSchema": {"type": "object", "properties": {"value": {"type": "number"}}},
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "x-annotation": None},
        "icons": [{"src": "https://example.invalid/icon.svg", "mimeType": "image/svg+xml"}],
        "_meta": {"fixture": "raw-tool"}, "x-tool-extension": {"preserve": None},
    }


def call_result(name):
    return {
        "content": [{"type": "text", "text": "  first\nsecond\n", "x-content": None}],
        "structuredContent": None if name == "raw_null" else {"nested": [None, True, {"value": 7}]},
        "isError": name == "raw_tool_error",
        "_meta": {"fixture": ["raw-result", None]}, "x-result": {"explicit": None},
    }


def list_result(cursor):
    body = {"tools": [tool(name) for name in (NAMES[:2] if cursor is None else NAMES[2:])],
            "resultType": "complete", "ttlMs": 0, "cacheScope": "private",
            "_meta": {"page": 1 if cursor is None else 2}, "x-page": None}
    if cursor is None:
        body["nextCursor"] = "p2"
    return body


def rpc_error(name):
    return {"code": -32602, "message": "raw fixture error",
            "data": None if name == "raw_error_null" else {"nested": [None, True, 7]}}
