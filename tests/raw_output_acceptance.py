"""Lossless protocol output matrix using local synthetic servers only."""
from fixtures import raw_semantics as raw


def raw_semantic_disclosure(f):
    from cli_e2e import Fixture, error_value, wait_for

    for modern in [False, True]:
        for transport in ["stdio", "stdio_crlf", "http", "post_sse", "get_sse"]:
            if modern and transport == "get_sse":
                continue
            for oneshot in [False, True]:
                label = f"raw-{modern}-{transport}-{oneshot}"
                case = Fixture(f.binary, f.root, label)
                try:
                    if not transport.startswith("stdio"):
                        mode = {"http": "raw_semantics", "post_sse": "raw_post_sse", "get_sse": "raw_get_sse"}[transport]
                        wire_log = case.use_http(mode, modern=modern)
                    else:
                        wire_log = case.log
                        case.settings["servers"]["state"]["args"] = [
                            str(case.script.with_name("protocol_server.py")),
                            "--mode", "raw_crlf" if transport == "stdio_crlf" else "raw_semantics",
                            "--log", str(case.log),
                            *(["--modern"] if modern else []),
                        ]
                        case.save_config()
                    (case.root / "AGENTS.md").write_text("Local policy is not remote instructions.\n", encoding="utf-8")
                    case.settings["agents"] = "AGENTS.md"
                    case.save_config()
                    flags = ["--oneshot"] if oneshot else []
                    server = case.value(*flags, "server", "state")
                    assert server == {
                        "source": "server/discover" if modern else "initialize",
                        "protocolVersion": "2026-07-28" if modern else "2025-11-25",
                        "result": raw.server_result(modern),
                    }, (label, server)
                    tools = case.value(*flags, "tools", "state")
                    assert tools == {"pages": [raw.list_result(None), raw.list_result("p2")]}, (label, tools)
                    inspected = case.value(*flags, "inspect", "state", "raw_object")
                    assert inspected == raw.tool("raw_object"), (label, inspected)
                    for name in ["raw_object", "raw_null", "raw_tool_error"]:
                        answer = case.value(*flags, "call", "state", name,
                                            code=3 if name == "raw_tool_error" else 0)
                        assert answer == raw.call_result(name), (label, name, answer)
                    for name in ["raw_error_object", "raw_error_null"]:
                        result = case.invoke(*flags, "call", "state", name, code=4)
                        assert not result.stdout, (label, name, result.stdout)
                        diagnostic = error_value(result)
                        assert diagnostic["rpc_error"] == raw.rpc_error(name), (label, name, diagnostic)
                    assert wire_log.read_text().splitlines().count("tools/call") == 5, label
                    def requests():
                        return [line for line in wire_log.read_text().splitlines()
                                if line in {"server/discover", "initialize", "tools/list", "tools/call"}]
                    before = requests()
                    for invalid in ['[]', '{"value":1,"value":2}']:
                        failure = case.invoke(*flags, "call", "state", "raw_object", "--args", invalid, code=2)
                        assert not failure.stdout, (label, failure.stdout)
                    assert requests() == before, (label, "invalid input reached server")
                    if transport == "get_sse":
                        wait_for(lambda: sum(line.startswith("session:delivered ")
                                             for line in wire_log.read_text().splitlines()) == 5,
                                 "GET fixture did not record all five deliveries")
                        events = wire_log.read_text().splitlines()
                        opened = [line.split()[1] for line in events if line.startswith("session:opened ")]
                        delivered = [line.split()[1] for line in events if line.startswith("session:delivered ")]
                        assert len(opened) == len(set(opened)) == (8 if oneshot else 1), (label, opened)
                        assert len(delivered) == 5 and set(delivered) <= set(opened), (label, delivered)
                        if oneshot:
                            closed = [line.split()[1] for line in events if line.startswith("session:closed ")]
                            assert set(closed) == set(opened), (label, closed)
                finally:
                    cleanup = case.cleanup()
                    assert not cleanup, (label, cleanup)
