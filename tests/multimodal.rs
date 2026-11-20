use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use mcpcli::{
    budget::ByteBudget,
    config::{Config, LoadedConfig, ServerConfig},
    daemon::{self, DaemonOptions},
    ipc::{self, Failure, Operation, Request, Response},
    mcp::{FailureKind, McpConnection, McpFailure, McpLimits},
    media::MediaStore,
    platform,
    runtime::{self, Paths},
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const BUDGET: Duration = Duration::from_secs(5);

fn python() -> String {
    std::env::var("MCPCLI_TEST_PYTHON")
        .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into())
}

fn temp_dir() -> tempfile::TempDir {
    #[cfg(unix)]
    return tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    tempfile::tempdir().unwrap()
}

fn fail_kind(error: &anyhow::Error) -> FailureKind {
    error
        .downcast_ref::<McpFailure>()
        .unwrap_or_else(|| panic!("expected McpFailure, got: {error:#}"))
        .kind
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn build_padded_call_response(id: &Value, target_json_len: usize) -> Vec<u8> {
    let template = |pad: &str| {
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{"type": "text", "text": pad}],
                "isError": false,
                "x-unknown": {"explicit": null}
            }
        }))
        .unwrap()
    };
    let base = template("").len();
    assert!(
        target_json_len >= base,
        "target {target_json_len} smaller than base {base}"
    );
    let out = template(&"a".repeat(target_json_len - base));
    assert_eq!(out.len(), target_json_len);
    out
}

#[tokio::test]
async fn stdio_exact_bounds_unknown_fields_and_memory_overload() {
    let dir = temp_dir();
    let base = dir.path().canonicalize().unwrap();
    let script = base.join("stdio_exact.py");
    let log = base.join("calls.log");
    std::fs::write(
        &script,
        r#"import json, sys
crlf = sys.argv[1] == "crlf"
log_path = sys.argv[2]
eol = b"\r\n" if crlf else b"\n"
for raw in sys.stdin.buffer:
    req = json.loads(raw.decode("utf-8"))
    method = req.get("method", "")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(method + "\n")
    if "id" not in req:
        continue
    if method == "server/discover":
        res = {"resultType": "complete", "supportedVersions": ["2026-07-28"],
               "capabilities": {"tools": {}}, "ttlMs": 0, "cacheScope": "private"}
        out = json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": res}, separators=(",", ":")).encode() + eol
    elif method == "tools/list":
        res = {"tools": [{"name": "sized", "description": "d", "inputSchema": {"type": "object"}}],
               "resultType": "complete", "ttlMs": 0, "cacheScope": "private"}
        out = json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": res}, separators=(",", ":")).encode() + eol
    elif method == "tools/call":
        target = int(req["params"]["arguments"]["target"])
        base_obj = {"jsonrpc": "2.0", "id": req["id"], "result": {
            "content": [{"type": "text", "text": ""}], "isError": False, "x-unknown": {"explicit": None}}}
        base_len = len(json.dumps(base_obj, separators=(",", ":")).encode()) + len(eol)
        base_obj["result"]["content"][0]["text"] = "x" * (target - base_len)
        out = json.dumps(base_obj, separators=(",", ":")).encode() + eol
        assert len(out) == target
    sys.stdout.buffer.write(out)
    sys.stdout.buffer.flush()
"#,
    )
    .unwrap();

    let limit = 4096usize;
    for mode in ["lf", "crlf"] {
        for target in [limit - 1, limit] {
            let _ = std::fs::remove_file(&log);
            let cfg = ServerConfig::Stdio {
                command: python(),
                args: vec![
                    script.to_string_lossy().into_owned(),
                    mode.into(),
                    log.to_string_lossy().into_owned(),
                ],
                env: BTreeMap::new(),
                cwd: None,
                description: None,
            };
            let budget = ByteBudget::new(64 * 1024);
            let limits = McpLimits {
                max_response_bytes: limit,
                max_tool_catalog_bytes: 16 * 1024,
                budget: budget.clone(),
            };
            let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
                .await
                .unwrap();
            let args = json!({"target": target}).as_object().unwrap().clone();
            let res = conn.call("sized", args, BUDGET).await.unwrap();
            assert_eq!(res["x-unknown"], json!({"explicit": null}));
            conn.close().await.unwrap();
            assert_eq!(budget.used(), 0);
        }

        // N + 1 rejects via stdio error slot without replay.
        let _ = std::fs::remove_file(&log);
        let cfg = ServerConfig::Stdio {
            command: python(),
            args: vec![
                script.to_string_lossy().into_owned(),
                mode.into(),
                log.to_string_lossy().into_owned(),
            ],
            env: BTreeMap::new(),
            cwd: None,
            description: None,
        };
        let budget = ByteBudget::new(64 * 1024);
        let limits = McpLimits {
            max_response_bytes: limit,
            max_tool_catalog_bytes: 16 * 1024,
            budget: budget.clone(),
        };
        let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
            .await
            .unwrap();
        let started = Instant::now();
        let args = json!({"target": limit + 1}).as_object().unwrap().clone();
        let err = conn.call("sized", args, BUDGET).await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(fail_kind(&err), FailureKind::Transport);
        assert!(err.to_string().contains("MCP stdio response exceeds"));
        assert!(err.downcast_ref::<McpFailure>().unwrap().outcome_unknown);
        let calls = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .filter(|&l| l == "tools/call")
            .count();
        assert_eq!(calls, 1);
        conn.close().await.unwrap();
        assert_eq!(budget.used(), 0);
    }

    // Memory budget overload on stdio terminates immediately and releases lease.
    let _ = std::fs::remove_file(&log);
    let cfg = ServerConfig::Stdio {
        command: python(),
        args: vec![
            script.to_string_lossy().into_owned(),
            "lf".into(),
            log.to_string_lossy().into_owned(),
        ],
        env: BTreeMap::new(),
        cwd: None,
        description: None,
    };
    let tight_budget = ByteBudget::new(1024);
    let limits = McpLimits {
        max_response_bytes: 16 * 1024,
        max_tool_catalog_bytes: 16 * 1024,
        budget: tight_budget.clone(),
    };
    let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
        .await
        .unwrap();
    let args = json!({"target": 2048}).as_object().unwrap().clone();
    let err = conn.call("sized", args, BUDGET).await.unwrap_err();
    assert_eq!(fail_kind(&err), FailureKind::Transport);
    assert!(err.to_string().contains("active memory limit"));
    conn.close().await.unwrap();
    assert_eq!(tight_budget.used(), 0);
}

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Option<(String, BTreeMap<String, String>, Vec<u8>)> {
    let mut reader = tokio::io::BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await.ok()? == 0 {
        return None;
    }
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((k, v)) = trimmed.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    let content_len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; content_len];
    if content_len > 0 {
        reader.read_exact(&mut body).await.ok()?;
    }
    Some((
        request_line.trim_end_matches(['\r', '\n']).to_owned(),
        headers,
        body,
    ))
}

#[tokio::test]
async fn http_json_post_sse_and_get_sse_exact_bounds_and_hard_errors() {
    #[derive(Clone, Copy)]
    enum Mode {
        JsonLen,
        JsonChunked,
        PostSseSingle,
        PostSseWithIdReject,
        PostSseCumulativePass,
        GetSseSingle,
        GetSseWithIdReject,
        GetSseOverload,
    }

    async fn spawn_http_mock(
        mode: Mode,
        sse_limit: usize,
    ) -> (
        ServerConfig,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let get_count = Arc::new(AtomicUsize::new(0));
        let call_count = Arc::new(AtomicUsize::new(0));
        let gets = get_count.clone();
        let calls = call_count.clone();
        let pending_get = Arc::new(tokio::sync::Mutex::new(Vec::<(Value, usize)>::new()));
        let notify = Arc::new(tokio::sync::Notify::new());

        let handle = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let gets = gets.clone();
                let calls = calls.clone();
                let pending_get = pending_get.clone();
                let notify = notify.clone();
                tokio::spawn(async move {
                    while let Some((req_line, _headers, body)) =
                        read_http_request(&mut socket).await
                    {
                        if req_line.starts_with("DELETE ") {
                            let _ = socket
                                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                                .await;
                            return;
                        }
                        if req_line.starts_with("GET ") {
                            gets.fetch_add(1, Ordering::AcqRel);
                            if !matches!(
                                mode,
                                Mode::GetSseSingle
                                    | Mode::GetSseWithIdReject
                                    | Mode::GetSseOverload
                            ) {
                                let _ = socket
                                    .write_all(
                                        b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                    )
                                    .await;
                                continue;
                            }
                            let _ = socket
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                                )
                                .await;
                            loop {
                                let item = {
                                    let mut q = pending_get.lock().await;
                                    if q.is_empty() {
                                        None
                                    } else {
                                        Some(q.remove(0))
                                    }
                                };
                                let Some((id, target)) = item else {
                                    notify.notified().await;
                                    continue;
                                };
                                let mut payload = Vec::new();
                                if matches!(mode, Mode::GetSseWithIdReject | Mode::GetSseOverload) {
                                    payload.extend_from_slice(
                                        b"id: g-1\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":1,\"progress\":1}}\n\n",
                                    );
                                }
                                // In EventBudget: single-line `data: <json>\n\n` retains `6 + json.len() + 1` = `7 + json.len()`.
                                let json_bytes = build_padded_call_response(&id, target - 7);
                                payload.extend_from_slice(b"data: ");
                                payload.extend_from_slice(&json_bytes);
                                payload.extend_from_slice(b"\n\n");
                                let chunk = format!("{:x}\r\n", payload.len());
                                if socket.write_all(chunk.as_bytes()).await.is_err()
                                    || socket.write_all(&payload).await.is_err()
                                    || socket.write_all(b"\r\n").await.is_err()
                                    || socket.flush().await.is_err()
                                {
                                    return;
                                }
                            }
                        }
                        let req: Value = serde_json::from_slice(&body).unwrap();
                        let method = req["method"].as_str().unwrap_or("");
                        if req.get("id").is_none() {
                            let _ = socket
                                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                                .await;
                            continue;
                        }
                        let id = req["id"].clone();
                        if method == "server/discover" {
                            let err = serde_json::to_vec(&json!({
                                "jsonrpc": "2.0", "id": id,
                                "error": {"code": -32601, "message": "legacy"}
                            }))
                            .unwrap();
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                err.len()
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(&err).await;
                            continue;
                        }
                        if method == "initialize" {
                            let init = serde_json::to_vec(&json!({
                                "jsonrpc": "2.0", "id": id,
                                "result": {
                                    "protocolVersion": "2025-11-25",
                                    "capabilities": {"tools": {}},
                                    "serverInfo": {"name": "mock", "version": "1"}
                                }
                            }))
                            .unwrap();
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: s1\r\nContent-Length: {}\r\n\r\n",
                                init.len()
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(&init).await;
                            continue;
                        }
                        if method == "tools/list" {
                            let list = serde_json::to_vec(&json!({
                                "jsonrpc": "2.0", "id": id,
                                "result": {
                                    "tools": [{"name": "sized", "description": "d", "inputSchema": {"type": "object"}}]
                                }
                            }))
                            .unwrap();
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                list.len()
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(&list).await;
                            continue;
                        }
                        if method == "tools/call" {
                            calls.fetch_add(1, Ordering::AcqRel);
                            let target =
                                req["params"]["arguments"]["target"].as_u64().unwrap() as usize;
                            match mode {
                                Mode::JsonLen => {
                                    let out = build_padded_call_response(&id, target);
                                    let head = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                        out.len()
                                    );
                                    let _ = socket.write_all(head.as_bytes()).await;
                                    let _ = socket.write_all(&out).await;
                                }
                                Mode::JsonChunked => {
                                    let out = build_padded_call_response(&id, target);
                                    let _ = socket
                                        .write_all(
                                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n",
                                        )
                                        .await;
                                    for piece in out.chunks(512) {
                                        let hdr = format!("{:x}\r\n", piece.len());
                                        let _ = socket.write_all(hdr.as_bytes()).await;
                                        let _ = socket.write_all(piece).await;
                                        let _ = socket.write_all(b"\r\n").await;
                                    }
                                    let _ = socket.write_all(b"0\r\n\r\n").await;
                                }
                                Mode::PostSseSingle => {
                                    let json_bytes = build_padded_call_response(&id, target - 7);
                                    let mut sse = b"data: ".to_vec();
                                    sse.extend_from_slice(&json_bytes);
                                    sse.extend_from_slice(b"\n\n");
                                    let head = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n",
                                        sse.len()
                                    );
                                    let _ = socket.write_all(head.as_bytes()).await;
                                    let _ = socket.write_all(&sse).await;
                                }
                                Mode::PostSseWithIdReject => {
                                    let _ = socket
                                        .write_all(
                                            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                                        )
                                        .await;
                                    let ev1 = b"id: p-1\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":1,\"progress\":1}}\n\n";
                                    let json_bytes = build_padded_call_response(&id, target - 7);
                                    let mut ev2 = b"data: ".to_vec();
                                    ev2.extend_from_slice(&json_bytes);
                                    ev2.extend_from_slice(b"\n\n");
                                    for part in [ev1.as_slice(), ev2.as_slice()] {
                                        let hdr = format!("{:x}\r\n", part.len());
                                        let _ = socket.write_all(hdr.as_bytes()).await;
                                        let _ = socket.write_all(part).await;
                                        let _ = socket.write_all(b"\r\n").await;
                                        let _ = socket.flush().await;
                                    }
                                    // Hold stream open so timeout would fire if hard error didn't wake caller.
                                    tokio::time::sleep(Duration::from_secs(10)).await;
                                    return;
                                }
                                Mode::PostSseCumulativePass => {
                                    let mut sse = Vec::new();
                                    let pad = "p".repeat(sse_limit - 140);
                                    for idx in 0..4 {
                                        let note = format!(
                                            "id: n-{idx}\ndata: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{\"progressToken\":1,\"progress\":{idx},\"message\":\"{pad}\"}}}}\n\n"
                                        );
                                        assert!(note.len() < sse_limit);
                                        sse.extend_from_slice(note.as_bytes());
                                    }
                                    let json_bytes =
                                        build_padded_call_response(&id, sse_limit - 16);
                                    sse.extend_from_slice(b"data: ");
                                    sse.extend_from_slice(&json_bytes);
                                    sse.extend_from_slice(b"\n\n");
                                    assert!(sse.len() > sse_limit * 3);
                                    let _ = socket
                                        .write_all(
                                            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                                        )
                                        .await;
                                    for piece in sse.chunks(300) {
                                        let hdr = format!("{:x}\r\n", piece.len());
                                        let _ = socket.write_all(hdr.as_bytes()).await;
                                        let _ = socket.write_all(piece).await;
                                        let _ = socket.write_all(b"\r\n").await;
                                        let _ = socket.flush().await;
                                    }
                                    let _ = socket.write_all(b"0\r\n\r\n").await;
                                }
                                Mode::GetSseSingle
                                | Mode::GetSseWithIdReject
                                | Mode::GetSseOverload => {
                                    pending_get.lock().await.push((id, target));
                                    notify.notify_waiters();
                                    let _ = socket
                                        .write_all(
                                            b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n",
                                        )
                                        .await;
                                }
                            }
                        }
                    }
                });
            }
        });

        let cfg = ServerConfig::Http {
            url: format!("http://127.0.0.1:{port}/mcp"),
            headers: BTreeMap::new(),
            description: None,
        };
        (cfg, get_count, call_count, handle)
    }

    let limit = 2048usize;

    // 1. HTTP JSON (Content-Length & chunked) + POST SSE + GET SSE: N-1 and N pass, N+1 rejects.
    for mode in [
        Mode::JsonLen,
        Mode::JsonChunked,
        Mode::PostSseSingle,
        Mode::GetSseSingle,
    ] {
        for target in [limit - 1, limit] {
            let (cfg, _gets, calls, server_task) = spawn_http_mock(mode, limit).await;
            let budget = ByteBudget::new(64 * 1024);
            let limits = McpLimits {
                max_response_bytes: limit,
                max_tool_catalog_bytes: 16 * 1024,
                budget: budget.clone(),
            };
            let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
                .await
                .unwrap();
            let args = json!({"target": target}).as_object().unwrap().clone();
            let res = conn.call("sized", args, BUDGET).await.unwrap();
            assert_eq!(res["x-unknown"], json!({"explicit": null}));
            assert_eq!(calls.load(Ordering::Acquire), 1);
            conn.close().await.unwrap();
            server_task.abort();
            assert_eq!(budget.used(), 0);
        }

        let (cfg, gets, calls, server_task) = spawn_http_mock(mode, limit).await;
        let budget = ByteBudget::new(64 * 1024);
        let limits = McpLimits {
            max_response_bytes: limit,
            max_tool_catalog_bytes: 16 * 1024,
            budget: budget.clone(),
        };
        let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
            .await
            .unwrap();
        let started = Instant::now();
        let args = json!({"target": limit + 1}).as_object().unwrap().clone();
        let err = conn.call("sized", args, BUDGET).await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(fail_kind(&err), FailureKind::Transport);
        assert!(err.to_string().contains("exceeds the"));
        assert_eq!(calls.load(Ordering::Acquire), 1);
        if matches!(mode, Mode::GetSseSingle) {
            assert_eq!(gets.load(Ordering::Acquire), 1);
        }
        conn.close().await.unwrap();
        server_task.abort();
        assert_eq!(budget.used(), 0);
    }

    // 2. POST SSE with prior event ID + N+1 hard rejection: fast error, zero GET reconnects.
    {
        let (cfg, gets, calls, server_task) =
            spawn_http_mock(Mode::PostSseWithIdReject, limit).await;
        let budget = ByteBudget::new(64 * 1024);
        let limits = McpLimits {
            max_response_bytes: limit,
            max_tool_catalog_bytes: 16 * 1024,
            budget: budget.clone(),
        };
        let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
            .await
            .unwrap();
        let started = Instant::now();
        let args = json!({"target": limit + 1}).as_object().unwrap().clone();
        let err = conn.call("sized", args, BUDGET).await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(fail_kind(&err), FailureKind::Transport);
        assert!(err.to_string().contains("SSE event exceeds"));
        // Initial legacy handshake sent 1 GET (405); zero reconnect GETs occurred after POST SSE failed.
        assert_eq!(gets.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        conn.close().await.unwrap();
        server_task.abort();
        assert_eq!(budget.used(), 0);
    }

    // 3. POST SSE per-event < N, cumulative > 3*N succeeds even with tight process budget (< cumulative bytes).
    {
        let (cfg, _gets, calls, server_task) =
            spawn_http_mock(Mode::PostSseCumulativePass, limit).await;
        let budget = ByteBudget::new(limit * 5 / 2);
        let limits = McpLimits {
            max_response_bytes: limit,
            max_tool_catalog_bytes: 16 * 1024,
            budget: budget.clone(),
        };
        let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
            .await
            .unwrap();
        let args = json!({"target": limit}).as_object().unwrap().clone();
        let res = conn.call("sized", args, BUDGET).await.unwrap();
        assert_eq!(res["x-unknown"], json!({"explicit": null}));
        assert_eq!(calls.load(Ordering::Acquire), 1);
        conn.close().await.unwrap();
        server_task.abort();
        assert_eq!(budget.used(), 0);
    }

    // 4. Background GET SSE with prior event ID + oversize AND memory overload: fast error, GET == 1, isolated from stdio.
    for (mode, max_resp, mem_cap, target, expected_msg) in [
        (
            Mode::GetSseWithIdReject,
            limit,
            64 * 1024,
            limit + 1,
            "SSE event exceeds",
        ),
        (
            Mode::GetSseOverload,
            64 * 1024,
            1024,
            1800,
            "active memory limit",
        ),
    ] {
        let (cfg, gets, calls, server_task) = spawn_http_mock(mode, max_resp).await;
        let budget = ByteBudget::new(mem_cap);
        let limits = McpLimits {
            max_response_bytes: max_resp,
            max_tool_catalog_bytes: 16 * 1024,
            budget: budget.clone(),
        };
        let mut conn = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
            .await
            .unwrap();
        let started = Instant::now();
        let args = json!({"target": target}).as_object().unwrap().clone();
        let err = conn.call("sized", args, BUDGET).await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "degraded to timeout: {:?}",
            started.elapsed()
        );
        assert_eq!(fail_kind(&err), FailureKind::Transport);
        assert!(
            err.to_string().contains(expected_msg),
            "unexpected message: {err}"
        );
        assert_eq!(gets.load(Ordering::Acquire), 1, "triggered GET reconnect");
        assert_eq!(calls.load(Ordering::Acquire), 1, "replayed tools/call");
        conn.close().await.unwrap();
        server_task.abort();
        assert_eq!(budget.used(), 0);
    }
}

#[tokio::test]
async fn tool_catalog_cap_is_separate_and_concurrent_leases_never_deadlock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let big_catalog = Arc::new(AtomicBool::new(true));
    let catalog_flag = big_catalog.clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let server_barrier = barrier.clone();

    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let catalog_flag = catalog_flag.clone();
            let server_barrier = server_barrier.clone();
            tokio::spawn(async move {
                while let Some((req_line, _, body)) = read_http_request(&mut socket).await {
                    if req_line.starts_with("DELETE ") {
                        let _ = socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                    let req: Value = serde_json::from_slice(&body).unwrap();
                    if req.get("id").is_none() {
                        let _ = socket
                            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        continue;
                    }
                    let id = req["id"].clone();
                    let method = req["method"].as_str().unwrap_or("");
                    if method == "server/discover" {
                        let out = serde_json::to_vec(&json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {"resultType": "complete", "supportedVersions": ["2026-07-28"],
                                       "capabilities": {"tools": {}}, "ttlMs": 0, "cacheScope": "private"}
                        }))
                        .unwrap();
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                            out.len()
                        );
                        let _ = socket.write_all(head.as_bytes()).await;
                        let _ = socket.write_all(&out).await;
                    } else if method == "tools/list" {
                        let desc = if catalog_flag.load(Ordering::Acquire) {
                            "d".repeat(600)
                        } else {
                            "ok".into()
                        };
                        let out = serde_json::to_vec(&json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {"tools": [{"name": "echo", "description": desc, "inputSchema": {"type": "object"}}],
                                       "resultType": "complete", "ttlMs": 0, "cacheScope": "private"}
                        }))
                        .unwrap();
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                            out.len()
                        );
                        let _ = socket.write_all(head.as_bytes()).await;
                        let _ = socket.write_all(&out).await;
                    } else if method == "tools/call" {
                        let sync_chunks = req["params"]["arguments"]
                            .get("sync")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let target_bytes = req["params"]["arguments"]
                            .get("bytes")
                            .and_then(Value::as_u64)
                            .unwrap_or(6000) as usize;
                        let out = build_padded_call_response(&id, target_bytes);
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n",
                            )
                            .await;
                        let split = target_bytes.saturating_sub(200);
                        for (idx, part) in [&out[..split], &out[split..]].into_iter().enumerate() {
                            let hdr = format!("{:x}\r\n", part.len());
                            if socket.write_all(hdr.as_bytes()).await.is_err()
                                || socket.write_all(part).await.is_err()
                                || socket.write_all(b"\r\n").await.is_err()
                                || socket.flush().await.is_err()
                            {
                                return;
                            }
                            if idx == 0 && sync_chunks {
                                let _ = tokio::time::timeout(
                                    Duration::from_millis(500),
                                    server_barrier.wait(),
                                )
                                .await;
                                tokio::time::sleep(Duration::from_millis(80)).await;
                            }
                        }
                        let _ = socket.write_all(b"0\r\n\r\n").await;
                    }
                }
            });
        }
    });

    let cfg = ServerConfig::Http {
        url: format!("http://127.0.0.1:{port}/mcp"),
        headers: BTreeMap::new(),
        description: None,
    };
    let shared_budget = ByteBudget::new(12_500);
    let limits = McpLimits {
        max_response_bytes: 32_768,
        max_tool_catalog_bytes: 400,
        budget: shared_budget.clone(),
    };
    let mut conn1 = McpConnection::connect_with_limits(&cfg, BUDGET, limits.clone())
        .await
        .unwrap();
    let cat_err = conn1.list_tool_pages(BUDGET).await.unwrap_err();
    assert_eq!(fail_kind(&cat_err), FailureKind::Protocol);
    assert!(cat_err.to_string().contains("Tool catalog exceeds"));

    // Shrink catalog below 400 B: call response of 6000 B (> 400 B catalog limit) succeeds!
    big_catalog.store(false, Ordering::Release);
    let res = conn1.call("echo", Map::new(), BUDGET).await.unwrap();
    assert_eq!(res["isError"], false);

    // Two concurrent 6000 B calls sharing a 12500 B budget must never deadlock.
    let mut conn2 = McpConnection::connect_with_limits(&cfg, BUDGET, limits)
        .await
        .unwrap();
    let sync1 = json!({"sync": true, "bytes": 6000})
        .as_object()
        .unwrap()
        .clone();
    let sync2 = json!({"sync": true, "bytes": 7200})
        .as_object()
        .unwrap()
        .clone();
    let started = Instant::now();
    let (r1, r2) = tokio::join!(
        conn1.call("echo", sync1, BUDGET),
        conn2.call("echo", sync2, BUDGET),
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        r1.is_err() || r2.is_err(),
        "12500 B budget admitted two overlapping 6000 B calls simultaneously"
    );
    for r in [&r1, &r2] {
        if let Err(err) = r {
            assert!(err.to_string().contains("active memory limit"), "{err}");
        }
    }
    conn1.close().await.unwrap();
    conn2.close().await.unwrap();
    server.abort();
    assert_eq!(shared_budget.used(), 0);
}

async fn read_ipc_frame<R: tokio::io::AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<Option<T>> {
    ipc::read_frame_with_budget(reader, max, ByteBudget::global()).await
}

async fn write_ipc_frame<W: tokio::io::AsyncWrite + Unpin, T: serde::Serialize>(
    writer: &mut W,
    value: &T,
    max: usize,
) -> std::io::Result<()> {
    let (frame, _lease) = ipc::encode_frame_leased(value, max, ByteBudget::global())?;
    writer.write_all(&frame).await?;
    writer.flush().await
}

#[tokio::test]
async fn ipc_two_stage_clocks_and_protocol_v3_rejection() {
    let dir = temp_dir();
    let root = dir.path().canonicalize().unwrap().join("rt");
    let cfg_path = dir.path().canonicalize().unwrap().join("mcpcli.json");
    std::fs::write(&cfg_path, r#"{"version":1,"servers":{}}"#).unwrap();
    let paths = Paths::with_root(cfg_path.clone(), "default", &root).unwrap();

    // 1. Mock local IPC listener to test exact two-stage client clocks and protocol mismatch.
    let mut listener = platform::local::Listener::bind(paths.socket.as_os_str()).unwrap();
    platform::secure_endpoint(&paths.socket).unwrap();

    // Case A: Protocol mismatch (daemon replies with protocol 2) -> daemon_mismatch (code 6).
    let server_paths = paths.clone();
    let accept_a = tokio::spawn(async move {
        let mut conn = listener.accept().await.unwrap();
        let _req: Request = read_ipc_frame(&mut conn, ipc::REQUEST_LIMIT)
            .await
            .unwrap()
            .unwrap();
        let bad = Response {
            protocol: 2,
            namespace: server_paths.namespace.clone(),
            build: server_paths.build.clone(),
            epoch: "e1".into(),
            result: Some(json!({"ok": true})),
            error: None,
        };
        write_ipc_frame(&mut conn, &bad, ipc::RESPONSE_LIMIT)
            .await
            .unwrap();
        listener
    });
    let req = paths
        .request(
            "d",
            Operation::Call {
                server: "s".into(),
                tool: "t".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_secs(2),
        )
        .unwrap();
    let err = runtime::exchange(&paths, &req).await.unwrap_err();
    let failure = err.downcast_ref::<Failure>().unwrap();
    assert_eq!(failure.code, 6);
    assert_eq!(failure.kind, "daemon_mismatch");
    let mut listener = accept_a.await.unwrap();

    // Case B: Late upstream answer (250ms of 350ms execution budget) + 280ms steady body transfer
    // with 180ms idle timeout (total ~530ms > 350ms execution budget, no explicit deadline) -> PASSES!
    let server_paths = paths.clone();
    let accept_b = tokio::spawn(async move {
        let mut conn = listener.accept().await.unwrap();
        let _req: Request = read_ipc_frame(&mut conn, ipc::REQUEST_LIMIT)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        let resp = Response {
            protocol: ipc::PROTOCOL,
            namespace: server_paths.namespace.clone(),
            build: server_paths.build.clone(),
            epoch: "e1".into(),
            result: Some(json!({"payload": "z".repeat(4096)})),
            error: None,
        };
        let (frame, _lease) =
            ipc::encode_frame_leased(&resp, ipc::RESPONSE_LIMIT, ByteBudget::global()).unwrap();
        // Send 4-byte header right before the 350ms execution budget expires, then trickle body.
        conn.write_all(&frame[..4]).await.unwrap();
        conn.flush().await.unwrap();
        for piece in frame[4..].chunks(600) {
            tokio::time::sleep(Duration::from_millis(45)).await;
            conn.write_all(piece).await.unwrap();
            conn.flush().await.unwrap();
        }
        listener
    });
    let req = paths
        .request_with_explicit(
            "d",
            Operation::Call {
                server: "s".into(),
                tool: "t".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_millis(350),
            false,
        )
        .unwrap();
    let dispatched = AtomicBool::new(false);
    let ok_resp = runtime::exchange_tracked_with_options(
        &paths,
        &req,
        &dispatched,
        Duration::from_millis(180),
        ByteBudget::global(),
    )
    .await
    .unwrap();
    assert_eq!(
        ok_resp.result.unwrap()["payload"].as_str().unwrap().len(),
        4096
    );
    let mut listener = accept_b.await.unwrap();

    // Case C: Body transfer stalls longer than idle_timeout -> FAILS with timeout & outcome_unknown.
    let server_paths = paths.clone();
    let accept_c = tokio::spawn(async move {
        let mut conn = listener.accept().await.unwrap();
        let _req: Request = read_ipc_frame(&mut conn, ipc::REQUEST_LIMIT)
            .await
            .unwrap()
            .unwrap();
        let resp = Response {
            protocol: ipc::PROTOCOL,
            namespace: server_paths.namespace.clone(),
            build: server_paths.build.clone(),
            epoch: "e1".into(),
            result: Some(json!({"payload": "z".repeat(2048)})),
            error: None,
        };
        let (frame, _lease) =
            ipc::encode_frame_leased(&resp, ipc::RESPONSE_LIMIT, ByteBudget::global()).unwrap();
        conn.write_all(&frame[..200]).await.unwrap();
        conn.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(320)).await;
        listener
    });
    let req = paths
        .request_with_explicit(
            "d",
            Operation::Call {
                server: "s".into(),
                tool: "t".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_secs(2),
            false,
        )
        .unwrap();
    let dispatched = AtomicBool::new(false);
    let err = runtime::exchange_tracked_with_options(
        &paths,
        &req,
        &dispatched,
        Duration::from_millis(120),
        ByteBudget::global(),
    )
    .await
    .unwrap_err();
    let failure = err.downcast_ref::<Failure>().unwrap();
    assert_eq!(failure.code, 5);
    assert_eq!(failure.kind, "timeout");
    assert!(failure.outcome_unknown);
    let mut listener = accept_c.await.unwrap();

    // Case D: Explicit user short total deadline is NOT extended by 60s transfer idle timeout.
    let server_paths = paths.clone();
    let accept_d = tokio::spawn(async move {
        let mut conn = listener.accept().await.unwrap();
        let _req: Request = read_ipc_frame(&mut conn, ipc::REQUEST_LIMIT)
            .await
            .unwrap()
            .unwrap();
        let resp = Response {
            protocol: ipc::PROTOCOL,
            namespace: server_paths.namespace.clone(),
            build: server_paths.build.clone(),
            epoch: "e1".into(),
            result: Some(json!({"payload": "z".repeat(2048)})),
            error: None,
        };
        let (frame, _lease) =
            ipc::encode_frame_leased(&resp, ipc::RESPONSE_LIMIT, ByteBudget::global()).unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        conn.write_all(&frame[..64]).await.unwrap();
        conn.flush().await.unwrap();
        for piece in frame[64..].chunks(256) {
            tokio::time::sleep(Duration::from_millis(80)).await;
            if conn.write_all(piece).await.is_err() {
                break;
            }
        }
    });
    let started = Instant::now();
    let req = paths
        .request_with_explicit(
            "d",
            Operation::Call {
                server: "s".into(),
                tool: "t".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_millis(260),
            true,
        )
        .unwrap();
    let dispatched = AtomicBool::new(false);
    let err = runtime::exchange_tracked_with_options(
        &paths,
        &req,
        &dispatched,
        Duration::from_secs(60),
        ByteBudget::global(),
    )
    .await
    .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    let failure = err.downcast_ref::<Failure>().unwrap();
    assert_eq!(failure.code, 5);
    assert_eq!(failure.kind, "timeout");
    assert!(failure.outcome_unknown);
    accept_d.await.unwrap();
    paths.remove_socket().unwrap();

    // 2. Real daemon rejects incoming protocol 2 handshake with daemon_mismatch (code 6).
    let loaded = LoadedConfig {
        path: cfg_path.clone(),
        config: Config {
            version: 1,
            agents: None,
            servers: BTreeMap::new(),
            skills: Vec::new(),
        },
        digest: "d".into(),
    };
    let daemon_paths = paths.clone();
    let daemon_task = tokio::spawn(async move {
        daemon::run_with_options(daemon_paths, loaded, DaemonOptions::default())
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !paths.running().unwrap() {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();

    let mut bad_req = paths
        .request("d", Operation::Status, Duration::from_secs(2))
        .unwrap();
    bad_req.protocol = 2;
    let mut client = platform::local::connect(paths.control_socket.as_os_str())
        .await
        .unwrap();
    write_ipc_frame(&mut client, &bad_req, ipc::REQUEST_LIMIT)
        .await
        .unwrap();
    let resp: Response = read_ipc_frame(&mut client, ipc::RESPONSE_LIMIT)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resp.protocol, ipc::PROTOCOL);
    assert_eq!(resp.error.unwrap().kind, "daemon_mismatch");
    drop(client);

    runtime::stop(&paths).await.unwrap();
    daemon_task.await.unwrap();
}

fn read_shared_clock(clock_file: &Path) -> std::io::Result<u64> {
    let text = std::fs::read_to_string(clock_file)?;
    text.trim()
        .parse::<u64>()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn write_shared_clock(clock_file: &Path, ms: u64) {
    let tmp = clock_file.with_extension("tmp");
    std::fs::write(&tmp, ms.to_string()).unwrap();
    platform::replace_file(&tmp, clock_file).unwrap();
}

#[tokio::test]
async fn cross_process_media_helper_entry() {
    let Ok(mode) = std::env::var("MCPCLI_MM_HELPER_MODE") else {
        return;
    };
    let media_dir = PathBuf::from(std::env::var("MCPCLI_MM_MEDIA_DIR").unwrap());
    let clock_file = PathBuf::from(std::env::var("MCPCLI_MM_CLOCK_FILE").unwrap());
    let max_bytes: u64 = std::env::var("MCPCLI_MM_MAX_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let store = MediaStore::with_options(
        media_dir.clone(),
        max_bytes,
        60_000,
        Duration::from_secs(5),
        Arc::new(move || read_shared_clock(&clock_file)),
    );
    let budget = ByteBudget::new(16 * 1024 * 1024);

    match mode.as_str() {
        "gc" => {
            let used = store.run_gc().await.unwrap();
            println!("USED:{used}");
        }
        "save_and_complete" => {
            let size: usize = std::env::var("MCPCLI_MM_PAYLOAD_BYTES")
                .unwrap()
                .parse()
                .unwrap();
            let b64 = encode_base64(&vec![0x5au8; size]);
            let mut val = json!({
                "content": [{"type": "image", "mimeType": "image/png", "data": b64}]
            });
            let mut lease = budget.try_acquire(b64.len()).unwrap();
            match store.extract_and_save(&mut val, &mut lease, &budget).await {
                Ok(Some(batch)) => {
                    batch.complete().await;
                    let p = val["savedMedia"][0]["savedPath"].as_str().unwrap();
                    println!("SAVED:{p}");
                }
                Ok(None) => panic!("expected saved media"),
                Err(failure) => {
                    println!("ERR:{}", failure.kind);
                    std::process::exit(11);
                }
            }
        }
        "hold_inflight_and_tmp" => {
            let b64 = encode_base64(&[0x77u8; 256]);
            let mut val = json!({
                "content": [{"type": "audio", "mimeType": "audio/wav", "data": b64}]
            });
            let mut lease = budget.try_acquire(b64.len()).unwrap();
            let _held = store
                .extract_and_save(&mut val, &mut lease, &budget)
                .await
                .unwrap()
                .unwrap();
            let leftover = media_dir.join(format!(".tmp-{}-leftover", std::process::id()));
            std::fs::write(&leftover, vec![0xeeu8; 128]).unwrap();
            let p = val["savedMedia"][0]["savedPath"].as_str().unwrap();
            println!("HOLDING:{p}");
            use std::io::Write as _;
            std::io::stdout().flush().unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        other => panic!("unknown helper mode: {other}"),
    }
}

fn run_media_helper(
    mode: &str,
    media_dir: &Path,
    clock_file: &Path,
    max_bytes: u64,
    payload_bytes: usize,
) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "cross_process_media_helper_entry", "--nocapture"])
        .env("MCPCLI_MM_HELPER_MODE", mode)
        .env("MCPCLI_MM_MEDIA_DIR", media_dir)
        .env("MCPCLI_MM_CLOCK_FILE", clock_file)
        .env("MCPCLI_MM_MAX_BYTES", max_bytes.to_string())
        .env("MCPCLI_MM_PAYLOAD_BYTES", payload_bytes.to_string())
        .output()
        .unwrap()
}

#[tokio::test]
async fn media_cache_cross_process_inflight_protection_crash_recovery_and_contention() {
    let dir = temp_dir();
    let base = dir.path().canonicalize().unwrap();
    let media_dir = base.join("media");
    let clock_file = base.join("clock.txt");
    write_shared_clock(&clock_file, 1_000);

    let clock_path = clock_file.clone();
    let store = MediaStore::with_options(
        media_dir.clone(),
        10_000,
        60_000,
        Duration::from_secs(5),
        Arc::new(move || read_shared_clock(&clock_path)),
    );
    let budget = ByteBudget::new(16 * 1024 * 1024);

    // 1. Publish file_a at t=1_000 and keep in-flight across t=75_000 while another real process runs GC.
    let b64_a = encode_base64(&[0x11u8; 500]);
    let mut val_a = json!({
        "content": [{
            "type": "resource",
            "resource": {"uri": "file:///a.mp4", "mimeType": "video/mp4", "blob": b64_a}
        }]
    });
    let mut lease_a = budget.try_acquire(b64_a.len()).unwrap();
    let inflight_a = store
        .extract_and_save(&mut val_a, &mut lease_a, &budget)
        .await
        .unwrap()
        .unwrap();
    let path_a = PathBuf::from(val_a["savedMedia"][0]["savedPath"].as_str().unwrap());
    assert!(path_a.exists());

    write_shared_clock(&clock_file, 75_000);
    let child_b = run_media_helper("save_and_complete", &media_dir, &clock_file, 10_000, 300);
    assert!(
        child_b.status.success(),
        "{:?}",
        String::from_utf8_lossy(&child_b.stderr)
    );
    let out_b = String::from_utf8(child_b.stdout).unwrap();
    let path_b = PathBuf::from(
        out_b
            .lines()
            .find_map(|l| l.strip_prefix("SAVED:"))
            .unwrap(),
    );
    // In-flight file_a MUST survive GC in Process B even after 74s!
    assert!(
        path_a.exists(),
        "in-flight file was prematurely deleted by cross-process GC"
    );
    assert!(path_b.exists());

    // Complete local writeback at t=80_000 -> refreshes file_a's 60s window to 80_000..140_000.
    write_shared_clock(&clock_file, 80_000);
    inflight_a.complete().await;

    write_shared_clock(&clock_file, 130_000);
    let gc_mid = run_media_helper("gc", &media_dir, &clock_file, 10_000, 0);
    assert!(gc_mid.status.success());
    assert!(
        path_a.exists(),
        "completed writeback did not refresh full 60s protection window"
    );
    assert!(path_b.exists());

    // At t=145_000 (>= 80_000 + 60_000), cross-process GC reclaims both.
    write_shared_clock(&clock_file, 145_000);
    let gc_after = run_media_helper("gc", &media_dir, &clock_file, 10_000, 0);
    assert!(gc_after.status.success());
    assert!(
        String::from_utf8(gc_after.stdout)
            .unwrap()
            .contains("USED:0")
    );
    assert!(!path_a.exists());
    assert!(!path_b.exists());

    // Write interruption (abort) does NOT refresh protection window.
    write_shared_clock(&clock_file, 200_000);
    let b64_abort = encode_base64(&[0x22u8; 200]);
    let mut val_abort = json!({
        "content": [{
            "type": "resource",
            "resource": {"uri": "file:///x.pdf", "mimeType": "application/pdf", "blob": b64_abort}
        }]
    });
    let mut lease_abort = budget.try_acquire(b64_abort.len()).unwrap();
    let inflight_abort = store
        .extract_and_save(&mut val_abort, &mut lease_abort, &budget)
        .await
        .unwrap()
        .unwrap();
    let path_abort = PathBuf::from(val_abort["savedMedia"][0]["savedPath"].as_str().unwrap());
    write_shared_clock(&clock_file, 270_000);
    inflight_abort.abort().await;
    let gc_abort = run_media_helper("gc", &media_dir, &clock_file, 10_000, 0);
    assert!(gc_abort.status.success());
    assert!(!path_abort.exists());

    // 2. Hard crash (SIGKILL / TerminateProcess) leftover .tmp-* and dead .inflight-* recovery.
    write_shared_clock(&clock_file, 300_000);
    let mut crash_child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "cross_process_media_helper_entry", "--nocapture"])
        .env("MCPCLI_MM_HELPER_MODE", "hold_inflight_and_tmp")
        .env("MCPCLI_MM_MEDIA_DIR", &media_dir)
        .env("MCPCLI_MM_CLOCK_FILE", &clock_file)
        .env("MCPCLI_MM_MAX_BYTES", "10000")
        .env("MCPCLI_MM_PAYLOAD_BYTES", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let crash_pid = crash_child.id();
    let mut reader = BufReader::new(crash_child.stdout.take().unwrap());
    let crashed_media_path = loop {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        if let Some(p) = line.trim().strip_prefix("HOLDING:") {
            break PathBuf::from(p);
        }
    };
    let leftover_tmp = media_dir.join(format!(".tmp-{crash_pid}-leftover"));
    assert!(leftover_tmp.exists());
    assert!(crashed_media_path.exists());
    let _ = crash_child.kill();
    let _ = crash_child.wait();

    // Publish a healthy file at t=350_000, then GC at t=370_000:
    // leftover .tmp-*, dead .inflight-*, and expired crashed_media_path are reclaimed; healthy survives!
    write_shared_clock(&clock_file, 350_000);
    let healthy_out = run_media_helper("save_and_complete", &media_dir, &clock_file, 10_000, 400);
    assert!(healthy_out.status.success());
    let healthy_path = PathBuf::from(
        String::from_utf8(healthy_out.stdout)
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("SAVED:"))
            .unwrap(),
    );

    write_shared_clock(&clock_file, 370_000);
    let gc_crash = run_media_helper("gc", &media_dir, &clock_file, 10_000, 0);
    assert!(gc_crash.status.success());
    assert!(
        String::from_utf8(gc_crash.stdout)
            .unwrap()
            .contains("USED:400")
    );
    assert!(!leftover_tmp.exists());
    assert!(!crashed_media_path.exists());
    assert!(healthy_path.exists());

    // 3. Real multi-process contention (daemon + 2 separate child OS processes) fighting for the last slot!
    // Expire healthy_path first, then seed 4_000 protected bytes out of 10_000 cap (6_000 remaining).
    write_shared_clock(&clock_file, 500_000);
    let seed = run_media_helper("save_and_complete", &media_dir, &clock_file, 10_000, 4_000);
    assert!(seed.status.success());

    // Spin up a real daemon with a live MCP server returning 4_500-byte media, plus 2 child processes!
    let mcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mcp_port = mcp_listener.local_addr().unwrap().port();
    let media_b64 = encode_base64(&[0xabu8; 4_500]);
    let server_b64 = media_b64.clone();
    let mcp_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = mcp_listener.accept().await {
            let server_b64 = server_b64.clone();
            tokio::spawn(async move {
                while let Some((req_line, _, body)) = read_http_request(&mut socket).await {
                    if req_line.starts_with("DELETE ") {
                        let _ = socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                    let req: Value = serde_json::from_slice(&body).unwrap();
                    if req.get("id").is_none() {
                        let _ = socket
                            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        continue;
                    }
                    let id = req["id"].clone();
                    let method = req["method"].as_str().unwrap_or("");
                    let payload = if method == "server/discover" {
                        json!({"jsonrpc": "2.0", "id": id, "result": {
                            "resultType": "complete", "supportedVersions": ["2026-07-28"],
                            "capabilities": {"tools": {}}, "ttlMs": 0, "cacheScope": "private"
                        }})
                    } else if method == "tools/list" {
                        json!({"jsonrpc": "2.0", "id": id, "result": {
                            "tools": [{"name": "media", "description": "m", "inputSchema": {"type": "object"}}],
                            "resultType": "complete", "ttlMs": 0, "cacheScope": "private"
                        }})
                    } else {
                        json!({"jsonrpc": "2.0", "id": id, "result": {
                            "content": [{"type": "image", "mimeType": "image/jpeg", "data": server_b64}]
                        }})
                    };
                    let bytes = serde_json::to_vec(&payload).unwrap();
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                        bytes.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&bytes).await;
                }
            });
        }
    });

    let rt_root = base.join("rt");
    let cfg_path = base.join("mcpcli.json");
    let cfg = Config {
        version: 1,
        agents: None,
        servers: BTreeMap::from([(
            "srv".into(),
            ServerConfig::Http {
                url: format!("http://127.0.0.1:{mcp_port}/mcp"),
                headers: BTreeMap::new(),
                description: None,
            },
        )]),
        skills: Vec::new(),
    };
    std::fs::write(&cfg_path, serde_json::to_vec(&cfg).unwrap()).unwrap();
    let loaded = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(loaded.path.clone(), "default", &rt_root).unwrap();
    let daemon_paths = paths.clone();
    let daemon_loaded = loaded.clone();
    let daemon_store = store.clone();
    let daemon_task = tokio::spawn(async move {
        daemon::run_with_options(
            daemon_paths,
            daemon_loaded,
            DaemonOptions {
                media: daemon_store,
                ..DaemonOptions::default()
            },
        )
        .await
        .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !paths.running().unwrap() {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
    // Warm up daemon server connection so all 3 contenders hit media cache concurrently.
    runtime::exchange(
        &paths,
        &paths
            .request(
                &loaded.digest,
                Operation::Tools {
                    server: "srv".into(),
                },
                Duration::from_secs(3),
            )
            .unwrap(),
    )
    .await
    .unwrap();

    let call_req = paths
        .request(
            &loaded.digest,
            Operation::Call {
                server: "srv".into(),
                tool: "media".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_secs(5),
        )
        .unwrap();
    let md1 = media_dir.clone();
    let cf1 = clock_file.clone();
    let md2 = media_dir.clone();
    let cf2 = clock_file.clone();

    let (daemon_res, p1, p2) = tokio::join!(
        runtime::exchange(&paths, &call_req),
        tokio::task::spawn_blocking(move || {
            run_media_helper("save_and_complete", &md1, &cf1, 10_000, 4_500)
        }),
        tokio::task::spawn_blocking(move || {
            run_media_helper("save_and_complete", &md2, &cf2, 10_000, 4_500)
        }),
    );
    let p1 = p1.unwrap();
    let p2 = p2.unwrap();

    let successes = usize::from(daemon_res.is_ok())
        + usize::from(p1.status.success())
        + usize::from(p2.status.success());
    assert_eq!(
        successes,
        1,
        "expected exactly 1 winner for last 4500 B slot in 6000 B remaining cap: daemon={:?}, p1={}, p2={}",
        daemon_res.as_ref().err(),
        String::from_utf8_lossy(&p1.stdout),
        String::from_utf8_lossy(&p2.stdout),
    );
    if let Err(err) = &daemon_res {
        assert_eq!(
            err.downcast_ref::<Failure>().unwrap().kind,
            "media_cache_full"
        );
    }
    for proc_out in [&p1, &p2] {
        if !proc_out.status.success() {
            assert!(String::from_utf8_lossy(&proc_out.stdout).contains("ERR:media_cache_full"));
        }
    }

    let mut total_bytes = 0u64;
    for entry in std::fs::read_dir(&media_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        assert!(!name.starts_with(".tmp-"), "leftover staging file: {name}");
        if !name.starts_with('.') {
            total_bytes += entry.metadata().unwrap().len();
        }
    }
    assert_eq!(total_bytes, 8_500);

    runtime::stop(&paths).await.unwrap();
    daemon_task.await.unwrap();
    mcp_server.abort();
}

#[tokio::test]
async fn daemon_send_idle_timeout_completes_slow_progress_and_aborts_stalled_inflight_batch() {
    let dir = temp_dir();
    let base = dir.path().canonicalize().unwrap();
    let media_dir = base.join("media");
    let now_ms = Arc::new(std::sync::atomic::AtomicU64::new(1_000));
    let clock_ref = now_ms.clone();
    let store = MediaStore::with_options(
        media_dir.clone(),
        1024 * 1024,
        60_000,
        Duration::from_secs(5),
        Arc::new(move || Ok(clock_ref.load(Ordering::Acquire))),
    );

    let mcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mcp_port = mcp_listener.local_addr().unwrap().port();
    let big_text = Arc::new("t".repeat(4 * 1024 * 1024));
    let media_b64 = encode_base64(&[0x42u8; 64]);
    let mcp_server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = mcp_listener.accept().await {
            let big_text = big_text.clone();
            let b64 = media_b64.clone();
            tokio::spawn(async move {
                while let Some((req_line, _headers, body)) = read_http_request(&mut socket).await {
                    if req_line.starts_with("GET ") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                            )
                            .await;
                        continue;
                    }
                    if req_line.starts_with("DELETE ") {
                        let _ = socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                    let req: Value = serde_json::from_slice(&body).unwrap();
                    let method = req["method"].as_str().unwrap_or("");
                    let id = req.get("id").cloned().unwrap_or(Value::Null);
                    let out = match method {
                        "server/discover" => serde_json::to_vec(&json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {
                                "resultType": "complete", "supportedVersions": ["2026-07-28"],
                                "capabilities": {"tools": {}}, "ttlMs": 0, "cacheScope": "private",
                                "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "s", "version": "1"}}
                            }
                        }))
                        .unwrap(),
                        "tools/list" => serde_json::to_vec(&json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {
                                "tools": [{"name": "big_media", "description": "m", "inputSchema": {"type": "object"}}],
                                "resultType": "complete", "ttlMs": 0, "cacheScope": "private"
                            }
                        }))
                        .unwrap(),
                        "tools/call" => serde_json::to_vec(&json!({
                            "jsonrpc": "2.0", "id": id,
                            "result": {
                                "content": [
                                    {"type": "text", "text": big_text.as_str()},
                                    {"type": "image", "mimeType": "image/png", "data": b64}
                                ],
                                "isError": false
                            }
                        }))
                        .unwrap(),
                        _ => Vec::new(),
                    };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                        out.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&out).await;
                }
            });
        }
    });

    let cfg_path = base.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_string(&json!({
            "version": 1,
            "servers": {"srv": {"transport": "http", "url": format!("http://127.0.0.1:{mcp_port}/mcp")}}
        }))
        .unwrap(),
    )
    .unwrap();
    let rt_root = base.join("rt");
    let loaded = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(loaded.path.clone(), "default", &rt_root).unwrap();
    let daemon_paths = paths.clone();
    let daemon_loaded = loaded.clone();
    let daemon_store = store.clone();
    let daemon_task = tokio::spawn(async move {
        daemon::run_with_options(
            daemon_paths,
            daemon_loaded,
            DaemonOptions {
                idle_timeout: Duration::from_millis(220),
                media: daemon_store,
                ..DaemonOptions::default()
            },
        )
        .await
        .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !paths.running().unwrap() {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();

    // Case 1: Slow IPC reader making steady progress (> 220ms total, < 220ms per step) across t=80_000:
    // daemon::send finishes and calls batch.complete(), refreshing the 60s retention window to 80_000..140_000!
    let call_req = paths
        .request(
            &loaded.digest,
            Operation::Call {
                server: "srv".into(),
                tool: "big_media".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    let mut client1 = platform::local::connect(paths.socket.as_os_str())
        .await
        .unwrap();
    write_ipc_frame(&mut client1, &call_req, ipc::REQUEST_LIMIT)
        .await
        .unwrap();
    let len1 = ipc::read_frame_header(&mut client1, ipc::RESPONSE_LIMIT)
        .await
        .unwrap()
        .unwrap();
    assert!(len1 > 4 * 1024 * 1024);
    // Advance clock past initial 60s window while response writeback is still blocked on kernel socket buffer!
    now_ms.store(80_000, Ordering::Release);
    assert_eq!(store.run_gc().await.unwrap(), 64);

    let mut body1 = vec![0u8; len1];
    let mut read1 = 0usize;
    while read1 < len1 {
        let end = (read1 + 256 * 1024).min(len1);
        let n = client1.read(&mut body1[read1..end]).await.unwrap();
        assert!(n > 0);
        read1 += n;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    drop(client1);
    let resp1: Response = serde_json::from_slice(&body1).unwrap();
    let saved1 = PathBuf::from(
        resp1.result.unwrap()["savedMedia"][0]["savedPath"]
            .as_str()
            .unwrap(),
    );
    // Wait briefly for daemon::send post-flush batch.complete() to update .index.json.
    tokio::time::sleep(Duration::from_millis(40)).await;
    now_ms.store(130_000, Ordering::Release);
    assert_eq!(store.run_gc().await.unwrap(), 64);
    assert!(
        saved1.exists(),
        "batch.complete() must refresh 60s window at writeback end"
    );

    now_ms.store(145_000, Ordering::Release);
    assert_eq!(store.run_gc().await.unwrap(), 0);
    assert!(!saved1.exists());

    // Case 2: Stalled IPC reader reads only the 4-byte header and stalls > 220ms idle_timeout:
    // daemon::send aborts writeback and calls batch.abort(), so GC at t=270_000 reclaims the file!
    now_ms.store(200_000, Ordering::Release);
    let call_req2 = paths
        .request(
            &loaded.digest,
            Operation::Call {
                server: "srv".into(),
                tool: "big_media".into(),
                arguments: Map::new(),
                inline_media: false,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    let mut client2 = platform::local::connect(paths.socket.as_os_str())
        .await
        .unwrap();
    write_ipc_frame(&mut client2, &call_req2, ipc::REQUEST_LIMIT)
        .await
        .unwrap();
    let len2 = ipc::read_frame_header(&mut client2, ipc::RESPONSE_LIMIT)
        .await
        .unwrap()
        .unwrap();
    assert!(len2 > 4 * 1024 * 1024);
    assert_eq!(store.run_gc().await.unwrap(), 64);

    // Advance clock past 200_000 + 60_000 BEFORE daemon idle timeout fires so a mistaken complete() would refresh to 270_000!
    now_ms.store(270_000, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(360)).await;
    drop(client2);
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(store.run_gc().await.unwrap(), 0);

    runtime::stop(&paths).await.unwrap();
    daemon_task.await.unwrap();
    mcp_server.abort();
}
