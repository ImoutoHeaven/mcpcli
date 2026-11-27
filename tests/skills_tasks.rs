use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_mcpcli")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(7 + 64);
    s.push_str("sha256:");
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

fn make_temp_env(name: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    #[cfg(unix)]
    let dir = tempfile::Builder::new()
        .prefix(&format!("mcpcli-{name}-"))
        .tempdir_in("/tmp")
        .unwrap();
    #[cfg(windows)]
    let dir = tempfile::Builder::new()
        .prefix(&format!("mcpcli-{name}-"))
        .tempdir()
        .unwrap();
    let root = dir.path().canonicalize().unwrap();
    let runtime_dir = root.join("run");
    let cfg_path = root.join("mcpcli.json");
    (dir, cfg_path, runtime_dir)
}

fn write_http_config(cfg_path: &Path, url: &str) {
    std::fs::write(
        cfg_path,
        serde_json::to_vec(&json!({
            "servers": {
                "srv": {
                    "transport": "http",
                    "url": url
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
}

fn run_cli(cfg: &Path, runtime_dir: &Path, session: &str, args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", runtime_dir)
        .arg("--config")
        .arg(cfg)
        .arg("--session")
        .arg(session)
        .args(args)
        .output()
        .unwrap()
}

fn stop_daemon(cfg: &Path, runtime_dir: &Path, session: &str) {
    let _ = run_cli(cfg, runtime_dir, session, &["daemon", "stop"]);
}

struct MockState {
    resources_read_calls: AtomicUsize,
    skills_get_calls: AtomicUsize,
    tools_call_calls: AtomicUsize,
    tasks_cancel_calls: AtomicUsize,
    tasks_update_calls: AtomicUsize,
    tasks_get_calls: AtomicUsize,
    delay_tools_list_ms: AtomicUsize,
    cancel_delay_ms: AtomicUsize,
    oversize_resource_uri: Mutex<Option<String>>,
    task_polls: Mutex<HashMap<String, usize>>,
    last_call_args: Mutex<Option<Value>>,
    stale_list_digest: AtomicBool,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            resources_read_calls: AtomicUsize::new(0),
            skills_get_calls: AtomicUsize::new(0),
            tools_call_calls: AtomicUsize::new(0),
            tasks_cancel_calls: AtomicUsize::new(0),
            tasks_update_calls: AtomicUsize::new(0),
            tasks_get_calls: AtomicUsize::new(0),
            delay_tools_list_ms: AtomicUsize::new(0),
            cancel_delay_ms: AtomicUsize::new(0),
            oversize_resource_uri: Mutex::new(None),
            task_polls: Mutex::new(HashMap::new()),
            last_call_args: Mutex::new(None),
            stale_list_digest: AtomicBool::new(false),
        }
    }
}

const DEMO_SKILL_MD: &[u8] =
    b"---\nname: demo\ndescription: Demo remote skill\n---\n# Demo\nHello from remote skill.\n";
const HEAL_SKILL_MD: &[u8] =
    b"---\nname: heal\ndescription: Self healing skill\n---\n# Heal\nFresh content.\n";
const REF_GUIDE_MD: &[u8] = b"# Guide\nReference text.\n";
// Minimal 1x1 PNG bytes.
const TINY_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
    0x42, 0x60, 0x82,
];

fn tiny_png_b64() -> String {
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg=="
        .to_owned()
}

fn demo_skill_json() -> Value {
    json!({
        "uri": "skill://team/demo/SKILL.md",
        "frontmatter": {
            "name": "demo",
            "description": "Demo remote skill"
        },
        "resources": [
            {
                "uri": "skill://team/demo/SKILL.md",
                "size": DEMO_SKILL_MD.len(),
                "digest": sha256_hex(DEMO_SKILL_MD),
                "mimeType": "text/markdown"
            },
            {
                "uri": "skill://team/demo/refs/guide.md",
                "size": REF_GUIDE_MD.len(),
                "digest": sha256_hex(REF_GUIDE_MD),
                "mimeType": "text/markdown"
            },
            {
                "uri": "skill://team/demo/assets/pic.png",
                "size": TINY_PNG.len(),
                "digest": sha256_hex(TINY_PNG),
                "mimeType": "image/png"
            }
        ]
    })
}

fn heal_skill_json(stale: bool) -> Value {
    let digest = if stale {
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned()
    } else {
        sha256_hex(HEAL_SKILL_MD)
    };
    json!({
        "uri": "skill://team/heal/SKILL.md",
        "frontmatter": {
            "name": "heal",
            "description": "Self healing skill"
        },
        "resources": [
            {
                "uri": "skill://team/heal/SKILL.md",
                "size": HEAL_SKILL_MD.len(),
                "digest": digest,
                "mimeType": "text/markdown"
            }
        ]
    })
}

fn dup_skill_json(prefix: &str) -> Value {
    let md = b"---\nname: dup\ndescription: Duplicate\n---\nBody\n";
    json!({
        "uri": format!("skill://{prefix}/dup/SKILL.md"),
        "frontmatter": {
            "name": "dup",
            "description": "Duplicate"
        },
        "resources": [
            {
                "uri": format!("skill://{prefix}/dup/SKILL.md"),
                "size": md.len(),
                "digest": sha256_hex(md),
                "mimeType": "text/markdown"
            }
        ]
    })
}

async fn write_http_json(socket: &mut tokio::net::TcpStream, body: &[u8]) {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(header.as_bytes()).await;
    let _ = socket.write_all(body).await;
    let _ = socket.flush().await;
}

async fn start_mock_mcp_server(state: Arc<MockState>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/mcp");

    let handle = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let state = state.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(socket);
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).await.unwrap_or(0) == 0 {
                    return;
                }
                let mut content_len = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':')
                        && k.eq_ignore_ascii_case("content-length")
                    {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_len];
                if content_len > 0 && reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                let mut socket = reader.into_inner();
                if req_line.starts_with("DELETE ") {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }
                if req_line.starts_with("GET ") {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }
                let Ok(req) = serde_json::from_slice::<Value>(&body) else {
                    return;
                };
                let id = req.get("id").cloned();
                let method = req.get("method").and_then(Value::as_str).unwrap_or("");
                let params = req.get("params").cloned().unwrap_or(Value::Null);
                if id.is_none() {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }
                let id = id.unwrap();

                let resp = match method {
                    "server/discover" => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "resultType": "complete",
                            "supportedVersions": ["2026-07-28"],
                            "capabilities": {
                                "tools": {},
                                "resources": {},
                                "extensions": {
                                    "io.modelcontextprotocol/skills": { "directoryRead": true },
                                    "io.modelcontextprotocol/tasks": {}
                                }
                            },
                            "ttlMs": 60000,
                            "cacheScope": "public",
                            "_meta": {
                                "io.modelcontextprotocol/serverInfo": {
                                    "name": "mock-2026",
                                    "version": "1.0.0"
                                }
                            }
                        }
                    }),
                    "initialize" => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "protocolVersion": "2026-07-28",
                            "capabilities": {
                                "tools": {},
                                "resources": {},
                                "extensions": {
                                    "io.modelcontextprotocol/skills": { "directoryRead": true },
                                    "io.modelcontextprotocol/tasks": {}
                                }
                            },
                            "serverInfo": { "name": "mock-2026", "version": "1.0.0" }
                        }
                    }),
                    "tools/list" => {
                        let delay = state.delay_tools_list_ms.load(Ordering::SeqCst);
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
                        }
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "tools": [
                                    {
                                        "name": "echo",
                                        "description": "Echo tool",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {
                                                "count": { "type": "number" },
                                                "flag": { "type": "boolean" },
                                                "label": { "type": "string" },
                                                "-dash": { "type": "integer" },
                                                "extra": { "type": "string" }
                                            }
                                        }
                                    },
                                    {
                                        "name": "task_ok",
                                        "description": "Completes after 1 poll with image",
                                        "inputSchema": { "type": "object" }
                                    },
                                    {
                                        "name": "task_fail",
                                        "description": "Fails during poll",
                                        "inputSchema": { "type": "object" }
                                    },
                                    {
                                        "name": "task_cancel",
                                        "description": "Cancelled on server",
                                        "inputSchema": { "type": "object" }
                                    },
                                    {
                                        "name": "task_input",
                                        "description": "Requires input",
                                        "inputSchema": { "type": "object" }
                                    },
                                    {
                                        "name": "task_ttl0",
                                        "description": "Initially completed with ttlMs 0",
                                        "inputSchema": { "type": "object" }
                                    },
                                    {
                                        "name": "task_slow",
                                        "description": "Stays working",
                                        "inputSchema": { "type": "object" }
                                    },
                                    {
                                        "name": "task_is_error",
                                        "description": "Completes with isError true",
                                        "inputSchema": { "type": "object" }
                                    }
                                ]
                            }
                        })
                    }
                    "tools/call" => {
                        state.tools_call_calls.fetch_add(1, Ordering::SeqCst);
                        let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                        let args = params.get("arguments").cloned().unwrap_or(json!({}));
                        *state.last_call_args.lock().unwrap() = Some(args.clone());
                        match name {
                            "echo" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "content": [{ "type": "text", "text": args.to_string() }]
                                }
                            }),
                            "task_ok" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "task",
                                    "taskId": "t-ok",
                                    "status": "working",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "ttlMs": 60000,
                                    "pollIntervalMs": 50
                                }
                            }),
                            "task_fail" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "task",
                                    "taskId": "t-fail",
                                    "status": "working",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "ttlMs": 60000,
                                    "pollIntervalMs": 50
                                }
                            }),
                            "task_cancel" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "task",
                                    "taskId": "t-cancel",
                                    "status": "working",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "ttlMs": 60000,
                                    "pollIntervalMs": 50
                                }
                            }),
                            "task_input" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "task",
                                    "taskId": "t-input",
                                    "status": "working",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "ttlMs": 60000,
                                    "pollIntervalMs": 50
                                }
                            }),
                            "task_ttl0" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "task",
                                    "taskId": "t-ttl0",
                                    "status": "completed",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "ttlMs": 0
                                }
                            }),
                            "task_slow" => {
                                let poll_ms =
                                    args.get("poll_ms").and_then(Value::as_u64).unwrap_or(200);
                                let tid = args
                                    .get("task_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or("t-slow");
                                json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": {
                                        "resultType": "task",
                                        "taskId": tid,
                                        "status": "working",
                                        "statusMessage": "still running",
                                        "createdAt": "2026-01-01T00:00:00Z",
                                        "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                        "ttlMs": 60000,
                                        "pollIntervalMs": poll_ms
                                    }
                                })
                            }
                            "task_is_error" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "task",
                                    "taskId": "t-err",
                                    "status": "working",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "ttlMs": 60000,
                                    "pollIntervalMs": 50
                                }
                            }),
                            _ => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32602, "message": "unknown tool" }
                            }),
                        }
                    }
                    "tasks/get" => {
                        state.tasks_get_calls.fetch_add(1, Ordering::SeqCst);
                        let task_id = params.get("taskId").and_then(Value::as_str).unwrap_or("");
                        let count = {
                            let mut map = state.task_polls.lock().unwrap();
                            let c = map.entry(task_id.to_owned()).or_insert(0);
                            *c += 1;
                            *c
                        };
                        match task_id {
                            "t-ok" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "taskId": "t-ok",
                                    "status": "completed",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "result": {
                                        "content": [
                                            { "type": "text", "text": "task done" },
                                            { "type": "image", "data": tiny_png_b64(), "mimeType": "image/png" }
                                        ]
                                    }
                                }
                            }),
                            "t-err" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "taskId": "t-err",
                                    "status": "completed",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "result": {
                                        "isError": true,
                                        "content": [{ "type": "text", "text": "tool failed" }]
                                    }
                                }
                            }),
                            "t-fail" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "taskId": "t-fail",
                                    "status": "failed",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "error": { "code": -32001, "message": "job crashed" }
                                }
                            }),
                            "t-cancel" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "taskId": "t-cancel",
                                    "status": "cancelled",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z"
                                }
                            }),
                            "t-input" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "taskId": "t-input",
                                    "status": "input_required",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "inputRequests": { "confirm": { "method": "roots/list" } }
                                }
                            }),
                            "t-ttl0" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32602, "message": "task expired" }
                            }),
                            "t-wait-then-ok" => {
                                if count >= 2 {
                                    json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "taskId": "t-wait-then-ok",
                                            "status": "completed",
                                            "createdAt": "2026-01-01T00:00:00Z",
                                            "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                            "result": {
                                                "content": [
                                                    { "type": "text", "text": "waited ok" },
                                                    { "type": "image", "data": tiny_png_b64(), "mimeType": "image/png" }
                                                ]
                                            }
                                        }
                                    })
                                } else {
                                    json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "taskId": "t-wait-then-ok",
                                            "status": "working",
                                            "createdAt": "2026-01-01T00:00:00Z",
                                            "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                            "pollIntervalMs": 50
                                        }
                                    })
                                }
                            }
                            other => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "taskId": other,
                                    "status": "working",
                                    "createdAt": "2026-01-01T00:00:00Z",
                                    "lastUpdatedAt": "2026-01-01T00:00:01Z",
                                    "pollIntervalMs": 200
                                }
                            }),
                        }
                    }
                    "tasks/update" => {
                        state.tasks_update_calls.fetch_add(1, Ordering::SeqCst);
                        // CustomResult with vendor extension fields (C10).
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "vendorAck": "accepted"
                            }
                        })
                    }
                    "tasks/cancel" => {
                        state.tasks_cancel_calls.fetch_add(1, Ordering::SeqCst);
                        let delay = state.cancel_delay_ms.load(Ordering::SeqCst);
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
                        }
                        // CustomResult with vendor extension fields (C10).
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "vendorCancel": true
                            }
                        })
                    }
                    "skills/list" => {
                        let stale = state.stale_list_digest.load(Ordering::SeqCst);
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "skills": [
                                    demo_skill_json(),
                                    heal_skill_json(stale),
                                    dup_skill_json("a"),
                                    dup_skill_json("b")
                                ]
                            }
                        })
                    }
                    "skills/get" => {
                        state.skills_get_calls.fetch_add(1, Ordering::SeqCst);
                        let uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
                        match uri {
                            "skill://team/demo/SKILL.md" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": { "skill": demo_skill_json() }
                            }),
                            "skill://team/heal/SKILL.md" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": { "skill": heal_skill_json(false) }
                            }),
                            _ => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32602, "message": "skill not found" }
                            }),
                        }
                    }
                    "resources/directory/read" => {
                        let uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resources": [
                                    {
                                        "uri": format!("{uri}/guide.md"),
                                        "name": "guide.md",
                                        "mimeType": "text/markdown"
                                    }
                                ]
                            }
                        })
                    }
                    "resources/read" => {
                        state.resources_read_calls.fetch_add(1, Ordering::SeqCst);
                        let uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
                        let oversize_match =
                            state.oversize_resource_uri.lock().unwrap().as_deref() == Some(uri);
                        if oversize_match {
                            // Stream a > 32 MiB JSON-RPC frame without holding a huge Value in memory.
                            let prefix = format!(
                                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"contents\":[{{\"uri\":{},\"mimeType\":\"text/plain\",\"text\":\"",
                                id,
                                serde_json::to_string(uri).unwrap()
                            );
                            let suffix = "\"}]}}";
                            let filler_len = 33usize << 20;
                            let total_len = prefix.len() + filler_len + suffix.len();
                            let header = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {total_len}\r\nConnection: close\r\n\r\n"
                            );
                            let _ = socket.write_all(header.as_bytes()).await;
                            let _ = socket.write_all(prefix.as_bytes()).await;
                            let chunk = vec![b'a'; 64 * 1024];
                            let mut rem = filler_len;
                            while rem > 0 {
                                let n = rem.min(chunk.len());
                                if socket.write_all(&chunk[..n]).await.is_err() {
                                    return;
                                }
                                rem -= n;
                            }
                            let _ = socket.write_all(suffix.as_bytes()).await;
                            let _ = socket.flush().await;
                            return;
                        }
                        match uri {
                            "skill://team/demo/SKILL.md" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "contents": [{
                                        "uri": uri,
                                        "mimeType": "text/markdown",
                                        "text": std::str::from_utf8(DEMO_SKILL_MD).unwrap()
                                    }]
                                }
                            }),
                            "skill://team/heal/SKILL.md" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "contents": [{
                                        "uri": uri,
                                        "mimeType": "text/markdown",
                                        "text": std::str::from_utf8(HEAL_SKILL_MD).unwrap()
                                    }]
                                }
                            }),
                            "skill://team/demo/refs/guide.md" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "contents": [{
                                        "uri": uri,
                                        "mimeType": "text/markdown",
                                        "text": std::str::from_utf8(REF_GUIDE_MD).unwrap()
                                    }]
                                }
                            }),
                            "skill://team/demo/assets/pic.png" => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "contents": [{
                                        "uri": uri,
                                        "mimeType": "image/png",
                                        "blob": tiny_png_b64()
                                    }]
                                }
                            }),
                            _ => json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32602, "message": "resource not found" }
                            }),
                        }
                    }
                    _ => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": -32601, "message": "method not found" }
                    }),
                };
                let bytes = serde_json::to_vec(&resp).unwrap();
                write_http_json(&mut socket, &bytes).await;
            });
        }
    });

    (url, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_remote_full_pipeline_list_show_files_dir_read_media_and_self_heal() {
    let state = Arc::new(MockState::default());
    state.stale_list_digest.store(true, Ordering::SeqCst);
    let (url, server) = start_mock_mcp_server(state.clone()).await;
    let (_tmp, cfg, run_dir) = make_temp_env("skills");
    write_http_config(&cfg, &url);

    // 1. skills list --server srv (JSON & --text)
    let out = run_cli(&cfg, &run_dir, "sk", &["skills", "list", "--server", "srv"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let val: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(val["server"], "srv");
    assert_eq!(val["skills"].as_array().unwrap().len(), 4);

    let out_txt = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &["--text", "skills", "list", "--server", "srv"],
    );
    assert_eq!(out_txt.status.code(), Some(0));
    let txt = String::from_utf8(out_txt.stdout).unwrap();
    assert!(txt.contains("demo\tskill://team/demo/SKILL.md\tDemo remote skill"));

    // 2. skills show by short name, prefix/name, and full URI
    for sel in ["demo", "team/demo", "skill://team/demo/SKILL.md"] {
        let out = run_cli(
            &cfg,
            &run_dir,
            "sk",
            &["skills", "show", "--server", "srv", sel],
        );
        assert_eq!(
            out.status.code(),
            Some(0),
            "selector {sel} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let val: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(val["uri"], "skill://team/demo/SKILL.md");
        assert_eq!(val["verified"], true);
        assert_eq!(val["dynamic"], false);
        assert!(
            val["content"]
                .as_str()
                .unwrap()
                .contains("Hello from remote skill.")
        );
    }

    // 3. ambiguous_skill returns exit 2 with candidates
    let amb = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &["skills", "show", "--server", "srv", "dup"],
    );
    assert_eq!(amb.status.code(), Some(2));
    let amb_err: Value = serde_json::from_slice(&amb.stderr).unwrap();
    assert_eq!(amb_err["error"]["kind"], "ambiguous_skill");
    assert_eq!(
        amb_err["error"]["data"]["candidates"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // 4. skills files (static manifest) and skills files --dir refs
    let files = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &["skills", "files", "--server", "srv", "demo"],
    );
    assert_eq!(files.status.code(), Some(0));
    let files_val: Value = serde_json::from_slice(&files.stdout).unwrap();
    assert_eq!(files_val["resources"].as_array().unwrap().len(), 2);

    let dir_files = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &[
            "skills", "files", "--server", "srv", "--dir", "refs", "demo",
        ],
    );
    assert_eq!(dir_files.status.code(), Some(0));
    let dir_val: Value = serde_json::from_slice(&dir_files.stdout).unwrap();
    assert_eq!(dir_val["uri"], "skill://team/demo/refs");
    assert_eq!(dir_val["resources"][0]["name"], "guide.md");

    // 5. Unlisted manifest file rejected with ZERO resources/read RPC
    let before_reads = state.resources_read_calls.load(Ordering::SeqCst);
    let unlisted = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &[
            "skills",
            "read",
            "--server",
            "srv",
            "demo",
            "refs/secret.md",
        ],
    );
    assert_eq!(unlisted.status.code(), Some(4));
    let unlisted_err: Value = serde_json::from_slice(&unlisted.stderr).unwrap();
    assert_eq!(unlisted_err["error"]["kind"], "skill_integrity");
    assert_eq!(
        state.resources_read_calls.load(Ordering::SeqCst),
        before_reads
    );

    // 6. from_list single-retry skills/get self-healing on stale digest
    let before_gets = state.skills_get_calls.load(Ordering::SeqCst);
    let healed = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &["skills", "show", "--server", "srv", "heal"],
    );
    assert_eq!(
        healed.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&healed.stderr)
    );
    let healed_val: Value = serde_json::from_slice(&healed.stdout).unwrap();
    assert_eq!(healed_val["verified"], true);
    assert!(state.skills_get_calls.load(Ordering::SeqCst) > before_gets);

    // 7. Binary resource read: MediaStore persistence vs --inline-media
    let saved = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &[
            "skills",
            "read",
            "--server",
            "srv",
            "demo",
            "assets/pic.png",
        ],
    );
    assert_eq!(
        saved.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&saved.stderr)
    );
    let saved_val: Value = serde_json::from_slice(&saved.stdout).unwrap();
    let saved_path = saved_val["savedMedia"][0]["savedPath"].as_str().unwrap();
    assert_eq!(std::fs::read(saved_path).unwrap(), TINY_PNG);

    let inline = run_cli(
        &cfg,
        &run_dir,
        "sk",
        &[
            "skills",
            "read",
            "--server",
            "srv",
            "--inline-media",
            "demo",
            "assets/pic.png",
        ],
    );
    assert_eq!(inline.status.code(), Some(0));
    let inline_val: Value = serde_json::from_slice(&inline.stdout).unwrap();
    assert_eq!(inline_val["blob"], tiny_png_b64());

    stop_daemon(&cfg, &run_dir, "sk");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skills_read_over_32mib_frame_fails_fast_with_exit_4() {
    let state = Arc::new(MockState::default());
    *state.oversize_resource_uri.lock().unwrap() = Some("skill://team/demo/refs/guide.md".into());
    let (url, server) = start_mock_mcp_server(state).await;
    let (_tmp, cfg, run_dir) = make_temp_env("skills32m");
    write_http_config(&cfg, &url);

    let started = Instant::now();
    let out = run_cli(
        &cfg,
        &run_dir,
        "sk32",
        &[
            "--oneshot",
            "skills",
            "read",
            "--server",
            "srv",
            "demo",
            "refs/guide.md",
        ],
    );
    assert_eq!(
        out.status.code(),
        Some(4),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(started.elapsed() < Duration::from_secs(10));
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert!(matches!(
        err["error"]["kind"].as_str(),
        Some("mcp_protocol" | "mcp_transport")
    ));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tasks_call_transparent_poll_detach_update_cancel_custom_result_and_media() {
    let state = Arc::new(MockState::default());
    let (url, server) = start_mock_mcp_server(state.clone()).await;
    let (_tmp, cfg, run_dir) = make_temp_env("tasks");
    write_http_config(&cfg, &url);

    // 1. call transparent polling -> completed with media extraction (wrap_saved_media)
    let ok = run_cli(&cfg, &run_dir, "tk", &["call", "srv", "task_ok"]);
    assert_eq!(
        ok.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let ok_val: Value = serde_json::from_slice(&ok.stdout).unwrap();
    assert_eq!(ok_val["result"]["content"][0]["text"], "task done");
    let saved_path = ok_val["savedMedia"][0]["savedPath"].as_str().unwrap();
    assert_eq!(std::fs::read(saved_path).unwrap(), TINY_PNG);

    // 2. call transparent polling -> completed with isError: true -> exit 3
    let is_err = run_cli(&cfg, &run_dir, "tk", &["call", "srv", "task_is_error"]);
    assert_eq!(is_err.status.code(), Some(3));

    // 3. call transparent polling -> failed -> exit 4, outcome_unknown: true, rpc_error, data.taskId
    let failed = run_cli(&cfg, &run_dir, "tk", &["call", "srv", "task_fail"]);
    assert_eq!(failed.status.code(), Some(4));
    let fail_err: Value = serde_json::from_slice(&failed.stderr).unwrap();
    assert_eq!(fail_err["error"]["kind"], "task_failed");
    assert_eq!(fail_err["error"]["outcome_unknown"], true);
    assert_eq!(fail_err["error"]["data"]["taskId"], "t-fail");
    assert_eq!(fail_err["error"]["rpc_error"]["message"], "job crashed");

    // 4. call transparent polling -> cancelled -> exit 4, outcome_unknown: true, data.taskId
    let cancelled = run_cli(&cfg, &run_dir, "tk", &["call", "srv", "task_cancel"]);
    assert_eq!(cancelled.status.code(), Some(4));
    let can_err: Value = serde_json::from_slice(&cancelled.stderr).unwrap();
    assert_eq!(can_err["error"]["kind"], "cancelled");
    assert_eq!(can_err["error"]["outcome_unknown"], true);
    assert_eq!(can_err["error"]["data"]["taskId"], "t-cancel");

    // 5. call transparent polling -> input_required -> exit 4, outcome_unknown: true, NO tasks/cancel
    let before_cancels = state.tasks_cancel_calls.load(Ordering::SeqCst);
    let inp = run_cli(&cfg, &run_dir, "tk", &["call", "srv", "task_input"]);
    assert_eq!(inp.status.code(), Some(4));
    let inp_err: Value = serde_json::from_slice(&inp.stderr).unwrap();
    assert_eq!(inp_err["error"]["kind"], "input_required");
    assert_eq!(inp_err["error"]["outcome_unknown"], true);
    assert_eq!(inp_err["error"]["data"]["taskId"], "t-input");
    assert_eq!(inp_err["error"]["data"]["status"], "input_required");
    assert_eq!(
        state.tasks_cancel_calls.load(Ordering::SeqCst),
        before_cancels
    );

    // 6. Initial completed with ttlMs: 0 where follow-up tasks/get fails -> outcome_unknown: true + taskId
    let ttl0 = run_cli(&cfg, &run_dir, "tk", &["call", "srv", "task_ttl0"]);
    assert_eq!(ttl0.status.code(), Some(4));
    let ttl0_err: Value = serde_json::from_slice(&ttl0.stderr).unwrap();
    assert_eq!(ttl0_err["error"]["outcome_unknown"], true);
    assert_eq!(ttl0_err["error"]["data"]["taskId"], "t-ttl0");

    // 7. call --detach on task tool (raw CreateTaskResult, no "result" unwrapping) & sync tool
    let det_task = run_cli(
        &cfg,
        &run_dir,
        "tk",
        &["call", "srv", "task_slow", "--detach"],
    );
    assert_eq!(det_task.status.code(), Some(0));
    let det_val: Value = serde_json::from_slice(&det_task.stdout).unwrap();
    assert_eq!(det_val["resultType"], "task");
    assert_eq!(det_val["taskId"], "t-slow");

    let det_sync = run_cli(
        &cfg,
        &run_dir,
        "tk",
        &["call", "srv", "echo", "--detach", "label:=hi"],
    );
    assert_eq!(det_sync.status.code(), Some(0));
    let det_sync_val: Value = serde_json::from_slice(&det_sync.stdout).unwrap();
    assert!(
        det_sync_val["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("hi")
    );

    // 8. tasks get (no --wait) on completed task with image: in-place media extraction, no double wrap!
    let tget = run_cli(&cfg, &run_dir, "tk", &["tasks", "get", "srv", "t-ok"]);
    assert_eq!(
        tget.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&tget.stderr)
    );
    let tget_val: Value = serde_json::from_slice(&tget.stdout).unwrap();
    assert_eq!(tget_val["taskId"], "t-ok");
    assert_eq!(tget_val["status"], "completed");
    assert_eq!(tget_val["result"]["content"][0]["text"], "task done");
    assert!(tget_val["result"].get("result").is_none());
    let tget_saved = tget_val["savedMedia"][0]["savedPath"].as_str().unwrap();
    assert_eq!(std::fs::read(tget_saved).unwrap(), TINY_PNG);

    // 9. task alias + --wait
    let twait = run_cli(
        &cfg,
        &run_dir,
        "tk",
        &["task", "get", "srv", "t-wait-then-ok", "--wait"],
    );
    assert_eq!(twait.status.code(), Some(0));
    let twait_val: Value = serde_json::from_slice(&twait.stdout).unwrap();
    assert_eq!(twait_val["status"], "completed");
    assert!(twait_val["savedMedia"].is_array());

    // 10. tasks update & tasks cancel with CustomResult vendor extension fields
    let tupd = run_cli(
        &cfg,
        &run_dir,
        "tk",
        &[
            "tasks",
            "update",
            "srv",
            "t-input",
            "--args",
            "{\"confirm\":true}",
        ],
    );
    assert_eq!(
        tupd.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&tupd.stderr)
    );
    let tupd_val: Value = serde_json::from_slice(&tupd.stdout).unwrap();
    assert_eq!(tupd_val["taskId"], "t-input");
    assert_eq!(tupd_val["acknowledged"], true);

    let tcan = run_cli(&cfg, &run_dir, "tk", &["tasks", "cancel", "srv", "t-slow"]);
    assert_eq!(
        tcan.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&tcan.stderr)
    );
    let tcan_val: Value = serde_json::from_slice(&tcan.stdout).unwrap();
    assert_eq!(tcan_val["taskId"], "t-slow");
    assert_eq!(tcan_val["acknowledged"], true);

    stop_daemon(&cfg, &run_dir, "tk");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tasks_max_timeout_grace_window_delivers_cancel_and_task_id() {
    let state = Arc::new(MockState::default());
    // Delay tasks/cancel by 500ms so +3s/+5s grace window is exercised after 1s max-timeout.
    state.cancel_delay_ms.store(500, Ordering::SeqCst);
    let (url, server) = start_mock_mcp_server(state.clone()).await;
    let (_tmp, cfg, run_dir) = make_temp_env("grace");
    write_http_config(&cfg, &url);

    let out = run_cli(
        &cfg,
        &run_dir,
        "gr",
        &[
            "--timeout",
            "1",
            "call",
            "srv",
            "task_slow",
            "--max-timeout",
            "1",
        ],
    );
    assert_eq!(
        out.status.code(),
        Some(5),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["kind"], "timeout");
    assert_eq!(err["error"]["outcome_unknown"], true);
    assert_eq!(err["error"]["data"]["taskId"], "t-slow");
    assert!(state.tasks_cancel_calls.load(Ordering::SeqCst) >= 1);

    stop_daemon(&cfg, &run_dir, "gr");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tasks_daemon_reset_during_poll_sleep_conn_id_zero_pollution() {
    let state = Arc::new(MockState::default());
    let (url, server) = start_mock_mcp_server(state.clone()).await;
    let (_tmp, cfg, run_dir) = make_temp_env("connid");
    write_http_config(&cfg, &url);

    // Sub-case A: pure reset during poll sleep -> old poller returns session_lost (6), daemon stays lazy!
    let poller_a = Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", &run_dir)
        .args(["--config"])
        .arg(&cfg)
        .args([
            "--session",
            "cid",
            "call",
            "srv",
            "task_slow",
            "poll_ms:=800",
            "task_id:=t-reset-a",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Wait until tools/call has been dispatched and entered poll sleep.
    let deadline = Instant::now() + Duration::from_secs(5);
    while state.tools_call_calls.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let reset_a = run_cli(&cfg, &run_dir, "cid", &["reset", "srv"]);
    assert_eq!(reset_a.status.code(), Some(0));

    let out_a = tokio::task::spawn_blocking(move || poller_a.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(
        out_a.status.code(),
        Some(6),
        "stderr: {}",
        String::from_utf8_lossy(&out_a.stderr)
    );
    let err_a: Value = serde_json::from_slice(&out_a.stderr).unwrap();
    assert_eq!(err_a["error"]["kind"], "session_lost");
    assert_eq!(err_a["error"]["outcome_unknown"], true);
    assert_eq!(err_a["error"]["data"]["taskId"], "t-reset-a");

    let status_a = run_cli(&cfg, &run_dir, "cid", &["daemon", "status"]);
    let status_a_val: Value = serde_json::from_slice(&status_a.stdout).unwrap();
    assert_eq!(status_a_val["servers"]["srv"]["state"], "lazy");

    // Sub-case B: reset + reconnect during poll sleep -> new connection stays connected (zero pollution)!
    let calls_before = state.tools_call_calls.load(Ordering::SeqCst);
    let poller_b = Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", &run_dir)
        .args(["--config"])
        .arg(&cfg)
        .args([
            "--session",
            "cid",
            "call",
            "srv",
            "task_slow",
            "poll_ms:=800",
            "task_id:=t-reset-b",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while state.tools_call_calls.load(Ordering::SeqCst) == calls_before && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let reset_b = run_cli(&cfg, &run_dir, "cid", &["reset", "srv"]);
    assert_eq!(reset_b.status.code(), Some(0));
    // Reconnect a fresh healthy connection before old poller wakes up.
    let reconnect = run_cli(&cfg, &run_dir, "cid", &["server", "srv"]);
    assert_eq!(reconnect.status.code(), Some(0));

    let out_b = tokio::task::spawn_blocking(move || poller_b.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(
        out_b.status.code(),
        Some(6),
        "stderr: {}",
        String::from_utf8_lossy(&out_b.stderr)
    );

    // New connection must NOT have been marked lost!
    let status_b = run_cli(&cfg, &run_dir, "cid", &["daemon", "status"]);
    let status_b_val: Value = serde_json::from_slice(&status_b.stdout).unwrap();
    assert_eq!(status_b_val["servers"]["srv"]["state"], "connected");
    let after = run_cli(&cfg, &run_dir, "cid", &["server", "srv"]);
    assert_eq!(after.status.code(), Some(0));

    stop_daemon(&cfg, &run_dir, "cid");
    server.abort();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigint_oneshot_and_daemon_tasks_and_sync_immediate_exit() {
    let state = Arc::new(MockState::default());
    let (url, server) = start_mock_mcp_server(state.clone()).await;
    let (_tmp, cfg, run_dir) = make_temp_env("sigint");
    write_http_config(&cfg, &url);

    let send_sigint = |pid: u32| unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGINT);
    };

    // 1. --oneshot call polling + SIGINT -> sends tasks/cancel, exits 130, outcome_unknown: true, taskId
    let cancels_before = state.tasks_cancel_calls.load(Ordering::SeqCst);
    let calls_before = state.tools_call_calls.load(Ordering::SeqCst);
    let child = Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", &run_dir)
        .args(["--config"])
        .arg(&cfg)
        .args([
            "--oneshot",
            "call",
            "srv",
            "task_slow",
            "poll_ms:=400",
            "task_id:=t-sig-one",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while state.tools_call_calls.load(Ordering::SeqCst) == calls_before && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(120)).await;
    send_sigint(child.id());
    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(130),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["kind"], "cancelled");
    assert_eq!(err["error"]["outcome_unknown"], true);
    assert_eq!(err["error"]["data"]["taskId"], "t-sig-one");
    assert!(state.tasks_cancel_calls.load(Ordering::SeqCst) > cancels_before);

    // 2. Daemon IPC call polling + SIGINT -> sends tasks/cancel, exits 130, outcome_unknown: true, taskId
    let cancels_before = state.tasks_cancel_calls.load(Ordering::SeqCst);
    let calls_before = state.tools_call_calls.load(Ordering::SeqCst);
    let child = Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", &run_dir)
        .args(["--config"])
        .arg(&cfg)
        .args([
            "--session",
            "sig",
            "call",
            "srv",
            "task_slow",
            "poll_ms:=400",
            "task_id:=t-sig-d",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while state.tools_call_calls.load(Ordering::SeqCst) == calls_before && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(120)).await;
    send_sigint(child.id());
    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(130),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["kind"], "cancelled");
    assert_eq!(err["error"]["outcome_unknown"], true);
    assert_eq!(err["error"]["data"]["taskId"], "t-sig-d");
    assert!(state.tasks_cancel_calls.load(Ordering::SeqCst) > cancels_before);

    // 3. Daemon IPC tasks get --wait + SIGINT -> does NOT send tasks/cancel, exits 130, outcome_unknown: false
    let cancels_before = state.tasks_cancel_calls.load(Ordering::SeqCst);
    let gets_before = state.tasks_get_calls.load(Ordering::SeqCst);
    let child = Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", &run_dir)
        .args(["--config"])
        .arg(&cfg)
        .args([
            "--session",
            "sig",
            "tasks",
            "get",
            "srv",
            "t-sig-wait",
            "--wait",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while state.tasks_get_calls.load(Ordering::SeqCst) == gets_before && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    send_sigint(child.id());
    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(130),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(err["error"]["kind"], "cancelled");
    assert_eq!(err["error"].get("outcome_unknown"), None);
    assert_eq!(
        state.tasks_cancel_calls.load(Ordering::SeqCst),
        cancels_before
    );

    // 4. Ordinary single-shot command + SIGINT -> immediate exit 130
    state.delay_tools_list_ms.store(5_000, Ordering::SeqCst);
    let child = Command::new(bin())
        .env("MCPCLI_RUNTIME_DIR", &run_dir)
        .args(["--config"])
        .arg(&cfg)
        .args(["--oneshot", "tools", "srv"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let sent = Instant::now();
    send_sigint(child.id());
    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(130));
    assert!(sent.elapsed() < Duration::from_secs(2));

    stop_daemon(&cfg, &run_dir, "sig");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_builtin_kv_args_and_schema_gate_e2e() {
    let state = Arc::new(MockState::default());
    let (url, server) = start_mock_mcp_server(state.clone()).await;
    let (tmp, cfg, run_dir) = make_temp_env("cli");

    // 1. skills builtin runs offline with NO mcpcli.json!
    let missing_cfg = tmp.path().join("does-not-exist.json");
    let builtin_json = Command::new(bin())
        .args(["--config"])
        .arg(&missing_cfg)
        .args(["skills", "builtin"])
        .output()
        .unwrap();
    assert_eq!(builtin_json.status.code(), Some(0));
    let bval: Value = serde_json::from_slice(&builtin_json.stdout).unwrap();
    assert_eq!(bval["name"], "mcpcli");
    assert_eq!(bval["version"], env!("CARGO_PKG_VERSION"));
    assert!(
        bval["content"]
            .as_str()
            .unwrap()
            .starts_with("---\nname: mcpcli\n")
    );

    let builtin_text = Command::new(bin())
        .args(["--config"])
        .arg(&missing_cfg)
        .args(["--text", "skills", "builtin"])
        .output()
        .unwrap();
    assert_eq!(builtin_text.status.code(), Some(0));
    assert!(
        String::from_utf8(builtin_text.stdout)
            .unwrap()
            .starts_with("---\nname: mcpcli\n")
    );

    // --help includes Agent skill guide line
    let help = Command::new(bin()).arg("--help").output().unwrap();
    assert_eq!(help.status.code(), Some(0));
    assert!(
        String::from_utf8(help.stdout)
            .unwrap()
            .contains("Agent skill guide: mcpcli --text skills builtin")
    );

    write_http_config(&cfg, &url);

    // 2. call KEY:=VALUE including -- -dash:=42
    let kv = run_cli(
        &cfg,
        &run_dir,
        "cl",
        &[
            "--oneshot",
            "call",
            "srv",
            "echo",
            "count:=3",
            "flag:=true",
            "label:=hello",
            "--",
            "-dash:=42",
        ],
    );
    assert_eq!(
        kv.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&kv.stderr)
    );
    let last_args = state.last_call_args.lock().unwrap().clone().unwrap();
    assert_eq!(last_args["count"], 3);
    assert_eq!(last_args["flag"], true);
    assert_eq!(last_args["label"], "hello");
    assert_eq!(last_args["-dash"], 42);

    // 3. inspect & call --schema compatible vs exact
    let compat_schema = tmp.path().join("compat.json");
    std::fs::write(
        &compat_schema,
        serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "count": { "type": "integer" },
                "flag": { "type": "boolean" },
                "label": { "type": "string" },
                "-dash": { "type": "integer" }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    // Compatible mode accepts widening integer -> number and extra optional property
    let insp_ok = run_cli(
        &cfg,
        &run_dir,
        "cl",
        &[
            "--oneshot",
            "inspect",
            "srv",
            "echo",
            "--schema",
            compat_schema.to_str().unwrap(),
        ],
    );
    assert_eq!(
        insp_ok.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&insp_ok.stderr)
    );

    // Exact mode rejects with exit 2 schema_mismatch and ZERO tools/call RPC!
    let calls_before = state.tools_call_calls.load(Ordering::SeqCst);
    let call_bad = run_cli(
        &cfg,
        &run_dir,
        "cl",
        &[
            "--oneshot",
            "call",
            "srv",
            "echo",
            "--schema",
            compat_schema.to_str().unwrap(),
            "--schema-mode",
            "exact",
            "label:=hi",
        ],
    );
    assert_eq!(call_bad.status.code(), Some(2));
    let bad_err: Value = serde_json::from_slice(&call_bad.stderr).unwrap();
    assert_eq!(bad_err["error"]["kind"], "schema_mismatch");
    assert_eq!(bad_err["error"].get("outcome_unknown"), None);
    assert_eq!(state.tools_call_calls.load(Ordering::SeqCst), calls_before);

    // 4. Usage rejections (exit 2)
    let bad_det = run_cli(
        &cfg,
        &run_dir,
        "cl",
        &["call", "srv", "echo", "--detach", "--max-timeout", "10"],
    );
    assert_eq!(bad_det.status.code(), Some(2));
    let bad_tget = run_cli(
        &cfg,
        &run_dir,
        "cl",
        &["tasks", "get", "srv", "t-1", "--max-timeout", "10"],
    );
    assert_eq!(bad_tget.status.code(), Some(2));
    let bad_local_sk = run_cli(
        &cfg,
        &run_dir,
        "cl",
        &["skills", "read", "demo", "a.md", "--inline-media"],
    );
    assert_eq!(bad_local_sk.status.code(), Some(2));

    server.abort();
}
