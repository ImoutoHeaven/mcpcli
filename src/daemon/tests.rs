use super::dispatch::{DaemonTaskPoll, ExecuteContext};
use super::error::*;
use super::state::*;
use super::transport::*;
use crate::config::LoadedConfig;
use crate::ipc::{self, Failure, Operation, Request, StreamFrameKind};
use crate::mcp::{FailureKind, McpFailure};
use crate::runtime::{self, Lock, Paths, State};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

async fn start_mock_events_http_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/mcp");
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let (read_half, mut write_half) = socket.into_split();
                let mut reader = BufReader::new(read_half);
                let mut content_len = 0usize;
                let mut line = String::new();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':')
                        && k.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_len];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                let Ok(req) = serde_json::from_slice::<Value>(&body) else {
                    return;
                };
                let id = req.get("id").cloned().unwrap_or(json!(1));
                let method = req.get("method").and_then(Value::as_str).unwrap_or("");
                match method {
                    "server/discover" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "supportedVersions": ["2026-07-28"],
                                "capabilities": {
                                    "extensions": {
                                        "io.modelcontextprotocol/events": { "listChanged": false }
                                    }
                                },
                                "ttlMs": 60000,
                                "cacheScope": "public",
                                "_meta": {
                                    "io.modelcontextprotocol/serverInfo": {
                                        "name": "mock-events",
                                        "version": "1.0"
                                    }
                                }
                            }
                        });
                        let payload = serde_json::to_vec(&resp).unwrap();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let _ = write_half.write_all(header.as_bytes()).await;
                        let _ = write_half.write_all(&payload).await;
                    }
                    "events/list" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "events": [{
                                    "name": "task.exited",
                                    "delivery": ["push", "poll"]
                                }]
                            }
                        });
                        let payload = serde_json::to_vec(&resp).unwrap();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let _ = write_half.write_all(header.as_bytes()).await;
                        let _ = write_half.write_all(&payload).await;
                    }
                    "events/poll" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "events": [{"eventId": "p1", "name": "task.exited", "data": {"ok": true}}],
                                "cursor": "1:1",
                                "truncated": false,
                                "hasMore": false,
                                "nextPollMs": 1000
                            }
                        });
                        let payload = serde_json::to_vec(&resp).unwrap();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let _ = write_half.write_all(header.as_bytes()).await;
                        let _ = write_half.write_all(&payload).await;
                    }
                    "events/stream" => {
                        let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
                        let _ = write_half.write_all(header.as_bytes()).await;
                        let active = json!({
                            "jsonrpc": "2.0",
                            "method": "notifications/events/active",
                            "params": {
                                "cursor": "1:10",
                                "truncated": false,
                                "heartbeatMs": 5000,
                                "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                            }
                        });
                        let ev = json!({
                            "jsonrpc": "2.0",
                            "method": "notifications/events/event",
                            "params": {
                                "eventId": "ev-11",
                                "name": "task.exited",
                                "data": { "code": 0 },
                                "cursor": "1:11",
                                "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                            }
                        });
                        let term = json!({
                            "jsonrpc": "2.0",
                            "method": "notifications/events/terminated",
                            "params": {
                                "error": { "code": -32024, "message": "done" },
                                "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                            }
                        });
                        for frame in [active, ev, term] {
                            let sse =
                                format!("data: {}\n\n", serde_json::to_string(&frame).unwrap());
                            let _ = write_half.write_all(sse.as_bytes()).await;
                            let _ = write_half.flush().await;
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }
                    _ => {
                        let header = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = write_half.write_all(header.as_bytes()).await;
                    }
                }
            });
        }
    });
    (url, task)
}

#[tokio::test]
async fn daemon_waits_for_running_probe_and_preserves_the_active_owner() {
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let config_path = base_path.join("mcpcli.json");
    std::fs::write(&config_path, r#"{"servers":{}}"#).unwrap();
    let loaded = LoadedConfig::load(&config_path).unwrap();
    let paths =
        Paths::with_root(loaded.path.clone(), "startup-probe", &base_path.join("run")).unwrap();

    let probe = Lock::try_acquire(&paths.daemon_lock()).unwrap().unwrap();
    let mut daemon = Box::pin(run_with_options(
        paths.clone(),
        loaded.clone(),
        DaemonOptions::default(),
    ));
    let first_poll = futures_util::poll!(&mut daemon);
    assert!(
        first_poll.is_pending(),
        "daemon must wait for the running probe to release its lock: {first_poll:?}"
    );
    assert!(State::load(&paths).unwrap().is_none());
    #[cfg(unix)]
    assert!(!paths.socket.exists() && !paths.control_socket.exists());
    drop(probe);

    let daemon_task = tokio::spawn(daemon);
    timeout(runtime::START_WAIT, async {
        loop {
            if runtime::exchange(
                &paths,
                &paths
                    .request("", Operation::Status, Duration::from_millis(200))
                    .unwrap(),
            )
            .await
            .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let state = State::load(&paths).unwrap().unwrap();

    let duplicate = timeout(
        runtime::START_WAIT + Duration::from_secs(2),
        run_with_options(paths.clone(), loaded, DaemonOptions::default()),
    )
    .await
    .unwrap()
    .unwrap_err();
    let diagnostic = format!("{duplicate:#}");
    assert!(diagnostic.contains("cannot acquire daemon lifetime lock"));
    assert!(diagnostic.contains("timed out waiting for runtime lock"));
    assert_eq!(State::load(&paths).unwrap().unwrap().epoch, state.epoch);
    assert!(!daemon_task.is_finished());
    let (status, _lease) = runtime::exchange(
        &paths,
        &paths
            .request("", Operation::Status, Duration::from_secs(2))
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(status.result.is_some());

    runtime::stop(&paths).await.unwrap();
    daemon_task.await.unwrap().unwrap();
    assert!(State::load(&paths).unwrap().unwrap().clean_stop);
}

#[tokio::test]
async fn daemon_ipc_v4_forwards_events_list_poll_and_stream_without_blocking_server() {
    let (url, server_task) = start_mock_events_http_server().await;
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_string(&json!({
            "servers": {
                "mock": {
                    "transport": "http",
                    "url": url
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let loaded = LoadedConfig::load(&cfg_path).unwrap();
    let root = base_path.join("runtime");
    let paths = Paths::with_root(loaded.path.clone(), "evtest", &root).unwrap();

    let daemon_paths = paths.clone();
    let daemon_loaded = loaded.clone();
    let daemon_task = tokio::spawn(async move {
        run_with_options(daemon_paths, daemon_loaded, DaemonOptions::default()).await
    });

    for _ in 0..80 {
        if paths.running().unwrap_or(false)
            && runtime::exchange(
                &paths,
                &paths
                    .request("", Operation::Status, Duration::from_millis(200))
                    .unwrap(),
            )
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 1. EventsList
    let (list_resp, _) = runtime::exchange(
        &paths,
        &paths
            .request(
                &loaded.digest,
                Operation::EventsList {
                    server: "mock".into(),
                },
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(list_resp.protocol, ipc::PROTOCOL);
    assert_eq!(
        list_resp.result.unwrap()["events"][0]["name"],
        "task.exited"
    );

    // 2. EventsPoll
    let (poll_resp, _) = runtime::exchange(
        &paths,
        &paths
            .request(
                &loaded.digest,
                Operation::EventsPoll {
                    server: "mock".into(),
                    event: "task.exited".into(),
                    arguments: None,
                    cursor: None,
                    max_age_ms: None,
                    max_events: Some(5),
                },
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(poll_resp.result.unwrap()["events"][0]["eventId"], "p1");

    // 3. EventsStream + concurrent EventsList while stream is open
    let mut stream = runtime::open_event_stream(
        &paths,
        &paths
            .request(
                &loaded.digest,
                Operation::EventsStream {
                    server: "mock".into(),
                    event: "task.exited".into(),
                    arguments: None,
                    cursor: None,
                    max_age_ms: None,
                    heartbeat_ms: Some(5000),
                },
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .await
    .unwrap();

    let (first_frame, _) = stream.next_frame().await.unwrap().unwrap();
    assert!(matches!(
        first_frame,
        StreamFrameKind::Notification { ref method, .. } if method == "notifications/events/active"
    ));

    // Concurrent EventsList succeeds immediately while EventsStream is active.
    let (concurrent_list, _) = runtime::exchange(
        &paths,
        &paths
            .request(
                &loaded.digest,
                Operation::EventsList {
                    server: "mock".into(),
                },
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        concurrent_list.result.unwrap()["events"][0]["name"],
        "task.exited"
    );

    let (second_frame, _) = stream.next_frame().await.unwrap().unwrap();
    assert!(matches!(
        second_frame,
        StreamFrameKind::Notification { ref method, .. } if method == "notifications/events/event"
    ));

    let (third_frame, _) = stream.next_frame().await.unwrap().unwrap();
    assert!(matches!(
        third_frame,
        StreamFrameKind::Notification { ref method, .. } if method == "notifications/events/terminated"
    ));

    let (end_frame, _) = stream.next_frame().await.unwrap().unwrap();
    assert!(matches!(end_frame, StreamFrameKind::End));

    let _ = runtime::stop(&paths).await;
    let _ = daemon_task.await;
    server_task.abort();
}
#[tokio::test]
async fn daemon_stream_eof_and_preclosed_requests_preserve_stderr() {
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let stop = base_path.join("stop");
    let log = base_path.join("requests");
    let config_path = base_path.join("mcpcli.json");
    let python = std::env::var("MCPCLI_TEST_PYTHON")
        .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({
            "servers": {
                "srv": {
                    "transport": "stdio",
                    "command": python,
                    "args": [
                        "-u", "-c",
                        include_str!("../../tests/fixtures/events_exit_server.py"),
                        "after-active", stop, log
                    ]
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let loaded = LoadedConfig::load(&config_path).unwrap();
    let paths =
        Paths::with_root(loaded.path.clone(), "stderr-eof", &base_path.join("run")).unwrap();
    let daemon_paths = paths.clone();
    let daemon_loaded = loaded.clone();
    let daemon_task = tokio::spawn(async move {
        run_with_options(daemon_paths, daemon_loaded, DaemonOptions::default()).await
    });
    timeout(Duration::from_secs(5), async {
        while !paths.running().unwrap_or(false) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut stream = runtime::open_event_stream(
        &paths,
        &paths
            .request(
                &loaded.digest,
                Operation::EventsStream {
                    server: "srv".into(),
                    event: "pr.merged".into(),
                    arguments: None,
                    cursor: None,
                    max_age_ms: None,
                    heartbeat_ms: None,
                },
                Duration::from_secs(5),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    let (active, lease) = stream.next_frame().await.unwrap().unwrap();
    assert!(matches!(
        active,
        StreamFrameKind::Notification { ref method, .. }
            if method == "notifications/events/active"
    ));
    drop(lease);
    std::fs::write(stop, b"stop").unwrap();
    let (frame, lease) = timeout(Duration::from_secs(5), stream.next_frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let StreamFrameKind::Error { error } = frame else {
        panic!("closed stdio stream must report its failure");
    };
    let assert_tail = |error: &Failure| {
        assert_eq!(error.code, 6);
        assert_eq!(error.kind, "session_lost");
        assert!(
            error.data.as_ref().unwrap()["stderr_tail"]
                .as_str()
                .unwrap()
                .contains("events fixture stopped: after-active")
        );
    };
    assert_tail(&error);
    drop(error);
    drop(lease);
    drop(stream);

    for operation in [
        Operation::EventsList {
            server: "srv".into(),
        },
        Operation::EventsPoll {
            server: "srv".into(),
            event: "pr.merged".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            max_events: None,
        },
        Operation::Server {
            server: "srv".into(),
        },
    ] {
        let request = paths
            .request(&loaded.digest, operation, Duration::from_secs(5))
            .unwrap();
        let error = runtime::exchange(&paths, &request).await.unwrap_err();
        assert_tail(error.downcast_ref::<Failure>().unwrap());
    }
    assert_eq!(
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["server/discover", "events/stream"]
    );
    runtime::stop(&paths).await.unwrap();
    daemon_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn queued_events_do_not_block_an_earlier_ordinary_request() {
    let (url, server_task) = start_mock_events_http_server().await;
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_vec(&json!({"servers": {"srv": {"transport": "http", "url": url}}}))
            .unwrap(),
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths =
        Paths::with_root(config.path.clone(), "lock-order", &base_path.join("run")).unwrap();
    let app = App {
        paths,
        config,
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::from([("srv".into(), Arc::new(Mutex::new(ServerState::default())))]),
        call_cancels: BTreeMap::from([("srv".into(), CallCancelSlot::default())]),
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let deadlines = ExecuteDeadlines {
        pre_dispatch: deadline,
        call_idle: Duration::from_secs(10),
        call_hard: None,
    };
    app.ensure_events_client("srv", deadline).await.unwrap();
    let state = app.servers["srv"].lock().await;
    let running = app.work.acquire().await.unwrap();
    let operation = Operation::Server {
        server: "srv".into(),
    };
    let dispatched = AtomicBool::new(false);
    let polling = AtomicBool::new(false);
    let mut ordinary_permit = None;
    let mut ordinary = Box::pin(app.execute(
        &operation,
        ExecuteContext {
            deadlines,
            dispatched: &dispatched,
            client_cancel: None,
            polling_started: &polling,
            grace_notify: None,
        },
        &mut ordinary_permit,
    ));
    assert!(futures_util::poll!(ordinary.as_mut()).is_pending());
    let operations: Vec<_> = (0..MAX_IN_FLIGHT)
        .map(|_| Operation::EventsList {
            server: "srv".into(),
        })
        .collect();
    let mut event_permits: Vec<Option<OwnedSemaphorePermit>> =
        (0..MAX_IN_FLIGHT).map(|_| None).collect();
    let mut events: Vec<_> = operations
        .iter()
        .zip(event_permits.iter_mut())
        .map(|(op, permit)| {
            Box::pin(app.execute(
                op,
                ExecuteContext {
                    deadlines,
                    dispatched: &dispatched,
                    client_cancel: None,
                    polling_started: &polling,
                    grace_notify: None,
                },
                permit,
            ))
        })
        .collect();
    for event in &mut events {
        assert!(futures_util::poll!(event.as_mut()).is_pending());
    }
    drop(state);
    drop(running);
    timeout(Duration::from_secs(2), async {
        ordinary.await.unwrap();
        for event in events {
            event.await.unwrap();
        }
    })
    .await
    .expect("mixed ordinary/events requests must make progress before their deadlines");
    let mut state = app.servers["srv"].lock().await;
    state.connection.as_mut().unwrap().close().await.unwrap();
    server_task.abort();
}

#[tokio::test]
async fn http_session_404_marks_server_lost_and_cancels_stream_tokens() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/mcp");
    let srv = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (read_half, mut write_half) = socket.into_split();
                let mut reader = BufReader::new(read_half);
                loop {
                    let mut content_len = 0usize;
                    let mut has_session = false;
                    let mut is_get = false;
                    let mut is_delete = false;
                    let mut line = String::new();
                    let mut first = true;
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        if first {
                            is_get = line.starts_with("GET ");
                            is_delete = line.starts_with("DELETE ");
                            first = false;
                        }
                        if line == "\r\n" || line == "\n" {
                            break;
                        }
                        if let Some((k, v)) = line.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("content-length") {
                                content_len = v.trim().parse().unwrap_or(0);
                            }
                            if k.trim().eq_ignore_ascii_case("mcp-session-id") {
                                has_session = true;
                            }
                        }
                    }
                    if is_get {
                        let _ = write_half
                            .write_all(
                                b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                            )
                            .await;
                        continue;
                    }
                    if is_delete {
                        let _ = write_half
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        continue;
                    }
                    let mut body = vec![0u8; content_len];
                    if reader.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let Ok(req) = serde_json::from_slice::<Value>(&body) else {
                        return;
                    };
                    let id = req.get("id").cloned().unwrap_or(json!(1));
                    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
                    match method {
                        "server/discover" => {
                            let err = json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}});
                            let payload = serde_json::to_vec(&err).unwrap();
                            let hdr = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                payload.len()
                            );
                            let _ = write_half.write_all(hdr.as_bytes()).await;
                            let _ = write_half.write_all(&payload).await;
                        }
                        "initialize" => {
                            let resp = json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "protocolVersion": "2025-11-25",
                                    "capabilities": { "tools": {} },
                                    "serverInfo": { "name": "stateful", "version": "1.0" }
                                }
                            });
                            let payload = serde_json::to_vec(&resp).unwrap();
                            let hdr = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: sess-1\r\nContent-Length: {}\r\n\r\n",
                                payload.len()
                            );
                            let _ = write_half.write_all(hdr.as_bytes()).await;
                            let _ = write_half.write_all(&payload).await;
                        }
                        "tools/list" if has_session => {
                            let _ = write_half
                                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                                .await;
                        }
                        _ => {
                            let _ = write_half
                                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                                .await;
                        }
                    }
                }
            });
        }
    });

    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_vec(&json!({"servers": {"srv": {"transport": "http", "url": url}}}))
            .unwrap(),
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(config.path.clone(), "exp404", &base_path.join("run")).unwrap();
    let app = App {
        paths,
        config,
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::from([("srv".into(), Arc::new(Mutex::new(ServerState::default())))]),
        call_cancels: BTreeMap::from([("srv".into(), CallCancelSlot::default())]),
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let deadlines = ExecuteDeadlines {
        pre_dispatch: deadline,
        call_idle: Duration::from_secs(10),
        call_hard: None,
    };
    let (_, stream_cancel) = app.ensure_events_client("srv", deadline).await.unwrap();
    assert!(!stream_cancel.is_cancelled());

    let dispatched = AtomicBool::new(false);
    let polling = AtomicBool::new(false);
    let mut permit = None;
    let err = app
        .execute(
            &Operation::Tools {
                server: "srv".into(),
            },
            ExecuteContext {
                deadlines,
                dispatched: &dispatched,
                client_cancel: None,
                polling_started: &polling,
                grace_notify: None,
            },
            &mut permit,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(err.code, 4);
    assert_eq!(err.kind, "mcp_transport");
    assert!(stream_cancel.is_cancelled());
    assert_eq!(app.status()["servers"]["srv"]["state"], "lost");

    let next_err = app
        .execute(
            &Operation::Server {
                server: "srv".into(),
            },
            ExecuteContext {
                deadlines,
                dispatched: &dispatched,
                client_cancel: None,
                polling_started: &polling,
                grace_notify: None,
            },
            &mut permit,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(next_err.code, 6);
    assert_eq!(next_err.kind, "session_lost");
    srv.abort();
}

#[test]
fn validate_enforces_max_timeout_ms_bounds_and_call_only() {
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        r#"{"servers":{"srv":{"transport":"http","url":"http://127.0.0.1:9001/mcp"}}}"#,
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(config.path.clone(), "valtest", &base_path.join("run")).unwrap();
    let app = App {
        paths: paths.clone(),
        config: config.clone(),
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::new(),
        call_cancels: BTreeMap::new(),
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };

    // max_timeout_ms on non-Call operation is rejected.
    let mut req = paths
        .request(
            &config.digest,
            Operation::Tools {
                server: "srv".into(),
            },
            Duration::from_secs(5),
        )
        .unwrap();
    req.max_timeout_ms = Some(10_000);
    let err = app.validate(&req).unwrap_err();
    assert_eq!(err.code, 2);
    assert_eq!(err.kind, "invalid_timeout");

    // max_timeout_ms < timeout_ms is rejected.
    let mut call_req = paths
        .request_call_with_budgets(
            &config.digest,
            Operation::Call {
                server: "srv".into(),
                tool: "t".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: false,
                expected_schema: None,
            },
            Duration::from_secs(10),
            Some(Duration::from_secs(5)),
            Duration::ZERO,
        )
        .unwrap();
    let err = app.validate(&call_req).unwrap_err();
    assert_eq!(err.code, 2);
    assert_eq!(err.kind, "invalid_timeout");

    // max_timeout_ms > 86_400_000 is rejected.
    call_req.max_timeout_ms = Some(86_400_001);
    let err = app.validate(&call_req).unwrap_err();
    assert_eq!(err.code, 2);
    assert_eq!(err.kind, "invalid_timeout");

    // Valid bounds succeed.
    call_req.max_timeout_ms = Some(10_000);
    app.validate(&call_req).unwrap();
    call_req.max_timeout_ms = Some(86_400_000);
    app.validate(&call_req).unwrap();
}

#[test]
fn mcp_error_maps_cancelled_and_merges_auth_and_stderr_tail() {
    let cancelled = McpFailure {
        kind: FailureKind::Cancelled,
        message: "MCP call cancelled".into(),
        outcome_unknown: true,
        rpc_error: None,
        auth: None,
        stderr_tail: None,
        lease: None,
    };
    let failure = mcp_error_for_server(cancelled.into(), true, Some("srv"));
    assert_eq!(failure.code, 4);
    assert_eq!(failure.kind, "cancelled");
    assert!(failure.outcome_unknown);
    assert!(failure.data.is_none());

    let auth = crate::auth::AuthChallengeInfo::from_response(
        http::StatusCode::UNAUTHORIZED,
        &http::HeaderMap::new(),
    );
    let combined = McpFailure {
        kind: FailureKind::AuthRequired,
        message: "auth required".into(),
        outcome_unknown: false,
        rpc_error: None,
        auth: Some(auth),
        stderr_tail: Some("trace line\n".into()),
        lease: None,
    };
    let failure = mcp_error_for_server(combined.into(), false, Some("srv"));
    let data = failure.data.expect("merged data object");
    assert_eq!(data["status"], 401);
    assert_eq!(data["stderr_tail"], "trace line\n");
}

async fn start_mock_progress_http_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/mcp");
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let (read_half, mut write_half) = socket.into_split();
                let mut reader = BufReader::new(read_half);
                let mut content_len = 0usize;
                let mut is_delete = false;
                let mut line = String::new();
                let mut first = true;
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if first {
                        is_delete = line.starts_with("DELETE ");
                        first = false;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':')
                        && k.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                }
                if is_delete {
                    let _ = write_half
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }
                let mut body = vec![0u8; content_len];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                let Ok(req) = serde_json::from_slice::<Value>(&body) else {
                    return;
                };
                let id = req.get("id").cloned().unwrap_or(json!(1));
                let method = req.get("method").and_then(Value::as_str).unwrap_or("");
                match method {
                    "server/discover" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "supportedVersions": ["2026-07-28"],
                                "capabilities": { "tools": {} },
                                "ttlMs": 60000,
                                "cacheScope": "public",
                                "_meta": {
                                    "io.modelcontextprotocol/serverInfo": {
                                        "name": "mock-progress",
                                        "version": "1.0"
                                    }
                                }
                            }
                        });
                        let payload = serde_json::to_vec(&resp).unwrap();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let _ = write_half.write_all(header.as_bytes()).await;
                        let _ = write_half.write_all(&payload).await;
                    }
                    "tools/list" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "tools": [
                                    { "name": "progress_ok", "inputSchema": { "type": "object" } },
                                    { "name": "progress_silent", "inputSchema": { "type": "object" } },
                                    { "name": "progress_forever", "inputSchema": { "type": "object" } }
                                ]
                            }
                        });
                        let payload = serde_json::to_vec(&resp).unwrap();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let _ = write_half.write_all(header.as_bytes()).await;
                        let _ = write_half.write_all(&payload).await;
                    }
                    "tools/call" => {
                        let tool = req["params"]["name"].as_str().unwrap_or("");
                        let token = req["params"]["_meta"]["progressToken"].clone();
                        let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
                        let _ = write_half.write_all(header.as_bytes()).await;
                        match tool {
                            "progress_ok" => {
                                let interval = req["params"]["arguments"]["interval_ms"]
                                    .as_u64()
                                    .unwrap_or(220);
                                let steps =
                                    req["params"]["arguments"]["steps"].as_u64().unwrap_or(6);
                                for step in 1..=steps {
                                    tokio::time::sleep(Duration::from_millis(interval)).await;
                                    let notif = json!({
                                        "jsonrpc": "2.0",
                                        "method": "notifications/progress",
                                        "params": {
                                            "progressToken": token,
                                            "progress": step,
                                            "total": steps
                                        }
                                    });
                                    let sse = format!(
                                        "data: {}\n\n",
                                        serde_json::to_string(&notif).unwrap()
                                    );
                                    if write_half.write_all(sse.as_bytes()).await.is_err() {
                                        return;
                                    }
                                    let _ = write_half.flush().await;
                                }
                                let done = json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": {
                                        "content": [{ "type": "text", "text": "completed" }]
                                    }
                                });
                                let sse =
                                    format!("data: {}\n\n", serde_json::to_string(&done).unwrap());
                                let _ = write_half.write_all(sse.as_bytes()).await;
                                let _ = write_half.flush().await;
                            }
                            "progress_silent" => {
                                tokio::time::sleep(Duration::from_millis(150)).await;
                                let notif = json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/progress",
                                    "params": {
                                        "progressToken": token,
                                        "progress": 1
                                    }
                                });
                                let sse =
                                    format!("data: {}\n\n", serde_json::to_string(&notif).unwrap());
                                let _ = write_half.write_all(sse.as_bytes()).await;
                                let _ = write_half.flush().await;
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                            "progress_forever" => {
                                for step in 1..=30 {
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                    let notif = json!({
                                        "jsonrpc": "2.0",
                                        "method": "notifications/progress",
                                        "params": {
                                            "progressToken": token,
                                            "progress": step
                                        }
                                    });
                                    let sse = format!(
                                        "data: {}\n\n",
                                        serde_json::to_string(&notif).unwrap()
                                    );
                                    if write_half.write_all(sse.as_bytes()).await.is_err() {
                                        return;
                                    }
                                    let _ = write_half.flush().await;
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {
                        let header = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = write_half.write_all(header.as_bytes()).await;
                    }
                }
            });
        }
    });
    (url, task)
}

#[tokio::test]
async fn daemon_uses_one_budget_for_mcp_results_and_media_wrappers() {
    let (url, server_task) = start_mock_progress_http_server().await;
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let config_path = base_path.join("mcpcli.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({
            "servers": { "mock": { "transport": "http", "url": url } }
        }))
        .unwrap(),
    )
    .unwrap();
    let loaded = LoadedConfig::load(&config_path).unwrap();
    let paths =
        Paths::with_root(loaded.path.clone(), "shared-budget", &base_path.join("run")).unwrap();
    let budget = crate::budget::ByteBudget::new(1024 * 1024);
    let separate_mcp_budget = crate::budget::ByteBudget::new(1024 * 1024);
    let options = DaemonOptions {
        budget: budget.clone(),
        mcp_limits: crate::mcp::McpLimits {
            budget: separate_mcp_budget.clone(),
            ..Default::default()
        },
        ..Default::default()
    };
    let daemon_task = tokio::spawn(run_with_options(paths.clone(), loaded.clone(), options));
    timeout(runtime::START_WAIT, async {
        loop {
            if runtime::exchange(
                &paths,
                &paths
                    .request("", Operation::Status, Duration::from_millis(200))
                    .unwrap(),
            )
            .await
            .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();

    let request = paths
        .request(
            &loaded.digest,
            Operation::Call {
                server: "mock".into(),
                tool: "progress_ok".into(),
                arguments: serde_json::from_value(json!({ "steps": 0 })).unwrap(),
                inline_media: false,
                detach: false,
                expected_schema: None,
            },
            Duration::from_secs(5),
        )
        .unwrap();
    let (response, _lease) = runtime::exchange(&paths, &request).await.unwrap();
    let value = response.result.unwrap();
    assert_eq!(value["result"]["content"][0]["text"], "completed");
    assert_eq!(value["savedMedia"], json!([]));
    assert!(
        budget.used() > 0,
        "the MCP connection retains its server info"
    );
    assert_eq!(separate_mcp_budget.used(), 0);

    runtime::stop(&paths).await.unwrap();
    daemon_task.await.unwrap().unwrap();
    assert_eq!(budget.used(), 0);
    assert_eq!(separate_mcp_budget.used(), 0);
    server_task.abort();
}

#[tokio::test]
async fn dual_clock_call_progress_timeout_queue_timeout_and_force_reset() {
    let (url, server_task) = start_mock_progress_http_server().await;
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_string(&json!({
            "servers": {
                "mock": {
                    "transport": "http",
                    "url": url
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let loaded = LoadedConfig::load(&cfg_path).unwrap();
    let root = base_path.join("runtime");
    let paths = Paths::with_root(loaded.path.clone(), "dualclk", &root).unwrap();

    let daemon_paths = paths.clone();
    let daemon_loaded = loaded.clone();
    let daemon_task = tokio::spawn(async move {
        run_with_options(daemon_paths, daemon_loaded, DaemonOptions::default()).await
    });

    for _ in 0..80 {
        if paths.running().unwrap_or(false)
            && runtime::exchange(
                &paths,
                &paths
                    .request("", Operation::Status, Duration::from_millis(200))
                    .unwrap(),
            )
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 1. Unbounded Call (max_timeout_ms: None) runs past idle timeout (600ms) to ~1.3s when progress arrives.
    let req_ok = paths
        .request_call_with_budgets(
            &loaded.digest,
            Operation::Call {
                server: "mock".into(),
                tool: "progress_ok".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: false,
                expected_schema: None,
            },
            Duration::from_millis(600),
            None,
            Duration::ZERO,
        )
        .unwrap();
    let (resp_ok, _) = runtime::exchange(&paths, &req_ok).await.unwrap();
    let val = resp_ok.result.unwrap();
    assert_eq!(
        val["result"]["content"][0]["text"], "completed",
        "got: {val}"
    );

    // Startup consumes admission time without shortening later progress gaps.
    for hard in [None, Some(Duration::from_millis(2_500))] {
        let request = paths
            .request_call_with_budgets(
                &loaded.digest,
                Operation::Call {
                    server: "mock".into(),
                    tool: "progress_ok".into(),
                    arguments: json!({"interval_ms": 700, "steps": 2})
                        .as_object()
                        .unwrap()
                        .clone(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_secs(1),
                hard,
                Duration::from_millis(400),
            )
            .unwrap();
        let (response, _) = runtime::exchange(&paths, &request).await.unwrap();
        assert_eq!(
            response.result.unwrap()["result"]["content"][0]["text"],
            "completed"
        );
    }

    // 2. Silent tool times out at ~500ms with exit 5 and outcome_unknown = true.
    let started = Instant::now();
    let req_silent = paths
        .request_call_with_budgets(
            &loaded.digest,
            Operation::Call {
                server: "mock".into(),
                tool: "progress_silent".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: false,
                expected_schema: None,
            },
            Duration::from_millis(500),
            None,
            Duration::ZERO,
        )
        .unwrap();
    let err_silent = runtime::exchange(&paths, &req_silent).await.unwrap_err();
    let fail_silent = err_silent.downcast_ref::<Failure>().unwrap();
    assert_eq!(fail_silent.code, 5);
    assert_eq!(fail_silent.kind, "timeout");
    assert!(fail_silent.outcome_unknown);
    assert!(started.elapsed() < Duration::from_secs(2));

    // 3. Explicit max_timeout_ms caps a continuously progressing tool at ~900ms.
    let started = Instant::now();
    let req_capped = paths
        .request_call_with_budgets(
            &loaded.digest,
            Operation::Call {
                server: "mock".into(),
                tool: "progress_forever".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: false,
                expected_schema: None,
            },
            Duration::from_millis(500),
            Some(Duration::from_millis(900)),
            Duration::ZERO,
        )
        .unwrap();
    let err_capped = runtime::exchange(&paths, &req_capped).await.unwrap_err();
    let fail_capped = err_capped.downcast_ref::<Failure>().unwrap();
    assert_eq!(fail_capped.code, 5);
    assert_eq!(fail_capped.kind, "timeout");
    assert!(fail_capped.outcome_unknown);
    assert!(started.elapsed() < Duration::from_secs(2));

    // 4. Queued Call times out promptly instead of hanging behind a long unbounded Call,
    //    and reset --force interrupts the in-flight long Call immediately.
    let long_paths = paths.clone();
    let long_digest = loaded.digest.clone();
    let long_call = tokio::spawn(async move {
        let req = long_paths
            .request_call_with_budgets(
                &long_digest,
                Operation::Call {
                    server: "mock".into(),
                    tool: "progress_forever".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_millis(800),
                None,
                Duration::ZERO,
            )
            .unwrap();
        runtime::exchange(&long_paths, &req).await
    });
    tokio::time::sleep(Duration::from_millis(250)).await;

    let q_started = Instant::now();
    let req_queued = paths
        .request_call_with_budgets(
            &loaded.digest,
            Operation::Call {
                server: "mock".into(),
                tool: "progress_ok".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: false,
                expected_schema: None,
            },
            Duration::from_millis(350),
            None,
            Duration::ZERO,
        )
        .unwrap();
    let err_queued = runtime::exchange(&paths, &req_queued).await.unwrap_err();
    let fail_queued = err_queued.downcast_ref::<Failure>().unwrap();
    assert_eq!(fail_queued.code, 5);
    assert_eq!(fail_queued.kind, "timeout");
    assert!(!fail_queued.outcome_unknown);
    assert!(fail_queued.message.contains("queued"));
    assert!(q_started.elapsed() < Duration::from_secs(2));

    let r_started = Instant::now();
    let req_reset = paths
        .request(
            &loaded.digest,
            Operation::Reset {
                server: "mock".into(),
                force: true,
            },
            Duration::from_secs(5),
        )
        .unwrap();
    let (reset_resp, _) = runtime::exchange(&paths, &req_reset).await.unwrap();
    assert_eq!(reset_resp.result.unwrap()["reset"], true);
    assert!(r_started.elapsed() < Duration::from_secs(2));

    let long_err = long_call.await.unwrap().unwrap_err();
    let long_fail = long_err.downcast_ref::<Failure>().unwrap();
    assert_eq!(long_fail.code, 4);
    assert_eq!(long_fail.kind, "cancelled");
    assert!(long_fail.outcome_unknown);

    let _ = runtime::stop(&paths).await;
    let _ = daemon_task.await;
    server_task.abort();
}

#[test]
fn force_reset_cancellation_survives_another_reset_finishing() {
    let slot = CallCancelSlot::default();
    let (active, token) = CallCancelRegistration::register(&slot, CancellationToken::new());
    let first = ForceResetCancellation::begin(&slot);
    let second = ForceResetCancellation::begin(&slot);
    assert!(token.is_cancelled());
    drop(active);
    drop(first);
    let (queued, token) = CallCancelRegistration::register(&slot, CancellationToken::new());
    assert!(token.is_cancelled());
    drop(queued);
    drop(second);
    let (_fresh, token) = CallCancelRegistration::register(&slot, CancellationToken::new());
    assert!(!token.is_cancelled());
}

#[tokio::test]
async fn force_reset_cancels_call_already_queued_behind_active_call() {
    let (url, server_task) = start_mock_progress_http_server().await;
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_vec(&json!({
            "servers": {"mock": {"transport": "http", "url": url}}
        }))
        .unwrap(),
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths =
        Paths::with_root(config.path.clone(), "resetqueue", &base_path.join("run")).unwrap();
    let app = App {
        paths,
        config,
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::from([("mock".into(), Arc::new(Mutex::new(ServerState::default())))]),
        call_cancels: BTreeMap::from([("mock".into(), CallCancelSlot::default())]),
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };
    let deadlines = ExecuteDeadlines {
        pre_dispatch: Instant::now() + Duration::from_secs(10),
        call_idle: Duration::from_secs(10),
        call_hard: None,
    };
    let operation = Operation::Call {
        server: "mock".into(),
        tool: "progress_forever".into(),
        arguments: serde_json::Map::new(),
        inline_media: true,
        detach: false,
        expected_schema: None,
    };
    let active_dispatched = AtomicBool::new(false);
    let polling = AtomicBool::new(false);
    let mut active_permit = None;
    let active = app.execute(
        &operation,
        ExecuteContext {
            deadlines,
            dispatched: &active_dispatched,
            client_cancel: None,
            polling_started: &polling,
            grace_notify: None,
        },
        &mut active_permit,
    );
    tokio::pin!(active);
    timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            assert!(active.as_mut().poll(cx).is_pending());
            if active_dispatched.load(Ordering::Acquire) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }),
    )
    .await
    .unwrap();

    let queued_dispatched = AtomicBool::new(false);
    let mut queued_permit = None;
    let queued = app.execute(
        &operation,
        ExecuteContext {
            deadlines,
            dispatched: &queued_dispatched,
            client_cancel: None,
            polling_started: &polling,
            grace_notify: None,
        },
        &mut queued_permit,
    );
    tokio::pin!(queued);
    assert!(futures_util::poll!(&mut queued).is_pending());
    let reset_operation = Operation::Reset {
        server: "mock".into(),
        force: true,
    };
    let reset_dispatched = AtomicBool::new(false);
    let mut reset_permit = None;
    let reset = app.execute(
        &reset_operation,
        ExecuteContext {
            deadlines,
            dispatched: &reset_dispatched,
            client_cancel: None,
            polling_started: &polling,
            grace_notify: None,
        },
        &mut reset_permit,
    );
    tokio::pin!(reset);
    assert!(futures_util::poll!(&mut reset).is_pending());

    let (active_result, queued_result, reset_result) = timeout(Duration::from_secs(3), async {
        tokio::join!(&mut active, &mut queued, &mut reset)
    })
    .await
    .expect("force reset must not wait for the queued call to run");
    let active_error = active_result.err().unwrap();
    assert_eq!(active_error.kind, "cancelled");
    assert!(active_error.outcome_unknown);
    let queued_error = queued_result.err().unwrap();
    assert_eq!(queued_error.kind, "cancelled");
    assert!(!queued_error.outcome_unknown);
    assert!(!queued_dispatched.load(Ordering::Acquire));
    assert_eq!(reset_result.unwrap().0["reset"], true);

    let fresh_operation = Operation::Call {
        server: "mock".into(),
        tool: "progress_ok".into(),
        arguments: json!({"interval_ms": 10, "steps": 1})
            .as_object()
            .unwrap()
            .clone(),
        inline_media: true,
        detach: false,
        expected_schema: None,
    };
    let fresh_dispatched = AtomicBool::new(false);
    let mut fresh_permit = None;
    let (value, _, _) = app
        .execute(
            &fresh_operation,
            ExecuteContext {
                deadlines,
                dispatched: &fresh_dispatched,
                client_cancel: None,
                polling_started: &polling,
                grace_notify: None,
            },
            &mut fresh_permit,
        )
        .await
        .unwrap();
    assert_eq!(value["content"][0]["text"], "completed");
    let mut state = app.servers["mock"].lock().await;
    state.connection.as_mut().unwrap().close().await.unwrap();
    server_task.abort();
}

#[test]
fn disabled_server_reports_disabled_status_and_fails_validate() {
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        serde_json::to_vec(&json!({
            "servers": {
                "on": {"transport": "http", "url": "http://127.0.0.1:9001/mcp"},
                "off": {"transport": "http", "url": "http://127.0.0.1:9002/mcp", "enabled": false}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(config.path.clone(), "disabled", &base_path.join("run")).unwrap();
    let app = App {
        paths,
        config,
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::from([
            ("on".into(), Arc::new(Mutex::new(ServerState::default()))),
            ("off".into(), Arc::new(Mutex::new(ServerState::default()))),
        ]),
        call_cancels: BTreeMap::from([
            ("on".into(), CallCancelSlot::default()),
            ("off".into(), CallCancelSlot::default()),
        ]),
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };

    let status = app.status();
    assert_eq!(status["servers"]["on"]["state"], "lazy");
    assert_eq!(status["servers"]["off"]["state"], "disabled");

    let ops = [
        Operation::Server {
            server: "off".into(),
        },
        Operation::Tools {
            server: "off".into(),
        },
        Operation::Call {
            server: "off".into(),
            tool: "ping".into(),
            arguments: serde_json::Map::new(),
            inline_media: false,
            detach: false,
            expected_schema: None,
        },
        Operation::Reset {
            server: "off".into(),
            force: false,
        },
        Operation::EventsList {
            server: "off".into(),
        },
        Operation::EventsPoll {
            server: "off".into(),
            event: "tick".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            max_events: None,
        },
        Operation::EventsStream {
            server: "off".into(),
            event: "tick".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            heartbeat_ms: None,
        },
    ];
    for op in ops {
        let req = Request {
            protocol: ipc::PROTOCOL,
            namespace: app.paths.namespace.clone(),
            build: app.paths.build.clone(),
            digest: app.config.digest.clone(),
            timeout_ms: 5_000,
            max_timeout_ms: None,
            deadline_ms: 5_000,
            explicit_deadline_ms: None,
            operation: op,
        };
        let err = app.validate(&req).unwrap_err();
        assert_eq!(err.code, 2);
        assert_eq!(err.kind, "server_disabled");
    }

    let enabled_req = Request {
        protocol: ipc::PROTOCOL,
        namespace: app.paths.namespace.clone(),
        build: app.paths.build.clone(),
        digest: app.config.digest.clone(),
        timeout_ms: 5_000,
        max_timeout_ms: None,
        deadline_ms: 5_000,
        explicit_deadline_ms: None,
        operation: Operation::Tools {
            server: "on".into(),
        },
    };
    assert!(app.validate(&enabled_req).is_ok());
}

#[tokio::test]
async fn waiting_task_poll_and_cancel_leave_work_for_the_server_lock_holder() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;

    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        r#"{"servers":{"on":{"transport":"http","url":"http://127.0.0.1:9001/mcp"}}}"#,
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(config.path.clone(), "tasklock", &base_path.join("run")).unwrap();
    let server = Arc::new(Mutex::new(ServerState::default()));
    let app = App {
        paths,
        config,
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::from([("on".into(), server.clone())]),
        call_cancels: BTreeMap::from([("on".into(), CallCancelSlot::default())]),
        work: Semaphore::new(1),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };
    let server_guard = server.lock().await;
    let connections = Arc::new(Semaphore::new(1));
    let mut conn_permit = Some(connections.clone().acquire_owned().await.unwrap());
    let polling_started = AtomicBool::new(false);
    let cancel = CancellationToken::new();
    let mut polling = Box::pin(app.poll_task_in_daemon(
        DaemonTaskPoll {
            server: "on",
            initial_conn_id: 0,
            seed: None,
            task_id: "task-1".into(),
            owns_task: true,
            deadlines: ExecuteDeadlines {
                pre_dispatch: Instant::now() + Duration::from_secs(5),
                call_idle: Duration::from_secs(5),
                call_hard: None,
            },
            cancel: cancel.clone(),
            polling_started: &polling_started,
            grace_notify: None,
        },
        &mut conn_permit,
    ));
    assert!(poll_fn(|cx| Poll::Ready(polling.as_mut().poll(cx).is_pending())).await);
    assert!(polling_started.load(Ordering::Acquire));
    assert_eq!(connections.available_permits(), 1);
    let work = app
        .work
        .try_acquire()
        .expect("the server lock holder must still be able to dispatch work");
    drop(work);

    cancel.cancel();
    assert!(poll_fn(|cx| Poll::Ready(polling.as_mut().poll(cx).is_pending())).await);
    let work = app
        .work
        .try_acquire()
        .expect("automatic cancellation must also wait for the server before taking work");
    drop(work);
    drop(server_guard);
    let error = timeout(Duration::from_secs(1), polling)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind, "cancelled");
    assert!(error.outcome_unknown);
    assert_eq!(error.data.as_ref().unwrap()["taskId"], "task-1");
    assert_eq!(app.task_waiters.available_permits(), MAX_TASK_WAITERS);
    assert_eq!(app.work.available_permits(), 1);
    assert!(!server.lock().await.lost);
}

#[test]
fn validate_new_skills_tasks_and_multi_slot_cancel() {
    let slot = CallCancelSlot::default();
    let (_r1, t1) = CallCancelRegistration::register(&slot, CancellationToken::new());
    let (_r2, t2) = CallCancelRegistration::register(&slot, CancellationToken::new());
    assert!(!t1.is_cancelled() && !t2.is_cancelled());
    let _fr = ForceResetCancellation::begin(&slot);
    assert!(t1.is_cancelled() && t2.is_cancelled());

    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let base_path = base.path().canonicalize().unwrap();
    let cfg_path = base_path.join("mcpcli.json");
    std::fs::write(
        &cfg_path,
        r#"{"servers":{"on":{"transport":"http","url":"http://127.0.0.1:9001/mcp"}}}"#,
    )
    .unwrap();
    let config = LoadedConfig::load(&cfg_path).unwrap();
    let paths = Paths::with_root(config.path.clone(), "valnew", &base_path.join("run")).unwrap();
    let app = App {
        paths: paths.clone(),
        config: config.clone(),
        state: State::fresh(),
        options: DaemonOptions::default(),
        servers: BTreeMap::from([("on".into(), Arc::new(Mutex::new(ServerState::default())))]),
        call_cancels: BTreeMap::from([("on".into(), CallCancelSlot::default())]),
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
        shutdown: CancellationToken::new(),
    };
    let make_req = |op: Operation, max_timeout_ms: Option<u64>| Request {
        protocol: ipc::PROTOCOL,
        namespace: app.paths.namespace.clone(),
        build: app.paths.build.clone(),
        digest: app.config.digest.clone(),
        timeout_ms: 5_000,
        max_timeout_ms,
        deadline_ms: 10_000,
        explicit_deadline_ms: None,
        operation: op,
    };

    // max_timeout_ms allowed on TasksGet { wait: true }, rejected on wait: false & Call { detach: true }
    assert!(
        app.validate(&make_req(
            Operation::TasksGet {
                server: "on".into(),
                task_id: "t-1".into(),
                wait: true,
                inline_media: false,
            },
            Some(10_000)
        ))
        .is_ok()
    );
    assert_eq!(
        app.validate(&make_req(
            Operation::TasksGet {
                server: "on".into(),
                task_id: "t-1".into(),
                wait: false,
                inline_media: false,
            },
            Some(10_000)
        ))
        .unwrap_err()
        .kind,
        "invalid_timeout"
    );
    assert_eq!(
        app.validate(&make_req(
            Operation::Call {
                server: "on".into(),
                tool: "t".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: true,
                expected_schema: None,
            },
            Some(10_000)
        ))
        .unwrap_err()
        .kind,
        "invalid_timeout"
    );

    // Selector, path, dir, task_id, empty input_responses, and deep expected_schema
    assert_eq!(
        app.validate(&make_req(
            Operation::SkillsGet {
                server: "on".into(),
                selector: "a\nb".into(),
            },
            None
        ))
        .unwrap_err()
        .kind,
        "invalid_skill_uri"
    );
    assert_eq!(
        app.validate(&make_req(
            Operation::SkillsRead {
                server: "on".into(),
                selector: "demo".into(),
                path: "".into(),
                inline_media: false,
            },
            None
        ))
        .unwrap_err()
        .kind,
        "invalid_skill_path"
    );
    assert_eq!(
        app.validate(&make_req(
            Operation::TasksCancel {
                server: "on".into(),
                task_id: "x".repeat(513),
            },
            None
        ))
        .unwrap_err()
        .kind,
        "invalid_task_id"
    );
    assert_eq!(
        app.validate(&make_req(
            Operation::TasksUpdate {
                server: "on".into(),
                task_id: "t-1".into(),
                input_responses: serde_json::Map::new(),
            },
            None
        ))
        .unwrap_err()
        .kind,
        "invalid_arguments"
    );
    let mut deep = json!({});
    for _ in 0..70 {
        deep = json!({"a": deep});
    }
    assert_eq!(
        app.validate(&make_req(
            Operation::Call {
                server: "on".into(),
                tool: "t".into(),
                arguments: serde_json::Map::new(),
                inline_media: false,
                detach: false,
                expected_schema: Some(crate::schema::ExpectedSchema {
                    input_schema: deep,
                    output_schema: None,
                    has_tool_envelope: false,
                    mode: crate::schema::SchemaMode::Compatible,
                }),
            },
            None
        ))
        .unwrap_err()
        .kind,
        "schema"
    );
}
