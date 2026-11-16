use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use mcpcli::{
    config::ServerConfig,
    mcp::{FailureKind, McpConnection, McpFailure},
};
use serde_json::{Map, json};
use tempfile::TempDir;

const BUDGET: Duration = Duration::from_secs(5);

fn python() -> String {
    std::env::var("MCPCLI_TEST_PYTHON")
        .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into())
}

struct Fixture {
    dir: TempDir,
    http: Option<Child>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            http: None,
        }
    }
    fn log(&self) -> PathBuf {
        self.dir.path().join("wire.log")
    }
    fn script() -> String {
        format!(
            "{}/tests/fixtures/protocol_server.py",
            env!("CARGO_MANIFEST_DIR")
        )
    }
    fn args(&self, modern: bool, mode: &str) -> Vec<String> {
        let mut args = vec![
            Self::script(),
            "--mode".into(),
            mode.into(),
            "--log".into(),
            self.log().to_string_lossy().into_owned(),
        ];
        if modern {
            args.push("--modern".into());
        }
        args
    }
    fn stdio(&self, modern: bool, mode: &str) -> ServerConfig {
        ServerConfig::Stdio {
            command: python(),
            args: self.args(modern, mode),
            env: BTreeMap::new(),
            cwd: None,
            description: None,
        }
    }
    fn http(&mut self, modern: bool, mode: &str) -> ServerConfig {
        let mut child = Command::new(python())
            .args(self.args(modern, mode))
            .arg("--http")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let port: u16 = line.trim().parse().unwrap();
        self.http = Some(child);
        ServerConfig::Http {
            url: format!("http://127.0.0.1:{port}/mcp"),
            headers: BTreeMap::new(),
            description: None,
        }
    }
    fn lines(&self) -> Vec<String> {
        std::fs::read_to_string(self.log())
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
    fn count(&self, value: &str) -> usize {
        self.lines()
            .iter()
            .filter(|line| line.as_str() == value)
            .count()
    }
    fn pid(&self) -> u32 {
        self.lines()
            .iter()
            .find_map(|line| line.strip_prefix("pid:"))
            .unwrap()
            .parse()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(child) = self.http.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn kind(error: &anyhow::Error) -> FailureKind {
    error
        .downcast_ref::<McpFailure>()
        .expect("classified failure")
        .kind
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition met");
}

fn running(pid: u32) -> bool {
    let status = Command::new(python())
        .arg(format!(
            "{}/tests/platform_support.py",
            env!("CARGO_MANIFEST_DIR")
        ))
        .args(["--running", &pid.to_string()])
        .status()
        .expect("run native process inspection");
    match status.code() {
        Some(0) => true,
        Some(1) => false,
        other => panic!("process inspection failed: {other:?}"),
    }
}

#[tokio::test]
async fn stdio_negotiates_both_eras_and_preserves_application_state() {
    for modern in [false, true] {
        let fixture = Fixture::new();
        let mut connection = McpConnection::connect(&fixture.stdio(modern, "normal"), BUDGET)
            .await
            .unwrap();
        let tools = connection.list_tools(BUDGET).await.unwrap();
        assert_eq!(tools.len(), 9);
        assert_eq!(tools[0]["name"], "get");
        assert_eq!(tools[1]["name"], "set");
        let arguments = json!({"stage": "canary"}).as_object().unwrap().clone();
        connection.call("set", arguments, BUDGET).await.unwrap();
        let result = connection.call("get", Map::new(), BUDGET).await.unwrap();
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("canary")
        );
        assert_eq!(result["structuredContent"]["name"], "get");
        assert_eq!(fixture.count("initialize"), usize::from(!modern));
        assert_eq!(fixture.count("server/discover"), 1);
        let pid = fixture.pid();
        assert!(running(pid), "process observer did not see live fixture");
        connection.close().await.unwrap();
        wait_until(|| !running(pid)).await;
    }
}

#[tokio::test]
async fn discovery_rejects_repeated_cursors_and_duplicate_names() {
    for mode in ["repeat_cursor", "duplicate"] {
        let fixture = Fixture::new();
        let mut connection = McpConnection::connect(&fixture.stdio(true, mode), BUDGET)
            .await
            .unwrap();
        let error = connection.list_tools(BUDGET).await.unwrap_err();
        assert_eq!(kind(&error), FailureKind::Protocol);
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn list_changed_marks_catalog_dirty_and_fresh_list_sees_new_tool() {
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(false, "normal"), BUDGET)
        .await
        .unwrap();
    assert!(!connection.take_tools_changed());
    connection.call("unlock", Map::new(), BUDGET).await.unwrap();
    wait_until(|| connection.take_tools_changed()).await;
    assert!(!connection.take_tools_changed());
    assert!(
        connection
            .list_tools(BUDGET)
            .await
            .unwrap()
            .iter()
            .any(|t| t["name"] == "admin")
    );
    connection.close().await.unwrap();
}

#[tokio::test]
async fn timeout_requests_cancellation_without_replaying_or_losing_connection() {
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(false, "normal"), BUDGET)
        .await
        .unwrap();
    let error = connection
        .call("slow", Map::new(), Duration::from_millis(80))
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Timeout);
    assert!(error.to_string().contains("outcome unknown"));
    wait_until(|| fixture.count("notifications/cancelled") == 1).await;
    assert_eq!(fixture.count("tools/call"), 1);
    let result = connection.call("get", Map::new(), BUDGET).await.unwrap();
    assert_eq!(result["isError"], false);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn protocol_errors_and_tool_errors_remain_distinct() {
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "normal"), BUDGET)
        .await
        .unwrap();
    let error = connection
        .call("invalid", Map::new(), BUDGET)
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Protocol);
    assert!(error.to_string().contains("-32602"));
    let result = connection
        .call("tool_error", Map::new(), BUDGET)
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn failed_handshake_kills_child_and_crashed_server_is_not_restarted() {
    let fixture = Fixture::new();
    let config = fixture.stdio(true, "hang_initialize");
    let connecting = McpConnection::connect(&config, BUDGET);
    tokio::pin!(connecting);
    tokio::select! {
        result = &mut connecting => {
            panic!("handshake ended before the blocked fixture was observed: {:?}", result.err());
        }
        () = wait_until(|| fixture.count("server/discover") == 1) => {}
    }
    let pid = fixture.pid();
    assert!(
        running(pid),
        "blocked handshake fixture exited before its deadline"
    );
    let error = tokio::time::timeout(BUDGET + Duration::from_secs(2), connecting)
        .await
        .expect("handshake timeout and cleanup remain bounded");
    assert_eq!(kind(&error.err().unwrap()), FailureKind::Timeout);
    wait_until(|| !running(pid)).await;

    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "normal"), BUDGET)
        .await
        .unwrap();
    let error = connection
        .call("die", Map::new(), BUDGET)
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Transport);
    assert!(error.to_string().contains("outcome unknown"));
    wait_until(|| connection.is_closed()).await;
    assert_eq!(fixture.count("server/discover"), 1);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn http_supports_legacy_modern_json_and_sse() {
    for (modern, mode) in [
        (false, "normal"),
        (true, "normal"),
        (true, "sse"),
        (true, "mixed_case_ct"),
        (true, "mixed_case_sse"),
        (false, "get_auth_leak"),
    ] {
        let mut fixture = Fixture::new();
        let config = fixture.http(modern, mode);
        let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
        assert_eq!(connection.list_tools(BUDGET).await.unwrap().len(), 9);
        let result = connection.call("get", Map::new(), BUDGET).await.unwrap();
        assert_eq!(result["structuredContent"]["name"], "get");
        connection.close().await.unwrap();
    }
    let mut fixture = Fixture::new();
    let config = fixture.http(true, "jsonp_ct");
    let error = McpConnection::connect(&config, BUDGET).await.err().unwrap();
    assert_eq!(kind(&error), FailureKind::Transport);
}

#[tokio::test]
async fn http_does_not_follow_redirects_or_expose_transport_secrets() {
    for mode in ["auth", "redirect"] {
        let mut fixture = Fixture::new();
        let mut config = fixture.http(true, mode);
        if let ServerConfig::Http { url, headers, .. } = &mut config {
            url.push_str("?token=secret-query-do-not-print");
            headers.insert(
                "Authorization".into(),
                "Bearer secret-header-do-not-print".into(),
            );
        }
        let error = McpConnection::connect(&config, BUDGET).await.err().unwrap();
        assert_eq!(kind(&error), FailureKind::Transport);
        assert!(!format!("{error:?}").contains("secret"));
        assert_eq!(fixture.count("server/discover"), 1);
    }
}

#[tokio::test]
async fn http_session_expiry_does_not_reinitialize_or_replay_tool() {
    let mut fixture = Fixture::new();
    let config = fixture.http(false, "expired");
    let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
    let error = connection
        .call("set", Map::new(), BUDGET)
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Transport);
    assert!(error.to_string().contains("not retried"));
    assert_eq!(fixture.count("initialize"), 1);
    assert_eq!(fixture.count("tools/call"), 1);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn http_rejects_oversize_json_and_reserved_header_overrides() {
    for mode in ["oversize", "oversize_chunked", "oversize_sse"] {
        let mut fixture = Fixture::new();
        let config = fixture.http(true, mode);
        let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
        let error = connection
            .call("get", Map::new(), BUDGET)
            .await
            .unwrap_err();
        assert_eq!(kind(&error), FailureKind::Transport, "{mode}: {error}");
        connection.close().await.unwrap();
    }
    for header in ["Mcp-Session-Id", "Last-Event-Id"] {
        let config = ServerConfig::Http {
            url: "http://127.0.0.1:1/mcp".into(),
            headers: BTreeMap::from([(header.into(), "user-supplied".into())]),
            description: None,
        };
        let error = McpConnection::connect(&config, BUDGET).await.err().unwrap();
        assert_eq!(kind(&error), FailureKind::Protocol);
    }
}

#[tokio::test]
async fn normal_close_cleans_up_server_process_group() {
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "normal"), BUDGET)
        .await
        .unwrap();
    connection
        .call("spawn_child", Map::new(), BUDGET)
        .await
        .unwrap();
    let descendant: u32 = fixture
        .lines()
        .iter()
        .find_map(|line| line.strip_prefix("desc:"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(running(descendant));
    connection.close().await.unwrap();
    wait_until(|| !running(descendant) && !running(fixture.pid())).await;
}

#[tokio::test]
async fn direct_http_call_discovers_header_annotations_and_rejects_invalid_tools() {
    let mut fixture = Fixture::new();
    let config = fixture.http(true, "headers");
    let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
    connection
        .call(
            "get",
            json!({"region": "ap-east"}).as_object().unwrap().clone(),
            BUDGET,
        )
        .await
        .unwrap();
    assert_eq!(fixture.count("param-region:ap-east"), 1);
    assert_eq!(fixture.count("method-header:tools/call"), 1);
    assert_eq!(fixture.count("tools/list"), 2);
    connection.close().await.unwrap();

    let mut fixture = Fixture::new();
    let config = fixture.http(true, "invalid_headers");
    let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
    let error = connection
        .call("get", Map::new(), BUDGET)
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Protocol);
    assert!(error.to_string().contains("not dispatched"));
    assert_eq!(fixture.count("tools/call"), 0);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn discovery_and_dispatch_share_deadline_and_missing_tools_never_execute() {
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "slow_list"), BUDGET)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let error = connection
        .call("get", Map::new(), Duration::from_millis(220))
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Timeout);
    assert!(error.to_string().contains("not dispatched"));
    assert!(!error.to_string().contains("outcome unknown"));
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(fixture.count("tools/call"), 0);
    connection.close().await.unwrap();

    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "normal"), BUDGET)
        .await
        .unwrap();
    let error = connection
        .call("not-advertised", Map::new(), BUDGET)
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Protocol);
    assert!(error.to_string().contains("not dispatched"));
    assert_eq!(fixture.count("tools/call"), 0);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn input_required_is_one_failed_call_with_possible_side_effects() {
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "normal"), BUDGET)
        .await
        .unwrap();
    let error = connection
        .call("input_required", Map::new(), BUDGET)
        .await
        .unwrap_err();
    assert_eq!(kind(&error), FailureKind::Protocol);
    assert!(error.to_string().contains("interactive input"));
    assert!(error.to_string().contains("side effects may have occurred"));
    assert_eq!(fixture.count("tools/call"), 1);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn close_joins_local_worker_and_reports_unconfirmed_remote_cleanup() {
    for mode in ["delete_hang", "delete_error", "delete_redirect"] {
        let mut fixture = Fixture::new();
        let config = fixture.http(false, mode);
        let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
        let started = std::time::Instant::now();
        let error = connection.close().await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(
            kind(&error),
            if mode == "delete_hang" {
                FailureKind::Timeout
            } else {
                FailureKind::Transport
            }
        );
        assert!(error.to_string().contains("Local MCP client stopped"));
        assert!(error.to_string().contains("remote session deletion"));
        assert!(connection.is_closed());
        assert!(connection.is_local_closed());
        assert_eq!(fixture.count("DELETE"), 1);
        let repeated = connection.close().await.unwrap_err();
        assert_eq!(repeated.to_string(), error.to_string());
        assert_eq!(fixture.count("DELETE"), 1);
        assert!(connection.list_tools(BUDGET).await.is_err());
        assert_eq!(fixture.count("tools/list"), 0);
    }
}

#[tokio::test]
async fn cancelled_close_resumes_the_same_owned_join() {
    let mut fixture = Fixture::new();
    let config = fixture.http(false, "delete_hang");
    let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(80), connection.close())
            .await
            .is_err()
    );
    assert!(!connection.is_local_closed());
    let error = connection.close().await.unwrap_err();
    assert!(connection.is_local_closed());
    assert_eq!(kind(&error), FailureKind::Timeout);
    assert!(error.to_string().contains("Local MCP client stopped"));
    assert_eq!(fixture.count("DELETE"), 1);
}

#[tokio::test]
async fn dispatch_marker_and_structured_uncertainty_follow_the_wire_boundary() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let fixture = Fixture::new();
    let mut connection = McpConnection::connect(&fixture.stdio(true, "normal"), BUDGET)
        .await
        .unwrap();
    let dispatched = AtomicBool::new(false);
    let error = connection
        .call_with_dispatch("not-advertised", Map::new(), BUDGET, &dispatched)
        .await
        .unwrap_err();
    assert!(!dispatched.load(Ordering::Acquire));
    assert!(!error.downcast_ref::<McpFailure>().unwrap().outcome_unknown);
    let error = connection
        .call_with_dispatch("slow", Map::new(), Duration::from_millis(80), &dispatched)
        .await
        .unwrap_err();
    assert!(dispatched.load(Ordering::Acquire));
    assert!(error.downcast_ref::<McpFailure>().unwrap().outcome_unknown);
    assert_eq!(fixture.count("tools/call"), 1);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn expired_or_unsupported_remote_delete_allows_local_close() {
    for mode in ["delete_expired", "delete_unsupported"] {
        let mut fixture = Fixture::new();
        let config = fixture.http(false, mode);
        let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
        connection.close().await.unwrap();
        assert!(connection.is_closed());
        connection.close().await.unwrap();
        assert_eq!(fixture.count("DELETE"), 1);
    }
}

#[tokio::test]
async fn get_sse_responses_validate_exact_media_types_and_bound_events() {
    for mode in ["get_sse", "get_sse_mixed"] {
        let mut fixture = Fixture::new();
        let config = fixture.http(false, mode);
        let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
        let result = connection.call("get", Map::new(), BUDGET).await.unwrap();
        assert_eq!(result["structuredContent"]["name"], "get", "{mode}");
        assert!(fixture.count("GET") > 0, "fixture never exercised GET SSE");
        connection.close().await.unwrap();
    }
    for mode in [
        "get_sse_invalid",
        "get_sse_oversize",
        "get_auth_401",
        "get_auth_403",
    ] {
        let mut fixture = Fixture::new();
        let mut config = fixture.http(false, mode);
        if let ServerConfig::Http { url, .. } = &mut config {
            url.push_str("?token=secret-query-do-not-print");
        }
        let mut connection = McpConnection::connect(&config, BUDGET).await.unwrap();
        let error = connection
            .call("get", Map::new(), Duration::from_millis(800))
            .await
            .expect_err("invalid or oversized GET stream must not deliver a result");
        assert!(
            !format!("{error:?}").contains("secret"),
            "{mode}: {error:?}"
        );
        assert!(fixture.count("GET") > 0, "fixture never exercised GET SSE");
        assert_eq!(
            fixture.count("tools/call"),
            1,
            "{mode}: tool call was replayed"
        );
        connection.close().await.unwrap();
    }
}
