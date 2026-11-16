//! MCP lifecycle and transports. Tool calls are sent once, never replayed.
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt,
    pin::Pin,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use futures_util::{StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue};
use rmcp::{
    ClientHandler, RoleClient,
    model::{
        CallToolRequestParams, CallToolResponse, ClientCapabilities, ClientConfig,
        ClientJsonRpcMessage, ClientRequest, ErrorData, Implementation, PaginatedRequestParams,
        ProtocolVersion, ServerJsonRpcMessage,
    },
    service::{
        ClientCacheConfig, ClientLifecycleMode, ClientServiceExt, NotificationContext, Peer,
        QuitReason, RunningService, ServiceError,
    },
    transport::{
        Transport,
        async_rw::AsyncRwTransport,
        streamable_http_client::{
            AuthRequiredError, HttpStatusError, SseError, StreamableHttpClient,
            StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
            StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use serde_json::{Map, Value};
use sse_stream::{Sse, SseStream};
use tokio::{
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

use crate::config::ServerConfig;

pub const MAX_CONNECT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const MAX_TOOLS: usize = 10_000;
const MAX_TOOL_PAGES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureKind {
    Timeout,
    Protocol,
    Transport,
}

#[derive(Clone, Debug)]
pub struct McpFailure {
    pub kind: FailureKind,
    pub message: String,
    pub outcome_unknown: bool,
}

impl fmt::Display for McpFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for McpFailure {}

fn failure(kind: FailureKind, message: impl Into<String>) -> anyhow::Error {
    failure_with_outcome(kind, message, false)
}

fn failure_with_outcome(
    kind: FailureKind,
    message: impl Into<String>,
    outcome_unknown: bool,
) -> anyhow::Error {
    McpFailure {
        kind,
        message: message.into(),
        outcome_unknown,
    }
    .into()
}

#[derive(Clone, Debug)]
struct CleanupRecord {
    failure: McpFailure,
    local_confirmed: bool,
}

type CleanupState = Arc<Mutex<Option<CleanupRecord>>>;

fn record_cleanup_failure(
    state: &CleanupState,
    kind: FailureKind,
    message: &str,
    local_confirmed: bool,
) {
    let mut stored = state.lock().unwrap_or_else(|poison| poison.into_inner());
    match stored.as_mut() {
        None => {
            *stored = Some(CleanupRecord {
                failure: McpFailure {
                    kind,
                    message: message.into(),
                    outcome_unknown: false,
                },
                local_confirmed,
            });
        }
        Some(existing) if existing.local_confirmed && !local_confirmed => {
            existing.failure = McpFailure {
                kind,
                message: message.into(),
                outcome_unknown: false,
            };
            existing.local_confirmed = false;
        }
        Some(_) => {}
    }
}

fn service_failure(error: ServiceError, calling: bool) -> anyhow::Error {
    let suffix = if calling {
        "; outcome unknown, the call was not retried"
    } else {
        ""
    };
    let fail = |kind, message| failure_with_outcome(kind, message, calling);
    match error {
        ServiceError::McpError(e) => fail(
            FailureKind::Protocol,
            format!("MCP error {}: {}{suffix}", e.code.0, e.message),
        ),
        ServiceError::Timeout { .. } => fail(
            FailureKind::Timeout,
            format!("MCP request timed out{suffix}"),
        ),
        ServiceError::UnexpectedResponse => fail(
            FailureKind::Protocol,
            format!("MCP server returned an unexpected response{suffix}"),
        ),
        _ => fail(
            FailureKind::Transport,
            format!("MCP connection failed{suffix}"),
        ),
    }
}

#[derive(Clone)]
struct Handler {
    tools_changed: Arc<AtomicBool>,
}

impl ClientHandler for Handler {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::new("mcpcli", env!("CARGO_PKG_VERSION")),
        )
    }

    async fn on_tool_list_changed(&self, _: NotificationContext<RoleClient>) {
        self.tools_changed.store(true, Ordering::Release);
    }
}

type CloseFuture = Pin<
    Box<dyn Future<Output = std::result::Result<QuitReason, tokio::task::JoinError>> + Send + Sync>,
>;

pub struct McpConnection {
    service: Option<RunningService<RoleClient, Handler>>,
    peer: Peer<RoleClient>,
    closing: Option<CloseFuture>,
    cleanup: CleanupState,
    tools_changed: Arc<AtomicBool>,
}

impl McpConnection {
    pub async fn connect(config: &ServerConfig, budget: Duration) -> Result<Self> {
        let tools_changed = Arc::new(AtomicBool::new(false));
        let cleanup = CleanupState::default();
        let handler = Handler {
            tools_changed: tools_changed.clone(),
        };
        let lifecycle = ClientLifecycleMode::Auto {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            legacy_version: Some(ProtocolVersion::V_2025_11_25),
        };
        let connection = async {
            match config {
                ServerConfig::Stdio {
                    command,
                    args,
                    env,
                    cwd,
                    ..
                } => {
                    let mut child = Command::new(command);
                    child.args(args).envs(env);
                    if let Some(cwd) = cwd {
                        child.current_dir(cwd);
                    }
                    let transport = ChildTransport::spawn(child, cleanup.clone()).map_err(|e| {
                        failure(
                            FailureKind::Transport,
                            format!("Cannot start MCP server: {e}"),
                        )
                    })?;
                    handler
                        .serve_with_lifecycle(transport, lifecycle)
                        .await
                        .map_err(init_failure)
                }
                ServerConfig::Http { url, headers, .. } => {
                    let mut parsed_headers = HashMap::new();
                    for (name, value) in headers {
                        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                            failure(FailureKind::Protocol, "Invalid HTTP header name")
                        })?;
                        if crate::config::is_reserved_header(name.as_str()) {
                            return Err(failure(
                                FailureKind::Protocol,
                                format!("HTTP header {name} is reserved"),
                            ));
                        }
                        let mut value = HeaderValue::from_str(value).map_err(|_| {
                            failure(FailureKind::Protocol, "Invalid HTTP header value")
                        })?;
                        value.set_sensitive(true);
                        parsed_headers.insert(name, value);
                    }
                    let client = reqwest::Client::builder()
                        .redirect(reqwest::redirect::Policy::none())
                        .retry(reqwest::retry::never())
                        .connect_timeout(MAX_CONNECT)
                        .build()
                        .map_err(|_| {
                            failure(FailureKind::Transport, "Cannot create HTTP client")
                        })?;
                    let transport_config =
                        StreamableHttpClientTransportConfig::with_uri(url.clone())
                            .custom_headers(parsed_headers)
                            .reinit_on_expired_session(false)
                            .max_sse_event_size(MAX_RESPONSE_BYTES);
                    let transport = StreamableHttpClientTransport::with_client(
                        BoundedHttp {
                            client,
                            cleanup: cleanup.clone(),
                        },
                        transport_config,
                    );
                    handler
                        .serve_with_lifecycle(transport, lifecycle)
                        .await
                        .map_err(init_failure)
                }
            }
        };
        let service = timeout(budget, connection)
            .await
            .map_err(|_| failure(FailureKind::Timeout, "MCP connection timed out"))??;
        // Explicit discovery must not silently return an expired SDK cache entry.
        service
            .peer()
            .set_response_cache_config(ClientCacheConfig::disabled())
            .await;
        Ok(Self {
            peer: service.peer().clone(),
            service: Some(service),
            closing: None,
            cleanup,
            tools_changed,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.service.as_ref().is_none_or(RunningService::is_closed)
            || self.peer.is_transport_closed()
    }

    pub fn is_local_closed(&self) -> bool {
        self.service.is_none()
            && self.closing.is_none()
            && self
                .cleanup
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_ref()
                .is_none_or(|record| record.local_confirmed)
    }

    pub fn take_tools_changed(&self) -> bool {
        self.tools_changed.swap(false, Ordering::AcqRel)
    }

    pub async fn list_tools(&self, budget: Duration) -> Result<Vec<Value>> {
        let listing = async {
            let mut tools = Vec::new();
            let mut cursor = None;
            let mut cursors = HashSet::new();
            let mut names = HashSet::new();
            let mut bytes = 0usize;
            for _ in 0..MAX_TOOL_PAGES {
                let mut params = PaginatedRequestParams::default();
                params.cursor = cursor;
                let page = self
                    .peer
                    .list_tools(Some(params))
                    .await
                    .map_err(|e| service_failure(e, false))?;
                for tool in page.tools {
                    if tool.name.is_empty() || !names.insert(tool.name.to_string()) {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Tool catalog contains an empty or duplicate name",
                        ));
                    }
                    let value = serde_json::to_value(tool)?;
                    bytes = bytes.saturating_add(serde_json::to_vec(&value)?.len());
                    if tools.len() >= MAX_TOOLS || bytes > MAX_RESPONSE_BYTES {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Tool catalog exceeds the size limit",
                        ));
                    }
                    tools.push(value);
                }
                cursor = page.next_cursor;
                match &cursor {
                    None => return Ok(tools),
                    Some(c) if !cursors.insert(c.clone()) => {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Tool catalog repeated a pagination cursor",
                        ));
                    }
                    _ => {}
                }
            }
            Err(failure(
                FailureKind::Protocol,
                "Tool catalog exceeds the pagination limit",
            ))
        };
        timeout(budget, listing)
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Tool discovery timed out"))?
    }

    pub async fn call(
        &self,
        name: &str,
        arguments: Map<String, Value>,
        budget: Duration,
    ) -> Result<Value> {
        let dispatched = AtomicBool::new(false);
        self.call_with_dispatch(name, arguments, budget, &dispatched)
            .await
    }

    pub async fn call_with_dispatch(
        &self,
        name: &str,
        arguments: Map<String, Value>,
        budget: Duration,
        dispatched: &AtomicBool,
    ) -> Result<Value> {
        let started = tokio::time::Instant::now();
        // HTTP header annotations need the SDK's current catalog; fresh discovery
        // also prevents invoking tools rejected by the transport or withdrawn.
        let tools = self.list_tools(budget).await.map_err(|error| {
            let kind = error
                .downcast_ref::<McpFailure>()
                .map_or(FailureKind::Transport, |error| error.kind);
            failure(
                kind,
                format!("Tool was not dispatched: discovery failed: {error}"),
            )
        })?;
        if !tools.iter().any(|tool| tool["name"].as_str() == Some(name)) {
            return Err(failure(
                FailureKind::Protocol,
                "Tool was not dispatched: not present in the current catalog",
            ));
        }
        let remaining = budget.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(failure(
                FailureKind::Timeout,
                "Tool was not dispatched: discovery exhausted the call timeout",
            ));
        }
        let request = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
        // This conservative boundary is also visible to the daemon's outer deadline.
        dispatched.store(true, Ordering::Release);
        // call_tool_once cancels on future drop; it does not drive interactive MRTR rounds.
        let response = timeout(remaining, self.peer.call_tool_once(request)).await
            .map_err(|_| failure_with_outcome(
                FailureKind::Timeout,
                "MCP call timed out; cancellation requested; outcome unknown, the call was not retried", true,
            ))?
            .map_err(|e| service_failure(e, true))?;
        match response {
            CallToolResponse::Complete(result) => Ok(serde_json::to_value(result)?),
            CallToolResponse::InputRequired(_) => Err(failure_with_outcome(
                FailureKind::Protocol,
                "The tool requires unsupported interactive input; side effects may have occurred, the call was not retried",
                true,
            )),
            CallToolResponse::Task(_) => Err(failure_with_outcome(
                FailureKind::Protocol,
                "The server returned an unsupported asynchronous task; side effects may have occurred, the call was not retried",
                true,
            )),
            _ => Err(failure_with_outcome(
                FailureKind::Protocol,
                "Unsupported MCP tool response; outcome unknown, the call was not retried",
                true,
            )),
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        if self.closing.is_none()
            && let Some(mut service) = self.service.take()
        {
            // Keep this future in self: cancelling a caller must not drop the
            // SDK's private JoinHandle after close() takes it from the service.
            self.closing = Some(Box::pin(async move { service.close().await }));
        }
        if let Some(closing) = self.closing.as_mut() {
            let joined = timeout(Duration::from_secs(8), closing)
                .await
                .map_err(|_| {
                    failure(
                        FailureKind::Timeout,
                        "MCP cleanup is still in progress; local closure is not confirmed",
                    )
                })?;
            self.closing = None;
            if !matches!(joined, Ok(QuitReason::Cancelled | QuitReason::Closed)) {
                record_cleanup_failure(
                    &self.cleanup,
                    FailureKind::Transport,
                    "MCP service task failed during cleanup; local closure is not confirmed",
                    false,
                );
            }
        }
        let error = self
            .cleanup
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|record| record.failure.clone());
        match error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}

fn init_failure(error: rmcp::service::ClientInitializeError) -> anyhow::Error {
    use rmcp::service::ClientInitializeError as E;
    match error {
        E::JsonRpcError(e) => failure(
            FailureKind::Protocol,
            format!("MCP initialization error {}: {}", e.code.0, e.message),
        ),
        E::NoCompatibleProtocolVersion { .. } => {
            failure(FailureKind::Protocol, "No compatible MCP protocol version")
        }
        // HTTP transport errors can contain URLs or response bodies. Do not leak them.
        _ => failure(
            FailureKind::Transport,
            "MCP connection or initialization failed; check the configured server and credentials",
        ),
    }
}

fn has_media_type(content_type: &str, expected: &str) -> bool {
    let essence = content_type
        .split_once(';')
        .map_or(content_type, |(essence, _)| essence)
        .trim();
    essence.eq_ignore_ascii_case(expected)
}

/// Own the child throughout shutdown, including cancellation of the close future.
struct ChildTransport {
    io: AsyncRwTransport<RoleClient, ChildStdout, ChildStdin>,
    child: Option<Child>,
    group: Option<i32>,
    cleanup: CleanupState,
}

impl ChildTransport {
    fn spawn(mut command: Command, cleanup: CleanupState) -> std::io::Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(false)
            .process_group(0);
        let parent = std::process::id() as libc::pid_t;
        // Only async-signal-safe libc calls run between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    libc::_exit(127);
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        let group = child.id().map(|id| id as i32);
        let stdout = child.stdout.take().expect("piped stdout");
        let stdin = child.stdin.take().expect("piped stdin");
        Ok(Self {
            io: AsyncRwTransport::new(stdout, stdin),
            child: Some(child),
            group,
            cleanup,
        })
    }

    fn signal_group(&self, signal: i32) {
        if let Some(group) = self.group {
            // This owns a trusted server's process group, not an escape-proof sandbox.
            unsafe {
                libc::kill(-group, signal);
            }
        }
    }

    fn exited_unreaped(&mut self) -> std::io::Result<bool> {
        let Some(pid) = self.group else {
            return Ok(true);
        };
        let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                // Another reaper has taken ownership; this numeric PGID is no longer ours.
                self.group = None;
            }
            return Err(error);
        }
        Ok(unsafe { info.si_pid() } == pid)
    }

    async fn wait_unreaped(&mut self) -> std::io::Result<bool> {
        timeout(Duration::from_secs(1), async {
            while !self.exited_unreaped()? {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(true)
        })
        .await
        .unwrap_or(Ok(false))
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        // A pending SDK send can hold the writer lock while the child stops
        // reading. Kill the still-owned process before waiting on that lock.
        if !matches!(
            timeout(Duration::from_millis(500), self.io.close()).await,
            Ok(Ok(()))
        ) {
            self.signal_group(libc::SIGKILL);
            if self.group.is_some()
                && let Some(child) = self.child.as_mut()
            {
                let _ = child.start_kill();
            }
        }
        if self.child.is_none() {
            return Ok(());
        }
        if !self.wait_unreaped().await? {
            self.signal_group(libc::SIGTERM);
            let _ = self.wait_unreaped().await?;
        }
        // The unreaped leader pins the PGID until every group signal is complete.
        self.signal_group(libc::SIGKILL);
        let waited = if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
            child.wait().await.map(|_| ())
        } else {
            Ok(())
        };
        self.group = None;
        self.child = None;
        waited
    }
}

impl Drop for ChildTransport {
    fn drop(&mut self) {
        self.signal_group(libc::SIGKILL);
        if let Some(mut child) = self.child.take() {
            if self.group.is_some() {
                let _ = child.start_kill();
            }
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

impl Transport<RoleClient> for ChildTransport {
    type Error = std::io::Error;
    fn send(
        &mut self,
        item: ClientJsonRpcMessage,
    ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
        self.io.send(item)
    }
    fn receive(&mut self) -> impl Future<Output = Option<ServerJsonRpcMessage>> + Send {
        self.io.receive()
    }
    async fn close(&mut self) -> std::io::Result<()> {
        let result = self.shutdown().await;
        if result.is_err() {
            record_cleanup_failure(
                &self.cleanup,
                FailureKind::Transport,
                "MCP child cleanup failed; local closure is not confirmed",
                false,
            );
        }
        result
    }
}

// rmcp 3.9 bounds built-in JSON/error bodies. Keep a total POST cap (including
// SSE), credential-safe errors, and observable DELETE cleanup in this adapter;
// lifecycle, SSE parsing, and routing remain with rmcp.
#[derive(Clone)]
struct BoundedHttp {
    client: reqwest::Client,
    cleanup: CleanupState,
}
type HttpError = StreamableHttpError<reqwest::Error>;

impl BoundedHttp {
    async fn bounded_body(
        mut response: reqwest::Response,
        limit: usize,
    ) -> Result<Vec<u8>, HttpError> {
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(HttpError::Io(std::io::Error::other(
                "HTTP response exceeds 16 MiB",
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(HttpError::Client)? {
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(HttpError::Io(std::io::Error::other(
                    "HTTP response exceeds 16 MiB",
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    async fn post(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        let had_session = session_id.is_some();
        let mut request = self
            .client
            .post(uri.as_ref())
            .header("accept", "application/json, text/event-stream");
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(session) = session_id {
            request = request.header("Mcp-Session-Id", session.as_ref());
        }
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        let response = request
            .json(&message)
            .send()
            .await
            .map_err(HttpError::Client)?;
        let status = response.status();
        if status == http::StatusCode::UNAUTHORIZED || status == http::StatusCode::FORBIDDEN {
            return Err(HttpError::AuthRequired(AuthRequiredError::new(
                String::new(),
            )));
        }
        if status == http::StatusCode::NOT_FOUND && had_session {
            return Err(HttpError::SessionExpired);
        }
        if status == http::StatusCode::ACCEPTED || status == http::StatusCode::NO_CONTENT {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let notification = !matches!(message, ClientJsonRpcMessage::Request(_));
        if status.is_success() && notification && response.content_length() == Some(0) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let session = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        if status.is_success() && has_media_type(&content_type, "text/event-stream") {
            let stream = response
                .bytes_stream()
                .scan((0usize, false), move |state, chunk| {
                    let output = if state.1 {
                        None
                    } else {
                        match chunk {
                            Ok(chunk) => {
                                state.0 = state.0.saturating_add(chunk.len());
                                if state.0 > limit {
                                    state.1 = true;
                                    Some(Err(std::io::Error::other("HTTP response exceeds 16 MiB")))
                                } else {
                                    Some(Ok(chunk))
                                }
                            }
                            Err(_) => {
                                state.1 = true;
                                Some(Err(std::io::Error::other("HTTP response body failed")))
                            }
                        }
                    };
                    std::future::ready(output)
                });
            return Ok(StreamableHttpPostResponse::Sse(
                SseStream::from_bytes_stream(stream).boxed(),
                session,
            ));
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .filter(|v| v.as_bytes().len() <= 128)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let is_json = has_media_type(&content_type, "application/json");
        if !status.is_success() {
            // Legacy middleware may reject discover before JSON-RPC dispatch.
            // Correlate only this read-only probe, never an ordinary tool call.
            let discover_request_id = if !had_session
                && status.is_client_error()
                && status != http::StatusCode::TOO_MANY_REQUESTS
                && let ClientJsonRpcMessage::Request(request) = &message
                && matches!(request.request, ClientRequest::DiscoverRequest(_))
            {
                Some(request.id.clone())
            } else {
                None
            };
            if status == http::StatusCode::TOO_MANY_REQUESTS
                || (discover_request_id.is_none() && !is_json)
            {
                return Err(HttpError::HttpStatus(HttpStatusError::new(
                    status.as_u16(),
                    retry_after,
                )));
            }
            let body = match Self::bounded_body(response, limit.min(MAX_ERROR_BYTES)).await {
                Ok(body) => body,
                Err(_) if discover_request_id.is_some() => Vec::new(),
                Err(_) => {
                    return Err(HttpError::HttpStatus(HttpStatusError::new(
                        status.as_u16(),
                        retry_after,
                    )));
                }
            };
            if let Some(request_id) = discover_request_id {
                let error = match serde_json::from_slice::<ServerJsonRpcMessage>(&body) {
                    Ok(ServerJsonRpcMessage::Error(error)) => error.error,
                    _ => ErrorData::invalid_request("server/discover is unavailable", None),
                };
                return Ok(StreamableHttpPostResponse::Json(
                    ServerJsonRpcMessage::error(error, Some(request_id)),
                    None,
                ));
            }
            if is_json
                && let Ok(message @ ServerJsonRpcMessage::Error(_)) = serde_json::from_slice(&body)
            {
                return Ok(StreamableHttpPostResponse::Json(message, session));
            }
            return Err(HttpError::HttpStatus(HttpStatusError::new(
                status.as_u16(),
                retry_after,
            )));
        }
        let body = Self::bounded_body(response, limit).await?;
        if notification && body.is_empty() {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !is_json {
            return Err(HttpError::UnexpectedContentType(None));
        }
        match serde_json::from_slice(&body) {
            Ok(message) => Ok(StreamableHttpPostResponse::Json(message, session)),
            Err(_) if notification => Ok(StreamableHttpPostResponse::Accepted),
            Err(_) => Err(HttpError::UnexpectedServerResponse(Cow::Borrowed(
                "Invalid JSON-RPC response",
            ))),
        }
    }
}

impl StreamableHttpClient for BoundedHttp {
    type Error = reqwest::Error;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post(uri, message, session, auth, headers, MAX_RESPONSE_BYTES)
            .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post(
            uri,
            message,
            session,
            auth,
            headers,
            limit.min(MAX_RESPONSE_BYTES),
        )
        .await
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session,
            last_id,
            auth,
            headers,
            MAX_RESPONSE_BYTES,
        )
        .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        limit: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        let had_session = session.is_some();
        self.client
            .get_stream_with_max_sse_event_size(
                uri,
                session,
                last_id,
                auth,
                headers,
                limit.min(MAX_RESPONSE_BYTES),
            )
            .await
            .map_err(|error| match error {
                HttpError::AuthRequired(_) | HttpError::InsufficientScope(_) => {
                    HttpError::AuthRequired(AuthRequiredError::new(String::new()))
                }
                HttpError::Client(ref err) if err.status().is_some() => {
                    let status = err.status().unwrap();
                    if matches!(
                        status,
                        http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN
                    ) {
                        HttpError::AuthRequired(AuthRequiredError::new(String::new()))
                    } else if status == http::StatusCode::NOT_FOUND && had_session {
                        HttpError::SessionExpired
                    } else {
                        HttpError::HttpStatus(HttpStatusError::new(status.as_u16(), None))
                    }
                }
                HttpError::UnexpectedContentType(_) => HttpError::UnexpectedContentType(None),
                other => other,
            })
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        match timeout(
            Duration::from_secs(2),
            self.client.delete_session(uri, session, auth, headers),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(HttpError::Client(error)))
                if matches!(
                    error.status(),
                    Some(http::StatusCode::NOT_FOUND | http::StatusCode::METHOD_NOT_ALLOWED)
                ) =>
            {
                // Legacy sessions may already be gone or disallow explicit deletion.
                Ok(())
            }
            Ok(Err(_)) => {
                record_cleanup_failure(
                    &self.cleanup,
                    FailureKind::Transport,
                    "Local MCP client stopped; remote session deletion was not confirmed",
                    true,
                );
                Err(HttpError::Io(std::io::Error::other(
                    "Session deletion failed",
                )))
            }
            Err(_) => {
                record_cleanup_failure(
                    &self.cleanup,
                    FailureKind::Timeout,
                    "Local MCP client stopped; remote session deletion timed out",
                    true,
                );
                Err(HttpError::Io(std::io::Error::other(
                    "Session deletion timed out",
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observing_exit_does_not_reap_leader_before_group_cleanup() {
        let mut command = Command::new("sh");
        command.args(["-c", "exit 0"]);
        let mut transport = ChildTransport::spawn(command, CleanupState::default()).unwrap();
        assert!(transport.wait_unreaped().await.unwrap());
        // A second WNOWAIT observation must still find our waitable leader.
        assert!(transport.exited_unreaped().unwrap());
        assert!(transport.group.is_some());
        transport.shutdown().await.unwrap();
        assert!(transport.group.is_none());
        assert!(transport.child.is_none());
    }

    #[tokio::test]
    async fn blocked_stdio_writer_cannot_prevent_owned_child_shutdown() {
        let mut command = Command::new("sh");
        command.args(["-c", "trap '' TERM; sleep 60"]);
        let mut transport = ChildTransport::spawn(command, CleanupState::default()).unwrap();
        let message = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "blocked", "arguments": {"data": "x".repeat(2 * 1024 * 1024)}}
        }))
        .unwrap();
        let send = tokio::spawn(transport.send(message));
        tokio::time::sleep(Duration::from_millis(30)).await;
        timeout(Duration::from_secs(3), transport.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(transport.group.is_none());
        assert!(transport.child.is_none());
        assert!(send.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn cancelling_shutdown_keeps_group_owned_until_drop() {
        let mut command = Command::new("sh");
        command.args(["-c", "trap '' TERM; cat >/dev/null; sleep 60"]);
        let mut transport = ChildTransport::spawn(command, CleanupState::default()).unwrap();
        let pid = transport.group.unwrap();
        assert!(
            timeout(Duration::from_millis(25), transport.shutdown())
                .await
                .is_err()
        );
        assert_eq!(transport.group, Some(pid));
        drop(transport);
        timeout(Duration::from_secs(3), async {
            loop {
                let alive = unsafe { libc::kill(pid, 0) } == 0;
                if !alive {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
