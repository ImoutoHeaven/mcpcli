//! MCP lifecycle and transports. Tool calls are sent once, never replayed.
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
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
use futures_util::stream::BoxStream;
use http::{HeaderName, HeaderValue};
use rmcp::{
    RoleClient,
    model::{
        CallToolRequest, CallToolRequestParams, CallToolResponse, CancelledNotification,
        CancelledNotificationParam, ClientCapabilities, ClientConfig, ClientJsonRpcMessage,
        ClientNotification, ClientRequest, ClientResult, CustomRequest, CustomResult, ElicitResult,
        ElicitationAction, ErrorCode, ErrorData, ExtensionCapabilities, Implementation,
        PaginatedRequestParams, ProtocolVersion, ServerJsonRpcMessage, ServerNotification,
        ServerRequest, ServerResult,
    },
    service::{
        ClientCacheConfig, ClientLifecycleMode, ClientServiceExt, NotificationContext, Peer,
        PeerRequestOptions, QuitReason, RequestContext, RunningService, Service, ServiceError,
    },
    transport::{
        Transport,
        streamable_http_client::{
            AuthRequiredError, HttpStatusError, SseError, StreamableHttpClient,
            StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
            StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use serde_json::{Map, Value};
use sse_stream::Sse;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    budget::{ByteBudget, ByteLease, Leased, format_bytes},
    config::ServerConfig,
};

mod events;
mod ingress;
mod sse;
#[cfg(windows)]
use crate::platform::windows_job;
pub use events::{
    EVENTS_EXTENSION_ID, EventNotification, EventStreamHandle, EventStreamReceiver,
    MAX_EVENT_NOTIFICATION_BYTES, MAX_EVENT_PAGES, PollEventsParams, StreamEventsParams,
};
use events::{EventNotificationTransport, EventRouter, EventSchemaCache};
use ingress::{Ingress, IngressError, ObserveOutcome};

pub const MAX_CONNECT: Duration = Duration::from_secs(30);
pub const MAX_RESPONSE_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_TOOL_CATALOG_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_STDERR_TAIL_BYTES: usize = 16 * 1024;
const MAX_STDERR_SNAPSHOT_BYTES: usize = 4096;
const STDERR_SNAPSHOT_WAIT: Duration = Duration::from_millis(100);
const MAX_ERROR_BYTES: usize = 64 * 1024;
const MAX_TOOLS: usize = 10_000;
const MAX_TOOL_PAGES: usize = 256;
const PREFERRED_PROTOCOL_VERSIONS: &[ProtocolVersion] = &[ProtocolVersion::V_2026_07_28];
#[cfg(test)]
const LINE_SHRINK_CAP: usize = 64 * 1024;
const UTF8_BOM: &[u8; 3] = b"\xef\xbb\xbf";
const INVALID_REQUEST_REPLY: &[u8] =
    b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,\"message\":\"Invalid request\"}}\n";

#[derive(Clone)]
pub struct McpLimits {
    pub max_response_bytes: usize,
    pub max_tool_catalog_bytes: usize,
    pub stderr_tail_bytes: usize,
    pub budget: Arc<ByteBudget>,
}

impl Default for McpLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: MAX_RESPONSE_BYTES,
            max_tool_catalog_bytes: MAX_TOOL_CATALOG_BYTES,
            stderr_tail_bytes: DEFAULT_STDERR_TAIL_BYTES,
            budget: ByteBudget::global().clone(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CallBudgets {
    pub pre_dispatch_deadline: Instant,
    pub idle: Duration,
    pub hard: Option<Duration>,
}

impl CallBudgets {
    fn request_options(self, started: Instant) -> Result<PeerRequestOptions> {
        let now = Instant::now();
        let remaining_hard = self
            .hard
            .map(|hard| hard.saturating_sub(now.saturating_duration_since(started)));
        if now >= self.pre_dispatch_deadline || remaining_hard.is_some_and(|hard| hard.is_zero()) {
            return Err(failure(
                FailureKind::Timeout,
                "Tool was not dispatched: discovery exhausted the call timeout",
            ));
        }
        let idle = remaining_hard.map_or(self.idle, |hard| self.idle.min(hard));
        let mut options = PeerRequestOptions::with_timeout(idle).reset_timeout_on_progress();
        if let Some(hard) = remaining_hard {
            options = options.with_max_total_timeout(hard);
        }
        Ok(options)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureKind {
    Timeout,
    Cancelled,
    Protocol,
    Transport,
    AuthRequired,
    InsufficientScope,
}

#[derive(Clone, Debug)]
pub struct McpFailure {
    pub kind: FailureKind,
    pub message: String,
    pub outcome_unknown: bool,
    pub rpc_error: Option<Arc<Value>>,
    pub auth: Option<crate::auth::AuthChallengeInfo>,
    pub stderr_tail: Option<String>,
    pub(crate) lease: Option<Arc<ByteLease>>,
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
        rpc_error: None,
        auth: None,
        stderr_tail: None,
        lease: None,
    }
    .into()
}

#[derive(Default)]
pub(super) struct TransportTermination {
    reason: Mutex<Option<McpFailure>>,
    cancelled: CancellationToken,
    // EOF must not cancel a response the SDK has already delivered.
    closed: AtomicBool,
}

impl TransportTermination {
    pub(super) fn terminate(&self, kind: FailureKind, message: String) {
        let mut guard = self.reason.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_none() {
            *guard = Some(McpFailure {
                kind,
                message,
                outcome_unknown: false,
                rpc_error: None,
                auth: None,
                stderr_tail: None,
                lease: None,
            });
        }
        drop(guard);
        self.cancelled.cancel();
    }

    pub(super) fn set_transient(&self, kind: FailureKind, message: String) {
        self.set_transient_failure(McpFailure {
            kind,
            message,
            outcome_unknown: false,
            rpc_error: None,
            auth: None,
            stderr_tail: None,
            lease: None,
        });
    }

    pub(super) fn set_transient_failure(&self, failure: McpFailure) {
        if self.cancelled.is_cancelled() {
            return;
        }
        let mut guard = self.reason.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_none() {
            *guard = Some(failure);
        }
    }

    pub(super) fn clear_transient(&self) {
        if !self.cancelled.is_cancelled() {
            *self.reason.lock().unwrap_or_else(|p| p.into_inner()) = None;
        }
    }

    pub(super) fn is_terminated(&self) -> bool {
        self.cancelled.is_cancelled()
    }

    pub(super) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.is_terminated()
    }

    pub(super) async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }

    pub(super) fn take_error(&self, calling: bool) -> Option<anyhow::Error> {
        let mut guard = self.reason.lock().unwrap_or_else(|p| p.into_inner());
        let mut record = if self.cancelled.is_cancelled() {
            guard.clone()?
        } else {
            guard.take()?
        };
        if calling && !record.outcome_unknown {
            record.outcome_unknown = true;
            record.message = format!(
                "{}; outcome unknown, the call was not retried",
                record.message
            );
        }
        Some(record.into())
    }

    pub(super) fn failure(&self, calling: bool) -> anyhow::Error {
        self.take_error(calling).unwrap_or_else(|| {
            let suffix = if calling {
                "; outcome unknown, the call was not retried"
            } else {
                ""
            };
            failure_with_outcome(
                FailureKind::Transport,
                format!("MCP connection failed{suffix}"),
                calling,
            )
        })
    }
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
                    rpc_error: None,
                    auth: None,
                    stderr_tail: None,
                    lease: None,
                },
                local_confirmed,
            });
        }
        Some(existing) if existing.local_confirmed && !local_confirmed => {
            existing.failure = McpFailure {
                kind,
                message: message.into(),
                outcome_unknown: false,
                rpc_error: None,
                auth: None,
                stderr_tail: None,
                lease: None,
            };
            existing.local_confirmed = false;
        }
        Some(_) => {}
    }
}

fn observed_result(
    response: Option<(&'static str, Result<Value>, ByteLease)>,
    expected: Option<&str>,
    calling: bool,
) -> Result<(&'static str, Value, ByteLease)> {
    if let Some((method, response, lease)) = response {
        let mut owned = Leased::new(response, lease);
        if expected.map_or(
            matches!(method, "initialize" | "server/discover"),
            |expected| expected == method,
        ) && let Ok(response) = owned.value_mut()
            && let Some(result) = response.get_mut("result")
        {
            let result = result.take();
            let (envelope, lease) = owned.into_parts();
            drop(envelope);
            return Ok((method, result, lease));
        }
    }
    Err(failure_with_outcome(
        FailureKind::Protocol,
        "MCP response metadata could not be captured",
        calling,
    ))
}

fn busy_capture() -> anyhow::Error {
    failure(
        FailureKind::Protocol,
        "MCP connection already has an operation in progress; request was not dispatched",
    )
}

fn rpc_failure(code: i32, diagnostic: String, calling: bool, ingress: &Ingress) -> anyhow::Error {
    let (message, rpc_error, lease) = match ingress.take() {
        Some((_, Ok(mut response), lease)) if response.get("error").is_some() => (
            diagnostic,
            response.get_mut("error").map(Value::take).map(Arc::new),
            Some(Arc::new(lease)),
        ),
        Some((_, Err(_), _lease)) => (
            format!(
                "MCP error {code}: response rejected because its JSON cannot be represented losslessly"
            ),
            None,
            None,
        ),
        _ => (diagnostic, None, None),
    };
    let suffix = if calling {
        "; outcome unknown, the call was not retried"
    } else {
        ""
    };
    McpFailure {
        kind: FailureKind::Protocol,
        message: format!("{message}{suffix}"),
        outcome_unknown: calling,
        rpc_error,
        auth: None,
        stderr_tail: None,
        lease,
    }
    .into()
}

fn service_failure(
    error: ServiceError,
    calling: bool,
    ingress: &Ingress,
    termination: &TransportTermination,
) -> anyhow::Error {
    let suffix = if calling {
        "; outcome unknown, the call was not retried"
    } else {
        ""
    };
    let fail = |kind, message| failure_with_outcome(kind, message, calling);
    match error {
        ServiceError::McpError(e) => rpc_failure(
            e.code.0,
            format!("MCP error {}: {}", e.code.0, e.message),
            calling,
            ingress,
        ),
        ServiceError::Timeout { .. } => fail(
            FailureKind::Timeout,
            format!("MCP request timed out{suffix}"),
        ),
        ServiceError::UnexpectedResponse => fail(
            FailureKind::Protocol,
            format!("MCP server returned an unexpected response{suffix}"),
        ),
        _ => termination.take_error(calling).unwrap_or_else(|| {
            fail(
                FailureKind::Transport,
                format!("MCP connection failed{suffix}"),
            )
        }),
    }
}

#[derive(Clone)]
struct Handler {
    tools_changed: Arc<AtomicBool>,
}

impl Service<RoleClient> for Handler {
    fn get_info(&self) -> ClientConfig {
        let mut extensions = ExtensionCapabilities::new();
        extensions.insert(EVENTS_EXTENSION_ID.to_owned(), Map::new());
        let mut capabilities = ClientCapabilities::default();
        capabilities.extensions = Some(extensions);
        ClientConfig::new(
            capabilities,
            Implementation::new("mcpcli", env!("CARGO_PKG_VERSION")),
        )
    }

    fn handle_request(
        &self,
        request: ServerRequest,
        context: RequestContext<RoleClient>,
    ) -> impl Future<Output = Result<ClientResult, ErrorData>> + Send {
        // These are the existing noninteractive defaults. Do not move remote
        // method strings into the local unsupported-method response.
        #[allow(deprecated)]
        let result = match &request {
            ServerRequest::PingRequest(_) => Ok(ClientResult::empty(())),
            ServerRequest::CreateMessageRequest(_) => Err(ErrorData::method_not_found::<
                rmcp::model::CreateMessageRequestMethod,
            >()),
            ServerRequest::ListRootsRequest(_) => Ok(ClientResult::ListRootsResult(
                rmcp::model::ListRootsResult::default(),
            )),
            ServerRequest::ElicitRequest(_) => Ok(ClientResult::ElicitResult(ElicitResult::new(
                ElicitationAction::Decline,
            ))),
            _ => Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "Unsupported server request",
                None,
            )),
        };
        drop(request);
        drop(context);
        std::future::ready(result)
    }

    fn handle_notification(
        &self,
        notification: ServerNotification,
        context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = Result<(), ErrorData>> + Send {
        if matches!(
            &notification,
            ServerNotification::ToolListChangedNotification(_)
        ) {
            self.tools_changed.store(true, Ordering::Release);
        }
        drop(notification);
        drop(context);
        std::future::ready(Ok(()))
    }
}

type CloseFuture = Pin<
    Box<dyn Future<Output = std::result::Result<QuitReason, tokio::task::JoinError>> + Send + Sync>,
>;

struct StderrRing {
    buf: VecDeque<u8>,
    cap: usize,
    lease: ByteLease,
}

impl StderrRing {
    fn try_new(cap: usize, mut lease: ByteLease) -> Option<Self> {
        let mut buf = VecDeque::new();
        buf.try_reserve_exact(cap).ok()?;
        if !lease.try_grow(buf.capacity().saturating_sub(lease.bytes())) {
            return None;
        }
        Some(Self { buf, cap, lease })
    }

    fn push(&mut self, bytes: &[u8]) {
        if self.cap == 0 || bytes.is_empty() {
            return;
        }
        if bytes.len() >= self.cap {
            self.buf.clear();
            self.buf.extend(&bytes[bytes.len() - self.cap..]);
            return;
        }
        let overflow = self
            .buf
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(self.cap);
        if overflow > 0 {
            self.buf.drain(..overflow);
        }
        self.buf.extend(bytes);
    }

    fn tail_bytes(&self, max_bytes: usize) -> Vec<u8> {
        let len = self.buf.len();
        let skip = len.saturating_sub(max_bytes);
        let (a, b) = self.buf.as_slices();
        let mut out = Vec::with_capacity(len - skip);
        if skip < a.len() {
            out.extend_from_slice(&a[skip..]);
            out.extend_from_slice(b);
        } else {
            out.extend_from_slice(&b[skip - a.len()..]);
        }
        out
    }
}

fn format_stderr_tail(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let start = bytes
        .iter()
        .position(|&b| !(0x80..=0xBF).contains(&b))
        .unwrap_or(bytes.len());
    let sliced = &bytes[start..];
    if sliced.is_empty() {
        return None;
    }
    let decoded = String::from_utf8_lossy(sliced);
    let sanitized = crate::discovery::safe_text(&decoded);
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
    }
}

pub(crate) struct StderrCapture {
    ring: Arc<Mutex<StderrRing>>,
    done_rx: tokio::sync::watch::Receiver<bool>,
    abort_handle: tokio::task::AbortHandle,
}

impl StderrCapture {
    fn spawn(mut stderr: ChildStderr, ring: StderrRing) -> Arc<Self> {
        let ring = Arc::new(Mutex::new(ring));
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let task_ring = ring.clone();
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        task_ring
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .push(&buf[..n]);
                    }
                }
            }
            let _ = done_tx.send(true);
        });
        Arc::new(Self {
            ring,
            done_rx,
            abort_handle: handle.abort_handle(),
        })
    }

    pub(crate) async fn snapshot_tail(&self, wait: Duration) -> Option<String> {
        let mut rx = self.done_rx.clone();
        if !*rx.borrow() {
            let _ = timeout(wait, rx.wait_for(|&done| done)).await;
        }
        let bytes = self
            .ring
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tail_bytes(MAX_STDERR_SNAPSHOT_BYTES);
        format_stderr_tail(&bytes)
    }

    pub(crate) fn abort(&self) {
        self.abort_handle.abort();
        let mut ring = self.ring.lock().unwrap_or_else(|p| p.into_inner());
        ring.buf = VecDeque::new();
        ring.cap = 0;
        ring.lease.release();
    }
}

impl Drop for StderrCapture {
    fn drop(&mut self) {
        self.abort();
    }
}

struct CancelOnDropGuard {
    peer: Peer<RoleClient>,
    request_id: Option<rmcp::model::RequestId>,
}

impl CancelOnDropGuard {
    fn new(peer: Peer<RoleClient>, request_id: rmcp::model::RequestId) -> Self {
        Self {
            peer,
            request_id: Some(request_id),
        }
    }

    fn disarm(&mut self) {
        self.request_id = None;
    }
}

impl Drop for CancelOnDropGuard {
    fn drop(&mut self) {
        if let Some(request_id) = self.request_id.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let peer = self.peer.clone();
            handle.spawn(async move {
                let notification = CancelledNotification::new(CancelledNotificationParam::new(
                    Some(request_id),
                    None,
                ));
                let _ = timeout(
                    Duration::from_secs(2),
                    peer.send_notification(ClientNotification::CancelledNotification(notification)),
                )
                .await;
            });
        }
    }
}

async fn attach_stderr_tail(
    mut error: anyhow::Error,
    capture: Option<&Arc<StderrCapture>>,
    is_closed: bool,
) -> anyhow::Error {
    if let Some(capture) = capture
        && let Some(failure) = error.downcast_mut::<McpFailure>()
        && failure.stderr_tail.is_none()
        && (failure.kind == FailureKind::Transport || is_closed)
    {
        failure.stderr_tail = capture.snapshot_tail(STDERR_SNAPSHOT_WAIT).await;
    }
    error
}

async fn attach_stderr_tail_and_abort(
    mut error: anyhow::Error,
    capture: Option<Arc<StderrCapture>>,
) -> anyhow::Error {
    if let Some(capture) = capture {
        if let Some(failure) = error.downcast_mut::<McpFailure>()
            && failure.stderr_tail.is_none()
        {
            failure.stderr_tail = capture.snapshot_tail(STDERR_SNAPSHOT_WAIT).await;
        }
        capture.abort();
    }
    error
}

pub struct McpConnection {
    service: Option<RunningService<RoleClient, Handler>>,
    peer: Peer<RoleClient>,
    ingress: Ingress,
    event_router: EventRouter,
    event_schemas: EventSchemaCache,
    termination: Arc<TransportTermination>,
    stderr_capture: Option<Arc<StderrCapture>>,
    limits: McpLimits,
    server: Leased<Vec<u8>>,
    closing: Option<CloseFuture>,
    cleanup: CleanupState,
    tools_changed: Arc<AtomicBool>,
    http: bool,
}

impl Drop for McpConnection {
    fn drop(&mut self) {
        if let Some(capture) = self.stderr_capture.take() {
            capture.abort();
        }
    }
}

impl McpConnection {
    pub async fn connect(config: &ServerConfig, budget: Duration) -> Result<Self> {
        Self::connect_with_limits(config, budget, McpLimits::default()).await
    }

    pub async fn connect_with_limits(
        config: &ServerConfig,
        budget: Duration,
        limits: McpLimits,
    ) -> Result<Self> {
        let tools_changed = Arc::new(AtomicBool::new(false));
        let cleanup = CleanupState::default();
        let ingress = Ingress::default();
        let event_router = EventRouter::new(limits.budget.clone());
        let event_schemas = EventSchemaCache::default();
        let termination = Arc::new(TransportTermination::default());
        let _capture = ingress.capture().ok_or_else(busy_capture)?;
        let handler = Handler {
            tools_changed: tools_changed.clone(),
        };
        let lifecycle = ClientLifecycleMode::Auto {
            preferred_versions: PREFERRED_PROTOCOL_VERSIONS.to_vec(),
            legacy_version: Some(ProtocolVersion::V_2025_11_25),
        };
        let stderr_slot: Arc<Mutex<Option<Arc<StderrCapture>>>> = Arc::new(Mutex::new(None));
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
                    let (transport, capture) = ChildTransport::spawn_observed(
                        child,
                        cleanup.clone(),
                        ingress.clone(),
                        termination.clone(),
                        limits.max_response_bytes,
                        limits.stderr_tail_bytes,
                        limits.budget.clone(),
                    )
                    .map_err(|e| {
                        failure(
                            FailureKind::Transport,
                            format!("Cannot start MCP server: {e}"),
                        )
                    })?;
                    *stderr_slot.lock().unwrap_or_else(|p| p.into_inner()) = capture;
                    let transport = EventNotificationTransport::new(
                        transport,
                        event_router.clone(),
                        termination.clone(),
                    );
                    let serve = handler.serve_with_lifecycle(transport, lifecycle);
                    tokio::select! {
                        biased;
                        res = serve => res.map_err(|error| init_failure(error, &ingress, &termination)),
                        () = termination.cancelled() => Err(termination.failure(false)),
                    }
                }
                ServerConfig::Http {
                    url,
                    headers,
                    oauth,
                    ..
                } => {
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
                    let builder = || {
                        reqwest::Client::builder()
                            .redirect(reqwest::redirect::Policy::none())
                            .retry(reqwest::retry::never())
                            .connect_timeout(MAX_CONNECT)
                    };
                    let client = builder()
                        .build()
                        .or_else(|error| {
                            if reqwest::Url::parse(url).is_ok_and(|url| url.scheme() == "http") {
                                // Minimal systems may have no CA store. Plain HTTP needs none;
                                // an empty trust store still rejects any TLS connection or proxy.
                                builder().tls_certs_only(std::iter::empty()).build()
                            } else {
                                Err(error)
                            }
                        })
                        .map_err(|_| {
                            failure(
                                FailureKind::Transport,
                                "Cannot create HTTP client; check the system CA certificates",
                            )
                        })?;
                    let has_static_auth = parsed_headers.contains_key(&http::header::AUTHORIZATION);
                    let auth_manager = if has_static_auth {
                        None
                    } else {
                        Some(
                            crate::auth::HttpAuthManager::for_server(url, oauth.as_ref()).map_err(
                                |e| {
                                    failure(
                                        FailureKind::Protocol,
                                        format!("Invalid HTTP server URL: {e}"),
                                    )
                                },
                            )?,
                        )
                    };
                    let transport_config =
                        StreamableHttpClientTransportConfig::with_uri(url.clone())
                            .custom_headers(parsed_headers)
                            .reinit_on_expired_session(false)
                            .max_sse_event_size(limits.max_response_bytes);
                    let transport = StreamableHttpClientTransport::with_client(
                        BoundedHttp {
                            client,
                            auth_manager,
                            cleanup: cleanup.clone(),
                            ingress: ingress.clone(),
                            event_schemas: event_schemas.clone(),
                            termination: termination.clone(),
                            max_response_bytes: limits.max_response_bytes,
                            budget: limits.budget.clone(),
                        },
                        transport_config,
                    );
                    let transport = EventNotificationTransport::new(
                        transport,
                        event_router.clone(),
                        termination.clone(),
                    );
                    let serve = handler.serve_with_lifecycle(transport, lifecycle);
                    tokio::select! {
                        biased;
                        res = serve => res.map_err(|error| init_failure(error, &ingress, &termination)),
                        () = termination.cancelled() => Err(termination.failure(false)),
                    }
                }
            }
        };
        let service = match timeout(budget, connection).await {
            Ok(Ok(service)) => service,
            Ok(Err(error)) => {
                let capture = stderr_slot.lock().unwrap_or_else(|p| p.into_inner()).take();
                return Err(attach_stderr_tail_and_abort(error, capture).await);
            }
            Err(_) => {
                let capture = stderr_slot.lock().unwrap_or_else(|p| p.into_inner()).take();
                return Err(attach_stderr_tail_and_abort(
                    failure(FailureKind::Timeout, "MCP connection timed out"),
                    capture,
                )
                .await);
            }
        };
        let stderr_capture = stderr_slot.lock().unwrap_or_else(|p| p.into_inner()).take();
        // Explicit discovery must not silently return an expired SDK cache entry.
        service
            .peer()
            .set_response_cache_config(ClientCacheConfig::disabled())
            .await;
        let observed = match observed_result(ingress.take(), None, false) {
            Ok(observed) => observed,
            Err(error) => {
                drop(service);
                return Err(attach_stderr_tail_and_abort(error, stderr_capture).await);
            }
        };
        // Keep the owner older than the payload through the fallible peer lookup.
        let result_lease = observed.2;
        let (source, result) = (observed.0, observed.1);
        let protocol = match service.peer().peer_info() {
            Some(protocol) => protocol,
            None => {
                drop(service);
                return Err(attach_stderr_tail_and_abort(
                    failure(FailureKind::Protocol, "MCP peer info is missing"),
                    stderr_capture,
                )
                .await);
            }
        };
        let mut server = Map::new();
        server.insert("source".into(), Value::String(source.into()));
        server.insert(
            "protocolVersion".into(),
            Value::String(protocol.protocol_version.as_str().into()),
        );
        server.insert("result".into(), result);
        let (server_bytes, server_lease) = match crate::json::encode_json_leased(
            &server,
            limits.max_response_bytes,
            &limits.budget,
        ) {
            Ok(pair) => pair,
            Err(error) => {
                drop(service);
                return Err(attach_stderr_tail_and_abort(error.into(), stderr_capture).await);
            }
        };
        drop(server);
        drop(result_lease);
        Ok(Self {
            ingress,
            event_router,
            event_schemas,
            termination,
            stderr_capture,
            limits,
            server: Leased::new(server_bytes, server_lease),
            peer: service.peer().clone(),
            service: Some(service),
            closing: None,
            cleanup,
            tools_changed,
            http: matches!(config, ServerConfig::Http { .. }),
        })
    }

    fn abort_service(&self) {
        if let Some(service) = &self.service {
            service.cancellation_token().cancel();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.termination.is_closed()
            || self.service.as_ref().is_none_or(RunningService::is_closed)
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

    pub fn server_info(&self) -> Result<Leased<Value>> {
        let lease = self
            .limits
            .budget
            .try_acquire(self.server.len())
            .ok_or_else(|| {
                failure(
                    FailureKind::Transport,
                    "Server metadata exceeds the active memory limit",
                )
            })?;
        let scratch = self
            .limits
            .budget
            .try_acquire(crate::json::scratch_bytes(&self.server))
            .ok_or_else(|| {
                failure(
                    FailureKind::Transport,
                    "Server metadata parsing exceeds the active memory limit",
                )
            })?;
        let value = serde_json::from_slice(&self.server)?;
        drop(scratch);
        Ok(Leased::new(value, lease))
    }

    async fn attach_stderr_tail(&self, error: anyhow::Error) -> anyhow::Error {
        attach_stderr_tail(error, self.stderr_capture.as_ref(), self.is_closed()).await
    }

    pub(crate) async fn closed_error(&self) -> anyhow::Error {
        self.attach_stderr_tail(self.termination.failure(false))
            .await
    }

    pub async fn list_tools(&self, budget: Duration) -> Result<Leased<Vec<Value>>> {
        let (pages, _) = self.walk_tools(budget, None).await?;
        let (pages, lease) = pages.into_parts();
        Ok(Leased::new(
            pages
                .into_iter()
                .flat_map(|mut page| {
                    page["tools"]
                        .take()
                        .as_array_mut()
                        .map(std::mem::take)
                        .unwrap_or_default()
                })
                .collect(),
            lease,
        ))
    }

    pub async fn list_tool_pages(&self, budget: Duration) -> Result<Leased<Vec<Value>>> {
        self.walk_tools(budget, None).await.map(|(pages, _)| pages)
    }

    async fn walk_tools(
        &self,
        budget: Duration,
        call_cancel: Option<&CancellationToken>,
    ) -> Result<(Leased<Vec<Value>>, Leased<HashSet<String>>)> {
        if call_cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(failure(FailureKind::Cancelled, "Tool discovery cancelled"));
        }
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        self.termination.clear_transient();
        let _capture = self.ingress.capture().ok_or_else(busy_capture)?;
        let listing = async {
            let mut pages_lease = self.limits.budget.empty_lease();
            let mut names_lease = self.limits.budget.empty_lease();
            let mut cursors_lease = self.limits.budget.empty_lease();
            let mut pages = Vec::new();
            let mut count = 0usize;
            let mut cursor_owner = self.limits.budget.empty_lease();
            let mut cursor = None;
            let mut cursors = HashSet::new();
            let mut names = HashSet::new();
            let mut bytes = 0usize;
            for _ in 0..MAX_TOOL_PAGES {
                let mut params = PaginatedRequestParams::default();
                params.cursor = cursor;
                let list_fut = self.peer.list_tools(Some(params));
                let mut page = tokio::select! {
                    biased;
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        return Err(self.termination.failure(false));
                    }
                    () = async {
                        match call_cancel {
                            Some(token) => token.cancelled().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        return Err(failure(FailureKind::Cancelled, "Tool discovery cancelled"));
                    }
                    res = list_fut => res.map_err(|e| service_failure(e, false, &self.ingress, &self.termination))?,
                };
                let (_, raw_page, page_lease) =
                    observed_result(self.ingress.take(), Some("tools/list"), false)?;
                pages_lease.merge(page_lease);
                for tool in std::mem::take(&mut page.tools) {
                    if !names_lease.try_grow(tool.name.len()) {
                        return Err(failure(
                            FailureKind::Transport,
                            "Tool names exceed the active memory limit",
                        ));
                    }
                    if tool.name.is_empty() || !names.insert(tool.name.into_owned()) {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Tool catalog contains an empty or duplicate name",
                        ));
                    }
                    count += 1;
                    if count > MAX_TOOLS {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Tool catalog exceeds the size limit",
                        ));
                    }
                }
                bytes = bytes.saturating_add(crate::json::serialized_len(&raw_page)?);
                if bytes > self.limits.max_tool_catalog_bytes {
                    return Err(failure(
                        FailureKind::Protocol,
                        format!(
                            "Tool catalog exceeds the {} size limit",
                            format_bytes(self.limits.max_tool_catalog_bytes)
                        ),
                    ));
                }
                pages.push(raw_page);
                // Charge the cursor while the application retains it between
                // pages. SDK-owned request/cache copies are outside this owner.
                let cursor_bytes = page.next_cursor.as_ref().map_or(0, String::capacity);
                if !cursor_owner.try_grow(cursor_bytes.saturating_sub(cursor_owner.bytes())) {
                    return Err(failure(
                        FailureKind::Transport,
                        "Tool cursors exceed the active memory limit",
                    ));
                }
                cursor_owner.shrink_to(cursor_bytes);
                cursor = page.next_cursor.take();
                drop(page);
                if let Some(c) = &cursor
                    && !cursors_lease.try_grow(c.len())
                {
                    return Err(failure(
                        FailureKind::Transport,
                        "Tool cursors exceed the active memory limit",
                    ));
                }
                match &cursor {
                    None => {
                        return Ok((
                            Leased::new(pages, pages_lease),
                            Leased::new(names, names_lease),
                        ));
                    }
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
        let result = timeout(budget, listing)
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Tool discovery timed out"))?;
        match result {
            Ok(pair) => Ok(pair),
            Err(error) => Err(self.attach_stderr_tail(error).await),
        }
    }

    pub async fn call(
        &self,
        name: &str,
        arguments: Map<String, Value>,
        budget: Duration,
    ) -> Result<Leased<Value>> {
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
    ) -> Result<Leased<Value>> {
        let input_lease = self
            .limits
            .budget
            .try_acquire(crate::json::serialized_len(&arguments)?)
            .ok_or_else(|| {
                failure(
                    FailureKind::Transport,
                    "Tool arguments exceed the active memory limit",
                )
            })?;
        let arguments = Leased::new(arguments, input_lease);
        let budgets = CallBudgets {
            pre_dispatch_deadline: Instant::now() + budget,
            idle: budget,
            hard: None,
        };
        self.call_with_dispatch_leased(
            name,
            &arguments,
            budgets,
            dispatched,
            CancellationToken::new(),
        )
        .await
        .map(|(value, lease)| Leased::new(value, lease))
    }

    pub async fn call_with_dispatch_leased(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        budgets: CallBudgets,
        dispatched: &AtomicBool,
        call_cancel: CancellationToken,
    ) -> Result<(Value, ByteLease)> {
        enum CallWaitOutcome {
            Terminated,
            Cancelled,
            Response(Box<std::result::Result<ServerResult, ServiceError>>),
        }

        let started = Instant::now();
        if call_cancel.is_cancelled() {
            return Err(failure(
                FailureKind::Cancelled,
                "Tool was not dispatched: cancelled before discovery",
            ));
        }
        let discovery_budget = budgets
            .pre_dispatch_deadline
            .saturating_duration_since(Instant::now());
        if discovery_budget.is_zero() {
            return Err(failure(
                FailureKind::Timeout,
                "Tool was not dispatched: pre-dispatch deadline exceeded",
            ));
        }
        // HTTP header annotations need the SDK's current catalog; fresh discovery
        // also prevents invoking tools rejected by the transport or withdrawn.
        let (pages, names) = self
            .walk_tools(discovery_budget, Some(&call_cancel))
            .await
            .map_err(|error| -> anyhow::Error {
                let message = format!("Tool was not dispatched: discovery failed: {error}");
                match error.downcast::<McpFailure>() {
                    Ok(mut error) => {
                        error.message = message;
                        error.outcome_unknown = false;
                        error.into()
                    }
                    Err(_) => failure(FailureKind::Transport, message),
                }
            })?;
        if !names.contains(name) {
            return Err(failure(
                FailureKind::Protocol,
                "Tool was not dispatched: not present in the current catalog",
            ));
        }
        let parameter_bytes = crate::json::serialized_len(arguments)?.saturating_add(name.len());
        let header_bytes = if self.http
            && self
                .peer
                .peer_info()
                .is_some_and(|info| info.protocol_version >= ProtocolVersion::STANDARD_HEADERS)
        {
            let schema = pages
                .iter()
                .filter_map(|page| page.get("tools").and_then(Value::as_array))
                .flatten()
                .find(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
                .and_then(|tool| tool.get("inputSchema"))
                .and_then(Value::as_object);
            events::routing_header_payload_bound(name, Some(arguments), schema)
        } else {
            0
        };
        drop(names);
        drop(pages);
        let copies = if self.http { 2 } else { 1 };
        // HTTP holds one request plus either its POST clone or header Value
        // projection. Header construction also has bounded temporary encodings.
        let _parameter_lease = self
            .limits
            .budget
            .try_acquire(
                parameter_bytes
                    .saturating_mul(copies)
                    .saturating_add(header_bytes.saturating_mul(5)),
            )
            .ok_or_else(|| {
                failure(
                    FailureKind::Transport,
                    "Tool arguments and headers exceed the active memory limit",
                )
            })?;
        self.termination.clear_transient();
        // This owner covers the application's request construction and wait.
        // It makes no claim about SDK copies retained after cancellation.
        let request = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments.clone());
        // This conservative boundary is also visible to the daemon's outer deadline.
        let _capture = self.ingress.capture().ok_or_else(busy_capture)?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        if call_cancel.is_cancelled() {
            return Err(failure(
                FailureKind::Cancelled,
                "Tool was not dispatched: cancelled before dispatch",
            ));
        }
        let options = budgets.request_options(started)?;
        dispatched.store(true, Ordering::Release);
        let handle = match self
            .peer
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(request)),
                options,
            )
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                let err = service_failure(error, true, &self.ingress, &self.termination);
                return Err(self.attach_stderr_tail(err).await);
            }
        };
        let request_id = handle.id.clone();
        let peer = self.peer.clone();
        let mut cancel_guard = CancelOnDropGuard::new(peer.clone(), request_id.clone());
        let outcome = {
            let response_fut = handle.await_response();
            tokio::pin!(response_fut);
            tokio::select! {
                biased;
                () = self.termination.cancelled() => CallWaitOutcome::Terminated,
                () = call_cancel.cancelled() => CallWaitOutcome::Cancelled,
                res = &mut response_fut => CallWaitOutcome::Response(Box::new(res)),
            }
        };
        let server_result = match outcome {
            CallWaitOutcome::Terminated => {
                cancel_guard.disarm();
                self.abort_service();
                return Err(self
                    .attach_stderr_tail(self.termination.failure(true))
                    .await);
            }
            CallWaitOutcome::Cancelled => {
                cancel_guard.disarm();
                let notification = CancelledNotification::new(CancelledNotificationParam::new(
                    Some(request_id),
                    Some("cancelled by client".into()),
                ));
                let _ = timeout(
                    Duration::from_secs(2),
                    peer.send_notification(ClientNotification::CancelledNotification(notification)),
                )
                .await;
                return Err(failure_with_outcome(
                    FailureKind::Cancelled,
                    "MCP call cancelled; outcome unknown, the call was not retried",
                    true,
                ));
            }
            CallWaitOutcome::Response(res) => {
                cancel_guard.disarm();
                match *res {
                    Ok(result) => result,
                    Err(ServiceError::Timeout { .. }) => {
                        return Err(failure_with_outcome(
                            FailureKind::Timeout,
                            "MCP call timed out; cancellation requested; outcome unknown, the call was not retried",
                            true,
                        ));
                    }
                    Err(error) => {
                        let err = service_failure(error, true, &self.ingress, &self.termination);
                        return Err(self.attach_stderr_tail(err).await);
                    }
                }
            }
        };
        let response = match server_result {
            ServerResult::CallToolResult(result) => CallToolResponse::Complete(result),
            ServerResult::InputRequiredResult(result) => CallToolResponse::InputRequired(result),
            ServerResult::CreateTaskResult(result) => CallToolResponse::Task(result),
            _ => {
                return Err(service_failure(
                    ServiceError::UnexpectedResponse,
                    true,
                    &self.ingress,
                    &self.termination,
                ));
            }
        };
        match response {
            CallToolResponse::Complete(sdk_result) => {
                drop(sdk_result);
                observed_result(self.ingress.take(), Some("tools/call"), true)
                    .map(|(_, value, lease)| (value, lease))
            }
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

    pub fn events_client(&self) -> McpEventsClient {
        McpEventsClient {
            peer: self.peer.clone(),
            service_cancel: Arc::new(std::sync::Mutex::new(
                self.service
                    .as_ref()
                    .map(RunningService::cancellation_token),
            )),
            ingress: self.ingress.clone(),
            event_router: self.event_router.clone(),
            event_schemas: self.event_schemas.clone(),
            termination: self.termination.clone(),
            stderr_capture: self.stderr_capture.clone(),
            limits: self.limits.clone(),
            http: self.http,
        }
    }

    pub fn supports_events(&self) -> bool {
        self.events_client().supports_events()
    }

    pub fn check_events_support(&self) -> Result<()> {
        self.events_client().check_events_support()
    }

    pub async fn list_events(&self, budget: Duration) -> Result<Leased<Vec<Value>>> {
        self.events_client().list_events(budget).await
    }

    pub async fn poll_events(
        &self,
        params: PollEventsParams,
        budget: Duration,
    ) -> Result<Leased<Value>> {
        self.events_client().poll_events(params, budget).await
    }

    pub async fn open_event_stream(
        &self,
        params: StreamEventsParams,
        budget: Duration,
    ) -> Result<(EventStreamHandle, EventStreamReceiver)> {
        self.events_client().open_event_stream(params, budget).await
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
        if let Some(capture) = self.stderr_capture.take() {
            capture.abort();
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

struct UntrackOnDrop {
    ingress: Ingress,
    id: Option<rmcp::model::RequestId>,
}

impl UntrackOnDrop {
    fn new(ingress: Ingress, id: rmcp::model::RequestId) -> Self {
        Self {
            ingress,
            id: Some(id),
        }
    }

    fn disarm(&mut self) {
        self.id = None;
    }
}

impl Drop for UntrackOnDrop {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.ingress.untrack(&id);
        }
    }
}

/// Cloneable handle for MCP Events operations that can run without holding `Mutex<ServerState>`.
#[derive(Clone)]
pub struct McpEventsClient {
    peer: Peer<RoleClient>,
    service_cancel: Arc<std::sync::Mutex<Option<rmcp::service::RunningServiceCancellationToken>>>,
    ingress: Ingress,
    event_router: EventRouter,
    event_schemas: EventSchemaCache,
    termination: Arc<TransportTermination>,
    stderr_capture: Option<Arc<StderrCapture>>,
    limits: McpLimits,
    http: bool,
}

impl McpEventsClient {
    fn service_failure(&self, error: ServiceError) -> anyhow::Error {
        let ServiceError::McpError(error) = error else {
            return service_failure(error, false, &self.ingress, &self.termination);
        };
        let Ok(bytes) = crate::json::serialized_len(&error) else {
            return failure(
                FailureKind::Protocol,
                "MCP event error could not be represented",
            );
        };
        let Some(mut lease) = self.limits.budget.try_acquire(bytes) else {
            return failure(
                FailureKind::Transport,
                "MCP event error exceeds the active memory limit",
            );
        };
        let code = error.code.0;
        let code_bytes = code.unsigned_abs().checked_ilog10().unwrap_or(0) as usize
            + 1
            + usize::from(code.is_negative());
        let message_bytes = error
            .message
            .len()
            .saturating_add(code_bytes + "MCP error : ".len());
        let mut message = String::new();
        if !lease.reserve_string(&mut message, message_bytes, self.limits.budget.limit()) {
            return failure(
                FailureKind::Transport,
                "MCP event error exceeds the active memory limit",
            );
        }
        use std::fmt::Write as _;
        write!(&mut message, "MCP error {code}: {}", error.message)
            .expect("formatting into a String cannot fail");
        let mut value = Map::new();
        value.insert("code".into(), Value::Number(code.into()));
        value.insert("message".into(), Value::String(error.message.into_owned()));
        if let Some(data) = error.data {
            value.insert("data".into(), data);
        }
        McpFailure {
            kind: FailureKind::Protocol,
            message,
            outcome_unknown: false,
            rpc_error: Some(Arc::new(Value::Object(value))),
            auth: None,
            stderr_tail: None,
            lease: Some(Arc::new(lease)),
        }
        .into()
    }

    fn event_request(
        &self,
        method: &'static str,
        params: &impl serde::Serialize,
    ) -> Result<Leased<ClientRequest>> {
        let bytes = crate::json::serialized_len(params)?.saturating_add(method.len());
        let lease = self.limits.budget.try_acquire(bytes).ok_or_else(|| {
            failure(
                FailureKind::Transport,
                "MCP event request exceeds the active memory limit",
            )
        })?;
        let request = ClientRequest::CustomRequest(CustomRequest::new(
            method,
            Some(serde_json::to_value(params)?),
        ));
        Ok(Leased::new(request, lease))
    }

    fn abort_service(&self) {
        if let Some(cancel) = self
            .service_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            cancel.cancel();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.termination.is_closed() || self.peer.is_transport_closed()
    }

    async fn attach_stderr_tail(&self, error: anyhow::Error) -> anyhow::Error {
        attach_stderr_tail(error, self.stderr_capture.as_ref(), self.is_closed()).await
    }

    pub(crate) async fn closed_error(&self) -> anyhow::Error {
        self.attach_stderr_tail(self.termination.failure(false))
            .await
    }

    pub fn supports_events(&self) -> bool {
        self.check_events_support().is_ok()
    }

    pub fn check_events_support(&self) -> Result<()> {
        events::check_events_capability(self.peer.peer_info().as_deref())
    }

    pub async fn list_events(&self, budget: Duration) -> Result<Leased<Vec<Value>>> {
        self.check_events_support()?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        self.termination.clear_transient();
        let listing = async {
            let mut lease = self.limits.budget.empty_lease();
            let mut events = Vec::new();
            let mut schema_lease = self.limits.budget.empty_lease();
            let mut schemas = HashMap::new();
            let mut pagination_lease = self.limits.budget.empty_lease();
            let mut cursors = HashSet::new();
            let mut cursor: Option<String> = None;
            let mut cursor_bytes = 0;

            for _ in 0..MAX_EVENT_PAGES {
                let params = cursor.take().map(|c| {
                    let mut map = Map::new();
                    map.insert("cursor".into(), Value::String(c));
                    Value::Object(map)
                });
                let request =
                    ClientRequest::CustomRequest(CustomRequest::new("events/list", params));
                let mut handle = self
                    .peer
                    .send_cancellable_request(request, PeerRequestOptions::no_options())
                    .await
                    .map_err(|e| self.service_failure(e))?;
                pagination_lease.shrink_by(cursor_bytes);
                let mut page_lease = self.limits.budget.empty_lease();
                let mut guard = UntrackOnDrop::new(self.ingress.clone(), handle.id.clone());
                let result = tokio::select! {
                    biased;
                    rpc = &mut handle.rx => match rpc {
                        Ok(res) => res.map_err(|e| self.service_failure(e))?,
                        Err(_) => return Err(self.termination.failure(false)),
                    },
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        return Err(self.termination.failure(false));
                    }
                };
                guard.disarm();
                let ServerResult::CustomResult(CustomResult(mut page)) = result else {
                    return Err(failure(
                        FailureKind::Protocol,
                        "MCP server returned an unexpected events/list response",
                    ));
                };
                let page_bytes = crate::json::serialized_len(&page)?;
                if !page_lease.try_grow(page_bytes) {
                    return Err(failure(
                        FailureKind::Transport,
                        "Event catalog exceeds the active memory limit",
                    ));
                }
                let next_cursor = match page.get("nextCursor").and_then(Value::as_str) {
                    Some(next) => {
                        cursor_bytes = crate::json::serialized_len(&next)?;
                        if !pagination_lease.try_grow(cursor_bytes.saturating_mul(2)) {
                            return Err(failure(
                                FailureKind::Transport,
                                "Event catalog cursors exceed the active memory limit",
                            ));
                        }
                        Some(next.to_owned())
                    }
                    None => None,
                };
                let page_events = page
                    .get_mut("events")
                    .and_then(Value::as_array_mut)
                    .map(std::mem::take)
                    .unwrap_or_default();
                for event in page_events {
                    if self.http
                        && let Some(schema) = event.get("inputSchema").and_then(Value::as_object)
                    {
                        let event_name = event
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("<unnamed>");
                        if let Err(reason) = events::validate_param_header_annotations(schema) {
                            eprintln!(
                                "mcpcli: ignoring HTTP event {:?} with invalid x-mcp-header annotation: {}",
                                crate::discovery::safe_text(event_name),
                                crate::discovery::safe_text(&reason)
                            );
                            continue;
                        }
                        if let Some(name) = event.get("name").and_then(Value::as_str) {
                            let schema_bytes =
                                crate::json::serialized_len(schema)?.saturating_add(name.len());
                            if !schema_lease.try_grow(schema_bytes) {
                                return Err(failure(
                                    FailureKind::Transport,
                                    "Event schema cache exceeds the active memory limit",
                                ));
                            }
                            schemas.insert(name.to_owned(), schema.clone());
                        }
                    }
                    let item_bytes = crate::json::serialized_len(&event)?;
                    lease.merge(
                        page_lease
                            .split(item_bytes)
                            .expect("event belongs to catalog page"),
                    );
                    events.push(event);
                }
                match next_cursor {
                    None => {
                        if self.http
                            && let Ok(mut guard) = self.event_schemas.write()
                        {
                            *guard = Some(Leased::new(schemas, schema_lease));
                        }
                        return Ok(Leased::new(events, lease));
                    }
                    Some(next) if !cursors.insert(next.clone()) => {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Event catalog repeated a pagination cursor",
                        ));
                    }
                    Some(next) => cursor = Some(next),
                }
            }
            Err(failure(
                FailureKind::Protocol,
                "Event catalog exceeds the 100-page pagination limit",
            ))
        };
        let result = timeout(budget, listing)
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Event discovery timed out"))?;
        match result {
            Ok(events) => Ok(events),
            Err(error) => Err(self.attach_stderr_tail(error).await),
        }
    }

    pub async fn poll_events(
        &self,
        params: PollEventsParams,
        budget: Duration,
    ) -> Result<Leased<Value>> {
        self.check_events_support()?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        let started = tokio::time::Instant::now();
        self.termination.clear_transient();
        for attempt in 0..2 {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(failure(FailureKind::Timeout, "Event poll timed out"));
            }
            let (request, request_lease) = self.event_request("events/poll", &params)?.into_parts();
            let res = timeout(remaining, async {
                let mut handle = self
                    .peer
                    .send_cancellable_request(request, PeerRequestOptions::no_options())
                    .await?;
                drop(request_lease);
                let mut guard = UntrackOnDrop::new(self.ingress.clone(), handle.id.clone());
                let res = tokio::select! {
                    biased;
                    rpc = &mut handle.rx => match rpc {
                        Ok(res) => res,
                        Err(_) => Err(ServiceError::TransportClosed),
                    },
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        Err(ServiceError::TransportClosed)
                    }
                };
                guard.disarm();
                res
            })
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Event poll timed out"))?;
            match res {
                Ok(ServerResult::CustomResult(CustomResult(value))) => {
                    let bytes = crate::json::serialized_len(&value)?;
                    let lease = self.limits.budget.try_acquire(bytes).ok_or_else(|| {
                        failure(
                            FailureKind::Transport,
                            "Event poll response exceeds the active memory limit",
                        )
                    })?;
                    return Ok(Leased::new(value, lease));
                }
                Ok(_) => {
                    return Err(failure(
                        FailureKind::Protocol,
                        "MCP server returned an unexpected events/poll response",
                    ));
                }
                Err(ServiceError::McpError(ref err))
                    if attempt == 0 && err.code == ErrorCode::HEADER_MISMATCH =>
                {
                    let refresh_budget = budget.saturating_sub(started.elapsed());
                    let _ = self.list_events(refresh_budget).await?;
                    continue;
                }
                Err(error) => {
                    return Err(self.attach_stderr_tail(self.service_failure(error)).await);
                }
            }
        }
        unreachable!("poll_events retry loop always returns")
    }

    pub async fn open_event_stream(
        &self,
        params: StreamEventsParams,
        budget: Duration,
    ) -> Result<(EventStreamHandle, EventStreamReceiver)> {
        enum EitherStreamStart {
            Notification(Leased<EventNotification>),
            Rpc(
                Box<
                    std::result::Result<
                        std::result::Result<ServerResult, ServiceError>,
                        tokio::sync::oneshot::error::RecvError,
                    >,
                >,
            ),
        }
        self.check_events_support()?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        let started = tokio::time::Instant::now();
        self.termination.clear_transient();
        for attempt in 0..2 {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(failure(FailureKind::Timeout, "Event stream open timed out"));
            }
            let (request, request_lease) =
                self.event_request("events/stream", &params)?.into_parts();
            let mut handle = match timeout(
                remaining,
                self.peer
                    .send_cancellable_request(request, PeerRequestOptions::no_options()),
            )
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Event stream open timed out"))?
            {
                Ok(handle) => handle,
                Err(e) => return Err(self.attach_stderr_tail(self.service_failure(e)).await),
            };
            drop(request_lease);
            let id = handle.id.clone();
            let (_, empty_response) = tokio::sync::oneshot::channel();
            let mut response = std::mem::replace(&mut handle.rx, empty_response);
            let mut rx = self.event_router.take_receiver(&id);
            // Own cancellation before awaiting the first notification or RPC response.
            let stream_handle =
                EventStreamHandle::new(handle, self.event_router.clone(), self.ingress.clone());
            let wait_budget = budget.saturating_sub(started.elapsed());
            let first_outcome = timeout(wait_budget, async {
                tokio::select! {
                    biased;
                    Some(notif) = rx.recv() => Ok(EitherStreamStart::Notification(notif)),
                    rpc = &mut response => Ok(EitherStreamStart::Rpc(Box::new(rpc))),
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        Err(self.termination.failure(false))
                    }
                }
            })
            .await;

            match first_outcome {
                Err(_) => {
                    stream_handle.cancel().await?;
                    return Err(failure(
                        FailureKind::Timeout,
                        "Timed out waiting for event stream start",
                    ));
                }
                Ok(Err(err)) => return Err(self.attach_stderr_tail(err).await),
                Ok(Ok(EitherStreamStart::Notification(first))) => {
                    return Ok((stream_handle, EventStreamReceiver::new(Some(first), rx)));
                }
                Ok(Ok(EitherStreamStart::Rpc(rpc_res))) => match *rpc_res {
                    Ok(Err(ServiceError::McpError(ref err)))
                        if attempt == 0 && err.code == ErrorCode::HEADER_MISMATCH =>
                    {
                        drop(stream_handle);
                        let refresh_budget = budget.saturating_sub(started.elapsed());
                        let _ = self.list_events(refresh_budget).await?;
                        continue;
                    }
                    Ok(Err(err)) => {
                        return Err(self.attach_stderr_tail(self.service_failure(err)).await);
                    }
                    Ok(Ok(_)) => {
                        return Ok((stream_handle, EventStreamReceiver::new(None, rx)));
                    }
                    Err(_) => {
                        return Err(self
                            .attach_stderr_tail(self.termination.failure(false))
                            .await);
                    }
                },
            }
        }
        unreachable!("open_event_stream retry loop always returns")
    }
}

fn init_failure(
    error: rmcp::service::ClientInitializeError,
    ingress: &Ingress,
    termination: &TransportTermination,
) -> anyhow::Error {
    use rmcp::service::ClientInitializeError as E;
    match error {
        E::JsonRpcError(e) => rpc_failure(
            e.code.0,
            format!("MCP initialization error {}: {}", e.code.0, e.message),
            false,
            ingress,
        ),
        E::NoCompatibleProtocolVersion { .. } => {
            let (rpc_error, lease, rejected) = match ingress.take() {
                Some((_, Ok(mut response), lease)) => {
                    (response.get_mut("error").map(Value::take).map(Arc::new), Some(Arc::new(lease)), false)
                }
                Some((_, Err(_), _)) => (None, None, true),
                None => (None, None, false),
            };
            McpFailure {
                kind: FailureKind::Protocol,
                message: if rejected {
                    "No compatible MCP protocol version; response JSON could not be represented losslessly"
                } else {
                    "No compatible MCP protocol version"
                }
                .into(),
                outcome_unknown: false,
                rpc_error,
                auth: None,
                stderr_tail: None,
                lease,
            }
            .into()
        }
        // HTTP transport errors can contain URLs or response bodies. Do not leak them.
        _ => termination.take_error(false).unwrap_or_else(|| {
            failure(
                FailureKind::Transport,
                "MCP connection or initialization failed; check the configured server and credentials",
            )
        }),
    }
}

struct OutgoingHttpBody {
    bytes: Leased<Vec<u8>>,
    _message: ClientJsonRpcMessage,
}

impl AsRef<[u8]> for OutgoingHttpBody {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

fn encode_mcp_message(
    message: &ClientJsonRpcMessage,
    limit: usize,
    budget: &Arc<ByteBudget>,
    termination: &TransportTermination,
) -> std::io::Result<Leased<Vec<u8>>> {
    crate::json::encode_json_leased(message, limit, budget).map(|(bytes, lease)| Leased::new(bytes, lease)).map_err(|error| {
        let reason = if error.kind() == std::io::ErrorKind::OutOfMemory {
            format!("MCP outgoing message exceeded the {} active memory limit", format_bytes(budget.limit()))
        } else { format!("MCP outgoing message could not be encoded within the {} message limit: {error}", format_bytes(limit)) };
        termination.terminate(FailureKind::Transport, reason.clone());
        std::io::Error::new(error.kind(), reason)
    })
}

fn has_media_type(content_type: &str, expected: &str) -> bool {
    let essence = content_type
        .split_once(';')
        .map_or(content_type, |(essence, _)| essence)
        .trim();
    essence.eq_ignore_ascii_case(expected)
}

fn is_standard_notification(method: &str) -> bool {
    matches!(
        method,
        "notifications/cancelled"
            | "notifications/initialized"
            | "notifications/message"
            | "notifications/progress"
            | "notifications/prompts/list_changed"
            | "notifications/resources/list_changed"
            | "notifications/resources/updated"
            | "notifications/roots/list_changed"
            | "notifications/subscriptions/acknowledged"
            | "notifications/tools/list_changed"
    )
}

fn is_standard_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "ping"
            | "server/discover"
            | "prompts/get"
            | "prompts/list"
            | "resources/list"
            | "resources/read"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "resources/templates/list"
            | "subscriptions/listen"
            | "tools/call"
            | "tools/list"
            | "completion/complete"
            | "elicitation/create"
            | "logging/setLevel"
            | "roots/list"
            | "sampling/createMessage"
    ) || is_standard_notification(method)
}

fn should_ignore_notification(json_value: &Value, method: &str) -> bool {
    if method.starts_with("notifications/events/") {
        return false;
    }
    if json_value.get("id").is_none() && !is_standard_method(method) {
        return true;
    }
    method.starts_with("notifications/") && !is_standard_notification(method)
}

enum StdioParseError {
    Ignore,
    InvalidRequest,
}

fn classify_stdio_error(
    error: serde_json::Error,
    ignore_notification: bool,
) -> std::result::Result<Option<ServerJsonRpcMessage>, StdioParseError> {
    if ignore_notification {
        return Ok(None);
    }
    match error.classify() {
        serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
            Err(StdioParseError::Ignore)
        }
        serde_json::error::Category::Data | serde_json::error::Category::Io => {
            Err(StdioParseError::InvalidRequest)
        }
    }
}

/// Own the child and bounded stdio framing throughout shutdown.
struct ChildTransport<R = ChildStdout, W = ChildStdin> {
    read: BufReader<R>,
    line_buf: Vec<u8>,
    line_lease: ByteLease,
    max_line_bytes: usize,
    budget: Arc<ByteBudget>,
    write: Arc<tokio::sync::Mutex<Option<W>>>,
    pending_error_reply: bool,
    ingress: Ingress,
    termination: Arc<TransportTermination>,
    child: Option<Child>,
    #[cfg(unix)]
    group: Option<i32>,
    #[cfg(windows)]
    job: Option<windows_job::Job>,
    cleanup: CleanupState,
}

impl ChildTransport<ChildStdout, ChildStdin> {
    fn spawn_observed(
        mut command: Command,
        cleanup: CleanupState,
        ingress: Ingress,
        termination: Arc<TransportTermination>,
        max_line_bytes: usize,
        stderr_tail_bytes: usize,
        budget: Arc<ByteBudget>,
    ) -> std::io::Result<(Self, Option<Arc<StderrCapture>>)> {
        let stderr_ring = if stderr_tail_bytes > 0 {
            budget
                .try_acquire(stderr_tail_bytes)
                .and_then(|lease| StderrRing::try_new(stderr_tail_bytes, lease))
        } else {
            None
        };
        let capture_stderr = stderr_ring.is_some();
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(if capture_stderr {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .kill_on_drop(false);
        #[cfg(unix)]
        crate::platform::configure_child_process_group(&mut command);
        #[cfg(windows)]
        command
            .creation_flags(
                windows_sys::Win32::System::Threading::CREATE_SUSPENDED
                    | windows_sys::Win32::System::Threading::CREATE_NO_WINDOW,
            )
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        #[cfg(unix)]
        let group = child.id().map(|id| id as i32);
        #[cfg(windows)]
        let job = match windows_job::Job::adopt(&child) {
            Ok(job) => Some(job),
            Err(error) => {
                let _ = child.start_kill();
                tokio::spawn(async move {
                    let _ = child.wait().await;
                });
                return Err(error);
            }
        };
        let stdout = child.stdout.take().expect("piped stdout");
        let stdin = child.stdin.take().expect("piped stdin");
        let stderr_capture = match (child.stderr.take(), stderr_ring) {
            (Some(stderr), Some(ring)) => Some(StderrCapture::spawn(stderr, ring)),
            _ => None,
        };
        let line_lease = budget.empty_lease();
        Ok((
            Self {
                read: BufReader::new(stdout),
                line_buf: Vec::new(),
                line_lease,
                max_line_bytes,
                budget,
                write: Arc::new(tokio::sync::Mutex::new(Some(stdin))),
                pending_error_reply: false,
                ingress,
                termination,
                child: Some(child),
                #[cfg(unix)]
                group,
                #[cfg(windows)]
                job,
                cleanup,
            },
            stderr_capture,
        ))
    }
}

impl<R, W> ChildTransport<R, W> {
    #[cfg(test)]
    fn from_parts(
        read: R,
        write: W,
        ingress: Ingress,
        termination: Arc<TransportTermination>,
        max_line_bytes: usize,
        budget: Arc<ByteBudget>,
    ) -> Self
    where
        R: AsyncRead,
    {
        let line_lease = budget.empty_lease();
        Self {
            read: BufReader::new(read),
            line_buf: Vec::new(),
            line_lease,
            max_line_bytes,
            budget,
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
            pending_error_reply: false,
            ingress,
            termination,
            child: None,
            #[cfg(unix)]
            group: None,
            #[cfg(windows)]
            job: None,
            cleanup: CleanupState::default(),
        }
    }

    fn clear_line_buf(&mut self) {
        self.line_buf = Vec::new();
        self.line_lease.release();
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) {
        if let Some(group) = self.group {
            crate::platform::signal_process_group(group, signal);
        }
    }

    #[cfg(all(unix, test))]
    fn exited_unreaped(&mut self) -> std::io::Result<bool> {
        crate::platform::child_exited_unreaped(&mut self.group)
    }

    #[cfg(unix)]
    async fn wait_unreaped(&mut self) -> std::io::Result<bool> {
        crate::platform::wait_child_unreaped(&mut self.group, Duration::from_secs(1)).await
    }
}

impl<R, W> ChildTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn close_writer(&mut self) -> std::io::Result<()> {
        let mut guard = self.write.lock().await;
        if let Some(mut stdin) = guard.take() {
            let _ = stdin.shutdown().await;
        }
        Ok(())
    }

    // Cancellation-safe bounded line read: fill_buf is the sole await point.
    async fn read_line(&mut self) -> bool {
        loop {
            let (consumed, done, hard_error) = {
                let available = match self.read.fill_buf().await {
                    Ok(available) => available,
                    Err(error) => {
                        self.clear_line_buf();
                        self.termination.terminate(
                            FailureKind::Transport,
                            format!("MCP stdio read failed: {error}"),
                        );
                        return false;
                    }
                };
                if available.is_empty() {
                    self.clear_line_buf();
                    return false;
                }
                let newline = available.iter().position(|&b| b == b'\n');
                let take = newline.map_or(available.len(), |idx| idx + 1);
                let next_len = self.line_buf.len().saturating_add(take);
                if next_len > self.max_line_bytes {
                    (
                        take,
                        false,
                        Some(format!(
                            "MCP stdio response exceeds the {} limit",
                            format_bytes(self.max_line_bytes)
                        )),
                    )
                } else if !self.line_lease.reserve_vec(
                    &mut self.line_buf,
                    next_len,
                    self.max_line_bytes,
                ) {
                    (
                        take,
                        false,
                        Some(format!(
                            "MCP stdio response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        )),
                    )
                } else {
                    self.line_buf.extend_from_slice(&available[..take]);
                    (take, newline.is_some(), None)
                }
            };
            self.read.consume(consumed);
            if let Some(reason) = hard_error {
                self.clear_line_buf();
                self.termination.terminate(FailureKind::Transport, reason);
                return false;
            }
            if done {
                return true;
            }
        }
    }

    #[cfg(unix)]
    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.clear_line_buf();
        // A pending SDK send can hold the writer lock while the child stops
        // reading. Kill the still-owned process before waiting on that lock.
        if !matches!(
            timeout(Duration::from_millis(500), self.close_writer()).await,
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

    #[cfg(windows)]
    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.clear_line_buf();
        let _ = timeout(Duration::from_millis(500), self.close_writer()).await;
        if let Some(job) = &self.job {
            job.terminate()?;
        }
        // Retain the job and child if cancellation interrupts the join.
        timeout(Duration::from_secs(3), async {
            if let Some(child) = &mut self.child {
                child.wait().await?;
            }
            if let Some(job) = &self.job {
                while !job.is_empty()? {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            Ok::<_, std::io::Error>(())
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "MCP process job did not close",
            )
        })??;
        self.child = None;
        self.job = None;
        Ok(())
    }
}

impl<R, W> Drop for ChildTransport<R, W> {
    fn drop(&mut self) {
        self.clear_line_buf();
        #[cfg(unix)]
        self.signal_group(libc::SIGKILL);
        #[cfg(windows)]
        if let Some(job) = &self.job {
            let _ = job.terminate();
        }
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            if self.group.is_some() {
                let _ = child.start_kill();
            }
            #[cfg(windows)]
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

impl<R, W> Transport<RoleClient> for ChildTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: ClientJsonRpcMessage,
    ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
        let terminated = self.termination.is_terminated();
        if !terminated {
            self.ingress.track(&item);
        }
        let write = self.write.clone();
        let termination = self.termination.clone();
        let budget = self.budget.clone();
        let limit = self.max_line_bytes.saturating_sub(1);
        async move {
            if terminated || termination.is_terminated() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Transport is terminated",
                ));
            }
            let bytes = encode_mcp_message(&item, limit, &budget, &termination)?;
            drop(item);
            let mut guard = write.lock().await;
            if termination.is_terminated() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Transport is terminated",
                ));
            }
            let stdin = guard.as_mut().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "Transport is closed")
            })?;
            stdin.write_all(&bytes).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        }
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        loop {
            if self.pending_error_reply {
                let mut guard = self.write.lock().await;
                let writer = guard.as_mut()?;
                if writer.write_all(INVALID_REQUEST_REPLY).await.is_err()
                    || writer.flush().await.is_err()
                {
                    return None;
                }
                self.pending_error_reply = false;
            }
            if !self.read_line().await {
                return None;
            }
            let mut raw = Leased::new(std::mem::take(&mut self.line_buf), self.line_lease.take());
            if raw.last() == Some(&b'\n') {
                raw.value_mut().pop();
            }
            if raw.last() == Some(&b'\r') {
                raw.value_mut().pop();
            }
            if raw.starts_with(UTF8_BOM) {
                raw.value_mut().drain(..UTF8_BOM.len());
            }
            let outcome = if raw.is_empty() {
                Ok(None)
            } else {
                match self
                    .ingress
                    .observe_owned(raw, self.max_line_bytes, &self.budget, false)
                {
                    Err(IngressError::MemoryExhausted) => Err((
                        FailureKind::Transport,
                        format!(
                            "MCP stdio response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        ),
                    )),
                    Err(IngressError::InvalidMetadata) => Err((
                        FailureKind::Protocol,
                        "MCP response JSON cannot be represented losslessly".into(),
                    )),
                    Ok(ObserveOutcome::Captured(message)) => {
                        self.clear_line_buf();
                        return Some(ingress::into_sdk(message));
                    }
                    Ok(ObserveOutcome::InvalidMessage(error, ignore)) => {
                        Ok(Some(classify_stdio_error(error, ignore)))
                    }
                    Ok(ObserveOutcome::NotMatched) => Ok(Some(Ok(None))),
                }
            };
            self.clear_line_buf();
            match outcome {
                Err((kind, reason)) => {
                    self.termination.terminate(kind, reason);
                    return None;
                }
                Ok(None) | Ok(Some(Ok(None))) | Ok(Some(Err(StdioParseError::Ignore))) => continue,
                Ok(Some(Ok(Some(message)))) => return Some(message),
                Ok(Some(Err(StdioParseError::InvalidRequest))) => {
                    self.pending_error_reply = true;
                }
            }
        }
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

#[derive(Clone)]
struct BoundedHttp {
    client: reqwest::Client,
    auth_manager: Option<Arc<crate::auth::HttpAuthManager>>,
    cleanup: CleanupState,
    ingress: Ingress,
    event_schemas: EventSchemaCache,
    termination: Arc<TransportTermination>,
    max_response_bytes: usize,
    budget: Arc<ByteBudget>,
}
type HttpError = StreamableHttpError<reqwest::Error>;

impl BoundedHttp {
    fn classify_auth_failure(
        &self,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        record_transient: bool,
    ) -> HttpError {
        let info = crate::auth::AuthChallengeInfo::from_response(status, headers);
        let summary = info.sanitized_summary();
        let required_scope =
            (!info.required_scopes.is_empty()).then(|| info.required_scopes.join(" "));
        let kind = if status == http::StatusCode::FORBIDDEN {
            FailureKind::InsufficientScope
        } else {
            FailureKind::AuthRequired
        };
        if record_transient {
            self.termination.set_transient_failure(McpFailure {
                kind,
                message: info.format_message(None),
                outcome_unknown: false,
                rpc_error: None,
                auth: Some(info),
                stderr_tail: None,
                lease: None,
            });
        }
        if status == http::StatusCode::FORBIDDEN {
            HttpError::InsufficientScope(
                rmcp::transport::streamable_http_client::InsufficientScopeError::new(
                    summary,
                    required_scope,
                ),
            )
        } else {
            HttpError::AuthRequired(AuthRequiredError::new(summary))
        }
    }
    async fn bounded_body(
        &self,
        mut response: reqwest::Response,
        limit: usize,
    ) -> Result<(Vec<u8>, ByteLease), HttpError> {
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            let msg = format!("HTTP response exceeds the {} limit", format_bytes(limit));
            self.termination
                .set_transient(FailureKind::Transport, msg.clone());
            return Err(HttpError::ResponseBodyTooLarge { max_size: limit });
        }
        let mut lease = self.budget.empty_lease();
        let mut bytes = Vec::new();
        if let Some(length) = response.content_length()
            && !lease.reserve_vec(&mut bytes, length as usize, limit)
        {
            let msg = format!(
                "HTTP response exceeded the {} active memory limit",
                format_bytes(self.budget.limit())
            );
            self.termination
                .set_transient(FailureKind::Transport, msg.clone());
            return Err(HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                msg,
            )));
        }
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| HttpError::Client(error.without_url()))?
        {
            let next_len = bytes.len().saturating_add(chunk.len());
            if next_len > limit {
                drop(bytes);
                lease.release();
                let msg = format!("HTTP response exceeds the {} limit", format_bytes(limit));
                self.termination.set_transient(FailureKind::Transport, msg);
                return Err(HttpError::ResponseBodyTooLarge { max_size: limit });
            }
            if !lease.reserve_vec(&mut bytes, next_len, limit) {
                drop(bytes);
                lease.release();
                let msg = format!(
                    "HTTP response exceeded the {} active memory limit",
                    format_bytes(self.budget.limit())
                );
                self.termination
                    .set_transient(FailureKind::Transport, msg.clone());
                return Err(HttpError::Io(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    msg,
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        lease.shrink_to(bytes.capacity());
        Ok((bytes, lease))
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
        if self.termination.is_terminated() {
            return Err(HttpError::TransportChannelClosed);
        }
        self.ingress.track(&message);
        let mut passthrough_guard = match &message {
            ClientJsonRpcMessage::Request(request)
                if matches!(
                    request.request.method(),
                    "events/list" | "events/poll" | "events/stream"
                ) =>
            {
                Some(UntrackOnDrop::new(self.ingress.clone(), request.id.clone()))
            }
            _ => None,
        };
        let had_session = session_id.is_some();
        let notification = !matches!(&message, ClientJsonRpcMessage::Request(_));
        let is_events_stream = matches!(
            &message,
            ClientJsonRpcMessage::Request(request) if request.request.method() == "events/stream"
        );
        let event_headers = if let ClientJsonRpcMessage::Request(request) = &message
            && let ClientRequest::CustomRequest(custom) = &request.request
        {
            events::event_request_headers(
                &custom.method,
                custom.params.as_ref(),
                &self.event_schemas,
                &self.budget,
            )
        } else {
            Some(Leased::new(Vec::new(), self.budget.empty_lease()))
        }
        .ok_or_else(|| {
            let message = format!(
                "MCP event headers exceeded the {} active memory limit",
                format_bytes(self.budget.limit())
            );
            self.termination
                .terminate(FailureKind::Transport, message.clone());
            HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                message,
            ))
        })?;
        let discover_id = match &message {
            ClientJsonRpcMessage::Request(request)
                if !had_session && matches!(request.request, ClientRequest::DiscoverRequest(_)) =>
            {
                Some(request.id.clone())
            }
            _ => None,
        };
        let body = encode_mcp_message(
            &message,
            self.max_response_bytes,
            &self.budget,
            &self.termination,
        )
        .map_err(HttpError::Io)?;
        let body = bytes::Bytes::from_owner(OutgoingHttpBody {
            bytes: body,
            _message: message,
        });
        let (mut dynamic_token, attempted_refresh) = match (&self.auth_manager, &auth) {
            (Some(mgr), None) => mgr.prepare_token(&self.client).await,
            _ => (None, false),
        };
        let send_post = |bearer: Option<&str>| {
            let mut request = self
                .client
                .post(uri.as_ref())
                .header("accept", "application/json, text/event-stream");
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            for (name, value) in event_headers.iter() {
                request = request.header(name, value);
            }
            if let Some(session) = &session_id {
                request = request.header("Mcp-Session-Id", session.as_ref());
            }
            if let Some(token) = auth.as_deref().or(bearer) {
                request = request.bearer_auth(token);
            }
            let built = request
                .body(body.clone())
                .build()
                .map_err(|error| HttpError::Client(error.without_url()));
            let client = self.client.clone();
            async move {
                let mut req = built?;
                req.headers_mut()
                    .entry(http::header::CONTENT_TYPE)
                    .or_insert(HeaderValue::from_static("application/json"));
                client
                    .execute(req)
                    .await
                    .map_err(|error| HttpError::Client(error.without_url()))
            }
        };
        let mut response = send_post(dynamic_token.as_deref()).await?;
        if response.status() == http::StatusCode::UNAUTHORIZED
            && !attempted_refresh
            && auth.is_none()
            && let Some(mgr) = &self.auth_manager
            && let Some(refreshed) = mgr
                .refresh_after_401(&self.client, dynamic_token.as_deref())
                .await
        {
            dynamic_token = Some(refreshed);
            response = send_post(dynamic_token.as_deref()).await?;
        }
        drop(event_headers);
        drop(body);
        let status = response.status();
        if status == http::StatusCode::UNAUTHORIZED || status == http::StatusCode::FORBIDDEN {
            return Err(self.classify_auth_failure(status, response.headers(), true));
        }
        if status == http::StatusCode::NOT_FOUND && had_session {
            self.termination.terminate(
                FailureKind::Transport,
                "MCP HTTP session expired; run reset before opening a new connection".into(),
            );
            return Err(HttpError::SessionExpired);
        }
        if status == http::StatusCode::ACCEPTED || status == http::StatusCode::NO_CONTENT {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
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
            if let Some(guard) = passthrough_guard.as_mut() {
                guard.disarm();
            }
            return Ok(StreamableHttpPostResponse::Sse(
                sse::bounded_stream(
                    response,
                    limit,
                    self.budget.clone(),
                    self.ingress.clone(),
                    self.termination.clone(),
                    is_events_stream,
                ),
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
            let discover_request_id = discover_id.filter(|_| {
                status.is_client_error() && status != http::StatusCode::TOO_MANY_REQUESTS
            });
            if status == http::StatusCode::TOO_MANY_REQUESTS
                || (discover_request_id.is_none() && !is_json)
            {
                return Err(HttpError::HttpStatus(HttpStatusError::new(
                    status.as_u16(),
                    retry_after,
                )));
            }
            let (body, lease) = match self
                .bounded_body(response, limit.min(MAX_ERROR_BYTES))
                .await
            {
                Ok(pair) => pair,
                Err(_) if discover_request_id.is_some() => {
                    self.termination.clear_transient();
                    (Vec::new(), self.budget.empty_lease())
                }
                Err(_) => {
                    return Err(HttpError::HttpStatus(HttpStatusError::new(
                        status.as_u16(),
                        retry_after,
                    )));
                }
            };
            let raw = Leased::new(body, lease);
            let observed = if is_json && !raw.is_empty() {
                Some(self.observe_response(raw, limit, true)?)
            } else {
                drop(raw);
                None
            };
            if let Some(request_id) = discover_request_id {
                if let Some(ObserveOutcome::Captured(message)) = observed
                    && let ServerJsonRpcMessage::Error(ref error) = **message
                    && error.id.as_ref() == Some(&request_id)
                {
                    return Ok(StreamableHttpPostResponse::Json(
                        ingress::into_sdk(message),
                        None,
                    ));
                }
                return Ok(StreamableHttpPostResponse::Json(
                    ServerJsonRpcMessage::error(
                        ErrorData::invalid_request("server/discover is unavailable", None),
                        Some(request_id),
                    ),
                    None,
                ));
            }
            if let Some(ObserveOutcome::Captured(message)) = observed
                && matches!(**message, ServerJsonRpcMessage::Error(_))
            {
                return Ok(StreamableHttpPostResponse::Json(
                    ingress::into_sdk(message),
                    session,
                ));
            }
            return Err(HttpError::HttpStatus(HttpStatusError::new(
                status.as_u16(),
                retry_after,
            )));
        }
        let (body, lease) = self.bounded_body(response, limit).await?;
        let raw = Leased::new(body, lease);
        if notification && raw.is_empty() {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !is_json {
            return Err(HttpError::UnexpectedContentType(None));
        }
        match self.observe_response(raw, limit, false)? {
            ObserveOutcome::Captured(message) => Ok(StreamableHttpPostResponse::Json(
                ingress::into_sdk(message),
                session,
            )),
            ObserveOutcome::NotMatched => Ok(StreamableHttpPostResponse::Accepted),
            ObserveOutcome::InvalidMessage(..) if notification => {
                Ok(StreamableHttpPostResponse::Accepted)
            }
            ObserveOutcome::InvalidMessage(..) => Err(HttpError::UnexpectedServerResponse(
                Cow::Borrowed("Invalid JSON-RPC response"),
            )),
        }
    }

    fn observe_response(
        &self,
        raw: Leased<Vec<u8>>,
        limit: usize,
        error_only: bool,
    ) -> Result<ObserveOutcome, HttpError> {
        self.ingress
            .observe_owned(raw, limit, &self.budget, error_only)
            .map_err(|error| {
                let (kind, io_kind, message) = match error {
                    IngressError::MemoryExhausted => (
                        FailureKind::Transport,
                        std::io::ErrorKind::OutOfMemory,
                        format!(
                            "HTTP response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        ),
                    ),
                    IngressError::InvalidMetadata => (
                        FailureKind::Protocol,
                        std::io::ErrorKind::InvalidData,
                        "MCP response JSON cannot be represented losslessly".into(),
                    ),
                };
                self.termination.terminate(kind, message.clone());
                HttpError::Io(std::io::Error::new(io_kind, message))
            })
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
        self.post(
            uri,
            message,
            session,
            auth,
            headers,
            self.max_response_bytes,
        )
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
            limit.min(self.max_response_bytes),
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
            self.max_response_bytes,
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
        // Local SSE hard rejections terminate immediately without reconnecting via GET.
        if self.termination.is_terminated() {
            return Err(HttpError::ServerDoesNotSupportSse);
        }
        let had_session = session.is_some();
        let resumed = last_id.is_some();
        let (mut dynamic_token, attempted_refresh) = match (&self.auth_manager, &auth) {
            (Some(mgr), None) => mgr.prepare_token(&self.client).await,
            _ => (None, false),
        };
        let send_get = |bearer: Option<&str>| {
            let mut request = self
                .client
                .get(uri.as_ref())
                .header("accept", "text/event-stream");
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            if let Some(session) = &session {
                request = request.header("Mcp-Session-Id", session.as_ref());
            }
            if let Some(last_id) = &last_id {
                request = request.header("Last-Event-Id", last_id.as_str());
            }
            if let Some(token) = auth.as_deref().or(bearer) {
                request = request.bearer_auth(token);
            }
            async move {
                request
                    .send()
                    .await
                    .map_err(|error| HttpError::Client(error.without_url()))
            }
        };
        let mut response = send_get(dynamic_token.as_deref()).await?;
        if response.status() == http::StatusCode::UNAUTHORIZED
            && !attempted_refresh
            && auth.is_none()
            && let Some(mgr) = &self.auth_manager
            && let Some(refreshed) = mgr
                .refresh_after_401(&self.client, dynamic_token.as_deref())
                .await
        {
            dynamic_token = Some(refreshed);
            response = send_get(dynamic_token.as_deref()).await?;
        }
        let status = response.status();
        if status == http::StatusCode::METHOD_NOT_ALLOWED {
            return Err(HttpError::ServerDoesNotSupportSse);
        }
        if matches!(
            status,
            http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN
        ) {
            return Err(self.classify_auth_failure(status, response.headers(), resumed));
        }
        if status == http::StatusCode::NOT_FOUND && had_session {
            self.termination.terminate(
                FailureKind::Transport,
                "MCP HTTP session expired; run reset before opening a new connection".into(),
            );
            return Err(HttpError::SessionExpired);
        }
        if !status.is_success() {
            return Err(HttpError::HttpStatus(HttpStatusError::new(
                status.as_u16(),
                None,
            )));
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !has_media_type(content_type, "text/event-stream") {
            return Err(HttpError::UnexpectedContentType(None));
        }
        Ok(sse::bounded_stream(
            response,
            limit.min(self.max_response_bytes),
            self.budget.clone(),
            self.ingress.clone(),
            self.termination.clone(),
            false,
        ))
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        let deletion = async {
            let dynamic_token = match (&self.auth_manager, &auth) {
                (Some(mgr), None) => mgr.prepare_token(&self.client).await.0,
                _ => None,
            };
            let mut request = self.client.delete(uri.as_ref());
            for (name, value) in headers {
                request = request.header(name, value);
            }
            request = request.header("Mcp-Session-Id", session.as_ref());
            if let Some(token) = auth.or(dynamic_token) {
                request = request.bearer_auth(token);
            }
            request.send().await
        };
        match timeout(Duration::from_secs(2), deletion).await {
            Ok(Ok(response)) => {
                let status = response.status();
                if status.is_success()
                    || matches!(
                        status,
                        http::StatusCode::NOT_FOUND | http::StatusCode::METHOD_NOT_ALLOWED
                    )
                {
                    // Legacy sessions may already be gone or disallow explicit deletion.
                    Ok(())
                } else {
                    record_cleanup_failure(
                        &self.cleanup,
                        FailureKind::Transport,
                        "Local MCP client stopped; remote session deletion was not confirmed",
                        true,
                    );
                    Err(HttpError::HttpStatus(HttpStatusError::new(
                        status.as_u16(),
                        None,
                    )))
                }
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observing_exit_does_not_reap_leader_before_group_cleanup() {
        let mut command = Command::new("sh");
        command.args(["-c", "exit 0"]);
        let (mut transport, _) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            0,
            ByteBudget::new(MAX_RESPONSE_BYTES),
        )
        .unwrap();
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
        let (mut transport, _) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            0,
            ByteBudget::new(MAX_RESPONSE_BYTES),
        )
        .unwrap();
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
        let (mut transport, _) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            0,
            ByteBudget::new(MAX_RESPONSE_BYTES),
        )
        .unwrap();
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

#[cfg(test)]
mod call_budget_tests {
    use super::*;

    #[test]
    fn dispatch_checks_pre_dispatch_deadline_with_and_without_hard_cap() {
        let now = Instant::now();
        for hard in [None, Some(Duration::from_secs(10))] {
            let budgets = CallBudgets {
                pre_dispatch_deadline: now - Duration::from_millis(1),
                idle: Duration::from_secs(1),
                hard,
            };
            let error = budgets.request_options(now).unwrap_err();
            let failure = error.downcast_ref::<McpFailure>().unwrap();
            assert_eq!(failure.kind, FailureKind::Timeout);
            assert!(!failure.outcome_unknown);
        }

        let budgets = CallBudgets {
            pre_dispatch_deadline: now + Duration::from_secs(10),
            idle: Duration::from_secs(1),
            hard: Some(Duration::from_secs(1)),
        };
        let error = budgets
            .request_options(now - Duration::from_secs(2))
            .unwrap_err();
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        assert_eq!(failure.kind, FailureKind::Timeout);
        assert!(!failure.outcome_unknown);
    }

    #[test]
    fn dispatch_keeps_progress_idle_independent_of_admission_time() {
        let now = Instant::now();
        let budgets = CallBudgets {
            pre_dispatch_deadline: now + Duration::from_secs(10),
            idle: Duration::from_secs(60),
            hard: None,
        };
        let options = budgets.request_options(now).unwrap();
        assert_eq!(options.timeout, Some(Duration::from_secs(60)));
        assert!(options.reset_timeout_on_progress);
        assert_eq!(options.max_total_timeout, None);

        let options = CallBudgets {
            hard: Some(Duration::from_secs(30)),
            ..budgets
        }
        .request_options(now)
        .unwrap();
        assert!(options.max_total_timeout.unwrap() <= Duration::from_secs(30));
        assert_eq!(options.timeout, options.max_total_timeout);
        assert!(options.reset_timeout_on_progress);
    }
}

#[cfg(test)]
mod stdio_transport_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn complete_tool_responses_remain_successful_after_transport_eof() {
        use rmcp::model::{InitializeResult, ServerCapabilities, ServerPeerInfo};
        use serde_json::json;

        async fn request(server: &mut BufReader<tokio::io::DuplexStream>) -> Value {
            let mut line = String::new();
            assert!(server.read_line(&mut line).await.unwrap() > 0);
            serde_json::from_str(&line).unwrap()
        }

        async fn respond(
            server: &mut BufReader<tokio::io::DuplexStream>,
            request: &Value,
            result: &Value,
        ) {
            let mut bytes = serde_json::to_vec(&json!({
                "jsonrpc": "2.0", "id": request["id"], "result": result
            }))
            .unwrap();
            bytes.push(b'\n');
            server.get_mut().write_all(&bytes).await.unwrap();
        }

        for method in ["tools/list", "tools/call"] {
            let budget = ByteBudget::new(64 * 1024);
            let ingress = Ingress::default();
            let termination = Arc::new(TransportTermination::default());
            let router = EventRouter::new(budget.clone());
            let tools_changed = Arc::new(AtomicBool::new(false));
            let (client, server) = tokio::io::duplex(4096);
            let (read, write) = tokio::io::split(client);
            let mut capabilities = ServerCapabilities::default();
            capabilities.tools = Some(Default::default());
            let mut info = ServerPeerInfo::from(InitializeResult::new(capabilities));
            info.protocol_version = ProtocolVersion::V_2026_07_28;
            let service = rmcp::service::serve_directly(
                Handler {
                    tools_changed: tools_changed.clone(),
                },
                EventNotificationTransport::new(
                    ChildTransport::from_parts(
                        read,
                        write,
                        ingress.clone(),
                        termination.clone(),
                        4096,
                        budget.clone(),
                    ),
                    router.clone(),
                    termination.clone(),
                ),
                Some(info),
            );
            let mut connection = McpConnection {
                peer: service.peer().clone(),
                service: Some(service),
                ingress,
                event_router: router,
                event_schemas: EventSchemaCache::default(),
                termination: termination.clone(),
                stderr_capture: None,
                limits: McpLimits {
                    budget: budget.clone(),
                    ..McpLimits::default()
                },
                server: Leased::new(Vec::new(), budget.empty_lease()),
                closing: None,
                cleanup: CleanupState::default(),
                tools_changed,
                http: false,
            };
            let mut server = BufReader::new(server);
            let catalog = json!({
                "resultType": "complete",
                "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]
            });
            let mut operation = Box::pin(async {
                if method == "tools/list" {
                    connection
                        .list_tool_pages(Duration::from_secs(5))
                        .await
                        .map(|pages| Value::Array(pages.to_vec()))
                } else {
                    connection
                        .call("echo", Map::new(), Duration::from_secs(5))
                        .await
                        .map(|result| (*result).clone())
                }
            });
            let mut received = tokio::select! {
                result = &mut operation => panic!("request completed before its reply: {result:?}"),
                request = request(&mut server) => request,
            };
            assert_eq!(received["method"], "tools/list");
            if method == "tools/call" {
                respond(&mut server, &received, &catalog).await;
                received = tokio::select! {
                    result = &mut operation => panic!("call completed before its reply: {result:?}"),
                    request = request(&mut server) => request,
                };
                assert_eq!(received["method"], "tools/call");
            }
            let result = if method == "tools/list" {
                catalog
            } else {
                json!({"content": [{"type": "text", "text": "finished"}], "isError": false})
            };
            respond(&mut server, &received, &result).await;
            server.get_mut().shutdown().await.unwrap();

            // Leave the application future unpolled until the SDK has received EOF.
            timeout(Duration::from_secs(5), async {
                while !termination.is_closed() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!termination.is_terminated());
            let result = operation.await.unwrap();
            if method == "tools/list" {
                assert_eq!(result[0]["tools"][0]["name"], "echo");
            } else {
                assert_eq!(result["content"][0]["text"], "finished");
            }
            assert!(connection.is_closed());
            assert!(connection.events_client().is_closed());
            connection.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn fragmented_stdio_line_charges_capacity_until_release() {
        let budget = ByteBudget::new(256);
        let (client_io, mut server_io) = tokio::io::duplex(8);
        let (read, write) = tokio::io::split(client_io);
        let mut transport = ChildTransport::from_parts(
            read,
            write,
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            128,
            budget.clone(),
        );
        let sender = tokio::spawn(async move {
            server_io
                .write_all(b"abcdefghijklmnopqrstuvwxyz012345\n")
                .await
                .unwrap();
        });
        assert!(transport.read_line().await);
        sender.await.unwrap();
        assert_eq!(transport.line_lease.bytes(), transport.line_buf.capacity());
        assert_eq!(budget.used(), transport.line_buf.capacity());
        transport.clear_line_buf();
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn stdio_transport_shrinks_large_line_buf_and_enforces_exact_bounds_and_leases() {
        let budget = ByteBudget::new(512 * 1024);
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        let termination = Arc::new(TransportTermination::default());
        let (client_io, mut server_io) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(client_io);
        let mut transport = ChildTransport::from_parts(
            read,
            write,
            ingress.clone(),
            termination.clone(),
            200_000,
            budget.clone(),
        );

        let req: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "big", "arguments": {}}
        }))
        .unwrap();
        ingress.track(&req);

        let big_payload = "a".repeat(100_000);
        let mut frame = format!(
            "\u{feff}{{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{big_payload}\"}}],\"x\":null}}}}\r\n"
        )
        .into_bytes();
        let write_fut = tokio::spawn(async move {
            server_io.write_all(&frame).await.unwrap();
            server_io.shutdown().await.unwrap();
            frame.clear();
            server_io
        });

        let received = transport.receive().await.expect("valid response");
        let _server_io = write_fut.await.unwrap();
        assert!(matches!(received, ServerJsonRpcMessage::Response(_)));
        assert!(transport.line_buf.capacity() <= LINE_SHRINK_CAP);
        assert_eq!(transport.line_lease.bytes(), 0);
        let (_, captured, lease) = ingress.take().unwrap();
        let captured = captured.unwrap();
        assert_eq!(captured["result"].get("x"), Some(&Value::Null));
        drop(captured);
        drop(received);
        drop(lease);
        assert_eq!(budget.used(), 0);

        // Exact N-1, N pass and N+1 reject including LF / CRLF.
        for crlf in [false, true] {
            let line_ending = if crlf { "\r\n" } else { "\n" };
            let sample = |pad: usize| {
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"tools\":[],\"p\":\"{}\"}}}}{line_ending}",
                    "x".repeat(pad)
                )
            };
            let exact = sample(10);
            let limit = exact.len();
            let under = sample(9);
            assert_eq!(under.len(), limit - 1);
            let over = sample(11);
            assert_eq!(over.len(), limit + 1);

            for valid in [under, exact] {
                let term = Arc::new(TransportTermination::default());
                let ingress = Ingress::default();
                let _capture = ingress.capture().unwrap();
                let request: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/list"
                }))
                .unwrap();
                ingress.track(&request);
                let (c, mut s) = tokio::io::duplex(4096);
                let (r, w) = tokio::io::split(c);
                let mut t = ChildTransport::from_parts(
                    r,
                    w,
                    ingress,
                    term.clone(),
                    limit,
                    ByteBudget::new(4096),
                );
                s.write_all(valid.as_bytes()).await.unwrap();
                // EOF also makes a broken correlation fail promptly instead of hanging CI.
                s.shutdown().await.unwrap();
                assert!(t.receive().await.is_some());
                assert!(!term.is_terminated());
            }

            let term = Arc::new(TransportTermination::default());
            let small_budget = ByteBudget::new(4096);
            let (c, mut s) = tokio::io::duplex(4096);
            let (r, w) = tokio::io::split(c);
            let mut t = ChildTransport::from_parts(
                r,
                w,
                Ingress::default(),
                term.clone(),
                limit,
                small_budget.clone(),
            );
            s.write_all(over.as_bytes()).await.unwrap();
            assert!(t.receive().await.is_none());
            assert!(term.is_terminated());
            let err = term.failure(true);
            assert!(err.to_string().contains("exceeds the"));
            assert!(err.to_string().contains("outcome unknown"));
            assert_eq!(small_budget.used(), 0);
        }

        // Non-blocking memory lease failure terminates and releases partial bytes.
        let tight = ByteBudget::new(32);
        let term = Arc::new(TransportTermination::default());
        let (c, mut s) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(c);
        let mut t =
            ChildTransport::from_parts(r, w, Ingress::default(), term.clone(), 4096, tight.clone());
        s.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\n")
            .await
            .unwrap();
        assert!(t.receive().await.is_none());
        assert!(term.is_terminated());
        assert!(
            term.failure(false)
                .to_string()
                .contains("active memory limit")
        );
        assert_eq!(tight.used(), 0);
        // Sending after termination refuses to write to dead stdin.
        let send_req: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }))
        .unwrap();
        assert!(t.send(send_req).await.is_err());

        // Half-line cancellation safety: dropping receive() mid-line retains buffered bytes and lease.
        let half_budget = ByteBudget::new(4096);
        let half_ingress = Ingress::default();
        let _half_cap = half_ingress.capture().unwrap();
        let half_term = Arc::new(TransportTermination::default());
        let (c, mut s) = tokio::io::duplex(4096);
        let (r, w) = tokio::io::split(c);
        let mut ht = ChildTransport::from_parts(
            r,
            w,
            half_ingress.clone(),
            half_term.clone(),
            4096,
            half_budget.clone(),
        );
        let req99: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/list"
        }))
        .unwrap();
        ht.send(req99).await.unwrap();
        let mut echoed = Vec::new();
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf).await.unwrap();
        echoed.extend_from_slice(&buf[..n]);
        assert!(echoed.ends_with(b"\n"));

        let first_half = b"{\"jsonrpc\":\"2.0\",\"id\":99,";
        s.write_all(first_half).await.unwrap();
        s.flush().await.unwrap();
        assert!(
            timeout(Duration::from_millis(30), ht.receive())
                .await
                .is_err()
        );
        assert_eq!(half_budget.used(), first_half.len());
        assert!(!half_term.is_terminated());

        let second_half = b"\"result\":{\"tools\":[],\"tag\":\"intact\"}}\n";
        s.write_all(second_half).await.unwrap();
        s.shutdown().await.unwrap();
        let full_msg = ht.receive().await.expect("completed line after cancel");
        assert!(matches!(full_msg, ServerJsonRpcMessage::Response(_)));
        assert_eq!(ht.line_lease.bytes(), 0);
        let (_, val, handed) = half_ingress.take().unwrap();
        assert_eq!(val.unwrap()["result"]["tag"], "intact");
        drop(handed);
        // The local frame and lossless capture are released. Ownership after
        // returning the typed message to the official SDK is outside this budget.
        assert_eq!(half_budget.used(), 0);
        drop(full_msg);
        assert_eq!(half_budget.used(), 0);
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn outgoing_body_capacity_follows_bytes_clones() {
        let message: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":"sample", "arguments":{"text":"ordinary\ntext"}}
        }))
        .unwrap();
        let length = crate::json::serialized_len(&message).unwrap();
        let budget = ByteBudget::new(length * 4);
        let termination = TransportTermination::default();
        let encoded = encode_mcp_message(&message, length, &budget, &termination).unwrap();
        let charged = encoded.capacity();
        assert_eq!(budget.used(), charged);
        let bytes = bytes::Bytes::from_owner(encoded);
        let other = bytes.clone();
        drop(bytes);
        assert_eq!(budget.used(), charged);
        assert_eq!(
            serde_json::from_slice::<Value>(&other).unwrap()["params"]["arguments"]["text"],
            "ordinary\ntext"
        );
        drop(other);
        assert_eq!(budget.used(), 0);
        let error = encode_mcp_message(&message, length - 1, &budget, &termination).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(termination.is_terminated());
        assert_eq!(budget.used(), 0);
        let budget = ByteBudget::new(1);
        let termination = TransportTermination::default();
        assert_eq!(
            encode_mcp_message(&message, length, &budget, &termination)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::OutOfMemory
        );
        assert!(termination.is_terminated());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn media_type_essence_is_exact_and_case_insensitive() {
        assert!(has_media_type(
            "Application/JSON; charset=UTF-8",
            "application/json"
        ));
        assert!(has_media_type(
            " TEXT/EVENT-STREAM ; charset=UTF-8",
            "text/event-stream"
        ));
        for value in [
            "application/jsonp",
            "application/json, text/plain",
            "",
            "application/jsonx",
        ] {
            assert!(!has_media_type(value, "application/json"));
        }
        assert!(!has_media_type(
            "text/event-stream-INVALID",
            "text/event-stream"
        ));
    }

    async fn read_framed_http(
        sock: &mut tokio::net::TcpStream,
        buf: &mut Vec<u8>,
    ) -> Option<(String, Vec<u8>)> {
        let mut scratch = [0u8; 2048];
        let header_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = sock.read(&mut scratch).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&scratch[..n]);
        };
        let header_str = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = header_str.split("\r\n");
        lines.next();
        let mut content_len = 0usize;
        for line in lines {
            if let Some((name, val)) = line.split_once(':')
                && name.trim().eq_ignore_ascii_case("content-length")
            {
                content_len = val.trim().parse().unwrap_or(0);
            }
        }
        while buf.len() < header_end + content_len {
            let n = sock.read(&mut scratch).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&scratch[..n]);
        }
        let body = buf[header_end..header_end + content_len].to_vec();
        buf.drain(..header_end + content_len);
        Some((header_str, body))
    }

    #[tokio::test]
    async fn event_http_headers_keep_their_lease_during_send() {
        let schema = serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": {
                "repo": { "type": "string", "x-mcp-header": "Repo" }
            }
        }))
        .unwrap();
        let event_schemas = Arc::new(std::sync::RwLock::new(Some(Leased::new(
            HashMap::from([("task.exited".into(), schema)]),
            ByteBudget::new(0).empty_lease(),
        ))));
        let message: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events/poll",
            "params": {"name": "task.exited", "arguments": {"repo": " leading"}}
        }))
        .unwrap();
        let mut http = BoundedHttp {
            client: reqwest::Client::new(),
            auth_manager: None,
            cleanup: CleanupState::default(),
            ingress: Ingress::default(),
            event_schemas,
            termination: Arc::new(TransportTermination::default()),
            max_response_bytes: 4096,
            budget: ByteBudget::new(16 * 1024),
        };
        let encoded = encode_mcp_message(&message, 4096, &http.budget, &http.termination).unwrap();
        let body_charge = encoded.capacity();
        drop(encoded);

        for completion in ["accepted", "closed", "cancelled"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (received, observed) = tokio::sync::oneshot::channel();
            let (finish, wait) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (headers, _) = read_framed_http(&mut socket, &mut Vec::new())
                    .await
                    .unwrap();
                received.send(headers).unwrap();
                if wait.await.unwrap_or(false) {
                    socket
                        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                }
            });
            let sender = http.clone();
            let request = message.clone();
            let sent = tokio::spawn(async move {
                sender
                    .post(
                        format!("http://{address}/mcp").into(),
                        request,
                        None,
                        None,
                        HashMap::new(),
                        4096,
                    )
                    .await
            });
            let headers = timeout(Duration::from_secs(5), observed)
                .await
                .unwrap()
                .unwrap();
            assert!(headers.contains("mcp-name: task.exited"));
            assert!(headers.contains("mcp-param-repo: =?base64?IGxlYWRpbmc=?="));
            assert!(
                http.budget.used() > body_charge,
                "event headers must remain charged while POST waits for its response"
            );
            if completion == "cancelled" {
                sent.abort();
                assert!(sent.await.unwrap_err().is_cancelled());
                finish.send(false).unwrap();
            } else {
                finish.send(completion == "accepted").unwrap();
                let result = sent.await.unwrap();
                if completion == "accepted" {
                    assert!(matches!(result, Ok(StreamableHttpPostResponse::Accepted)));
                } else {
                    assert!(result.is_err());
                }
            }
            server.await.unwrap();
            assert_eq!(http.budget.used(), 0);
        }

        http.budget = ByteBudget::new(1);
        let error = http
            .post(
                "http://127.0.0.1:1/mcp".into(),
                message,
                None,
                None,
                HashMap::new(),
                4096,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HttpError::Io(ref error) if error.kind() == std::io::ErrorKind::OutOfMemory
        ));
        assert!(error.to_string().contains("event headers"));
        assert!(http.termination.is_terminated());
        assert_eq!(
            http.termination
                .failure(false)
                .downcast_ref::<McpFailure>()
                .unwrap()
                .kind,
            FailureKind::Transport
        );
        assert_eq!(http.budget.used(), 0);
    }

    #[tokio::test]
    async fn chunked_http_body_charges_capacity_until_release() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_framed_http(&mut socket, &mut Vec::new())
                .await
                .unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n8\r\nabcdefgh\r\n",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            socket
                .write_all(b"9\r\nijklmnopq\r\n0\r\n\r\n")
                .await
                .unwrap();
        });
        let budget = ByteBudget::new(128);
        let client = reqwest::Client::new();
        let http = BoundedHttp {
            client: client.clone(),
            auth_manager: None,
            cleanup: CleanupState::default(),
            ingress: Ingress::default(),
            event_schemas: EventSchemaCache::default(),
            termination: Arc::new(TransportTermination::default()),
            max_response_bytes: 64,
            budget: budget.clone(),
        };
        let response = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        let (body, lease) = http.bounded_body(response, 64).await.unwrap();
        assert_eq!(body, b"abcdefghijklmnopq");
        assert_eq!(budget.used(), body.capacity());
        assert_eq!(lease.bytes(), body.capacity());
        drop(body);
        drop(lease);
        assert_eq!(budget.used(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn discover_oversize_error_body_clears_transient_before_initialize() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    while let Some((req_line, body)) =
                        read_framed_http(&mut sock, &mut pending).await
                    {
                        if req_line.starts_with("GET ") {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                            continue;
                        }
                        let body_str = String::from_utf8_lossy(&body);
                        if body_str.contains("\"server/discover\"") {
                            let big_err = "e".repeat(MAX_ERROR_BYTES + 4096);
                            let resp = format!(
                                "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                                big_err.len(),
                                big_err
                            );
                            let _ = sock.write_all(resp.as_bytes()).await;
                        } else if body_str.contains("\"initialize\"") {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                        } else {
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                        }
                    }
                });
            }
        });

        let cfg = ServerConfig::Http {
            url: format!("http://127.0.0.1:{port}/mcp"),
            headers: BTreeMap::new(),
            description: None,
            enabled: true,
            timeout: None,
            oauth: None,
        };
        let err = McpConnection::connect(&cfg, Duration::from_secs(5))
            .await
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            !msg.contains("65536") && !msg.contains("exceeds the"),
            "stale discover transient leaked into initialize failure: {msg}"
        );
        assert_eq!(
            err.downcast_ref::<McpFailure>().unwrap().kind,
            FailureKind::AuthRequired
        );
        assert!(msg.contains("401"), "unexpected: {msg}");
        srv.abort();
    }

    #[tokio::test]
    async fn http_events_list_poll_header_retry_and_stream_terminated_cancel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_get_reconnect = Arc::new(AtomicBool::new(false));
        let saw_cancelled = Arc::new(AtomicBool::new(false));
        let poll_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let list_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let saw_get_srv = saw_get_reconnect.clone();
        let saw_cancel_srv = saw_cancelled.clone();
        let poll_attempts_srv = poll_attempts.clone();
        let list_calls_srv = list_calls.clone();

        let srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let saw_get_srv = saw_get_srv.clone();
                let saw_cancel_srv = saw_cancel_srv.clone();
                let poll_attempts_srv = poll_attempts_srv.clone();
                let list_calls_srv = list_calls_srv.clone();
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    while let Some((header_str, body)) =
                        read_framed_http(&mut sock, &mut pending).await
                    {
                        if header_str.starts_with("GET ") {
                            if header_str.to_ascii_lowercase().contains("last-event-id:") {
                                saw_get_srv.store(true, Ordering::Release);
                            }
                            let _ = sock
                                .write_all(
                                    b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n",
                                )
                                .await;
                            continue;
                        }
                        let msg: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
                        let id = msg.get("id").cloned().unwrap_or(Value::Null);
                        let lower_headers = header_str.to_ascii_lowercase();

                        let send_json = |status: &str, payload: Value| {
                            let encoded = serde_json::to_string(&payload).unwrap();
                            format!(
                                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                encoded.len(),
                                encoded
                            )
                        };

                        match method {
                            "server/discover" => {
                                let resp = send_json(
                                    "200 OK",
                                    serde_json::json!({
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
                                                    "name": "events-demo",
                                                    "version": "1.0"
                                                }
                                            }
                                        }
                                    }),
                                );
                                let _ = sock.write_all(resp.as_bytes()).await;
                            }
                            "events/list" => {
                                let call_num = list_calls_srv.fetch_add(1, Ordering::AcqRel);
                                let cursor = msg
                                    .get("params")
                                    .and_then(|p| p.get("cursor"))
                                    .and_then(Value::as_str);
                                let payload = if cursor.is_none() && call_num == 0 {
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "events": [
                                                {
                                                    "name": "bad.event",
                                                    "inputSchema": {
                                                        "type": "object",
                                                        "properties": {
                                                            "x": { "type": "number", "x-mcp-header": "InvalidNumber" }
                                                        }
                                                    }
                                                }
                                            ],
                                            "nextCursor": "page-2"
                                        }
                                    })
                                } else {
                                    serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "resultType": "complete",
                                            "events": [
                                                {
                                                    "name": "pr.merged",
                                                    "inputSchema": {
                                                        "type": "object",
                                                        "properties": {
                                                            "repo": { "type": "string", "x-mcp-header": "Repo" }
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    })
                                };
                                let resp = send_json("200 OK", payload);
                                let _ = sock.write_all(resp.as_bytes()).await;
                            }
                            "events/poll" => {
                                let attempt = poll_attempts_srv.fetch_add(1, Ordering::AcqRel);
                                let has_name = lower_headers.contains("mcp-name: pr.merged");
                                let has_param = lower_headers.contains("mcp-param-repo: org/app");
                                if attempt == 0 {
                                    // Simulate stale schema header mismatch (-32020) on first attempt.
                                    let resp = send_json(
                                        "400 Bad Request",
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "error": { "code": -32020, "message": "header mismatch" }
                                        }),
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                } else if has_name && has_param {
                                    let resp = send_json(
                                        "200 OK",
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "result": {
                                                "resultType": "complete",
                                                "events": [{"eventId": "ev-1", "data": {"ok": true}}],
                                                "cursor": "1:1",
                                                "truncated": false,
                                                "hasMore": false,
                                                "nextPollMs": 2000
                                            }
                                        }),
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                } else {
                                    let resp = send_json(
                                        "400 Bad Request",
                                        serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "id": id,
                                            "error": { "code": -32020, "message": "missing headers" }
                                        }),
                                    );
                                    let _ = sock.write_all(resp.as_bytes()).await;
                                }
                            }
                            "events/stream" => {
                                assert!(lower_headers.contains("mcp-name: pr.merged"));
                                assert!(lower_headers.contains("mcp-param-repo: org/app"));
                                let active = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/events/active",
                                    "params": {
                                        "cursor": "1:1",
                                        "truncated": false,
                                        "heartbeatMs": 30000,
                                        "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                                    }
                                });
                                let terminated = serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/events/terminated",
                                    "params": {
                                        "error": { "code": -32025, "message": "done" },
                                        "_meta": { "io.modelcontextprotocol/subscriptionId": id }
                                    }
                                });
                                let sse_body = format!(
                                    "id: sse-1\ndata: {active}\n\nid: sse-2\ndata: {terminated}\n\n"
                                );
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n{}",
                                    sse_body.len(),
                                    sse_body
                                );
                                let _ = sock.write_all(resp.as_bytes()).await;
                            }
                            "notifications/cancelled" => {
                                saw_cancel_srv.store(true, Ordering::Release);
                                let _ = sock
                                    .write_all(
                                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n",
                                    )
                                    .await;
                            }
                            _ => {
                                let _ = sock
                                    .write_all(
                                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n",
                                    )
                                    .await;
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
            enabled: true,
            timeout: None,
            oauth: None,
        };
        let mut conn = McpConnection::connect(&cfg, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(conn.supports_events());

        let events = conn.list_events(Duration::from_secs(5)).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["name"], "pr.merged");
        drop(events);

        let mut args = Map::new();
        args.insert("repo".into(), Value::String("org/app".into()));
        let poll = conn
            .poll_events(
                PollEventsParams {
                    name: "pr.merged".into(),
                    arguments: Some(args.clone()),
                    cursor: None,
                    max_age_ms: None,
                    max_events: Some(10),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(poll["cursor"], "1:1");
        assert_eq!(poll_attempts.load(Ordering::Acquire), 2);
        drop(poll);

        let (handle, mut rx) = conn
            .open_event_stream(
                StreamEventsParams {
                    name: "pr.merged".into(),
                    arguments: Some(args),
                    cursor: Some("1:1".into()),
                    max_age_ms: None,
                    heartbeat_ms: Some(30_000),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let first = rx.recv().await.unwrap();
        assert_eq!(first.method, "notifications/events/active");
        let second = rx.recv().await.unwrap();
        assert_eq!(second.method, "notifications/events/terminated");
        drop(first);
        drop(second);

        // Verify RequestHandle was automatically cancelled upon notifications/events/terminated.
        timeout(Duration::from_secs(2), async {
            while !handle.is_cancelled().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!saw_get_reconnect.load(Ordering::Acquire));
        let _ = saw_cancelled.load(Ordering::Acquire);

        handle.cancel().await.unwrap();
        conn.close().await.unwrap();
        srv.abort();
    }
}

#[cfg(test)]
mod rpc_fidelity_tests {
    use super::*;

    #[tokio::test]
    async fn default_request_handler_drops_payload_before_context_and_bounds_errors() {
        use rmcp::model::GetExtensions;
        #[derive(Clone)]
        struct Payload(Arc<AtomicBool>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                assert!(self.0.load(Ordering::Acquire));
            }
        }
        struct Idle;
        impl Transport<RoleClient> for Idle {
            type Error = std::io::Error;
            fn send(
                &mut self,
                _: ClientJsonRpcMessage,
            ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
                std::future::ready(Ok(()))
            }
            async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
                std::future::pending().await
            }
            async fn close(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let handler = Handler {
            tools_changed: Arc::new(AtomicBool::new(false)),
        };
        let mut service = rmcp::service::serve_directly(handler.clone(), Idle, None);
        for method in ["ping".to_owned(), "local/".to_owned() + &"x".repeat(4096)] {
            let dropped = Arc::new(AtomicBool::new(false));
            let mut request: ServerRequest = serde_json::from_value(
                serde_json::json!({"method":method,"params":{"note":"ordinary text"}}),
            )
            .unwrap();
            request.extensions_mut().insert(Payload(dropped.clone()));
            let mut context =
                RequestContext::new(rmcp::model::RequestId::Number(7), service.peer().clone());
            context.extensions.insert(Arc::new(Guard(dropped.clone())));
            let result = handler.handle_request(request, context).await;
            assert!(dropped.load(Ordering::Acquire));
            if method == "ping" {
                assert!(result.is_ok());
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code, ErrorCode::METHOD_NOT_FOUND);
                assert_eq!(error.message, "Unsupported server request");
                assert!(error.data.is_none());
            }
        }
        service.close().await.unwrap();
    }

    #[test]
    fn rpc_error_fidelity_and_non_lossy_rejection() {
        let budget = ByteBudget::new(4096);
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        assert!(ingress.capture().is_none());
        let request = serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0","id":17,"method":"tools/list"
        }))
        .unwrap();
        ingress.track(&request);
        let ok_err = br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":null,"x-extra":7}}"#;
        let _ = ingress.observe_bytes(ok_err, 4096, &budget, false).unwrap();
        let error = rpc_failure(-32603, "diagnostic".into(), true, &ingress);
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        let raw = failure.rpc_error.as_ref().unwrap();
        assert_eq!(raw.get("data"), Some(&Value::Null));
        assert_eq!(raw["x-extra"], 7);
        assert!(failure.outcome_unknown);
        assert_eq!(budget.used(), ok_err.len());
        let cloned = failure.clone();
        assert!(Arc::ptr_eq(
            failure.rpc_error.as_ref().unwrap(),
            cloned.rpc_error.as_ref().unwrap()
        ));
        drop(error);
        assert_eq!(budget.used(), ok_err.len());
        let ipc_failure = crate::daemon::mcp_error(cloned.into(), true);
        assert_eq!(budget.used(), ok_err.len());
        let copied_failure = ipc_failure.clone();
        drop(ipc_failure);
        assert_eq!(budget.used(), ok_err.len());
        drop(copied_failure);
        assert_eq!(budget.used(), 0);

        ingress.track(&request);
        let bad_err = br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":{"x":1,"x":2}}}"#;
        assert!(matches!(
            ingress.observe_bytes(bad_err, 4096, &budget, false),
            Err(IngressError::InvalidMetadata)
        ));
        assert!(ingress.take().is_none());
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
mod event_lifecycle_tests {
    use super::*;
    use rmcp::model::{RequestId, ServerCapabilities, ServerPeerInfo};
    use serde_json::json;
    use tokio::sync::mpsc;

    struct Wire {
        sent: mpsc::Sender<ClientJsonRpcMessage>,
        incoming: mpsc::Receiver<ServerJsonRpcMessage>,
        ingress: Ingress,
    }

    impl Transport<RoleClient> for Wire {
        type Error = std::io::Error;

        fn send(
            &mut self,
            message: ClientJsonRpcMessage,
        ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
            self.ingress.track(&message);
            let sent = self.sent.clone();
            async move { sent.send(message).await.map_err(std::io::Error::other) }
        }

        async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
            let message = self.incoming.recv().await?;
            match &message {
                ServerJsonRpcMessage::Response(response) => self.ingress.untrack(&response.id),
                ServerJsonRpcMessage::Error(error) => {
                    if let Some(id) = &error.id {
                        self.ingress.untrack(id);
                    }
                }
                _ => {}
            }
            Some(message)
        }

        async fn close(&mut self) -> std::io::Result<()> {
            self.incoming.close();
            Ok(())
        }
    }

    struct Fixture {
        client: McpEventsClient,
        service: RunningService<RoleClient, Handler>,
        sent: mpsc::Receiver<ClientJsonRpcMessage>,
        incoming: mpsc::Sender<ServerJsonRpcMessage>,
    }

    impl Fixture {
        fn new(budget: Arc<ByteBudget>) -> Self {
            let (sent_tx, sent) = mpsc::channel(16);
            let (incoming, incoming_rx) = mpsc::channel(16);
            let ingress = Ingress::default();
            let router = EventRouter::new(budget.clone());
            let termination = Arc::new(TransportTermination::default());
            let mut extensions = ExtensionCapabilities::new();
            extensions.insert(EVENTS_EXTENSION_ID.into(), Map::new());
            let mut capabilities = ServerCapabilities::default();
            capabilities.extensions = Some(extensions);
            let mut info = ServerPeerInfo::from(rmcp::model::InitializeResult::new(capabilities));
            info.protocol_version = ProtocolVersion::V_2026_07_28;
            let service = rmcp::service::serve_directly(
                Handler {
                    tools_changed: Arc::new(AtomicBool::new(false)),
                },
                EventNotificationTransport::new(
                    Wire {
                        sent: sent_tx,
                        incoming: incoming_rx,
                        ingress: ingress.clone(),
                    },
                    router.clone(),
                    termination.clone(),
                ),
                Some(info),
            );
            let client = McpEventsClient {
                peer: service.peer().clone(),
                service_cancel: Arc::new(Mutex::new(None)),
                ingress,
                event_router: router,
                event_schemas: EventSchemaCache::default(),
                termination,
                stderr_capture: None,
                limits: McpLimits {
                    budget,
                    ..McpLimits::default()
                },
                http: true,
            };
            Self {
                client,
                service,
                sent,
                incoming,
            }
        }

        async fn request(&mut self, method: &str) -> RequestId {
            let message = timeout(Duration::from_secs(2), self.sent.recv())
                .await
                .unwrap()
                .unwrap();
            let ClientJsonRpcMessage::Request(request) = message else {
                panic!("expected request: {message:?}")
            };
            assert_eq!(request.request.method(), method);
            request.id
        }

        async fn respond(&self, message: Value) {
            self.incoming
                .send(serde_json::from_value(message).unwrap())
                .await
                .unwrap();
        }
    }

    fn stream_params() -> StreamEventsParams {
        StreamEventsParams {
            name: "sample.changed".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            heartbeat_ms: Some(30_000),
        }
    }

    fn poll_params() -> PollEventsParams {
        PollEventsParams {
            name: "sample.changed".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            max_events: Some(1),
        }
    }

    #[tokio::test]
    async fn dropping_stream_handshake_cancels_the_upstream_request() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let client = fixture.client.clone();
        let mut opening =
            Box::pin(client.open_event_stream(stream_params(), Duration::from_secs(5)));
        let id = tokio::select! {
            biased;
            _ = &mut opening => panic!("stream started without a server response"),
            id = fixture.request("events/stream") => id,
        };
        drop(opening);
        let cancelled = timeout(Duration::from_secs(2), fixture.sent.recv())
            .await
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_value(cancelled).unwrap();
        assert_eq!(encoded["method"], "notifications/cancelled");
        assert_eq!(
            encoded["params"]["requestId"],
            serde_json::to_value(&id).unwrap()
        );
        let late_response =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":{}})).unwrap();
        assert!(matches!(
            fixture
                .client
                .ingress
                .observe_bytes(&late_response, 4096, &budget, false)
                .unwrap(),
            ObserveOutcome::NotMatched
        ));
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn final_stream_response_drains_queued_notifications_and_closes_only_that_stream() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let client = fixture.client.clone();
        let opening = tokio::spawn(async move {
            client
                .open_event_stream(stream_params(), Duration::from_secs(5))
                .await
        });
        let id = fixture.request("events/stream").await;
        for (method, fields) in [
            ("active", json!({"heartbeatMs":30000,"cursor":null})),
            (
                "event",
                json!({"eventId":"one","cursor":"1","data":{"value":1}}),
            ),
            (
                "event",
                json!({"eventId":"two","cursor":"2","data":{"value":2}}),
            ),
        ] {
            let mut params = fields.as_object().unwrap().clone();
            params.insert(
                "_meta".into(),
                json!({"io.modelcontextprotocol/subscriptionId":id}),
            );
            fixture.respond(json!({"jsonrpc":"2.0","method":format!("notifications/events/{method}"),"params":params})).await;
        }
        fixture
            .respond(json!({"jsonrpc":"2.0","id":id,"result":{"resultType":"complete"}}))
            .await;
        let (handle, mut receiver) = opening.await.unwrap().unwrap();
        for method in ["active", "event", "event"] {
            let notification = timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                notification.method,
                format!("notifications/events/{method}")
            );
        }
        assert!(
            timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .is_none()
        );
        assert!(handle.is_cancelled().await);
        assert!(!fixture.client.is_closed());
        drop(handle);
        drop(receiver);
        assert_eq!(budget.used(), 0);

        let client = fixture.client.clone();
        let poll = tokio::spawn(async move {
            client
                .poll_events(poll_params(), Duration::from_secs(5))
                .await
        });
        let id = fixture.request("events/poll").await;
        fixture.respond(json!({"jsonrpc":"2.0","id":id,"result":{"resultType":"complete","events":[],"cursor":null}})).await;
        drop(poll.await.unwrap().unwrap());
        assert!(fixture.sent.try_recv().is_err());
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_rpc_errors_preserve_code_data_and_tool_capture() {
        let budget = ByteBudget::new(16 * 1024);
        let mut fixture = Fixture::new(budget.clone());
        let ingress = fixture.client.ingress.clone();
        let _capture = ingress.capture().unwrap();
        let tool_request =
            serde_json::from_value(json!({"jsonrpc":"2.0","id":900,"method":"tools/list"}))
                .unwrap();
        ingress.track(&tool_request);
        let tool_error =
            br#"{"jsonrpc":"2.0","id":900,"error":{"code":-32603,"message":"tool error"}}"#;
        let _ = ingress
            .observe_bytes(tool_error, 4096, &budget, false)
            .unwrap();
        let captured_bytes = budget.used();
        for (method, code) in [
            ("events/list", -32602),
            ("events/poll", -32023),
            ("events/stream", -32026),
        ] {
            let client = fixture.client.clone();
            let operation = tokio::spawn(async move {
                match method {
                    "events/list" => client.list_events(Duration::from_secs(5)).await.map(|_| ()),
                    "events/poll" => client
                        .poll_events(poll_params(), Duration::from_secs(5))
                        .await
                        .map(|_| ()),
                    _ => client
                        .open_event_stream(stream_params(), Duration::from_secs(5))
                        .await
                        .map(|_| ()),
                }
            });
            let id = fixture.request(method).await;
            let expected = json!({"code":code,"message":"subscription unavailable","data":{"modes":["poll"],"detail":null}});
            fixture
                .respond(json!({"jsonrpc":"2.0","id":id,"error":expected}))
                .await;
            let error = operation.await.unwrap().unwrap_err();
            let failure = error.downcast_ref::<McpFailure>().unwrap();
            assert_eq!(failure.rpc_error.as_deref(), Some(&expected));
            assert_eq!(
                budget.used(),
                captured_bytes
                    + crate::json::serialized_len(&expected).unwrap()
                    + failure.message.capacity()
            );
            let retained = failure.clone();
            drop(error);
            assert!(budget.used() > captured_bytes);
            drop(retained);
            assert_eq!(budget.used(), captured_bytes);
        }
        let (_, captured, lease) = ingress
            .take()
            .expect("Events must leave the ordinary capture untouched");
        assert_eq!(captured.unwrap()["error"]["message"], "tool error");
        drop(lease);
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_error_moves_data_and_accounts_for_retained_diagnostic() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let rpc_message = "m".repeat(512);
        let message_ptr = rpc_message.as_ptr();
        let data = json!({"items":[{"details":{"note":"ordinary nested value","attempts":[1,2]}}]});
        let data_ptr = data["items"][0]["details"]["note"]
            .as_str()
            .unwrap()
            .as_ptr();
        let rpc = ErrorData::new(ErrorCode(-32026), rpc_message, Some(data));
        let rpc_bytes = crate::json::serialized_len(&rpc).unwrap();
        let error = fixture.client.service_failure(ServiceError::McpError(rpc));
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        let value = failure.rpc_error.as_ref().unwrap();
        assert_eq!(value["code"], -32026);
        assert_eq!(value["message"].as_str().unwrap().as_ptr(), message_ptr);
        assert_eq!(
            value["data"]["items"][0]["details"]["note"]
                .as_str()
                .unwrap()
                .as_ptr(),
            data_ptr
        );
        assert_eq!(
            failure.message,
            format!("MCP error -32026: {}", "m".repeat(512))
        );
        let charged = rpc_bytes + failure.message.capacity();
        assert_eq!(budget.used(), charged);
        let retained = failure.clone();
        assert!(Arc::ptr_eq(retained.rpc_error.as_ref().unwrap(), value));
        drop(error);
        assert_eq!(budget.used(), charged);
        let ipc_failure = crate::daemon::mcp_error(retained.into(), false);
        assert_eq!(budget.used(), charged);
        drop(ipc_failure);
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_error_rejects_insufficient_diagnostic_budget_without_retaining_json() {
        let rpc = ErrorData::new(
            ErrorCode(-32026),
            "m".repeat(512),
            Some(json!({"items":[{"details":{"note":"ordinary value"}}]})),
        );
        let rpc_bytes = crate::json::serialized_len(&rpc).unwrap();
        let diagnostic_bytes = "MCP error -32026: ".len() + rpc.message.len();
        let budget = ByteBudget::new(rpc_bytes + diagnostic_bytes - 1);
        let mut fixture = Fixture::new(budget.clone());
        let error = fixture.client.service_failure(ServiceError::McpError(rpc));
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        assert_eq!(failure.kind, FailureKind::Transport);
        assert!(failure.rpc_error.is_none());
        assert!(failure.lease.is_none());
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn event_schema_cache_keeps_its_own_lease_after_catalog_is_dropped() {
        let budget = ByteBudget::new(4096);
        let mut fixture = Fixture::new(budget.clone());
        let client = fixture.client.clone();
        let listing = tokio::spawn(async move { client.list_events(Duration::from_secs(5)).await });
        let id = fixture.request("events/list").await;
        let schema = json!({"type":"object","properties":{"label":{"type":"string","x-mcp-header":"Label"}}});
        fixture.respond(json!({"jsonrpc":"2.0","id":id,"result":{"resultType":"complete","events":[{"name":"sample.changed","inputSchema":schema}]}})).await;
        let catalog = listing.await.unwrap().unwrap();
        let held_with_catalog = budget.used();
        drop(catalog);
        assert_eq!(
            budget.used(),
            crate::json::serialized_len(&schema).unwrap() + "sample.changed".len()
        );
        assert!(budget.used() < held_with_catalog);
        fixture.client.event_schemas.write().unwrap().take();
        assert_eq!(budget.used(), 0);
        fixture.service.close().await.unwrap();
    }

    #[tokio::test]
    async fn stdio_events_list_ignores_x_mcp_header_while_http_filters_invalid_annotations() {
        let budget = ByteBudget::new(8192);
        let bad_schema = json!({
            "type": "object",
            "properties": {
                "ratio": { "type": "number", "x-mcp-header": "InvalidFloatHeader" }
            }
        });
        for is_http in [false, true] {
            let mut fixture = Fixture::new(budget.clone());
            fixture.client.http = is_http;
            let client = fixture.client.clone();
            let listing =
                tokio::spawn(async move { client.list_events(Duration::from_secs(5)).await });
            let id = fixture.request("events/list").await;
            fixture
                .respond(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "resultType": "complete",
                        "events": [
                            { "name": "bad.annotated", "inputSchema": bad_schema },
                            { "name": "ok.plain", "inputSchema": { "type": "object" } }
                        ]
                    }
                }))
                .await;
            let listed = listing.await.unwrap().unwrap();
            if is_http {
                assert_eq!(listed.len(), 1);
                assert_eq!(listed[0]["name"], "ok.plain");
            } else {
                assert_eq!(listed.len(), 2);
                assert_eq!(listed[0]["name"], "bad.annotated");
                assert!(fixture.client.event_schemas.read().unwrap().is_none());
            }
            drop(listed);
            fixture.client.event_schemas.write().unwrap().take();
            fixture.service.close().await.unwrap();
        }
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn event_request_budget_precedes_the_json_copy() {
        let budget = ByteBudget::new(32);
        let mut fixture = Fixture::new(budget.clone());
        let mut params = stream_params();
        params.arguments = Some(
            json!({"label":"ordinary value"})
                .as_object()
                .unwrap()
                .clone(),
        );
        assert!(
            fixture
                .client
                .event_request("events/stream", &params)
                .is_err()
        );
        assert_eq!(budget.used(), 0);
        assert!(fixture.sent.try_recv().is_err());
        fixture.service.close().await.unwrap();
    }
}

#[cfg(test)]
mod stderr_capture_tests {
    use super::*;

    #[test]
    fn format_stderr_tail_skips_leading_utf8_continuation_bytes_and_sanitizes() {
        assert_eq!(format_stderr_tail(b""), None);
        // Simulate slicing inside a 3-byte UTF-8 sequence ("你" = E4 BD A0) followed by valid text and control bytes.
        let mut raw = vec![0xBD, 0xA0];
        raw.extend_from_slice("你好\x07\n".as_bytes());
        let formatted = format_stderr_tail(&raw).unwrap();
        assert_eq!(formatted, "你好\\u{7}\n");

        // Ring slicing at 4096 bytes across 3-byte characters produces valid UTF-8 with no replacement char.
        let budget = ByteBudget::new(32 * 1024);
        let lease = budget.try_acquire(DEFAULT_STDERR_TAIL_BYTES).unwrap();
        let mut ring = StderrRing::try_new(DEFAULT_STDERR_TAIL_BYTES, lease).unwrap();
        let payload = "你".repeat(2000); // 6000 bytes (> 4096)
        ring.push(payload.as_bytes());
        let tail = format_stderr_tail(&ring.tail_bytes(MAX_STDERR_SNAPSHOT_BYTES)).unwrap();
        assert!(!tail.contains('\u{FFFD}'));
        assert!(tail.ends_with("你好".chars().next().unwrap()));
        assert!(tail.len() <= MAX_STDERR_SNAPSHOT_BYTES);
    }

    #[test]
    fn stderr_ring_preallocates_charged_capacity_and_never_grows() {
        for cap in [31, 4_097, DEFAULT_STDERR_TAIL_BYTES] {
            let budget = ByteBudget::new(64 * 1024);
            let lease = budget.try_acquire(cap).unwrap();
            let mut ring = StderrRing::try_new(cap, lease).unwrap();
            let allocated = ring.buf.capacity();
            assert!(allocated >= cap);
            assert_eq!(ring.lease.bytes(), allocated);
            assert_eq!(budget.used(), allocated);
            for _ in 0..8 {
                ring.push(&[b'a'; 4_096]);
                ring.push(b"tail");
                assert!(ring.buf.len() <= cap);
                assert_eq!(ring.buf.capacity(), allocated);
                assert_eq!(budget.used(), allocated);
                assert_eq!(ring.tail_bytes(4), b"tail");
            }
            drop(ring);
            assert_eq!(budget.used(), 0);
        }
    }

    #[tokio::test]
    async fn stderr_capture_budget_fallback_keeps_child_stdio_working() {
        let budget = ByteBudget::new(4_096);
        let python = std::env::var("MCPCLI_TEST_PYTHON")
            .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
        let mut command = Command::new(python);
        command.args([
            "-c",
            "import sys; sys.stderr.write('fallback diagnostic\\n'); sys.stderr.flush(); print('ready', flush=True)",
        ]);
        let (mut transport, capture) = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            4_097,
            budget.clone(),
        )
        .unwrap();
        assert!(capture.is_none());
        assert_eq!(budget.used(), 0);
        let mut output = String::new();
        timeout(
            Duration::from_secs(5),
            transport.read.read_line(&mut output),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output.trim_end(), "ready");
        transport.shutdown().await.unwrap();
        assert_eq!(budget.used(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stderr_capture_fast_exit_large_output_and_abort_lease_release() {
        // 1. Fast exit returns immediately on EOF without waiting the 500ms snapshot window.
        let budget = ByteBudget::new(64 * 1024);
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "printf 'fast_err_line\\n' >&2; exit 1"]);
        let (mut transport, capture) = ChildTransport::spawn_observed(
            cmd,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            DEFAULT_STDERR_TAIL_BYTES,
            budget.clone(),
        )
        .unwrap();
        let capture = capture.expect("stderr capture enabled");
        let _ = transport.wait_unreaped().await.unwrap();
        let started = Instant::now();
        let tail = capture.snapshot_tail(Duration::from_millis(500)).await;
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "fast exit should not wait full timeout, elapsed={:?}",
            started.elapsed()
        );
        assert_eq!(tail.as_deref(), Some("fast_err_line\n"));
        capture.abort();
        assert_eq!(budget.used(), 0);
        transport.shutdown().await.unwrap();

        // 2. Large (>1 MiB) stderr output drains continuously without deadlocking the child.
        let mut big_cmd = Command::new("python3");
        big_cmd.args([
            "-c",
            "import sys; sys.stderr.buffer.write(b'A'*1200000 + b'TAIL_END\\n'); sys.stderr.flush()",
        ]);
        let (mut big_transport, big_capture) = ChildTransport::spawn_observed(
            big_cmd,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            DEFAULT_STDERR_TAIL_BYTES,
            budget.clone(),
        )
        .unwrap();
        let big_capture = big_capture.expect("stderr capture enabled");
        assert!(
            timeout(Duration::from_secs(5), big_transport.wait_unreaped())
                .await
                .expect("noisy child must not deadlock on stderr pipe")
                .unwrap()
        );
        let big_tail = big_capture
            .snapshot_tail(Duration::from_millis(500))
            .await
            .unwrap();
        assert!(big_tail.ends_with("TAIL_END\n"));
        assert!(big_tail.len() <= MAX_STDERR_SNAPSHOT_BYTES);
        big_capture.abort();
        assert_eq!(budget.used(), 0);
        big_transport.shutdown().await.unwrap();

        // 3. Detached grandchild holding stderr open does not leak ByteLease after abort().
        let mut grand_cmd = Command::new("sh");
        grand_cmd.args(["-c", "printf 'parent_msg\\n' >&2; (sleep 30) &"]);
        let (mut grand_transport, grand_capture) = ChildTransport::spawn_observed(
            grand_cmd,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
            DEFAULT_STDERR_TAIL_BYTES,
            budget.clone(),
        )
        .unwrap();
        let grand_capture = grand_capture.expect("stderr capture enabled");
        assert_eq!(budget.used(), DEFAULT_STDERR_TAIL_BYTES);
        grand_capture.abort();
        assert_eq!(budget.used(), 0);
        grand_transport.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_events_list_failure_surfaces_stderr_tail_and_close_releases_lease() {
        let script = r#"
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "server/discover":
        resp = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "resultType": "complete",
                "supportedVersions": ["2026-07-28"],
                "capabilities": {
                    "extensions": {
                        "io.modelcontextprotocol/events": {"listChanged": False}
                    }
                },
                "ttlMs": 60000,
                "cacheScope": "public",
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "events-crash",
                        "version": "0.1.0"
                    }
                }
            }
        }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()
    elif method == "events/list":
        sys.stderr.write("events_list_boom_stderr\n")
        sys.stderr.flush()
        sys.exit(1)
"#;
        let config = ServerConfig::Stdio {
            command: "python3".into(),
            args: vec!["-u".into(), "-c".into(), script.into()],
            env: std::collections::BTreeMap::new(),
            cwd: None,
            description: None,
            enabled: true,
            timeout: None,
        };
        let budget = ByteBudget::new(1024 * 1024);
        let limits = McpLimits {
            budget: budget.clone(),
            ..McpLimits::default()
        };
        let mut conn = McpConnection::connect_with_limits(&config, Duration::from_secs(5), limits)
            .await
            .expect("connect to stdio events server");
        let events_client = conn.events_client();
        let err = events_client
            .list_events(Duration::from_secs(5))
            .await
            .unwrap_err();
        let failure = err
            .downcast_ref::<McpFailure>()
            .expect("expected McpFailure");
        assert_eq!(failure.kind, FailureKind::Transport);
        assert!(
            failure
                .stderr_tail
                .as_deref()
                .is_some_and(|tail| tail.contains("events_list_boom_stderr")),
            "expected stderr_tail to contain crash diagnostic, got {:?}",
            failure.stderr_tail
        );
        let _ = conn.close().await;
        drop(conn);
        // ByteLease must already be released even while a cloned McpEventsClient is still alive.
        assert_eq!(budget.used(), 0);
        drop(events_client);
    }
}
