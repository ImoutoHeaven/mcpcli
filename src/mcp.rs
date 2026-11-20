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
use futures_util::stream::BoxStream;
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
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    budget::{ByteBudget, ByteLease, format_bytes},
    config::ServerConfig,
};

mod ingress;
mod sse;
use ingress::{Ingress, IngressMemoryExhausted, ObserveOutcome};
#[cfg(windows)]
mod windows_job;

pub const MAX_CONNECT: Duration = Duration::from_secs(30);
pub const MAX_RESPONSE_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_TOOL_CATALOG_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const MAX_TOOLS: usize = 10_000;
const MAX_TOOL_PAGES: usize = 256;
const LINE_SHRINK_CAP: usize = 64 * 1024;
const UTF8_BOM: &[u8; 3] = b"\xef\xbb\xbf";
const INVALID_REQUEST_REPLY: &[u8] =
    b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,\"message\":\"Invalid request\"}}\n";

#[derive(Clone)]
pub struct McpLimits {
    pub max_response_bytes: usize,
    pub max_tool_catalog_bytes: usize,
    pub budget: Arc<ByteBudget>,
}

impl Default for McpLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: MAX_RESPONSE_BYTES,
            max_tool_catalog_bytes: MAX_TOOL_CATALOG_BYTES,
            budget: ByteBudget::global().clone(),
        }
    }
}

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
    pub rpc_error: Option<Value>,
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
    }
    .into()
}

#[derive(Default)]
pub(super) struct TransportTermination {
    reason: Mutex<Option<McpFailure>>,
    cancelled: CancellationToken,
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
            });
        }
        drop(guard);
        self.cancelled.cancel();
    }

    pub(super) fn set_transient(&self, kind: FailureKind, message: String) {
        if self.cancelled.is_cancelled() {
            return;
        }
        let mut guard = self.reason.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_none() {
            *guard = Some(McpFailure {
                kind,
                message,
                outcome_unknown: false,
                rpc_error: None,
            });
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
    if let Some((method, Ok(mut response), lease)) = response
        && expected.map_or(
            matches!(method, "initialize" | "server/discover"),
            |expected| expected == method,
        )
        && let Some(result) = response.get_mut("result")
    {
        return Ok((method, result.take(), lease));
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
    let (message, rpc_error) = match ingress.take() {
        Some((_, Ok(mut response), _lease)) if response.get("error").is_some() => {
            (diagnostic, response.get_mut("error").map(Value::take))
        }
        Some((_, Err(_), _lease)) => (
            format!(
                "MCP error {code}: response rejected because its JSON cannot be represented losslessly"
            ),
            None,
        ),
        _ => (diagnostic, None),
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
    ingress: Ingress,
    termination: Arc<TransportTermination>,
    limits: McpLimits,
    server: Vec<u8>,
    closing: Option<CloseFuture>,
    cleanup: CleanupState,
    tools_changed: Arc<AtomicBool>,
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
        let termination = Arc::new(TransportTermination::default());
        let _capture = ingress.capture().ok_or_else(busy_capture)?;
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
                    let transport = ChildTransport::spawn_observed(
                        child,
                        cleanup.clone(),
                        ingress.clone(),
                        termination.clone(),
                        limits.max_response_bytes,
                        limits.budget.clone(),
                    )
                    .map_err(|e| {
                        failure(
                            FailureKind::Transport,
                            format!("Cannot start MCP server: {e}"),
                        )
                    })?;
                    let serve = handler.serve_with_lifecycle(transport, lifecycle);
                    tokio::select! {
                        biased;
                        res = serve => res.map_err(|error| init_failure(error, &ingress, &termination)),
                        () = termination.cancelled() => Err(termination.failure(false)),
                    }
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
                    let transport_config =
                        StreamableHttpClientTransportConfig::with_uri(url.clone())
                            .custom_headers(parsed_headers)
                            .reinit_on_expired_session(false)
                            .max_sse_event_size(limits.max_response_bytes);
                    let transport = StreamableHttpClientTransport::with_client(
                        BoundedHttp {
                            client,
                            cleanup: cleanup.clone(),
                            ingress: ingress.clone(),
                            termination: termination.clone(),
                            max_response_bytes: limits.max_response_bytes,
                            budget: limits.budget.clone(),
                        },
                        transport_config,
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
        let service = timeout(budget, connection)
            .await
            .map_err(|_| failure(FailureKind::Timeout, "MCP connection timed out"))??;
        // Explicit discovery must not silently return an expired SDK cache entry.
        service
            .peer()
            .set_response_cache_config(ClientCacheConfig::disabled())
            .await;
        let (source, result, _lease) = observed_result(ingress.take(), None, false)?;
        let protocol = service
            .peer()
            .peer_info()
            .ok_or_else(|| failure(FailureKind::Protocol, "MCP peer info is missing"))?;
        let server = serde_json::json!({"source":source,"protocolVersion":protocol.protocol_version,"result":result});
        Ok(Self {
            ingress,
            termination,
            limits,
            server: serde_json::to_vec(&server)?,
            peer: service.peer().clone(),
            service: Some(service),
            closing: None,
            cleanup,
            tools_changed,
        })
    }

    fn abort_service(&self) {
        if let Some(service) = &self.service {
            service.cancellation_token().cancel();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.termination.is_terminated()
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

    pub fn server_info(&self) -> Result<Value> {
        Ok(serde_json::from_slice(&self.server)?)
    }

    pub async fn list_tools(&self, budget: Duration) -> Result<Vec<Value>> {
        let (pages, _) = self.walk_tools(budget).await?;
        Ok(pages
            .into_iter()
            .flat_map(|mut page| {
                page["tools"]
                    .take()
                    .as_array_mut()
                    .map(std::mem::take)
                    .unwrap_or_default()
            })
            .collect())
    }

    pub async fn list_tool_pages(&self, budget: Duration) -> Result<Vec<Value>> {
        self.walk_tools(budget).await.map(|(pages, _)| pages)
    }

    async fn walk_tools(&self, budget: Duration) -> Result<(Vec<Value>, HashSet<String>)> {
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self.termination.failure(false));
        }
        self.termination.clear_transient();
        let _capture = self.ingress.capture().ok_or_else(busy_capture)?;
        let listing = async {
            let mut pages = Vec::new();
            let mut count = 0usize;
            let mut cursor = None;
            let mut cursors = HashSet::new();
            let mut names = HashSet::new();
            let mut bytes = 0usize;
            for _ in 0..MAX_TOOL_PAGES {
                let mut params = PaginatedRequestParams::default();
                params.cursor = cursor;
                let list_fut = self.peer.list_tools(Some(params));
                let page = tokio::select! {
                    biased;
                    res = list_fut => res.map_err(|e| service_failure(e, false, &self.ingress, &self.termination))?,
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        return Err(self.termination.failure(false));
                    }
                };
                let (_, raw_page, _page_lease) =
                    observed_result(self.ingress.take(), Some("tools/list"), false)?;
                for tool in page.tools {
                    if tool.name.is_empty() || !names.insert(tool.name.to_string()) {
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
                bytes = bytes.saturating_add(serde_json::to_vec(&raw_page)?.len());
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
                cursor = page.next_cursor;
                match &cursor {
                    None => return Ok((pages, names)),
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
        self.call_with_dispatch_leased(name, arguments, budget, dispatched)
            .await
            .map(|(value, _lease)| value)
    }

    pub async fn call_with_dispatch_leased(
        &self,
        name: &str,
        arguments: Map<String, Value>,
        budget: Duration,
        dispatched: &AtomicBool,
    ) -> Result<(Value, ByteLease)> {
        let started = tokio::time::Instant::now();
        // HTTP header annotations need the SDK's current catalog; fresh discovery
        // also prevents invoking tools rejected by the transport or withdrawn.
        let (_, names) = self
            .walk_tools(budget)
            .await
            .map_err(|error| -> anyhow::Error {
                let previous = error.downcast_ref::<McpFailure>();
                McpFailure {
                    kind: previous.map_or(FailureKind::Transport, |error| error.kind),
                    message: format!("Tool was not dispatched: discovery failed: {error}"),
                    outcome_unknown: false,
                    rpc_error: previous.and_then(|error| error.rpc_error.clone()),
                }
                .into()
            })?;
        if !names.contains(name) {
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
        self.termination.clear_transient();
        let request = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
        // This conservative boundary is also visible to the daemon's outer deadline.
        let _capture = self.ingress.capture().ok_or_else(busy_capture)?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self.termination.failure(false));
        }
        dispatched.store(true, Ordering::Release);
        // call_tool_once cancels on future drop; it does not drive interactive MRTR rounds.
        let response = timeout(remaining, async {
            let call_fut = self.peer.call_tool_once(request);
            tokio::pin!(call_fut);
            tokio::select! {
                biased;
                () = self.termination.cancelled() => {
                    self.abort_service();
                    Err(self.termination.failure(true))
                }
                res = &mut call_fut => res.map_err(|e| service_failure(e, true, &self.ingress, &self.termination)),
            }
        })
        .await
        .map_err(|_| {
            failure_with_outcome(
                FailureKind::Timeout,
                "MCP call timed out; cancellation requested; outcome unknown, the call was not retried",
                true,
            )
        })??;
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
            let (rpc_error, rejected) = match ingress.take() {
                Some((_, Ok(mut response), _)) => {
                    (response.get_mut("error").map(Value::take), false)
                }
                Some((_, Err(_), _)) => (None, true),
                None => (None, false),
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
    line: &[u8],
    error: serde_json::Error,
) -> std::result::Result<Option<ServerJsonRpcMessage>, StdioParseError> {
    if let Ok(value) = serde_json::from_slice::<Value>(line)
        && let Some(method) = value.get("method").and_then(Value::as_str)
        && should_ignore_notification(&value, method)
    {
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

fn parse_stdio_message(
    line: &[u8],
) -> std::result::Result<Option<ServerJsonRpcMessage>, StdioParseError> {
    match serde_json::from_slice::<ServerJsonRpcMessage>(line) {
        Ok(message) => Ok(Some(message)),
        Err(error) => classify_stdio_error(line, error),
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
        budget: Arc<ByteBudget>,
    ) -> std::io::Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(target_os = "linux")]
        {
            let parent = std::process::id() as libc::pid_t;
            // SAFETY: only async-signal-safe libc calls run between fork and exec.
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
        }
        #[cfg(target_os = "freebsd")]
        {
            let parent = std::process::id() as libc::pid_t;
            // SAFETY: procctl/getppid/_exit do not allocate or acquire Rust locks.
            unsafe {
                command.pre_exec(move || {
                    let signal: libc::c_int = libc::SIGKILL;
                    if libc::procctl(
                        libc::P_PID,
                        0,
                        libc::PROC_PDEATHSIG_CTL,
                        (&signal as *const libc::c_int).cast_mut().cast(),
                    ) == -1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::getppid() != parent {
                        libc::_exit(127);
                    }
                    Ok(())
                });
            }
        }
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
        let line_lease = budget.empty_lease();
        Ok(Self {
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
        })
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
        self.line_buf.clear();
        if self.line_buf.capacity() > LINE_SHRINK_CAP {
            self.line_buf = Vec::new();
        }
        self.line_lease.release();
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) {
        if let Some(group) = self.group {
            // This owns a trusted server's process group, not an escape-proof sandbox.
            unsafe {
                libc::kill(-group, signal);
            }
        }
    }

    #[cfg(unix)]
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

    #[cfg(unix)]
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
                } else if !self.line_lease.try_grow(take) {
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
        async move {
            if terminated || termination.is_terminated() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Transport is terminated",
                ));
            }
            let mut bytes = serde_json::to_vec(&item).map_err(std::io::Error::other)?;
            bytes.push(b'\n');
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
            let outcome = {
                let without_lf = self.line_buf.strip_suffix(b"\n").unwrap_or(&self.line_buf);
                let without_crlf = without_lf.strip_suffix(b"\r").unwrap_or(without_lf);
                let trimmed = without_crlf
                    .strip_prefix(UTF8_BOM.as_slice())
                    .unwrap_or(without_crlf);
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    match self.ingress.observe_bytes(
                        trimmed,
                        self.max_line_bytes,
                        &self.budget,
                        false,
                    ) {
                        Err(IngressMemoryExhausted) => Err(format!(
                            "MCP stdio response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        )),
                        Ok(ObserveOutcome::Captured(message)) => Ok(Some(Ok(Some(*message)))),
                        Ok(ObserveOutcome::InvalidMessage(error)) => {
                            Ok(Some(classify_stdio_error(trimmed, error)))
                        }
                        Ok(ObserveOutcome::NotMatched) => Ok(Some(parse_stdio_message(trimmed))),
                    }
                }
            };
            self.clear_line_buf();
            match outcome {
                Err(reason) => {
                    self.termination.terminate(FailureKind::Transport, reason);
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
    cleanup: CleanupState,
    ingress: Ingress,
    termination: Arc<TransportTermination>,
    max_response_bytes: usize,
    budget: Arc<ByteBudget>,
}
type HttpError = StreamableHttpError<reqwest::Error>;

impl BoundedHttp {
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
        let mut lease = if let Some(length) = response.content_length() {
            let length = length as usize;
            match self.budget.try_acquire(length) {
                Some(lease) => lease,
                None => {
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
            }
        } else {
            self.budget.empty_lease()
        };
        let mut bytes = Vec::new();
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
            if next_len > lease.bytes() && !lease.try_grow(next_len - lease.bytes()) {
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
        lease.shrink_to(bytes.len());
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
            .map_err(|error| HttpError::Client(error.without_url()))?;
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
            return Ok(StreamableHttpPostResponse::Sse(
                sse::bounded_stream(
                    response,
                    limit,
                    self.budget.clone(),
                    self.ingress.clone(),
                    self.termination.clone(),
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
            if let Some(request_id) = discover_request_id {
                if is_json && !body.is_empty() {
                    match self.ingress.observe_bytes(&body, limit, &self.budget, true) {
                        Err(IngressMemoryExhausted) => {}
                        Ok(ObserveOutcome::Captured(message)) => {
                            if let ServerJsonRpcMessage::Error(ref error) = *message
                                && error.id.as_ref() == Some(&request_id)
                            {
                                drop(body);
                                drop(lease);
                                return Ok(StreamableHttpPostResponse::Json(*message, None));
                            }
                        }
                        Ok(_) => {}
                    }
                }
                drop(body);
                drop(lease);
                return Ok(StreamableHttpPostResponse::Json(
                    ServerJsonRpcMessage::error(
                        ErrorData::invalid_request("server/discover is unavailable", None),
                        Some(request_id),
                    ),
                    None,
                ));
            }
            if is_json {
                match self.ingress.observe_bytes(&body, limit, &self.budget, true) {
                    Err(IngressMemoryExhausted) => {
                        drop(body);
                        drop(lease);
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
                    Ok(ObserveOutcome::Captured(message))
                        if matches!(*message, ServerJsonRpcMessage::Error(_)) =>
                    {
                        drop(body);
                        drop(lease);
                        return Ok(StreamableHttpPostResponse::Json(*message, session));
                    }
                    Ok(ObserveOutcome::NotMatched) => {
                        if let Ok(message @ ServerJsonRpcMessage::Error(_)) =
                            serde_json::from_slice(&body)
                        {
                            drop(body);
                            drop(lease);
                            return Ok(StreamableHttpPostResponse::Json(message, session));
                        }
                    }
                    Ok(_) => {}
                }
            }
            drop(body);
            drop(lease);
            return Err(HttpError::HttpStatus(HttpStatusError::new(
                status.as_u16(),
                retry_after,
            )));
        }
        let (body, lease) = self.bounded_body(response, limit).await?;
        if notification && body.is_empty() {
            drop(body);
            drop(lease);
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !is_json {
            drop(body);
            drop(lease);
            return Err(HttpError::UnexpectedContentType(None));
        }
        let parsed = match self
            .ingress
            .observe_bytes(&body, limit, &self.budget, false)
        {
            Err(IngressMemoryExhausted) => {
                drop(body);
                drop(lease);
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
            Ok(ObserveOutcome::Captured(message)) => Ok(*message),
            Ok(ObserveOutcome::InvalidMessage(error)) => Err(error),
            Ok(ObserveOutcome::NotMatched) => serde_json::from_slice(&body),
        };
        drop(body);
        drop(lease);
        match parsed {
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
        let mut request = self
            .client
            .get(uri.as_ref())
            .header("accept", "text/event-stream");
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(session) = session {
            request = request.header("Mcp-Session-Id", session.as_ref());
        }
        if let Some(last_id) = last_id {
            request = request.header("Last-Event-Id", last_id);
        }
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        let response = request
            .send()
            .await
            .map_err(|error| HttpError::Client(error.without_url()))?;
        let status = response.status();
        if status == http::StatusCode::METHOD_NOT_ALLOWED {
            return Err(HttpError::ServerDoesNotSupportSse);
        }
        if matches!(
            status,
            http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN
        ) {
            return Err(HttpError::AuthRequired(AuthRequiredError::new(
                String::new(),
            )));
        }
        if status == http::StatusCode::NOT_FOUND && had_session {
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
        ))
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        let mut request = self.client.delete(uri.as_ref());
        for (name, value) in headers {
            request = request.header(name, value);
        }
        request = request.header("Mcp-Session-Id", session.as_ref());
        if let Some(auth) = auth {
            request = request.bearer_auth(auth);
        }
        match timeout(Duration::from_secs(2), request.send()).await {
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
        let mut transport = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
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
        let mut transport = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
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
        let mut transport = ChildTransport::spawn_observed(
            command,
            CleanupState::default(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
            MAX_RESPONSE_BYTES,
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
mod stdio_transport_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
                let (c, mut s) = tokio::io::duplex(4096);
                let (r, w) = tokio::io::split(c);
                let mut t = ChildTransport::from_parts(
                    r,
                    w,
                    Ingress::default(),
                    term.clone(),
                    limit,
                    ByteBudget::new(4096),
                );
                s.write_all(valid.as_bytes()).await.unwrap();
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
        s.flush().await.unwrap();
        let full_msg = ht.receive().await.expect("completed line after cancel");
        assert!(matches!(full_msg, ServerJsonRpcMessage::Response(_)));
        assert_eq!(ht.line_lease.bytes(), 0);
        let (_, val, handed) = half_ingress.take().unwrap();
        assert_eq!(val.unwrap()["result"]["tag"], "intact");
        drop(handed);
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
        let req_line = lines.next().unwrap_or("").to_owned();
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
        Some((req_line, body))
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
        assert!(msg.contains("initialization failed"), "unexpected: {msg}");
        srv.abort();
    }
}

#[cfg(test)]
mod rpc_fidelity_tests {
    use super::*;

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

        ingress.track(&request);
        let bad_err = br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":{"x":1,"x":2}}}"#;
        let _ = ingress
            .observe_bytes(bad_err, 4096, &budget, false)
            .unwrap();
        let error = rpc_failure(-32603, "diagnostic".into(), false, &ingress);
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        assert!(failure.rpc_error.is_none());
        assert!(failure.message.contains("cannot be represented losslessly"));
        assert!(!failure.outcome_unknown);
        assert_eq!(budget.used(), 0);
    }
}
