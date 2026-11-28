//! `McpConnection` lifecycle, handshake, tool listing, and tool calls.
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use http::{HeaderName, HeaderValue};
use rmcp::{
    RoleClient,
    model::{
        CallToolRequest, CallToolRequestParams, CallToolResponse, CancelledNotification,
        CancelledNotificationParam, ClientCapabilities, ClientConfig, ClientNotification,
        ClientRequest, ClientResult, ElicitResult, ElicitationAction, ErrorCode, ErrorData,
        ExtensionCapabilities, Implementation, PaginatedRequestParams, ProtocolVersion,
        ServerNotification, ServerRequest, ServerResult, TASKS_EXTENSION_ID,
    },
    service::{
        ClientCacheConfig, ClientLifecycleMode, ClientServiceExt, NotificationContext, Peer,
        QuitReason, RequestContext, RunningService, Service, ServiceError,
    },
    transport::streamable_http_client::{
        StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Map, Value};
use tokio::{
    process::Command,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

use super::{
    events::{
        self, EVENTS_EXTENSION_ID, EventNotificationTransport, EventRouter, EventSchemaCache,
        EventStreamHandle, EventStreamReceiver, PollEventsParams, StreamEventsParams,
    },
    events_client::McpEventsClient,
    http::BoundedHttp,
    ingress::{Ingress, busy_capture, observed_result, rpc_failure, service_failure},
    stdio::{ChildTransport, StderrCapture, attach_stderr_tail, attach_stderr_tail_and_abort},
    types::{
        CallBudgets, CallOptions, CallOutcome, CancelOnDropGuard, CleanupState, FailureKind,
        MAX_CONNECT, McpFailure, McpLimits, McpOpError, PREFERRED_PROTOCOL_VERSIONS, TaskSeed,
        TransportTermination, check_tasks_capability, failure, failure_with_outcome,
        record_cleanup_failure,
    },
};
use crate::{
    budget::{ByteLease, Leased, format_bytes},
    config::ServerConfig,
};

const MAX_TOOLS: usize = 10_000;
const MAX_TOOL_PAGES: usize = 256;

#[derive(Clone)]
pub(super) struct Handler {
    pub(super) tools_changed: Arc<AtomicBool>,
}

impl Service<RoleClient> for Handler {
    fn get_info(&self) -> ClientConfig {
        let mut extensions = ExtensionCapabilities::new();
        extensions.insert(EVENTS_EXTENSION_ID.to_owned(), Map::new());
        extensions.insert(TASKS_EXTENSION_ID.to_owned(), Map::new());
        let mut capabilities = ClientCapabilities::default();
        capabilities.extensions = Some(extensions);
        capabilities.elicitation = None;
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

pub struct McpConnection {
    pub(super) service: Option<RunningService<RoleClient, Handler>>,
    pub(super) peer: Peer<RoleClient>,
    pub(super) ingress: Ingress,
    pub(super) event_router: EventRouter,
    pub(super) event_schemas: EventSchemaCache,
    pub(super) termination: Arc<TransportTermination>,
    pub(super) stderr_capture: Option<Arc<StderrCapture>>,
    pub(super) limits: McpLimits,
    pub(super) server: Leased<Vec<u8>>,
    pub(super) closing: Option<CloseFuture>,
    pub(super) cleanup: CleanupState,
    pub(super) tools_changed: Arc<AtomicBool>,
    pub(super) http: bool,
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

    pub(super) fn abort_service(&self) {
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

    pub(super) async fn attach_stderr_tail(&self, error: anyhow::Error) -> anyhow::Error {
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
        match self
            .call_inner(name, arguments, None, budgets, dispatched, call_cancel)
            .await?
        {
            CallOutcome::Complete { value, lease } => Ok((value, lease)),
            CallOutcome::Task { .. } => Err(failure_with_outcome(
                FailureKind::Protocol,
                "The server returned an asynchronous task; side effects may have occurred, the call was not retried",
                true,
            )),
        }
    }

    pub async fn call_with_options_leased(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        options: CallOptions<'_>,
        budgets: CallBudgets,
        dispatched: &AtomicBool,
        call_cancel: CancellationToken,
    ) -> std::result::Result<CallOutcome, McpOpError> {
        self.call_inner(
            name,
            arguments,
            options.expected_schema,
            budgets,
            dispatched,
            call_cancel,
        )
        .await
        .map_err(|err| McpOpError::from_error(err, dispatched.load(Ordering::Acquire)))
    }

    async fn call_inner(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        expected_schema: Option<&crate::schema::ExpectedSchema>,
        budgets: CallBudgets,
        dispatched: &AtomicBool,
        call_cancel: CancellationToken,
    ) -> Result<CallOutcome> {
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
        let tool = pages
            .iter()
            .filter_map(|page| page.get("tools").and_then(Value::as_array))
            .flatten()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some(name));
        if let Some(expected) = expected_schema {
            let live_input = tool
                .and_then(|t| t.get("inputSchema"))
                .unwrap_or(&Value::Null);
            let live_output = tool
                .and_then(|t| t.get("outputSchema"))
                .filter(|v| !v.is_null());
            if let Err(reason) =
                crate::schema::verify_tool_schema(expected, live_input, live_output)
            {
                return Err(McpOpError::new(
                    2,
                    "schema_mismatch",
                    format!("Tool {name:?} schema mismatch: {reason}"),
                )
                .into());
            }
        }
        let parameter_bytes = crate::json::serialized_len(arguments)?.saturating_add(name.len());
        let header_bytes = if self.http
            && self
                .peer
                .peer_info()
                .is_some_and(|info| info.protocol_version >= ProtocolVersion::STANDARD_HEADERS)
        {
            let schema = tool
                .and_then(|t| t.get("inputSchema"))
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
                let (_, value, lease) =
                    observed_result(self.ingress.take(), Some("tools/call"), true)?;
                Ok(CallOutcome::Complete { value, lease })
            }
            CallToolResponse::InputRequired(_) => Err(failure_with_outcome(
                FailureKind::Protocol,
                "The tool requires unsupported interactive input; side effects may have occurred, the call was not retried",
                true,
            )),
            CallToolResponse::Task(create_res) => {
                if !self.supports_tasks() {
                    let mut fail = McpOpError::new(
                        4,
                        "mcp_protocol",
                        "The server returned an asynchronous task without negotiating the tasks capability; side effects may have occurred, the call was not retried",
                    )
                    .uncertain(true);
                    let mut data = Map::new();
                    data.insert(
                        "taskId".into(),
                        Value::String(create_res.task.task_id.clone()),
                    );
                    fail.data = Some(Box::new(Value::Object(data)));
                    return Err(fail.into());
                }
                let seed = TaskSeed::from_create_task_result(&create_res);
                drop(create_res);
                let (_, value, lease) =
                    observed_result(self.ingress.take(), Some("tools/call"), true)?;
                Ok(CallOutcome::Task { seed, value, lease })
            }
            _ => Err(failure_with_outcome(
                FailureKind::Protocol,
                "Unsupported MCP tool response; outcome unknown, the call was not retried",
                true,
            )),
        }
    }

    pub fn supports_tasks(&self) -> bool {
        check_tasks_capability(self.peer.peer_info().as_deref()).is_ok()
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

pub(super) fn init_failure(
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
