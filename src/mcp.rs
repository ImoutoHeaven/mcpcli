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

mod ingress;
mod sse;
use ingress::{Ingress, ObservedRead};
#[cfg(windows)]
mod windows_job;

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
    response: Option<(&'static str, Result<Value>)>,
    expected: Option<&str>,
    calling: bool,
) -> Result<(&'static str, Value)> {
    if let Some((method, Ok(mut response))) = response
        && expected.map_or(
            matches!(method, "initialize" | "server/discover"),
            |expected| expected == method,
        )
        && let Some(result) = response.get_mut("result")
    {
        return Ok((method, result.take()));
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
        Some((_, Ok(mut response))) if response.get("error").is_some() => {
            (diagnostic, response.get_mut("error").map(Value::take))
        }
        Some((_, Err(_))) => (
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

fn service_failure(error: ServiceError, calling: bool, ingress: &Ingress) -> anyhow::Error {
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
    ingress: Ingress,
    server: Vec<u8>,
    closing: Option<CloseFuture>,
    cleanup: CleanupState,
    tools_changed: Arc<AtomicBool>,
}

impl McpConnection {
    pub async fn connect(config: &ServerConfig, budget: Duration) -> Result<Self> {
        let tools_changed = Arc::new(AtomicBool::new(false));
        let cleanup = CleanupState::default();
        let ingress = Ingress::default();
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
                    let transport =
                        ChildTransport::spawn_observed(child, cleanup.clone(), ingress.clone())
                            .map_err(|e| {
                                failure(
                                    FailureKind::Transport,
                                    format!("Cannot start MCP server: {e}"),
                                )
                            })?;
                    handler
                        .serve_with_lifecycle(transport, lifecycle)
                        .await
                        .map_err(|error| init_failure(error, &ingress))
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
                            .max_sse_event_size(MAX_RESPONSE_BYTES);
                    let transport = StreamableHttpClientTransport::with_client(
                        BoundedHttp {
                            client,
                            cleanup: cleanup.clone(),
                            ingress: ingress.clone(),
                        },
                        transport_config,
                    );
                    handler
                        .serve_with_lifecycle(transport, lifecycle)
                        .await
                        .map_err(|error| init_failure(error, &ingress))
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
        let (source, result) = observed_result(ingress.take(), None, false)?;
        let protocol = service
            .peer()
            .peer_info()
            .ok_or_else(|| failure(FailureKind::Protocol, "MCP peer info is missing"))?;
        let server = serde_json::json!({"source":source,"protocolVersion":protocol.protocol_version,"result":result});
        Ok(Self {
            ingress,
            server: serde_json::to_vec(&server)?,
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
                let page = self
                    .peer
                    .list_tools(Some(params))
                    .await
                    .map_err(|e| service_failure(e, false, &self.ingress))?;
                let (_, raw_page) =
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
                if bytes > MAX_RESPONSE_BYTES {
                    return Err(failure(
                        FailureKind::Protocol,
                        "Tool catalog exceeds the size limit",
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
        let request = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
        // This conservative boundary is also visible to the daemon's outer deadline.
        let _capture = self.ingress.capture().ok_or_else(busy_capture)?;
        dispatched.store(true, Ordering::Release);
        // call_tool_once cancels on future drop; it does not drive interactive MRTR rounds.
        let response = timeout(remaining, self.peer.call_tool_once(request)).await
            .map_err(|_| failure_with_outcome(
                FailureKind::Timeout,
                "MCP call timed out; cancellation requested; outcome unknown, the call was not retried", true,
            ))?
            .map_err(|e| service_failure(e, true, &self.ingress))?;
        match response {
            CallToolResponse::Complete(_) => {
                observed_result(self.ingress.take(), Some("tools/call"), true)
                    .map(|(_, value)| value)
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

fn init_failure(error: rmcp::service::ClientInitializeError, ingress: &Ingress) -> anyhow::Error {
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
                Some((_, Ok(mut response))) => (response.get_mut("error").map(Value::take), false),
                Some((_, Err(_))) => (None, true),
                None => (None, false),
            };
            McpFailure {
                kind: FailureKind::Protocol,
                message: if rejected {
                    "No compatible MCP protocol version; response JSON could not be represented losslessly"
                } else {
                    "No compatible MCP protocol version"
                }.into(),
                outcome_unknown: false,
                rpc_error,
            }.into()
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
    io: AsyncRwTransport<RoleClient, ObservedRead<ChildStdout>, ChildStdin>,
    ingress: Ingress,
    child: Option<Child>,
    #[cfg(unix)]
    group: Option<i32>,
    #[cfg(windows)]
    job: Option<windows_job::Job>,
    cleanup: CleanupState,
}

impl ChildTransport {
    #[cfg(test)]
    fn spawn(command: Command, cleanup: CleanupState) -> std::io::Result<Self> {
        Self::spawn_observed(command, cleanup, Ingress::default())
    }

    fn spawn_observed(
        mut command: Command,
        cleanup: CleanupState,
        ingress: Ingress,
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
        Ok(Self {
            io: AsyncRwTransport::new(ObservedRead::new(stdout, ingress.clone()), stdin),
            ingress,
            child: Some(child),
            #[cfg(unix)]
            group,
            #[cfg(windows)]
            job,
            cleanup,
        })
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

    #[cfg(unix)]
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

    #[cfg(windows)]
    async fn shutdown(&mut self) -> std::io::Result<()> {
        let _ = timeout(Duration::from_millis(500), self.io.close()).await;
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

impl Drop for ChildTransport {
    fn drop(&mut self) {
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

impl Transport<RoleClient> for ChildTransport {
    type Error = std::io::Error;
    fn send(
        &mut self,
        item: ClientJsonRpcMessage,
    ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
        self.ingress.track(&item);
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
    ingress: Ingress,
}
type HttpError = StreamableHttpError<reqwest::Error>;

impl BoundedHttp {
    fn observe_sse(
        &self,
        stream: BoxStream<'static, Result<Sse, SseError>>,
    ) -> BoxStream<'static, Result<Sse, SseError>> {
        let ingress = self.ingress.clone();
        stream
            .map(move |event| {
                if let Ok(sse) = &event {
                    ingress.observe_sse(sse);
                }
                event
            })
            .boxed()
    }

    async fn bounded_body(
        mut response: reqwest::Response,
        limit: usize,
    ) -> Result<Vec<u8>, HttpError> {
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(HttpError::Io(std::io::Error::other(format!(
                "HTTP response exceeds {limit} bytes"
            ))));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| HttpError::Client(error.without_url()))?
        {
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(HttpError::Io(std::io::Error::other(format!(
                    "HTTP response exceeds {limit} bytes"
                ))));
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
                self.observe_sse(SseStream::from_bytes_stream(stream).boxed()),
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
                if is_json
                    && let Ok(message @ ServerJsonRpcMessage::Error(_)) =
                        serde_json::from_slice::<ServerJsonRpcMessage>(&body)
                    && let ServerJsonRpcMessage::Error(ref error) = message
                    && error.id.as_ref() == Some(&request_id)
                {
                    self.ingress.observe(&body);
                    return Ok(StreamableHttpPostResponse::Json(message, None));
                }
                return Ok(StreamableHttpPostResponse::Json(
                    ServerJsonRpcMessage::error(
                        ErrorData::invalid_request("server/discover is unavailable", None),
                        Some(request_id),
                    ),
                    None,
                ));
            }
            if is_json
                && let Ok(message @ ServerJsonRpcMessage::Error(_)) = serde_json::from_slice(&body)
            {
                self.ingress.observe(&body);
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
            Ok(message) => {
                self.ingress.observe(&body);
                Ok(StreamableHttpPostResponse::Json(message, session))
            }
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
        Ok(self.observe_sse(sse::bounded_stream(response, limit.min(MAX_RESPONSE_BYTES))))
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

#[cfg(test)]
mod http_tests {
    use super::*;

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
}

#[cfg(test)]
mod rpc_fidelity_tests {
    use super::*;

    #[test]
    fn rpc_error_fidelity_and_non_lossy_rejection() {
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        assert!(ingress.capture().is_none());
        let request = serde_json::from_value(serde_json::json!({
            "jsonrpc":"2.0","id":17,"method":"tools/list"
        }))
        .unwrap();
        ingress.track(&request);
        ingress.observe(br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":null,"x-extra":7}}"#);
        let error = rpc_failure(-32603, "diagnostic".into(), true, &ingress);
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        let raw = failure.rpc_error.as_ref().unwrap();
        assert_eq!(raw.get("data"), Some(&Value::Null));
        assert_eq!(raw["x-extra"], 7);
        assert!(failure.outcome_unknown);

        ingress.track(&request);
        ingress.observe(br#"{"jsonrpc":"2.0","id":17,"error":{"code":-32603,"message":"failed","data":{"x":1,"x":2}}}"#);
        let error = rpc_failure(-32603, "diagnostic".into(), false, &ingress);
        let failure = error.downcast_ref::<McpFailure>().unwrap();
        assert!(failure.rpc_error.is_none());
        assert!(failure.message.contains("cannot be represented losslessly"));
        assert!(!failure.outcome_unknown);
    }
}
