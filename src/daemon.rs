use crate::budget::{ByteBudget, ByteLease, Leased, format_bytes};
use crate::config::LoadedConfig;
use crate::ipc::{self, Failure, Operation, Request, Response};
use crate::mcp::{FailureKind, MAX_CONNECT, McpConnection, McpFailure, McpLimits};
use crate::media::{InflightBatch, MediaStore};
use crate::platform::{
    self,
    local::{Conn, Listener},
};
use crate::runtime::{Lock, Paths, State};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

const MAX_CONNECTIONS: usize = 32;
const MAX_CONTROL_CONNECTIONS: usize = 8;
const MAX_IN_FLIGHT: usize = 8;
const FRAME_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct DaemonOptions {
    pub frame_wait: Duration,
    pub idle_timeout: Duration,
    pub response_limit: usize,
    pub budget: Arc<ByteBudget>,
    pub media: MediaStore,
    pub mcp_limits: McpLimits,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            frame_wait: FRAME_WAIT,
            idle_timeout: ipc::TRANSFER_IDLE_TIMEOUT,
            response_limit: ipc::RESPONSE_LIMIT,
            budget: ByteBudget::global().clone(),
            media: MediaStore::default(),
            mcp_limits: McpLimits::default(),
        }
    }
}

#[derive(Default)]
struct ServerState {
    connection: Option<McpConnection>,
    lost: bool,
    calls: u64,
}
struct App {
    paths: Paths,
    config: LoadedConfig,
    state: State,
    options: DaemonOptions,
    servers: BTreeMap<String, Arc<Mutex<ServerState>>>,
    work: Semaphore,
    shutdown: CancellationToken,
}

pub fn mcp_error(error: anyhow::Error, calling: bool) -> Failure {
    if let Ok(error) = error.downcast::<McpFailure>() {
        let (code, kind) = match error.kind {
            FailureKind::Timeout => (5, "timeout"),
            FailureKind::Protocol => (4, "mcp_protocol"),
            FailureKind::Transport => (4, "mcp_transport"),
        };
        let mut failure = Failure::new(code, kind, error.message).uncertain(error.outcome_unknown);
        failure.rpc_error = error.rpc_error;
        failure.lease = error.lease;
        failure
    } else {
        Failure::new(4, "mcp_error", "MCP operation failed").uncertain(calling)
    }
}

type ExecutionOutcome = std::result::Result<(Value, ByteLease, Option<InflightBatch>), Failure>;

impl App {
    fn response(&self, result: std::result::Result<Value, Failure>) -> Response {
        let (result, error) = match result {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(e)),
        };
        Response {
            protocol: ipc::PROTOCOL,
            namespace: self.paths.namespace.clone(),
            build: self.paths.build.clone(),
            epoch: self.state.epoch.clone(),
            result,
            error,
        }
    }
    fn status(&self) -> Value {
        let servers: BTreeMap<_, _> = self
            .servers
            .iter()
            .map(|(name, state)| {
                let value = match state.try_lock() {
                    Ok(state) => json!({
                        "state": if state.lost {
                            "lost"
                        } else {
                            match &state.connection {
                                Some(c) if c.is_closed() => "lost",
                                Some(_) => "connected",
                                None => "lazy",
                            }
                        },
                        "calls": state.calls
                    }),
                    Err(_) => json!({"state": "busy"}),
                };
                (name.clone(), value)
            })
            .collect();
        json!({"running":true,"pid":self.state.pid,"epoch":self.state.epoch,
            "session":self.paths.session,"namespace":self.paths.namespace,"version":env!("CARGO_PKG_VERSION"),
            "config_digest":self.config.digest,"build":self.paths.build,
            "started_unix_ms":self.state.started_unix_ms,"servers":servers,
            "endpoint":self.paths.socket,"control_endpoint":self.paths.control_socket})
    }
    fn validate(&self, request: &Request) -> std::result::Result<(), Failure> {
        if request.protocol != ipc::PROTOCOL || request.namespace != self.paths.namespace {
            return Err(Failure::new(
                6,
                "daemon_mismatch",
                "daemon protocol or namespace does not match",
            ));
        }
        if !request.operation.management()
            && (request.build != self.paths.build || request.digest != self.config.digest)
        {
            return Err(Failure::new(
                6,
                "daemon_mismatch",
                "daemon configuration or binary changed; run daemon restart (this discards session state)",
            ));
        }
        if !(1..=3_600_000).contains(&request.timeout_ms) {
            return Err(Failure::new(
                2,
                "invalid_timeout",
                "timeout must be between 1ms and one hour",
            ));
        }
        if let Operation::Call {
            tool, arguments, ..
        } = &request.operation
        {
            if tool.is_empty() || tool.len() > 512 || tool.chars().any(char::is_control) {
                return Err(Failure::new(2, "invalid_tool", "invalid tool name"));
            }
            let size = crate::json::serialized_len(arguments)
                .map_err(|_| Failure::new(2, "invalid_arguments", "cannot encode arguments"))?;
            if size > ipc::ARGUMENT_LIMIT {
                return Err(Failure::new(
                    2,
                    "invalid_arguments",
                    "arguments exceed the 1 MiB limit",
                ));
            }
        }
        Ok(())
    }
    async fn execute(
        &self,
        operation: &Operation,
        deadline: Instant,
        dispatched: &AtomicBool,
    ) -> ExecutionOutcome {
        let empty = || self.options.budget.empty_lease();
        let server = match operation {
            Operation::Status => return Ok((self.status(), empty(), None)),
            Operation::Stop => return Ok((json!({"stopping":true}), empty(), None)),
            Operation::Server { server }
            | Operation::Tools { server }
            | Operation::Call { server, .. }
            | Operation::Reset { server, .. } => server,
        };
        let state = self.servers.get(server).ok_or_else(|| {
            Failure::new(2, "unknown_server", format!("unknown server {server:?}"))
        })?;
        let mut state = state.lock().await;
        let _work = self
            .work
            .acquire()
            .await
            .map_err(|_| Failure::new(1, "stopping", "daemon is stopping"))?;
        if Instant::now() >= deadline {
            return Err(Failure::new(
                5,
                "timeout",
                "request deadline exceeded while queued",
            ));
        }
        if let Operation::Reset { force, .. } = operation {
            state.lost = true;
            let mut warning = None;
            if let Some(connection) = state.connection.as_mut()
                && let Err(error) = connection.close().await
            {
                let local_closed = connection.is_local_closed();
                if *force && local_closed {
                    warning = Some(format!(
                        "server {server:?} reset forced after unconfirmed remote cleanup: {error}"
                    ));
                } else {
                    let hint = if local_closed {
                        format!(
                            "; use `mcpcli reset --force {server}` to discard unconfirmed remote session"
                        )
                    } else {
                        String::new()
                    };
                    return Err(Failure::new(
                        1,
                        "cleanup",
                        format!(
                            "server {server:?} cleanup failed: {error}; session remains unavailable{hint}"
                        ),
                    ));
                }
            }
            state.connection = None;
            state.lost = false;
            let mut value = json!({"server":server,"reset":true,"state":"lazy"});
            if let Some(warning) = warning {
                value["cleanup_warning"] = Value::String(warning);
            }
            return Ok((value, empty(), None));
        }
        if state.lost
            || state
                .connection
                .as_ref()
                .is_some_and(McpConnection::is_closed)
        {
            state.lost = true;
            return Err(Failure::new(
                6,
                "session_lost",
                format!(
                    "server {server:?} disconnected; state may be lost. Run reset {server} before opening a new connection"
                ),
            ));
        }
        if state.connection.is_none() {
            let budget = deadline
                .saturating_duration_since(Instant::now())
                .min(MAX_CONNECT);
            if budget.is_zero() {
                return Err(Failure::new(
                    5,
                    "timeout",
                    "request deadline exceeded before connecting",
                ));
            }
            let config = &self.config.config.servers[server];
            state.connection = Some(
                McpConnection::connect_with_limits(config, budget, self.options.mcp_limits.clone())
                    .await
                    .map_err(|e| mcp_error(e, false))?,
            );
        }
        let budget = deadline.saturating_duration_since(Instant::now());
        if budget.is_zero() {
            return Err(Failure::new(
                5,
                "timeout",
                "request deadline exceeded before dispatch",
            ));
        }
        if operation.is_call() {
            state.calls += 1;
        }
        let connection = state
            .connection
            .as_ref()
            .expect("connection was established");
        match operation {
            Operation::Server { .. } => {
                let result = connection.server_info();
                if connection.is_closed() {
                    state.lost = true;
                }
                result
                    .map(|value| {
                        let (value, lease) = value.into_parts();
                        (value, lease, None)
                    })
                    .map_err(|e| mcp_error(e, false))
            }
            Operation::Tools { .. } => {
                let result = connection.list_tool_pages(budget).await.map(|pages| {
                    let (pages, lease) = pages.into_parts();
                    let mut value = serde_json::Map::new();
                    value.insert("pages".into(), Value::Array(pages));
                    (Value::Object(value), lease)
                });
                if connection.is_closed() {
                    state.lost = true;
                }
                result
                    .map(|(value, lease)| (value, lease, None))
                    .map_err(|e| mcp_error(e, false))
            }
            Operation::Call {
                tool,
                arguments,
                inline_media,
                ..
            } => {
                let result = connection
                    .call_with_dispatch_leased(tool, arguments, budget, dispatched)
                    .await;
                if connection.is_closed() {
                    state.lost = true;
                }
                let (mut lease, mut value) = result
                    .map(|(value, lease)| (lease, value))
                    .map_err(|e| mcp_error(e, dispatched.load(Ordering::Acquire)))?;
                let inflight = if *inline_media {
                    None
                } else {
                    self.options
                        .media
                        .extract_and_save(&mut value, &mut lease, &self.options.budget)
                        .await?
                };
                Ok((value, lease, inflight))
            }
            _ => unreachable!(),
        }
    }
}

async fn send(
    stream: &mut Conn,
    app: &App,
    outcome: ExecutionOutcome,
    dispatched: bool,
    hard_deadline: Option<Instant>,
) {
    let (result_lease, mut result, mut inflight) = match outcome {
        Ok((value, lease, inflight)) => (Some(lease), Ok(value), inflight),
        Err(error) => (None, Err(error), None),
    };
    if let Some(batch) = &inflight
        && let Err(error) = batch.begin_writeback()
    {
        result = Err(Failure::new(
            1,
            "media_cache",
            format!("cannot start media writeback: {error:#}; the operation was not replayed"),
        )
        .uncertain(dispatched));
        if let Some(batch) = inflight.take()
            && let Err(error) = batch.abort().await
        {
            eprintln!("mcpcli daemon: media cache error: {error:#}");
        }
    }
    let response = app.response(result);
    let encoded =
        ipc::encode_frame_leased(&response, app.options.response_limit, &app.options.budget);
    // Drop the unserialized Value and its lease before waiting on a slow IPC client.
    drop(response);
    drop(result_lease);

    let started = AtomicBool::new(false);
    match encoded {
        Ok((frame, frame_lease)) => {
            let frame = Leased::new(frame, frame_lease);
            let written = ipc::write_raw_idle(
                stream,
                &frame,
                app.options.idle_timeout,
                hard_deadline,
                &started,
            )
            .await;
            drop(frame);
            if let Some(batch) = inflight.take() {
                let finalized = if written.is_ok() {
                    batch.complete().await
                } else {
                    batch.abort().await
                };
                if let Err(error) = finalized {
                    // The response is already written. Record this local commit failure
                    // on the daemon's stderr; GC retains an unconfirmed batch.
                    eprintln!(
                        "mcpcli daemon: media cache writeback failed after response: {error:#}; the operation was not replayed"
                    );
                }
            }
        }
        Err(error) => {
            if let Some(batch) = inflight.take()
                && let Err(error) = batch.abort().await
            {
                eprintln!("mcpcli daemon: media cache error: {error:#}");
            }
            let fallback = match error.kind() {
                io::ErrorKind::InvalidData => Some(
                    Failure::new(
                        4,
                        "result_too_large",
                        format!(
                            "result exceeds the {} IPC output limit; the operation is not replayed",
                            format_bytes(app.options.response_limit)
                        ),
                    )
                    .uncertain(dispatched),
                ),
                io::ErrorKind::OutOfMemory => Some(
                    Failure::new(
                        1,
                        "memory_limit_exceeded",
                        format!(
                            "result exceeded the {} active memory limit; the operation is not replayed",
                            format_bytes(app.options.budget.limit())
                        ),
                    )
                    .uncertain(dispatched),
                ),
                _ => None,
            };
            if let Some(failure) = fallback
                && let Ok((fb_frame, fb_lease)) = ipc::encode_frame_leased(
                    &app.response(Err(failure)),
                    app.options.response_limit,
                    &app.options.budget,
                )
            {
                let fb_frame = Leased::new(fb_frame, fb_lease);
                let _ = ipc::write_raw_idle(
                    stream,
                    &fb_frame,
                    app.options.idle_timeout,
                    hard_deadline,
                    &started,
                )
                .await;
            }
        }
    }
}

async fn handle(mut stream: Conn, app: Arc<App>, control: bool) {
    if platform::validate_client(&stream).is_err() {
        return;
    }
    let request = match timeout(
        app.options.frame_wait,
        ipc::read_frame_with_budget::<_, Request>(
            &mut stream,
            ipc::REQUEST_LIMIT,
            &app.options.budget,
        ),
    )
    .await
    {
        Ok(Ok(Some((request, lease)))) => Leased::new(request, lease),
        _ => return,
    };
    if control && !request.operation.management() {
        send(
            &mut stream,
            &app,
            Err(Failure::new(
                2,
                "control_only",
                "control endpoint only accepts status and stop",
            )),
            false,
            None,
        )
        .await;
        return;
    }
    if let Err(error) = app.validate(&request) {
        send(&mut stream, &app, Err(error), false, None).await;
        return;
    }
    let now_ms = match platform::monotonic_millis() {
        Ok(now) => now,
        Err(_) => return,
    };
    let remaining = request
        .deadline_ms
        .saturating_sub(now_ms)
        .min(request.timeout_ms);
    if remaining == 0 {
        send(
            &mut stream,
            &app,
            Err(Failure::new(
                5,
                "timeout",
                "request expired before admission",
            )),
            false,
            None,
        )
        .await;
        return;
    }
    let admitted = Instant::now();
    let hard_deadline = request.explicit_deadline_ms.map(|exp_ms| {
        admitted + Duration::from_millis(exp_ms.saturating_sub(now_ms).min(request.timeout_ms))
    });
    if matches!(request.operation, Operation::Stop) {
        send(
            &mut stream,
            &app,
            Ok((
                json!({"stopping":true}),
                app.options.budget.empty_lease(),
                None,
            )),
            false,
            hard_deadline,
        )
        .await;
        app.shutdown.cancel();
        return;
    }
    let deadline = admitted + Duration::from_millis(remaining);
    let dispatched = AtomicBool::new(false);
    let execute = timeout(
        Duration::from_millis(remaining),
        app.execute(&request.operation, deadline, &dispatched),
    );
    let mut extra = [0u8; 1];
    let result = tokio::select! {
        biased;
        _ = app.shutdown.cancelled() => Err(Failure::new(4,"stopping","daemon is stopping; interrupted calls are not replayed")
            .uncertain(dispatched.load(Ordering::Acquire))),
        // One request per connection; EOF or extra input abandons the in-flight operation.
        _ = stream.read(&mut extra) => return,
        result = execute => match result {
            Ok(result)=>result,
            Err(_)=>Err(Failure::new(5,"timeout","request deadline exceeded, including queue and connection time; cancellation is not rollback")
                .uncertain(dispatched.load(Ordering::Acquire))),
        },
    };
    let send_deadline = hard_deadline.filter(|_| result.is_ok());
    send(
        &mut stream,
        &app,
        result,
        dispatched.load(Ordering::Acquire),
        send_deadline,
    )
    .await;
}

pub async fn run(paths: Paths, config: LoadedConfig) -> Result<()> {
    run_with_options(paths, config, DaemonOptions::default()).await
}

pub async fn run_with_options(
    paths: Paths,
    config: LoadedConfig,
    options: DaemonOptions,
) -> Result<()> {
    let _lifetime =
        Lock::try_acquire(&paths.daemon_lock())?.context("daemon is already running")?;
    paths.remove_socket()?;
    let mut listener = Listener::bind(paths.socket.as_os_str())?;
    let mut control_listener = Listener::bind(paths.control_socket.as_os_str())?;
    for socket in [&paths.socket, &paths.control_socket] {
        platform::secure_endpoint(socket)?;
    }
    let mut state = State::fresh();
    state.store(&paths)?;
    let servers = config
        .config
        .servers
        .keys()
        .map(|name| (name.clone(), Arc::new(Mutex::new(ServerState::default()))))
        .collect();
    let app = Arc::new(App {
        paths: paths.clone(),
        config,
        state: state.clone(),
        options,
        servers,
        work: Semaphore::new(MAX_IN_FLIGHT),
        shutdown: CancellationToken::new(),
    });
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let control_connections = Arc::new(Semaphore::new(MAX_CONTROL_CONNECTIONS));
    let mut tasks = JoinSet::new();
    let interrupted = platform::interrupted();
    tokio::pin!(interrupted);
    loop {
        tokio::select! {
            _ = app.shutdown.cancelled() => break,
            signal = &mut interrupted => { signal?; break; },
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = async {
                let permit = connections.clone().acquire_owned().await.ok()?;
                Some((listener.accept().await, permit))
            } => {
                if let Some((accepted, permit)) = accepted {
                    let stream = accepted?;
                    let app = app.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        handle(stream, app, false).await;
                    });
                }
            }
            accepted = async {
                let permit = control_connections.clone().acquire_owned().await.ok()?;
                Some((control_listener.accept().await, permit))
            } => {
                if let Some((accepted, permit)) = accepted {
                    let stream = accepted?;
                    let app = app.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        handle(stream, app, true).await;
                    });
                }
            }
        }
    }
    drop(listener);
    drop(control_listener);
    app.shutdown.cancel();
    let drained = timeout(Duration::from_secs(6), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    let cleanup =
        futures_util::future::join_all(app.servers.iter().map(|(name, server)| async move {
            let mut state = server.lock().await;
            if let Some(connection) = state.connection.as_mut() {
                connection
                    .close()
                    .await
                    .map_err(|error| format!("server {name:?}: {error}"))?;
            }
            state.connection = None;
            Ok::<_, String>(())
        }));
    state.cleanup_errors = match timeout(Duration::from_secs(10), cleanup).await {
        Ok(results) => results.into_iter().filter_map(Result::err).collect(),
        Err(_) => vec!["daemon cleanup exceeded its deadline".into()],
    };
    paths.remove_socket()?;
    state.clean_stop = state.cleanup_errors.is_empty();
    state.store(&paths)?;
    if !state.clean_stop {
        anyhow::bail!("daemon cleanup failed: {}", state.cleanup_errors.join("; "));
    }
    Ok(())
}
