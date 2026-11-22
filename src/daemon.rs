use crate::budget::{ByteBudget, ByteLease, Leased, format_bytes};
use crate::config::LoadedConfig;
use crate::ipc::{self, Failure, Operation, Request, Response, StreamFrame, StreamFrameKind};
use crate::mcp::{
    FailureKind, MAX_CONNECT, McpConnection, McpEventsClient, McpFailure, McpLimits,
    PollEventsParams, StreamEventsParams,
};
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
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

const MAX_CONNECTIONS: usize = 32;
const MAX_CONTROL_CONNECTIONS: usize = 8;
const MAX_IN_FLIGHT: usize = 8;
const MAX_EVENT_STREAMS: usize = 32;
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

struct ServerState {
    connection: Option<McpConnection>,
    lost: bool,
    calls: u64,
    streams_cancel: CancellationToken,
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            connection: None,
            lost: false,
            calls: 0,
            streams_cancel: CancellationToken::new(),
        }
    }
}

struct App {
    paths: Paths,
    config: LoadedConfig,
    state: State,
    options: DaemonOptions,
    servers: BTreeMap<String, Arc<Mutex<ServerState>>>,
    work: Semaphore,
    event_streams: Semaphore,
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
        if let Operation::EventsPoll {
            event, arguments, ..
        }
        | Operation::EventsStream {
            event, arguments, ..
        } = &request.operation
        {
            if event.is_empty() || event.len() > 512 || event.chars().any(char::is_control) {
                return Err(Failure::new(2, "invalid_event", "invalid event name"));
            }
            if let Some(arguments) = arguments {
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
        }
        Ok(())
    }

    fn event_arguments_lease(
        &self,
        event: &str,
        arguments: &Option<serde_json::Map<String, Value>>,
        cursor: &Option<String>,
    ) -> std::result::Result<ByteLease, Failure> {
        let bytes = crate::json::serialized_len(arguments)
            .map_err(|_| Failure::new(2, "invalid_arguments", "cannot encode event arguments"))?
            .saturating_add(event.len())
            .saturating_add(cursor.as_ref().map_or(0, String::len));
        self.options.budget.try_acquire(bytes).ok_or_else(|| {
            Failure::new(
                1,
                "memory_limit_exceeded",
                "event arguments exceed the active memory limit",
            )
        })
    }

    fn stream_frame(&self, kind: StreamFrameKind) -> StreamFrame {
        StreamFrame {
            protocol: ipc::PROTOCOL,
            namespace: self.paths.namespace.clone(),
            build: self.paths.build.clone(),
            epoch: self.state.epoch.clone(),
            kind,
        }
    }

    async fn ensure_events_client(
        &self,
        server: &str,
        deadline: Instant,
    ) -> std::result::Result<(McpEventsClient, CancellationToken), Failure> {
        let state = self.servers.get(server).ok_or_else(|| {
            Failure::new(2, "unknown_server", format!("unknown server {server:?}"))
        })?;
        let mut state = state.lock().await;
        if Instant::now() >= deadline {
            return Err(Failure::new(
                5,
                "timeout",
                "request deadline exceeded while queued",
            ));
        }
        if state.lost
            || state
                .connection
                .as_ref()
                .is_some_and(McpConnection::is_closed)
        {
            state.lost = true;
            state.streams_cancel.cancel();
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
        let client = state
            .connection
            .as_ref()
            .expect("connection was established")
            .events_client();
        let cancel = state.streams_cancel.child_token();
        Ok((client, cancel))
    }

    async fn mark_server_lost_if_closed(&self, server: &str) {
        if let Some(server_state) = self.servers.get(server) {
            let mut state = server_state.lock().await;
            if state
                .connection
                .as_ref()
                .is_some_and(McpConnection::is_closed)
            {
                state.lost = true;
                state.streams_cancel.cancel();
            }
        }
    }
    async fn execute(
        &self,
        operation: &Operation,
        deadline: Instant,
        dispatched: &AtomicBool,
    ) -> ExecutionOutcome {
        let empty = || self.options.budget.empty_lease();
        match operation {
            Operation::Status => return Ok((self.status(), empty(), None)),
            Operation::Stop => return Ok((json!({"stopping":true}), empty(), None)),
            Operation::EventsList { server } => {
                let (client, _) = self.ensure_events_client(server, deadline).await?;
                let work = self
                    .work
                    .acquire()
                    .await
                    .map_err(|_| Failure::new(1, "stopping", "daemon is stopping"))?;
                let budget = deadline.saturating_duration_since(Instant::now());
                if budget.is_zero() {
                    return Err(Failure::new(
                        5,
                        "timeout",
                        "request deadline exceeded before dispatch",
                    ));
                }
                let result = client.list_events(budget).await;
                drop(work);
                if client.is_closed() {
                    self.mark_server_lost_if_closed(server).await;
                }
                let events = result.map_err(|e| mcp_error(e, false))?;
                let (events, lease) = events.into_parts();
                let mut value = serde_json::Map::new();
                value.insert("events".into(), Value::Array(events));
                return Ok((Value::Object(value), lease, None));
            }
            Operation::EventsPoll {
                server,
                event,
                arguments,
                cursor,
                max_age_ms,
                max_events,
            } => {
                let (client, _) = self.ensure_events_client(server, deadline).await?;
                let work = self
                    .work
                    .acquire()
                    .await
                    .map_err(|_| Failure::new(1, "stopping", "daemon is stopping"))?;
                let budget = deadline.saturating_duration_since(Instant::now());
                if budget.is_zero() {
                    return Err(Failure::new(
                        5,
                        "timeout",
                        "request deadline exceeded before dispatch",
                    ));
                }
                let _params_lease = self.event_arguments_lease(event, arguments, cursor)?;
                let params = PollEventsParams {
                    name: event.clone(),
                    arguments: arguments.clone(),
                    cursor: cursor.clone(),
                    max_age_ms: *max_age_ms,
                    max_events: *max_events,
                };
                let result = client.poll_events(params, budget).await;
                drop(work);
                if client.is_closed() {
                    self.mark_server_lost_if_closed(server).await;
                }
                let polled = result.map_err(|e| mcp_error(e, false))?;
                let (value, lease) = polled.into_parts();
                return Ok((value, lease, None));
            }
            _ => {}
        }
        let server = match operation {
            Operation::Server { server }
            | Operation::Tools { server }
            | Operation::Call { server, .. }
            | Operation::Reset { server, .. } => server,
            _ => unreachable!(),
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
            state.streams_cancel.cancel();
            state.streams_cancel = CancellationToken::new();
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
            state.streams_cancel.cancel();
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

async fn write_stream_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    app: &App,
    kind: StreamFrameKind,
) -> io::Result<()> {
    let frame_msg = app.stream_frame(kind);
    let (frame, lease) =
        ipc::encode_frame_leased(&frame_msg, app.options.response_limit, &app.options.budget)?;
    drop(frame_msg);
    let frame = Leased::new(frame, lease);
    let started = AtomicBool::new(false);
    // Streaming IPC never enforces the 1-hour total deadline; only per-frame idle timeout applies.
    ipc::write_raw_idle(writer, &frame, app.options.idle_timeout, None, &started).await
}

async fn handle_events_stream(
    mut stream: Conn,
    app: Arc<App>,
    request: Leased<Request>,
    control: bool,
    conn_permit: OwnedSemaphorePermit,
) {
    if control {
        let _ = write_stream_frame(
            &mut stream,
            &app,
            StreamFrameKind::Error {
                error: Failure::new(
                    2,
                    "control_only",
                    "control endpoint only accepts status and stop",
                ),
            },
        )
        .await;
        return;
    }
    if let Err(error) = app.validate(&request) {
        let _ = write_stream_frame(&mut stream, &app, StreamFrameKind::Error { error }).await;
        return;
    }
    let Ok(_stream_permit) = app.event_streams.try_acquire() else {
        let _ = write_stream_frame(
            &mut stream,
            &app,
            StreamFrameKind::Error {
                error: Failure::new(
                    4,
                    "stream_limit",
                    "too many concurrent event streams on daemon",
                ),
            },
        )
        .await;
        return;
    };
    drop(conn_permit);
    let Operation::EventsStream {
        server,
        event,
        arguments,
        cursor,
        max_age_ms,
        heartbeat_ms,
    } = &request.operation
    else {
        unreachable!();
    };
    let handshake_ms = request.timeout_ms.clamp(1, 30_000);
    let handshake_deadline = Instant::now() + Duration::from_millis(handshake_ms);
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut extra = [0u8; 1];

    let connect_result = tokio::select! {
        biased;
        _ = app.shutdown.cancelled() => Err(Failure::new(4, "stopping", "daemon is stopping")),
        _ = reader.read(&mut extra) => return,
        res = app.ensure_events_client(server, handshake_deadline) => res,
    };
    let (client, server_cancel) = match connect_result {
        Ok(pair) => pair,
        Err(error) => {
            let _ = write_stream_frame(&mut writer, &app, StreamFrameKind::Error { error }).await;
            return;
        }
    };

    let open_budget = handshake_deadline.saturating_duration_since(Instant::now());
    if open_budget.is_zero() {
        let _ = write_stream_frame(
            &mut writer,
            &app,
            StreamFrameKind::Error {
                error: Failure::new(5, "timeout", "event stream handshake timed out"),
            },
        )
        .await;
        return;
    }

    let params_lease = match app.event_arguments_lease(event, arguments, cursor) {
        Ok(lease) => lease,
        Err(error) => {
            let _ = write_stream_frame(&mut writer, &app, StreamFrameKind::Error { error }).await;
            return;
        }
    };
    let params = StreamEventsParams {
        name: event.clone(),
        arguments: arguments.clone(),
        cursor: cursor.clone(),
        max_age_ms: *max_age_ms,
        heartbeat_ms: *heartbeat_ms,
    };
    let open_result = tokio::select! {
        biased;
        _ = app.shutdown.cancelled() => Err(Failure::new(4, "stopping", "daemon is stopping")),
        _ = server_cancel.cancelled() => Err(Failure::new(
            6,
            "session_lost",
            format!("server {server:?} was reset or disconnected"),
        )),
        _ = reader.read(&mut extra) => return,
        res = client.open_event_stream(params, open_budget) => {
            if client.is_closed() {
                app.mark_server_lost_if_closed(server).await;
            }
            res.map_err(|e| mcp_error(e, false))
        }
    };

    drop(params_lease);
    let (stream_handle, mut rx) = match open_result {
        Ok(pair) => pair,
        Err(error) => {
            let _ = write_stream_frame(&mut writer, &app, StreamFrameKind::Error { error }).await;
            return;
        }
    };

    // Release request memory lease once the upstream stream is active.
    let server = server.clone();
    drop(request);

    loop {
        let next = tokio::select! {
            biased;
            _ = app.shutdown.cancelled() => {
                let _ = stream_handle.cancel().await;
                let _ = write_stream_frame(
                    &mut writer,
                    &app,
                    StreamFrameKind::Error {
                        error: Failure::new(4, "stopping", "daemon is stopping"),
                    },
                )
                .await;
                return;
            }
            _ = server_cancel.cancelled() => {
                let _ = stream_handle.cancel().await;
                let _ = write_stream_frame(
                    &mut writer,
                    &app,
                    StreamFrameKind::Error {
                        error: Failure::new(
                            6,
                            "session_lost",
                            format!("server {server:?} was reset or disconnected"),
                        ),
                    },
                )
                .await;
                return;
            }
            _ = reader.read(&mut extra) => {
                // Foreground CLI disconnected (EOF) or sent unexpected input: cancel upstream immediately.
                let _ = stream_handle.cancel().await;
                return;
            }
            notif = rx.recv() => notif,
        };

        let Some(leased) = next else {
            if client.is_closed() {
                app.mark_server_lost_if_closed(&server).await;
                let _ = write_stream_frame(
                    &mut writer,
                    &app,
                    StreamFrameKind::Error {
                        error: Failure::new(
                            6,
                            "session_lost",
                            format!("server {server:?} disconnected"),
                        ),
                    },
                )
                .await;
            } else {
                let _ = write_stream_frame(&mut writer, &app, StreamFrameKind::End).await;
            }
            return;
        };

        let (notif, notif_lease) = leased.into_parts();
        let is_terminated = notif.method == "notifications/events/terminated";
        let kind = StreamFrameKind::Notification {
            method: notif.method,
            params: notif.params,
        };
        let write_ok = tokio::select! {
            biased;
            _ = reader.read(&mut extra) => {
                let _ = stream_handle.cancel().await;
                return;
            }
            res = write_stream_frame(&mut writer, &app, kind) => res.is_ok(),
        };
        drop(notif_lease);
        if !write_ok {
            let _ = stream_handle.cancel().await;
            return;
        }
        if is_terminated {
            let _ = write_stream_frame(&mut writer, &app, StreamFrameKind::End).await;
            return;
        }
    }
}

async fn handle(mut stream: Conn, app: Arc<App>, control: bool, conn_permit: OwnedSemaphorePermit) {
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
    if request.operation.is_streaming() {
        handle_events_stream(stream, app, request, control, conn_permit).await;
        return;
    }
    let _conn_permit = conn_permit;
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
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
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
                        handle(stream, app, false, permit).await;
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
                        handle(stream, app, true, permit).await;
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
            state.streams_cancel.cancel();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

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
            work: Semaphore::new(MAX_IN_FLIGHT),
            event_streams: Semaphore::new(MAX_EVENT_STREAMS),
            shutdown: CancellationToken::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        app.ensure_events_client("srv", deadline).await.unwrap();
        let state = app.servers["srv"].lock().await;
        let running = app.work.acquire().await.unwrap();
        let operation = Operation::Server {
            server: "srv".into(),
        };
        let dispatched = AtomicBool::new(false);
        let mut ordinary = Box::pin(app.execute(&operation, deadline, &dispatched));
        assert!(futures_util::poll!(ordinary.as_mut()).is_pending());
        let operations: Vec<_> = (0..MAX_IN_FLIGHT)
            .map(|_| Operation::EventsList {
                server: "srv".into(),
            })
            .collect();
        let mut events: Vec<_> = operations
            .iter()
            .map(|op| Box::pin(app.execute(op, deadline, &dispatched)))
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
}
