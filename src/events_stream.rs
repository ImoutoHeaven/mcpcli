//! Foreground `mcpcli events stream` engine: push/poll auto-selection, dead-stream
//! reconnect, cursor persistence, and at-least-once callback delivery.
use crate::{
    config::LoadedConfig,
    daemon,
    events_exec::{CallbackExecutor, CursorStore, ExecMode},
    ipc::{Failure, Operation, StreamFrameKind},
    mcp::{
        EventNotification, EventStreamHandle, EventStreamReceiver, McpConnection, PollEventsParams,
        StreamEventsParams,
    },
    runtime::{self, DaemonEventStream, Paths},
};
use anyhow::Result;
use serde_json::{Map, Value};
use std::{
    collections::hash_map::RandomState,
    future::Future,
    hash::{BuildHasher, Hasher},
    io::Write,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

pub const ACTIVE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_HEARTBEAT_MS: u64 = 30_000;
pub const MIN_POLL_INTERVAL_MS: u64 = 1_000;
pub const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamMode {
    Auto,
    Push,
    Poll,
}

#[derive(Clone, Debug)]
pub struct StreamConfig {
    pub server: String,
    pub event: String,
    pub arguments: Option<Map<String, Value>>,
    pub mode: StreamMode,
    pub initial_cursor: Option<String>,
    pub cursor_file: Option<CursorStore>,
    pub max_age_ms: Option<u64>,
    pub heartbeat_ms: Option<u64>,
    pub once: bool,
    pub timeout: Option<Duration>,
    pub callback_argv: Option<Vec<String>>,
    pub exec_mode: ExecMode,
    pub exec_timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct StreamTimings {
    pub handshake_timeout: Duration,
    pub min_poll_interval: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for StreamTimings {
    fn default() -> Self {
        Self {
            handshake_timeout: ACTIVE_HANDSHAKE_TIMEOUT,
            min_poll_interval: Duration::from_millis(MIN_POLL_INTERVAL_MS),
            initial_backoff: INITIAL_BACKOFF,
            max_backoff: MAX_BACKOFF,
        }
    }
}

#[derive(Debug)]
pub enum RawStreamEvent {
    Notification {
        method: String,
        params: Option<Value>,
    },
    Error(Failure),
    End,
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait EventStreamChannel: Send {
    fn next_item(&mut self) -> BoxFuture<'_, Result<Option<RawStreamEvent>>>;
    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}

pub trait EventTransport: Send {
    fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Vec<Value>>>;
    fn poll_events(
        &mut self,
        params: PollEventsParams,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Value>>;
    fn open_stream(
        &mut self,
        params: StreamEventsParams,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>>;
}

// --- Direct (oneshot) transport ---

struct DirectChannel {
    handle: EventStreamHandle,
    rx: EventStreamReceiver,
}

impl EventStreamChannel for DirectChannel {
    fn next_item(&mut self) -> BoxFuture<'_, Result<Option<RawStreamEvent>>> {
        Box::pin(async move {
            match self.rx.recv().await {
                Some(leased) => {
                    let (EventNotification { method, params, .. }, lease) = leased.into_parts();
                    drop(lease);
                    Ok(Some(RawStreamEvent::Notification { method, params }))
                }
                None => Ok(Some(RawStreamEvent::End)),
            }
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let _ = self.handle.cancel().await;
        })
    }
}

pub struct DirectTransport {
    connection: McpConnection,
}

impl DirectTransport {
    pub fn new(connection: McpConnection) -> Self {
        Self { connection }
    }

    pub fn into_connection(self) -> McpConnection {
        self.connection
    }
}

impl EventTransport for DirectTransport {
    fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Vec<Value>>> {
        Box::pin(async move {
            let events = self
                .connection
                .list_events(budget)
                .await
                .map_err(|e| anyhow::Error::from(daemon::mcp_error(e, false)))?;
            let (events, lease) = events.into_parts();
            drop(lease);
            Ok(events)
        })
    }

    fn poll_events(
        &mut self,
        params: PollEventsParams,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let polled = self
                .connection
                .poll_events(params, budget)
                .await
                .map_err(|e| anyhow::Error::from(daemon::mcp_error(e, false)))?;
            let (value, lease) = polled.into_parts();
            drop(lease);
            Ok(value)
        })
    }

    fn open_stream(
        &mut self,
        params: StreamEventsParams,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
        Box::pin(async move {
            let (handle, rx) = self
                .connection
                .open_event_stream(params, budget)
                .await
                .map_err(|e| anyhow::Error::from(daemon::mcp_error(e, false)))?;
            Ok(Box::new(DirectChannel { handle, rx }) as Box<dyn EventStreamChannel>)
        })
    }
}

// --- Daemon IPC v4 transport ---

struct DaemonChannel {
    stream: DaemonEventStream,
}

impl EventStreamChannel for DaemonChannel {
    fn next_item(&mut self) -> BoxFuture<'_, Result<Option<RawStreamEvent>>> {
        Box::pin(async move {
            match self.stream.next_frame().await? {
                Some((StreamFrameKind::Notification { method, params }, lease)) => {
                    drop(lease);
                    Ok(Some(RawStreamEvent::Notification { method, params }))
                }
                Some((StreamFrameKind::Error { error }, lease)) => {
                    drop(lease);
                    Ok(Some(RawStreamEvent::Error(error)))
                }
                Some((StreamFrameKind::End, lease)) => {
                    drop(lease);
                    Ok(Some(RawStreamEvent::End))
                }
                None => Ok(None),
            }
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            drop(self);
        })
    }
}

pub struct DaemonTransport {
    paths: Paths,
    loaded: LoadedConfig,
    server: String,
}

impl DaemonTransport {
    pub fn new(paths: Paths, loaded: LoadedConfig, server: String) -> Self {
        Self {
            paths,
            loaded,
            server,
        }
    }
}

impl EventTransport for DaemonTransport {
    fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Vec<Value>>> {
        Box::pin(async move {
            let req = self.paths.request(
                &self.loaded.digest,
                Operation::EventsList {
                    server: self.server.clone(),
                },
                budget,
            )?;
            let (resp, lease) = runtime::exchange(&self.paths, &req).await?;
            let result = resp.result.unwrap_or(Value::Null);
            drop(lease);
            let events = result
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            Ok(events)
        })
    }

    fn poll_events(
        &mut self,
        params: PollEventsParams,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let req = self.paths.request(
                &self.loaded.digest,
                Operation::EventsPoll {
                    server: self.server.clone(),
                    event: params.name,
                    arguments: params.arguments,
                    cursor: params.cursor,
                    max_age_ms: params.max_age_ms,
                    max_events: params.max_events,
                },
                budget,
            )?;
            let (resp, lease) = runtime::exchange(&self.paths, &req).await?;
            let result = resp.result.unwrap_or(Value::Null);
            drop(lease);
            Ok(result)
        })
    }

    fn open_stream(
        &mut self,
        params: StreamEventsParams,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
        Box::pin(async move {
            let req = self.paths.request(
                &self.loaded.digest,
                Operation::EventsStream {
                    server: self.server.clone(),
                    event: params.name,
                    arguments: params.arguments,
                    cursor: params.cursor,
                    max_age_ms: params.max_age_ms,
                    heartbeat_ms: params.heartbeat_ms,
                },
                budget,
            )?;
            let stream = runtime::open_event_stream(&self.paths, &req).await?;
            Ok(Box::new(DaemonChannel { stream }) as Box<dyn EventStreamChannel>)
        })
    }
}

// --- Core stream runner ---

fn jittered_duration(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    if base_ms == 0 {
        return Duration::ZERO;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u128(nanos);
    hasher.write_u32(std::process::id());
    let jitter_span = (base_ms / 4).max(1);
    let jitter = hasher.finish() % jitter_span;
    Duration::from_millis(base_ms.saturating_add(jitter))
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

fn is_fatal_error(error: &anyhow::Error) -> bool {
    let Some(failure) = error.downcast_ref::<Failure>() else {
        return false;
    };
    if matches!(
        failure.kind.as_str(),
        "unknown_server"
            | "invalid_event"
            | "invalid_arguments"
            | "daemon_mismatch"
            | "session_lost"
            | "stopping"
            | "stream_terminated"
    ) {
        return true;
    }
    if let Some(rpc) = &failure.rpc_error
        && let Some(code) = rpc.get("code").and_then(Value::as_i64)
    {
        // -32601 MethodNotFound, -32602 InvalidParams, -32023 NotFound, -32024 Forbidden, -32026 Unsupported
        return matches!(code, -32601 | -32602 | -32023 | -32024 | -32026);
    }
    false
}

fn is_unsupported_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<Failure>()
        .and_then(|f| f.rpc_error.as_ref())
        .and_then(|rpc| rpc.get("code"))
        .and_then(Value::as_i64)
        == Some(-32026)
}

fn select_auto_mode(events: &[Value], event_name: &str) -> StreamMode {
    for event in events {
        if event.get("name").and_then(Value::as_str) == Some(event_name)
            && let Some(modes) = event.get("delivery").and_then(Value::as_array)
        {
            let has_push = modes.iter().any(|m| m.as_str() == Some("push"));
            let has_poll = modes.iter().any(|m| m.as_str() == Some("poll"));
            if has_push {
                return StreamMode::Push;
            }
            if has_poll {
                return StreamMode::Poll;
            }
        }
    }
    StreamMode::Push
}

async fn sleep_interruptible(
    duration: Duration,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
) -> Result<()> {
    let wait = match deadline {
        Some(dl) => {
            let rem = dl.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(Failure::new(5, "timeout", "event stream timeout exceeded").into());
            }
            duration.min(rem)
        }
        None => duration,
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Failure::new(130, "cancelled", "interrupted").into()),
        _ = sleep(wait) => {
            if deadline.is_some_and(|dl| Instant::now() >= dl) {
                Err(Failure::new(5, "timeout", "event stream timeout exceeded").into())
            } else {
                Ok(())
            }
        }
    }
}

fn remaining_budget(default: Duration, deadline: Option<Instant>) -> Result<Duration> {
    match deadline {
        Some(dl) => {
            let rem = dl.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                Err(Failure::new(5, "timeout", "event stream timeout exceeded").into())
            } else {
                Ok(default.min(rem))
            }
        }
        None => Ok(default),
    }
}

struct Sink<'a> {
    executor: Option<CallbackExecutor>,
    stdout: Option<&'a mut (dyn Write + Send)>,
}

impl<'a> Sink<'a> {
    async fn deliver_with_retry(
        &mut self,
        payload: &Value,
        timings: StreamTimings,
        deadline: Option<Instant>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let line = serde_json::to_string(payload)?;
        let Some(executor) = self.executor.as_mut() else {
            if let Some(writer) = self.stdout.as_mut() {
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            } else {
                let mut stdout = std::io::stdout().lock();
                stdout.write_all(line.as_bytes())?;
                stdout.write_all(b"\n")?;
                stdout.flush()?;
            }
            return Ok(());
        };

        let mut backoff = timings.initial_backoff;
        loop {
            if cancel.is_cancelled() {
                return Err(Failure::new(130, "cancelled", "interrupted").into());
            }
            if deadline.is_some_and(|dl| Instant::now() >= dl) {
                return Err(Failure::new(5, "timeout", "event stream timeout exceeded").into());
            }
            let deliver_res = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(Failure::new(130, "cancelled", "interrupted").into());
                }
                res = executor.deliver(&line) => res,
            };
            match deliver_res {
                Ok(()) => return Ok(()),
                Err(err) => {
                    eprintln!(
                        "mcpcli: callback failed ({err}); retrying in {}ms",
                        backoff.as_millis()
                    );
                    sleep_interruptible(jittered_duration(backoff), deadline, cancel).await?;
                    backoff = next_backoff(backoff, timings.max_backoff);
                }
            }
        }
    }

    async fn shutdown(&mut self) {
        if let Some(executor) = self.executor.as_mut() {
            executor.shutdown().await;
        }
    }
}

fn persist_cursor(
    cursor_store: Option<&CursorStore>,
    current: &mut Option<String>,
    candidate: &str,
) -> Result<()> {
    if candidate.is_empty() || current.as_deref() == Some(candidate) {
        return Ok(());
    }
    if let Some(store) = cursor_store {
        store.write(candidate)?;
    }
    *current = Some(candidate.to_owned());
    Ok(())
}

fn inject_truncated(mut event: Value, pending_truncated: bool) -> Value {
    if pending_truncated && let Value::Object(ref mut map) = event {
        map.insert("truncated".into(), Value::Bool(true));
    }
    event
}

/// Run the `events stream` loop until `--once` completes, `--timeout` expires,
/// `notifications/events/terminated` is received, or `cancel` is triggered.
pub async fn run_stream<T: EventTransport>(
    transport: &mut T,
    config: &StreamConfig,
    timings: StreamTimings,
    cancel: CancellationToken,
    stdout_override: Option<&mut (dyn Write + Send)>,
) -> Result<()> {
    let deadline = config.timeout.map(|d| Instant::now() + d);
    let mut current_cursor = match (&config.initial_cursor, &config.cursor_file) {
        (Some(c), _) => Some(c.clone()),
        (None, Some(store)) => store.read()?,
        (None, None) => None,
    };

    let executor = match &config.callback_argv {
        Some(argv) => Some(
            CallbackExecutor::new(argv.clone(), config.exec_mode, config.exec_timeout)
                .map_err(|e| Failure::new(2, "invalid_callback", e.to_string()))?,
        ),
        None => None,
    };
    let mut sink = Sink {
        executor,
        stdout: stdout_override,
    };

    let outcome = run_stream_inner(
        transport,
        config,
        timings,
        deadline,
        &cancel,
        &mut current_cursor,
        &mut sink,
    )
    .await;

    sink.shutdown().await;
    outcome
}

async fn run_stream_inner<T: EventTransport>(
    transport: &mut T,
    config: &StreamConfig,
    timings: StreamTimings,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    current_cursor: &mut Option<String>,
    sink: &mut Sink<'_>,
) -> Result<()> {
    let mut effective_mode = match config.mode {
        StreamMode::Push => StreamMode::Push,
        StreamMode::Poll => StreamMode::Poll,
        StreamMode::Auto => {
            let list_budget = remaining_budget(timings.handshake_timeout, deadline)?;
            match transport.list_events(list_budget).await {
                Ok(events) => select_auto_mode(&events, &config.event),
                Err(err) if is_fatal_error(&err) => return Err(err),
                Err(_) => StreamMode::Push,
            }
        }
    };

    if effective_mode == StreamMode::Push {
        match run_push_loop(
            transport,
            config,
            timings,
            deadline,
            cancel,
            current_cursor,
            sink,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if config.mode == StreamMode::Auto && is_unsupported_error(&err) => {
                eprintln!(
                    "mcpcli: event {:?} does not support push; falling back to poll",
                    config.event
                );
                effective_mode = StreamMode::Poll;
            }
            Err(err) => return Err(err),
        }
    }

    if effective_mode == StreamMode::Poll {
        run_poll_loop(
            transport,
            config,
            timings,
            deadline,
            cancel,
            current_cursor,
            sink,
        )
        .await?;
    }
    Ok(())
}

async fn run_poll_loop<T: EventTransport>(
    transport: &mut T,
    config: &StreamConfig,
    timings: StreamTimings,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    current_cursor: &mut Option<String>,
    sink: &mut Sink<'_>,
) -> Result<()> {
    let mut backoff = timings.initial_backoff;
    let mut pending_truncated = false;
    let mut initial_max_age = config.max_age_ms;

    loop {
        if cancel.is_cancelled() {
            return Err(Failure::new(130, "cancelled", "interrupted").into());
        }
        let poll_budget = remaining_budget(timings.handshake_timeout, deadline)?;
        let params = PollEventsParams {
            name: config.event.clone(),
            arguments: config.arguments.clone(),
            cursor: current_cursor.clone(),
            max_age_ms: current_cursor.as_ref().and(initial_max_age),
            max_events: config.once.then_some(1),
        };
        let polled = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Failure::new(130, "cancelled", "interrupted").into()),
            res = transport.poll_events(params, poll_budget) => res,
        };
        let response = match polled {
            Ok(val) => {
                backoff = timings.initial_backoff;
                initial_max_age = None;
                val
            }
            Err(err) if is_fatal_error(&err) => return Err(err),
            Err(err) => {
                eprintln!(
                    "mcpcli: poll failed ({err:#}); retrying in {}ms",
                    backoff.as_millis()
                );
                sleep_interruptible(jittered_duration(backoff), deadline, cancel).await?;
                backoff = next_backoff(backoff, timings.max_backoff);
                continue;
            }
        };

        if response.get("truncated").and_then(Value::as_bool) == Some(true) {
            eprintln!(
                "mcpcli: event stream {:?} truncated on server; some events may have been missed",
                config.event
            );
            pending_truncated = true;
        }

        let events = response
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let next_cursor = response
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let has_more = response
            .get("hasMore")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_poll_ms = response
            .get("nextPollMs")
            .and_then(Value::as_u64)
            .unwrap_or(MIN_POLL_INTERVAL_MS);

        let batch_len = events.len();
        let mut delivered_any = false;
        for event in events {
            let payload = inject_truncated(event, pending_truncated);
            sink.deliver_with_retry(&payload, timings, deadline, cancel)
                .await?;
            pending_truncated = false;
            delivered_any = true;
            if config.once {
                break;
            }
        }

        if (!config.once || batch_len <= 1)
            && let Some(c) = next_cursor.as_deref()
        {
            persist_cursor(config.cursor_file.as_ref(), current_cursor, c)?;
        }

        if config.once && delivered_any {
            return Ok(());
        }

        if has_more {
            continue;
        }

        let wait = Duration::from_millis(next_poll_ms).max(timings.min_poll_interval);
        sleep_interruptible(wait, deadline, cancel).await?;
    }
}

async fn run_push_loop<T: EventTransport>(
    transport: &mut T,
    config: &StreamConfig,
    timings: StreamTimings,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    current_cursor: &mut Option<String>,
    sink: &mut Sink<'_>,
) -> Result<()> {
    let mut backoff = timings.initial_backoff;
    let mut pending_truncated = false;
    let mut initial_max_age = config.max_age_ms;

    loop {
        if cancel.is_cancelled() {
            return Err(Failure::new(130, "cancelled", "interrupted").into());
        }
        let open_budget = remaining_budget(timings.handshake_timeout, deadline)?;
        let params = StreamEventsParams {
            name: config.event.clone(),
            arguments: config.arguments.clone(),
            cursor: current_cursor.clone(),
            max_age_ms: current_cursor.as_ref().and(initial_max_age),
            heartbeat_ms: config.heartbeat_ms,
        };
        let open_res = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Failure::new(130, "cancelled", "interrupted").into()),
            res = transport.open_stream(params, open_budget) => res,
        };
        let mut channel = match open_res {
            Ok(ch) => ch,
            Err(err) if is_fatal_error(&err) => return Err(err),
            Err(err) => {
                eprintln!(
                    "mcpcli: failed to open event stream ({err:#}); reconnecting in {}ms",
                    backoff.as_millis()
                );
                sleep_interruptible(jittered_duration(backoff), deadline, cancel).await?;
                backoff = next_backoff(backoff, timings.max_backoff);
                continue;
            }
        };

        let mut active_received = false;
        let mut heartbeat_ms = config.heartbeat_ms.unwrap_or(DEFAULT_HEARTBEAT_MS);
        let handshake_deadline = Instant::now() + timings.handshake_timeout;
        let reconnect_reason: String;

        loop {
            let idle_wait = if active_received {
                Duration::from_millis(heartbeat_ms.saturating_mul(3).max(1))
            } else {
                handshake_deadline.saturating_duration_since(Instant::now())
            };
            let step_wait = match remaining_budget(idle_wait, deadline) {
                Ok(w) => w,
                Err(timeout_err) => {
                    channel.close().await;
                    return Err(timeout_err);
                }
            };

            let frame_res = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    channel.close().await;
                    return Err(Failure::new(130, "cancelled", "interrupted").into());
                }
                res = timeout(step_wait, channel.next_item()) => res,
            };

            let item = match frame_res {
                Err(_) => {
                    if deadline.is_some_and(|dl| Instant::now() >= dl) {
                        channel.close().await;
                        return Err(
                            Failure::new(5, "timeout", "event stream timeout exceeded").into()
                        );
                    }
                    reconnect_reason = if active_received {
                        format!(
                            "no event frames received for {}ms (3x heartbeat)",
                            heartbeat_ms.saturating_mul(3)
                        )
                    } else {
                        format!(
                            "timed out waiting {}ms for notifications/events/active",
                            timings.handshake_timeout.as_millis()
                        )
                    };
                    break;
                }
                Ok(Err(err)) => {
                    if is_fatal_error(&err) {
                        channel.close().await;
                        return Err(err);
                    }
                    reconnect_reason = format!("stream read error: {err:#}");
                    break;
                }
                Ok(Ok(None)) | Ok(Ok(Some(RawStreamEvent::End))) => {
                    reconnect_reason = "event stream closed by server".to_owned();
                    break;
                }
                Ok(Ok(Some(RawStreamEvent::Error(failure)))) => {
                    let err: anyhow::Error = failure.into();
                    if is_fatal_error(&err) {
                        channel.close().await;
                        return Err(err);
                    }
                    reconnect_reason = format!("stream error: {err:#}");
                    break;
                }
                Ok(Ok(Some(RawStreamEvent::Notification { method, params }))) => (method, params),
            };

            let (method, params) = item;
            match method.as_str() {
                "notifications/events/active" => {
                    let was_active = active_received;
                    active_received = true;
                    backoff = timings.initial_backoff;
                    initial_max_age = None;
                    if let Some(hb) = params
                        .as_ref()
                        .and_then(|p| p.get("heartbeatMs"))
                        .and_then(Value::as_u64)
                    {
                        heartbeat_ms = hb;
                    }
                    if params
                        .as_ref()
                        .and_then(|p| p.get("truncated"))
                        .and_then(Value::as_bool)
                        == Some(true)
                    {
                        if was_active {
                            eprintln!(
                                "mcpcli: event stream {:?} experienced mid-stream truncation; next event will carry truncated=true",
                                config.event
                            );
                        } else {
                            eprintln!(
                                "mcpcli: event stream {:?} truncated on server; some events may have been missed",
                                config.event
                            );
                        }
                        pending_truncated = true;
                    }
                    if let Some(c) = params
                        .as_ref()
                        .and_then(|p| p.get("cursor"))
                        .and_then(Value::as_str)
                        && let Err(err) =
                            persist_cursor(config.cursor_file.as_ref(), current_cursor, c)
                    {
                        channel.close().await;
                        return Err(err);
                    }
                }
                "notifications/events/heartbeat" => {
                    if let Some(c) = params
                        .as_ref()
                        .and_then(|p| p.get("cursor"))
                        .and_then(Value::as_str)
                        && let Err(err) =
                            persist_cursor(config.cursor_file.as_ref(), current_cursor, c)
                    {
                        channel.close().await;
                        return Err(err);
                    }
                }
                "notifications/events/event" => {
                    let event_obj = params.unwrap_or_else(|| Value::Object(Map::new()));
                    let event_cursor = event_obj
                        .get("cursor")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let payload = inject_truncated(event_obj, pending_truncated);
                    if let Err(err) = sink
                        .deliver_with_retry(&payload, timings, deadline, cancel)
                        .await
                    {
                        channel.close().await;
                        return Err(err);
                    }
                    pending_truncated = false;
                    if let Some(c) = event_cursor.as_deref()
                        && let Err(err) =
                            persist_cursor(config.cursor_file.as_ref(), current_cursor, c)
                    {
                        channel.close().await;
                        return Err(err);
                    }
                    if config.once {
                        channel.close().await;
                        return Ok(());
                    }
                }
                "notifications/events/error" => {
                    let msg = params
                        .as_ref()
                        .and_then(|p| p.get("error"))
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("recoverable upstream event stream error");
                    eprintln!("mcpcli: event stream error: {msg}");
                }
                "notifications/events/terminated" => {
                    let msg = params
                        .as_ref()
                        .and_then(|p| p.get("error"))
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("event stream terminated by server");
                    let code = params
                        .as_ref()
                        .and_then(|p| p.get("error"))
                        .and_then(|e| e.get("code"))
                        .and_then(Value::as_i64);
                    let detail = match code {
                        Some(c) => format!("event stream terminated ({c}): {msg}"),
                        None => format!("event stream terminated: {msg}"),
                    };
                    channel.close().await;
                    return Err(Failure::new(4, "stream_terminated", detail).into());
                }
                _ => {}
            }
        }

        channel.close().await;
        eprintln!(
            "mcpcli: {reconnect_reason}; reconnecting in {}ms",
            backoff.as_millis()
        );
        sleep_interruptible(jittered_duration(backoff), deadline, cancel).await?;
        backoff = next_backoff(backoff, timings.max_backoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;

    enum MockStep {
        Yield(RawStreamEvent),
        DelayThen(Duration, RawStreamEvent),
    }

    struct MockChannel {
        steps: VecDeque<MockStep>,
    }

    impl EventStreamChannel for MockChannel {
        fn next_item(&mut self) -> BoxFuture<'_, Result<Option<RawStreamEvent>>> {
            Box::pin(async move {
                match self.steps.pop_front() {
                    Some(MockStep::Yield(ev)) => Ok(Some(ev)),
                    Some(MockStep::DelayThen(d, ev)) => {
                        sleep(d).await;
                        Ok(Some(ev))
                    }
                    None => Ok(Some(RawStreamEvent::End)),
                }
            })
        }

        fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    struct MockTransport {
        listed: Vec<Value>,
        polls: VecDeque<Value>,
        poll_cursors: Vec<Option<String>>,
        streams: VecDeque<VecDeque<MockStep>>,
        stream_cursors: Vec<Option<String>>,
    }

    impl EventTransport for MockTransport {
        fn list_events(&mut self, _budget: Duration) -> BoxFuture<'_, Result<Vec<Value>>> {
            let listed = self.listed.clone();
            Box::pin(async move { Ok(listed) })
        }

        fn poll_events(
            &mut self,
            params: PollEventsParams,
            _budget: Duration,
        ) -> BoxFuture<'_, Result<Value>> {
            self.poll_cursors.push(params.cursor);
            let next = self
                .polls
                .pop_front()
                .unwrap_or_else(|| json!({"events": [], "hasMore": false, "nextPollMs": 1000}));
            Box::pin(async move { Ok(next) })
        }

        fn open_stream(
            &mut self,
            params: StreamEventsParams,
            _budget: Duration,
        ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
            self.stream_cursors.push(params.cursor);
            let steps = self.streams.pop_front().unwrap_or_default();
            Box::pin(
                async move { Ok(Box::new(MockChannel { steps }) as Box<dyn EventStreamChannel>) },
            )
        }
    }

    fn base_config(cursor_file: Option<CursorStore>) -> StreamConfig {
        StreamConfig {
            server: "srv".into(),
            event: "pr.merged".into(),
            arguments: None,
            mode: StreamMode::Auto,
            initial_cursor: None,
            cursor_file,
            max_age_ms: None,
            heartbeat_ms: Some(50),
            once: false,
            timeout: Some(Duration::from_secs(5)),
            callback_argv: None,
            exec_mode: ExecMode::Oneshot,
            exec_timeout: Duration::from_secs(5),
        }
    }

    fn fast_timings() -> StreamTimings {
        StreamTimings {
            handshake_timeout: Duration::from_millis(80),
            min_poll_interval: Duration::from_millis(20),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
        }
    }

    #[tokio::test]
    async fn auto_mode_downgrades_to_poll_handles_has_more_and_advances_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = CursorStore::new(dir.path().join("poll.cursor"));
        let mut transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["poll"]})],
            polls: VecDeque::from([
                json!({
                    "events": [],
                    "cursor": "b:1",
                    "truncated": false,
                    "hasMore": true,
                    "nextPollMs": 5000
                }),
                json!({
                    "events": [{"eventId": "e1", "name": "pr.merged", "data": {"n": 1}}],
                    "cursor": "b:2",
                    "truncated": false,
                    "hasMore": false,
                    "nextPollMs": 100
                }),
            ]),
            poll_cursors: Vec::new(),
            streams: VecDeque::new(),
            stream_cursors: Vec::new(),
        };
        let mut cfg = base_config(Some(store.clone()));
        cfg.once = true;
        let mut out = Vec::new();
        run_stream(
            &mut transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            Some(&mut out),
        )
        .await
        .unwrap();

        assert_eq!(transport.poll_cursors, vec![None, Some("b:1".into())]);
        assert_eq!(store.read().unwrap().as_deref(), Some("b:2"));
        let rendered = String::from_utf8(out).unwrap();
        let line: Value = serde_json::from_str(rendered.trim()).unwrap();
        assert_eq!(line["eventId"], "e1");
    }

    #[tokio::test]
    async fn push_detects_dead_stream_advances_heartbeat_cursor_and_passes_midstream_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = CursorStore::new(dir.path().join("push.cursor"));
        let mut transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["push", "poll"]})],
            polls: VecDeque::new(),
            poll_cursors: Vec::new(),
            streams: VecDeque::from([
                // Stream 1: active -> heartbeat advances cursor -> stalls > 3x heartbeatMs (dead stream)
                VecDeque::from([
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/active".into(),
                        params: Some(
                            json!({"cursor": "b:10", "truncated": false, "heartbeatMs": 20}),
                        ),
                    }),
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/heartbeat".into(),
                        params: Some(json!({"cursor": "b:11"})),
                    }),
                    MockStep::DelayThen(Duration::from_millis(200), RawStreamEvent::End),
                ]),
                // Stream 2: reconnects with cursor b:11 -> active -> second active(truncated:true) -> event
                VecDeque::from([
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/active".into(),
                        params: Some(
                            json!({"cursor": "b:11", "truncated": false, "heartbeatMs": 50}),
                        ),
                    }),
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/active".into(),
                        params: Some(
                            json!({"cursor": "b:15", "truncated": true, "heartbeatMs": 50}),
                        ),
                    }),
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/event".into(),
                        params: Some(
                            json!({"eventId": "e16", "name": "pr.merged", "data": {}, "cursor": "b:16"}),
                        ),
                    }),
                ]),
            ]),
            stream_cursors: Vec::new(),
        };

        let mut cfg = base_config(Some(store.clone()));
        cfg.once = true;
        let mut out = Vec::new();
        run_stream(
            &mut transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            Some(&mut out),
        )
        .await
        .unwrap();

        assert_eq!(transport.stream_cursors, vec![None, Some("b:11".into())]);
        assert_eq!(store.read().unwrap().as_deref(), Some("b:16"));
        let line: Value = serde_json::from_str(std::str::from_utf8(&out).unwrap().trim()).unwrap();
        assert_eq!(line["eventId"], "e16");
        assert_eq!(line["truncated"], true);
    }

    #[tokio::test]
    async fn push_exits_nonzero_on_terminated_notification() {
        let mut transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["push"]})],
            polls: VecDeque::new(),
            poll_cursors: Vec::new(),
            streams: VecDeque::from([VecDeque::from([
                MockStep::Yield(RawStreamEvent::Notification {
                    method: "notifications/events/active".into(),
                    params: Some(json!({"cursor": "b:1", "truncated": false, "heartbeatMs": 50})),
                }),
                MockStep::Yield(RawStreamEvent::Notification {
                    method: "notifications/events/terminated".into(),
                    params: Some(json!({"error": {"code": -32024, "message": "revoked"}})),
                }),
            ])]),
            stream_cursors: Vec::new(),
        };

        let cfg = base_config(None);
        let mut out = Vec::new();
        let err = run_stream(
            &mut transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            Some(&mut out),
        )
        .await
        .unwrap_err();
        let failure = err.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.kind, "stream_terminated");
        assert_ne!(failure.code, 0);
        assert!(failure.message.contains("revoked"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn callback_retries_on_failure_and_only_advances_cursor_after_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let store = CursorStore::new(dir.path().join("cb.cursor"));
        let flag = dir.path().join("failed_once");
        // Fail on first attempt, succeed on second attempt.
        let script = format!(
            "cat >/dev/null; if [ ! -f '{f}' ]; then touch '{f}'; exit 1; fi; exit 0",
            f = flag.display()
        );
        let mut transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["push"]})],
            polls: VecDeque::new(),
            poll_cursors: Vec::new(),
            streams: VecDeque::from([VecDeque::from([
                MockStep::Yield(RawStreamEvent::Notification {
                    method: "notifications/events/active".into(),
                    params: Some(json!({"cursor": "b:1", "truncated": false, "heartbeatMs": 50})),
                }),
                MockStep::Yield(RawStreamEvent::Notification {
                    method: "notifications/events/event".into(),
                    params: Some(
                        json!({"eventId": "e2", "name": "pr.merged", "data": {}, "cursor": "b:2"}),
                    ),
                }),
            ])]),
            stream_cursors: Vec::new(),
        };

        let mut cfg = base_config(Some(store.clone()));
        cfg.once = true;
        cfg.callback_argv = Some(vec!["/bin/sh".into(), "-c".into(), script]);
        run_stream(
            &mut transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();

        assert!(flag.exists());
        assert_eq!(store.read().unwrap().as_deref(), Some("b:2"));
    }
}
