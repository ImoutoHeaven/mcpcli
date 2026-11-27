//! Foreground `mcpcli events stream` engine: push/poll auto-selection, dead-stream
//! reconnect, cursor persistence, and at-least-once callback delivery.
use crate::{
    budget::{ByteBudget, ByteLease, Leased},
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
    io::{self, Write},
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    time::{Instant, sleep, timeout},
};
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
    pub deadline: Option<Instant>,
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
    fn next_item(&mut self) -> BoxFuture<'_, Result<Option<Leased<RawStreamEvent>>>>;
    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}

pub trait EventTransport: Send {
    fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Leased<Vec<Value>>>>;
    fn poll_events(
        &mut self,
        params: Leased<PollEventsParams>,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Leased<Value>>>;
    fn open_stream(
        &mut self,
        params: Leased<StreamEventsParams>,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>>;
}

// --- Direct (oneshot) transport ---

struct DirectChannel {
    handle: EventStreamHandle,
    rx: EventStreamReceiver,
}

impl EventStreamChannel for DirectChannel {
    fn next_item(&mut self) -> BoxFuture<'_, Result<Option<Leased<RawStreamEvent>>>> {
        Box::pin(async move {
            match self.rx.recv().await {
                Some(leased) => {
                    let (EventNotification { method, params, .. }, lease) = leased.into_parts();
                    Ok(Some(Leased::new(
                        RawStreamEvent::Notification { method, params },
                        lease,
                    )))
                }
                None => Ok(None),
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
    server: Option<String>,
}

impl DirectTransport {
    pub fn new(connection: McpConnection) -> Self {
        Self {
            connection,
            server: None,
        }
    }

    pub fn with_server(connection: McpConnection, server: String) -> Self {
        Self {
            connection,
            server: Some(server),
        }
    }

    pub fn into_connection(self) -> McpConnection {
        self.connection
    }

    async fn check_connection(&self) -> Result<()> {
        if self.connection.is_closed() {
            return Err(self.request_error(self.connection.closed_error().await));
        }
        Ok(())
    }

    fn request_error(&self, error: anyhow::Error) -> anyhow::Error {
        let mut failure = daemon::mcp_error_for_server(error, false, self.server.as_deref());
        if self.connection.is_closed() {
            failure.code = 6;
            failure.kind = "session_lost".into();
        }
        failure.into()
    }
}

impl EventTransport for DirectTransport {
    fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Leased<Vec<Value>>>> {
        Box::pin(async move {
            self.check_connection().await?;
            let events = self
                .connection
                .list_events(budget)
                .await
                .map_err(|e| self.request_error(e))?;
            Ok(events)
        })
    }

    fn poll_events(
        &mut self,
        params: Leased<PollEventsParams>,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Leased<Value>>> {
        Box::pin(async move {
            self.check_connection().await?;
            let (params, _params_lease) = params.into_parts();
            let polled = self
                .connection
                .poll_events(params, budget)
                .await
                .map_err(|e| self.request_error(e))?;
            Ok(polled)
        })
    }

    fn open_stream(
        &mut self,
        params: Leased<StreamEventsParams>,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
        Box::pin(async move {
            self.check_connection().await?;
            let (params, _params_lease) = params.into_parts();
            let (handle, rx) = self
                .connection
                .open_event_stream(params, budget)
                .await
                .map_err(|e| self.request_error(e))?;
            Ok(Box::new(DirectChannel { handle, rx }) as Box<dyn EventStreamChannel>)
        })
    }
}

// --- Daemon IPC v4 transport ---

struct DaemonChannel {
    stream: DaemonEventStream,
}

impl EventStreamChannel for DaemonChannel {
    fn next_item(&mut self) -> BoxFuture<'_, Result<Option<Leased<RawStreamEvent>>>> {
        Box::pin(async move {
            match self.stream.next_frame().await? {
                Some((StreamFrameKind::Notification { method, params }, lease)) => Ok(Some(
                    Leased::new(RawStreamEvent::Notification { method, params }, lease),
                )),
                Some((StreamFrameKind::Error { error }, lease)) => {
                    Ok(Some(Leased::new(RawStreamEvent::Error(error), lease)))
                }
                Some((StreamFrameKind::End, lease)) => {
                    Ok(Some(Leased::new(RawStreamEvent::End, lease)))
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
    fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Leased<Vec<Value>>>> {
        Box::pin(async move {
            let req = self.paths.request(
                &self.loaded.digest,
                Operation::EventsList {
                    server: self.server.clone(),
                },
                budget,
            )?;
            let (resp, lease) = runtime::exchange(&self.paths, &req).await?;
            let mut result = resp.result.unwrap_or(Value::Null);
            let events = result
                .get_mut("events")
                .and_then(Value::as_array_mut)
                .map(std::mem::take)
                .unwrap_or_default();
            Ok(Leased::new(events, lease))
        })
    }

    fn poll_events(
        &mut self,
        params: Leased<PollEventsParams>,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Leased<Value>>> {
        Box::pin(async move {
            let (params, params_lease) = params.into_parts();
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
            let req = Leased::new(req, params_lease);
            let (resp, lease) = runtime::exchange(&self.paths, &req).await?;
            let result = resp.result.unwrap_or(Value::Null);
            Ok(Leased::new(result, lease))
        })
    }

    fn open_stream(
        &mut self,
        params: Leased<StreamEventsParams>,
        budget: Duration,
    ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
        Box::pin(async move {
            let (params, params_lease) = params.into_parts();
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
            let req = Leased::new(req, params_lease);
            let stream = runtime::open_event_stream(&self.paths, &req).await?;
            Ok(Box::new(DaemonChannel { stream }) as Box<dyn EventStreamChannel>)
        })
    }
}

// --- Core stream runner ---

fn request_params_lease(config: &StreamConfig, cursor: &Option<String>) -> Result<ByteLease> {
    let bytes = crate::json::serialized_len(&config.arguments)?
        .saturating_add(config.event.len())
        .saturating_add(config.server.len())
        .saturating_add(cursor.as_ref().map_or(0, String::len));
    ByteBudget::global().try_acquire(bytes).ok_or_else(|| {
        Failure::new(
            1,
            "memory_limit_exceeded",
            "event arguments exceed the active memory limit",
        )
        .into()
    })
}

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
    if failure.kind == "mcp_protocol" && failure.rpc_error.is_none() {
        return true;
    }
    if matches!(
        failure.kind.as_str(),
        "unknown_server"
            | "invalid_event"
            | "invalid_arguments"
            | "daemon_mismatch"
            | "session_lost"
            | "stopping"
            | "stream_terminated"
            | "auth_required"
            | "insufficient_scope"
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
        .filter(|failure| failure.kind == "mcp_protocol")
        .and_then(|failure| failure.rpc_error.as_ref())
        .and_then(|rpc| rpc.get("code"))
        .and_then(Value::as_i64)
        == Some(-32026)
}

fn select_auto_mode(events: &[Value], event_name: &str) -> Result<StreamMode> {
    for event in events {
        if event.get("name").and_then(Value::as_str) == Some(event_name)
            && let Some(modes) = event.get("delivery").and_then(Value::as_array)
        {
            let has_push = modes.iter().any(|m| m.as_str() == Some("push"));
            let has_poll = modes.iter().any(|m| m.as_str() == Some("poll"));
            if has_push {
                return Ok(StreamMode::Push);
            }
            if has_poll {
                return Ok(StreamMode::Poll);
            }
            if !modes.is_empty() {
                let declared: Vec<&str> = modes.iter().filter_map(Value::as_str).collect();
                let message = if declared.is_empty() {
                    format!("event {event_name:?} does not support push or poll delivery")
                } else {
                    format!(
                        "event {event_name:?} does not support push or poll delivery (declared: {})",
                        declared.join(", ")
                    )
                };
                return Err(Failure::new(4, "mcp_protocol", message).into());
            }
        }
    }
    Ok(StreamMode::Push)
}

fn check_running(deadline: Option<Instant>, cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        return Err(Failure::new(130, "cancelled", "interrupted").into());
    }
    if deadline.is_some_and(|dl| Instant::now() >= dl) {
        return Err(Failure::new(5, "timeout", "event stream timeout exceeded").into());
    }
    Ok(())
}

async fn wait_for_command<T>(
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    future: impl Future<Output = T>,
) -> Result<T> {
    check_running(deadline, cancel)?;
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(Failure::new(130, "cancelled", "interrupted").into()),
        _ = async {
            match deadline {
                Some(dl) => tokio::time::sleep_until(dl).await,
                None => std::future::pending::<()>().await,
            }
        } => return Err(Failure::new(5, "timeout", "event stream timeout exceeded").into()),
        result = future => result,
    };
    check_running(deadline, cancel)?;
    Ok(result)
}

async fn sleep_interruptible(
    duration: Duration,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
) -> Result<()> {
    wait_for_command(deadline, cancel, sleep(duration)).await
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

/// Keep each complete operation inside the foreground command's original deadline.
pub async fn wait_with_budget<T>(
    maximum: Duration,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    wait_for_command(deadline, cancel, timeout(maximum, future))
        .await?
        .map_err(|_| Failure::new(5, "timeout", "event stream timeout exceeded"))?
}

fn write_stdout_line(line: &str) -> io::Result<()> {
    // A blocked write must not hold Rust's global stdout lock during process shutdown.
    #[cfg(unix)]
    let mut stdout = {
        use std::os::fd::AsFd;
        std::fs::File::from(std::io::stdout().as_fd().try_clone_to_owned()?)
    };
    #[cfg(windows)]
    let mut stdout = {
        use std::os::windows::io::AsHandle;
        std::fs::File::from(std::io::stdout().as_handle().try_clone_to_owned()?)
    };
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::Console::{GetConsoleMode, WriteConsoleW};

        let mut mode = 0;
        // Named-pipe terminals need byte writes; only a console accepts WriteConsoleW.
        // SAFETY: stdout owns the handle and mode is valid writable storage.
        if unsafe { GetConsoleMode(stdout.as_raw_handle(), &mut mode) } != 0 {
            let mut chars = line.chars().peekable();
            let mut buffer = [0u16; 1024];
            while chars.peek().is_some() {
                let mut len = 0;
                while let Some(ch) = chars.peek() {
                    let needed = ch.len_utf16();
                    if len + needed > buffer.len() {
                        break;
                    }
                    chars.next().unwrap().encode_utf16(&mut buffer[len..]);
                    len += needed;
                }
                let mut written = 0;
                while written < len {
                    let mut count = 0;
                    // SAFETY: stdout owns the console handle and the UTF-16 slice is valid
                    // for the synchronous call. The fixed scratch buffer adds no heap copy.
                    let ok = unsafe {
                        WriteConsoleW(
                            stdout.as_raw_handle(),
                            buffer[written..len].as_ptr(),
                            (len - written) as u32,
                            &mut count,
                            std::ptr::null(),
                        )
                    };
                    if ok == 0 {
                        return Err(io::Error::last_os_error());
                    }
                    if count == 0 {
                        return Err(io::ErrorKind::WriteZero.into());
                    }
                    written += count as usize;
                }
            }
            return Ok(());
        }
    }
    stdout.write_all(line.as_bytes())?;
    stdout.flush()
}

async fn write_blocking(
    line: Leased<String>,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    write: impl FnOnce(&str) -> io::Result<()> + Send + 'static,
) -> Result<()> {
    let worker_cancel = cancel.clone();
    wait_for_command(deadline, cancel, async move {
        // Cancellation stops waiting; the worker retains its buffer and lease until I/O ends.
        tokio::task::spawn_blocking(move || {
            check_running(deadline, &worker_cancel)?;
            write(&line)?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    })
    .await??;
    Ok(())
}

struct Sink<'a> {
    executor: Option<CallbackExecutor>,
    stdout: Option<&'a mut (dyn AsyncWrite + Unpin + Send)>,
}

impl<'a> Sink<'a> {
    async fn deliver_with_retry(
        &mut self,
        payload: &Value,
        timings: StreamTimings,
        deadline: Option<Instant>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        check_running(deadline, cancel)?;
        let budget = ByteBudget::global();
        let (mut bytes, mut lease) =
            crate::json::encode_json_leased(payload, crate::ipc::RESPONSE_LIMIT, budget)?;
        let needed = bytes.len().saturating_add(1);
        if !lease.reserve_vec(&mut bytes, needed, crate::ipc::RESPONSE_LIMIT + 1) {
            return Err(Failure::new(
                1,
                "memory_limit_exceeded",
                "event callback exceeds the active memory limit",
            )
            .into());
        }
        bytes.push(b'\n');
        let line = Leased::new(String::from_utf8(bytes).expect("JSON is UTF-8"), lease);
        let Some(executor) = self.executor.as_mut() else {
            if let Some(writer) = self.stdout.as_mut() {
                wait_for_command(deadline, cancel, async {
                    writer.write_all(line.as_bytes()).await?;
                    writer.flush().await
                })
                .await??;
            } else {
                write_blocking(line, deadline, cancel, write_stdout_line).await?;
            }
            return check_running(deadline, cancel);
        };

        let mut backoff = timings.initial_backoff;
        loop {
            match wait_for_command(deadline, cancel, executor.deliver(&line)).await? {
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
    candidate: Option<&str>,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
) -> Result<()> {
    check_running(deadline, cancel)?;
    if current.as_deref() == candidate {
        return Ok(());
    }
    if let Some(store) = cursor_store {
        match candidate {
            Some(cursor) => store.write(cursor)?,
            None => store.clear()?,
        }
    }
    *current = candidate.map(str::to_owned);
    Ok(())
}

/// Run the `events stream` loop until `--once` completes, `--timeout` expires,
/// `notifications/events/terminated` is received, or `cancel` is triggered.
pub async fn run_stream<T: EventTransport>(
    transport: &mut T,
    config: &StreamConfig,
    timings: StreamTimings,
    cancel: CancellationToken,
    stdout_override: Option<&mut (dyn AsyncWrite + Unpin + Send)>,
) -> Result<()> {
    let deadline = config.deadline;
    check_running(deadline, &cancel)?;
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
            let listed = wait_with_budget(
                list_budget,
                deadline,
                cancel,
                transport.list_events(list_budget),
            )
            .await;
            check_running(deadline, cancel)?;
            match listed {
                Ok(events) => select_auto_mode(&events, &config.event)?,
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
    let mut initial_max_age = config.max_age_ms;

    loop {
        if cancel.is_cancelled() {
            return Err(Failure::new(130, "cancelled", "interrupted").into());
        }
        let poll_budget = remaining_budget(timings.handshake_timeout, deadline)?;
        let params_lease = request_params_lease(config, current_cursor)?;
        let params = PollEventsParams {
            name: config.event.clone(),
            arguments: config.arguments.clone(),
            cursor: current_cursor.clone(),
            max_age_ms: current_cursor.as_ref().and(initial_max_age),
            max_events: config.once.then_some(1),
        };
        let params = Leased::new(params, params_lease);
        let polled = wait_with_budget(
            poll_budget,
            deadline,
            cancel,
            transport.poll_events(params, poll_budget),
        )
        .await;
        check_running(deadline, cancel)?;
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
        }

        let has_more = response
            .get("hasMore")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next_poll_ms = response
            .get("nextPollMs")
            .and_then(Value::as_u64)
            .unwrap_or(MIN_POLL_INTERVAL_MS);

        let (mut response, lease) = response.into_parts();
        let events = response
            .get_mut("events")
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default();
        let next_cursor = response
            .get_mut("cursor")
            .map(Value::take)
            .unwrap_or(Value::Null);
        let mut batch = Leased::new((events, next_cursor), lease);
        drop(response);
        let batch_len = batch.0.len();
        let mut delivered_any = false;
        for event in batch.value_mut().0.drain(..) {
            sink.deliver_with_retry(&event, timings, deadline, cancel)
                .await?;
            delivered_any = true;
            if config.once {
                break;
            }
        }

        if !config.once || batch_len <= 1 {
            persist_cursor(
                config.cursor_file.as_ref(),
                current_cursor,
                batch.1.as_str(),
                deadline,
                cancel,
            )?;
        }

        if config.once && delivered_any {
            return check_running(deadline, cancel);
        }
        drop(batch);

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
    let mut initial_max_age = config.max_age_ms;

    loop {
        if cancel.is_cancelled() {
            return Err(Failure::new(130, "cancelled", "interrupted").into());
        }
        let open_budget = remaining_budget(timings.handshake_timeout, deadline)?;
        let params_lease = request_params_lease(config, current_cursor)?;
        let params = StreamEventsParams {
            name: config.event.clone(),
            arguments: config.arguments.clone(),
            cursor: current_cursor.clone(),
            max_age_ms: current_cursor.as_ref().and(initial_max_age),
            heartbeat_ms: config.heartbeat_ms,
        };
        let params = Leased::new(params, params_lease);
        let open_res = wait_with_budget(
            open_budget,
            deadline,
            cancel,
            transport.open_stream(params, open_budget),
        )
        .await;
        check_running(deadline, cancel)?;
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

            let frame_res =
                match wait_for_command(deadline, cancel, timeout(step_wait, channel.next_item()))
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        channel.close().await;
                        return Err(err);
                    }
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
                Ok(Ok(None)) => {
                    reconnect_reason = "event stream closed by server".to_owned();
                    break;
                }
                Ok(Ok(Some(item))) => item,
            };
            let (item, lease) = item.into_parts();
            let mut notification = match item {
                RawStreamEvent::End => {
                    reconnect_reason = "event stream closed by server".to_owned();
                    break;
                }
                RawStreamEvent::Error(mut failure) => {
                    failure.lease = Some(std::sync::Arc::new(lease));
                    let err: anyhow::Error = failure.into();
                    if is_fatal_error(&err) {
                        channel.close().await;
                        return Err(err);
                    }
                    reconnect_reason = format!("stream error: {err:#}");
                    break;
                }
                RawStreamEvent::Notification { method, params } => {
                    Leased::new((method, params), lease)
                }
            };

            let (method, params) = notification.value_mut();
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
                                "mcpcli: event stream {:?} truncated mid-stream; some events may have been missed",
                                config.event
                            );
                        } else {
                            eprintln!(
                                "mcpcli: event stream {:?} truncated on server; some events may have been missed",
                                config.event
                            );
                        }
                    }
                    let cursor = params
                        .as_ref()
                        .and_then(|p| p.get("cursor"))
                        .and_then(Value::as_str);
                    if let Err(err) = persist_cursor(
                        config.cursor_file.as_ref(),
                        current_cursor,
                        cursor,
                        deadline,
                        cancel,
                    ) {
                        channel.close().await;
                        return Err(err);
                    }
                }
                "notifications/events/heartbeat" => {
                    let cursor = params
                        .as_ref()
                        .and_then(|p| p.get("cursor"))
                        .and_then(Value::as_str);
                    if let Err(err) = persist_cursor(
                        config.cursor_file.as_ref(),
                        current_cursor,
                        cursor,
                        deadline,
                        cancel,
                    ) {
                        channel.close().await;
                        return Err(err);
                    }
                }
                "notifications/events/event" => {
                    let payload = params.take().unwrap_or_else(|| Value::Object(Map::new()));
                    if let Err(err) = sink
                        .deliver_with_retry(&payload, timings, deadline, cancel)
                        .await
                    {
                        channel.close().await;
                        return Err(err);
                    }
                    if let Err(err) = persist_cursor(
                        config.cursor_file.as_ref(),
                        current_cursor,
                        payload.get("cursor").and_then(Value::as_str),
                        deadline,
                        cancel,
                    ) {
                        channel.close().await;
                        return Err(err);
                    }
                    if config.once {
                        let outcome = check_running(deadline, cancel);
                        channel.close().await;
                        return outcome;
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
        budget: std::sync::Arc<ByteBudget>,
    }

    impl EventStreamChannel for MockChannel {
        fn next_item(&mut self) -> BoxFuture<'_, Result<Option<Leased<RawStreamEvent>>>> {
            Box::pin(async move {
                let event = match self.steps.pop_front() {
                    Some(MockStep::Yield(ev)) => ev,
                    Some(MockStep::DelayThen(d, ev)) => {
                        sleep(d).await;
                        ev
                    }
                    None => return Ok(None),
                };
                let bytes = match &event {
                    RawStreamEvent::Notification { method, params } => {
                        method.len() + crate::json::serialized_len(params)?
                    }
                    RawStreamEvent::Error(error) => crate::json::serialized_len(error)?,
                    RawStreamEvent::End => 0,
                };
                let lease = self
                    .budget
                    .try_acquire(bytes)
                    .expect("test notification budget");
                Ok(Some(Leased::new(event, lease)))
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
        budget: std::sync::Arc<ByteBudget>,
    }

    impl EventTransport for MockTransport {
        fn list_events(&mut self, _budget: Duration) -> BoxFuture<'_, Result<Leased<Vec<Value>>>> {
            let lease = self
                .budget
                .try_acquire(crate::json::serialized_len(&self.listed).unwrap())
                .unwrap();
            let listed = self.listed.clone();
            Box::pin(async move { Ok(Leased::new(listed, lease)) })
        }

        fn poll_events(
            &mut self,
            params: Leased<PollEventsParams>,
            _budget: Duration,
        ) -> BoxFuture<'_, Result<Leased<Value>>> {
            let (params, _params_lease) = params.into_parts();
            self.poll_cursors.push(params.cursor);
            let next = self
                .polls
                .pop_front()
                .unwrap_or_else(|| json!({"events": [], "hasMore": false, "nextPollMs": 1000}));
            let lease = self
                .budget
                .try_acquire(crate::json::serialized_len(&next).unwrap())
                .unwrap();
            Box::pin(async move { Ok(Leased::new(next, lease)) })
        }

        fn open_stream(
            &mut self,
            params: Leased<StreamEventsParams>,
            _budget: Duration,
        ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
            let (params, _params_lease) = params.into_parts();
            self.stream_cursors.push(params.cursor);
            let steps = self.streams.pop_front().unwrap_or_default();
            let budget = self.budget.clone();
            Box::pin(async move {
                Ok(Box::new(MockChannel { steps, budget }) as Box<dyn EventStreamChannel>)
            })
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
            deadline: Some(Instant::now() + Duration::from_secs(5)),
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

    fn exit_server_config(
        phase: &str,
        stop: &std::path::Path,
        log: &std::path::Path,
    ) -> crate::config::ServerConfig {
        let python = std::env::var("MCPCLI_TEST_PYTHON")
            .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
        crate::config::ServerConfig::Stdio {
            command: python,
            args: vec![
                "-u".into(),
                "-c".into(),
                include_str!("../tests/fixtures/events_exit_server.py").into(),
                phase.into(),
                stop.to_string_lossy().into_owned(),
                log.to_string_lossy().into_owned(),
            ],
            env: std::collections::BTreeMap::new(),
            cwd: None,
            description: None,
            enabled: true,
            timeout: None,
        }
    }

    fn assert_closed_diagnostic(error: &anyhow::Error, phase: &str) {
        let failure = error.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.code, 6);
        assert_eq!(failure.kind, "session_lost");
        assert!(is_fatal_error(error));
        assert!(
            failure.data.as_ref().unwrap()["stderr_tail"]
                .as_str()
                .unwrap()
                .contains(&format!("events fixture stopped: {phase}"))
        );
    }

    #[tokio::test]
    async fn direct_stream_requests_preserve_stderr_on_server_exit() {
        for (phase, mode) in [
            ("events/list", StreamMode::Auto),
            ("events/poll", StreamMode::Poll),
            ("events/stream", StreamMode::Push),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let log = dir.path().join("requests");
            let config = exit_server_config(phase, &dir.path().join("stop"), &log);
            let budget = ByteBudget::new(1024 * 1024);
            let limits = crate::mcp::McpLimits {
                budget: budget.clone(),
                ..crate::mcp::McpLimits::default()
            };
            let connection =
                McpConnection::connect_with_limits(&config, Duration::from_secs(5), limits)
                    .await
                    .unwrap();
            let mut transport = DirectTransport::with_server(connection, "srv".into());
            let mut config = base_config(None);
            config.mode = mode;
            let timings = StreamTimings {
                handshake_timeout: Duration::from_secs(5),
                ..fast_timings()
            };
            let error = run_stream(
                &mut transport,
                &config,
                timings,
                CancellationToken::new(),
                Some(&mut Vec::new()),
            )
            .await
            .unwrap_err();
            assert_closed_diagnostic(&error, phase);
            assert_eq!(
                std::fs::read_to_string(log)
                    .unwrap()
                    .lines()
                    .collect::<Vec<_>>(),
                ["server/discover", phase]
            );
            drop(error);
            let mut connection = transport.into_connection();
            connection.close().await.unwrap();
            drop(connection);
            assert_eq!(budget.used(), 0);
        }
    }

    #[tokio::test]
    async fn direct_preclosed_requests_keep_diagnostics_and_error_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let stop = dir.path().join("stop");
        let log = dir.path().join("requests");
        let config = exit_server_config("after-active", &stop, &log);
        let budget = ByteBudget::new(1024 * 1024);
        let limits = crate::mcp::McpLimits {
            budget: budget.clone(),
            ..crate::mcp::McpLimits::default()
        };
        let connection =
            McpConnection::connect_with_limits(&config, Duration::from_secs(5), limits)
                .await
                .unwrap();
        let mut transport = DirectTransport::with_server(connection, "srv".into());
        let params = || StreamEventsParams {
            name: "pr.merged".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            heartbeat_ms: None,
        };
        let mut channel = transport
            .open_stream(
                Leased::new(params(), budget.empty_lease()),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let active = channel.next_item().await.unwrap().unwrap();
        assert!(matches!(
            &*active,
            RawStreamEvent::Notification { method, .. }
                if method == "notifications/events/active"
        ));
        drop(active);
        std::fs::write(stop, b"stop").unwrap();
        assert!(
            timeout(Duration::from_secs(5), channel.next_item())
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        channel.close().await;
        timeout(Duration::from_secs(5), async {
            while !transport.connection.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let error = transport
            .list_events(Duration::from_secs(5))
            .await
            .unwrap_err();
        assert_closed_diagnostic(&error, "after-active");
        drop(error);
        let poll = PollEventsParams {
            name: "pr.merged".into(),
            arguments: None,
            cursor: None,
            max_age_ms: None,
            max_events: None,
        };
        let error = transport
            .poll_events(
                Leased::new(poll, budget.empty_lease()),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();
        assert_closed_diagnostic(&error, "after-active");
        drop(error);
        let error = transport
            .open_stream(
                Leased::new(params(), budget.empty_lease()),
                Duration::from_secs(5),
            )
            .await
            .err()
            .unwrap();
        assert_closed_diagnostic(&error, "after-active");
        drop(error);
        assert_eq!(
            std::fs::read_to_string(log)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            ["server/discover", "events/stream"]
        );

        let payload_budget = ByteBudget::new(64);
        let rpc_error = std::sync::Arc::new(json!({
            "code": -32026, "message": "push unavailable", "data": {"retry": "poll"}
        }));
        let error = crate::mcp::McpFailure {
            kind: crate::mcp::FailureKind::Protocol,
            message: "specific subscription diagnostic".into(),
            outcome_unknown: true,
            rpc_error: Some(rpc_error.clone()),
            auth: None,
            stderr_tail: Some("ordinary stderr diagnostic".into()),
            lease: Some(std::sync::Arc::new(payload_budget.try_acquire(64).unwrap())),
        };
        let mapped = transport.request_error(error.into());
        let failure = mapped.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.code, 6);
        assert_eq!(failure.kind, "session_lost");
        assert_eq!(failure.message, "specific subscription diagnostic");
        assert!(failure.outcome_unknown);
        assert!(std::sync::Arc::ptr_eq(
            failure.rpc_error.as_ref().unwrap(),
            &rpc_error
        ));
        assert_eq!(
            failure.data.as_ref().unwrap()["stderr_tail"],
            "ordinary stderr diagnostic"
        );
        assert!(!is_unsupported_error(&mapped));
        let retained = failure.clone();
        drop(mapped);
        assert_eq!(payload_budget.used(), 64);
        drop(retained);
        assert_eq!(payload_budget.used(), 0);
        let mut connection = transport.into_connection();
        connection.close().await.unwrap();
        drop(connection);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn auto_mode_rpc_fallback_requires_a_live_session() {
        for closed in [false, true] {
            let mut failure = if closed {
                Failure::new(6, "session_lost", "connection ended")
            } else {
                Failure::new(4, "mcp_protocol", "push unavailable")
            };
            failure.rpc_error = Some(std::sync::Arc::new(json!({
                "code": -32026, "message": "push unavailable"
            })));
            let mut transport = MockTransport {
                listed: vec![json!({"name": "pr.merged", "delivery": ["push", "poll"]})],
                polls: VecDeque::from([json!({
                    "events": [{"eventId": "one", "name": "pr.merged", "data": {}}],
                    "hasMore": false
                })]),
                poll_cursors: Vec::new(),
                streams: VecDeque::from([VecDeque::from([MockStep::Yield(
                    RawStreamEvent::Error(failure),
                )])]),
                stream_cursors: Vec::new(),
                budget: ByteBudget::new(1 << 20),
            };
            let mut config = base_config(None);
            config.once = true;
            let result = run_stream(
                &mut transport,
                &config,
                fast_timings(),
                CancellationToken::new(),
                Some(&mut Vec::new()),
            )
            .await;
            if closed {
                assert_eq!(
                    result.unwrap_err().downcast_ref::<Failure>().unwrap().kind,
                    "session_lost"
                );
                assert!(transport.poll_cursors.is_empty());
            } else {
                result.unwrap();
                assert_eq!(transport.poll_cursors, [None]);
            }
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
                    "truncated": true,
                    "hasMore": false,
                    "nextPollMs": 100
                }),
            ]),
            poll_cursors: Vec::new(),
            streams: VecDeque::new(),
            stream_cursors: Vec::new(),
            budget: ByteBudget::new(1 << 20),
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
        assert!(!line.as_object().unwrap().contains_key("truncated"));
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
            budget: ByteBudget::new(1 << 20),
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
        assert!(!line.as_object().unwrap().contains_key("truncated"));
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
            budget: ByteBudget::new(1 << 20),
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
            budget: ByteBudget::new(1 << 20),
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
    #[test]
    fn protocol_capability_errors_are_fatal_without_hiding_transient_rpc_errors() {
        let missing: anyhow::Error =
            Failure::new(4, "mcp_protocol", "events extension missing").into();
        assert!(is_fatal_error(&missing));
        let mut busy = Failure::new(4, "mcp_protocol", "subscription limit reached");
        busy.rpc_error = Some(std::sync::Arc::new(
            json!({"code": -32025, "message": "busy"}),
        ));
        assert!(!is_fatal_error(&busy.into()));
        let network: anyhow::Error = Failure::new(4, "mcp_transport", "HTTP stream ended").into();
        assert!(!is_fatal_error(&network));
    }

    #[tokio::test]
    async fn delivered_cursor_preserves_empty_tokens_and_clears_null_or_omitted_tokens() {
        for mode in [StreamMode::Poll, StreamMode::Push] {
            for cursor in [Some(json!("")), Some(Value::Null), None] {
                let dir = tempfile::tempdir().unwrap();
                let store = CursorStore::new(dir.path().join("cursor"));
                store.write("old").unwrap();
                let expected = cursor.as_ref().and_then(Value::as_str).map(str::to_owned);
                let mut event = json!({"eventId": "e1", "name": "pr.merged", "data": {}});
                let mut polled = json!({"events": [event.clone()], "hasMore": false});
                if let Some(cursor) = cursor {
                    event["cursor"] = cursor.clone();
                    polled["cursor"] = cursor;
                }
                let mut transport = MockTransport {
                    listed: vec![],
                    polls: VecDeque::from([polled]),
                    poll_cursors: vec![],
                    streams: VecDeque::from([VecDeque::from([
                        MockStep::Yield(RawStreamEvent::Notification {
                            method: "notifications/events/active".into(),
                            params: Some(json!({"cursor": "old", "heartbeatMs": 1000})),
                        }),
                        MockStep::Yield(RawStreamEvent::Notification {
                            method: "notifications/events/event".into(),
                            params: Some(event),
                        }),
                    ])]),
                    stream_cursors: vec![],
                    budget: ByteBudget::new(1 << 20),
                };
                let mut cfg = base_config(Some(store.clone()));
                cfg.mode = mode;
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
                assert_eq!(store.read().unwrap(), expected, "mode={mode:?}");
            }
        }
    }

    #[tokio::test]
    async fn total_deadline_cancels_both_callback_modes_without_committing_cursor() {
        let python = std::env::var("MCPCLI_TEST_PYTHON")
            .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
        for mode in [ExecMode::Oneshot, ExecMode::Stream] {
            let dir = tempfile::tempdir().unwrap();
            let store = CursorStore::new(dir.path().join("callback.cursor"));
            store.write("before").unwrap();
            let mut transport = MockTransport {
                listed: vec![],
                polls: VecDeque::from([
                    json!({"events": [{"eventId": "e1", "name": "pr.merged", "data": {}}], "cursor": "after", "hasMore": false}),
                ]),
                poll_cursors: vec![],
                streams: VecDeque::new(),
                stream_cursors: vec![],
                budget: ByteBudget::new(1 << 20),
            };
            let mut cfg = base_config(Some(store.clone()));
            cfg.mode = StreamMode::Poll;
            cfg.once = true;
            cfg.deadline = Some(Instant::now() + Duration::from_millis(100));
            cfg.exec_mode = mode;
            cfg.exec_timeout = Duration::from_secs(5);
            cfg.callback_argv = Some(vec![python.clone(), "-c".into(), "import sys,time; sys.stdin.readline(); time.sleep(.3); print('{\"ok\":true}', flush=True)".into()]);
            let error = run_stream(
                &mut transport,
                &cfg,
                fast_timings(),
                CancellationToken::new(),
                None,
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<Failure>().unwrap().kind,
                "timeout",
                "mode={mode:?}"
            );
            assert_eq!(store.read().unwrap().as_deref(), Some("before"));
        }
    }
    #[tokio::test]
    async fn event_payload_lease_survives_callback_wait_for_push_and_poll() {
        let python = std::env::var("MCPCLI_TEST_PYTHON")
            .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
        for mode in [StreamMode::Push, StreamMode::Poll] {
            for exec_mode in [ExecMode::Oneshot, ExecMode::Stream] {
                let dir = tempfile::tempdir().unwrap();
                let ready = dir.path().join("ready");
                let release = dir.path().join("release");
                let budget = ByteBudget::new(64 * 1024);
                let data = "normal event payload".repeat(400);
                let event = json!({"eventId":"e1","name":"pr.merged","data":data,"cursor":"after"});
                let mut transport = MockTransport {
                    listed: vec![],
                    polls: VecDeque::from([
                        json!({"events":[event.clone()],"cursor":"after","hasMore":false}),
                    ]),
                    poll_cursors: vec![],
                    streams: VecDeque::from([VecDeque::from([
                        MockStep::Yield(RawStreamEvent::Notification {
                            method: "notifications/events/active".into(),
                            params: Some(json!({"cursor":"before","heartbeatMs":1000})),
                        }),
                        MockStep::Yield(RawStreamEvent::Notification {
                            method: "notifications/events/event".into(),
                            params: Some(event),
                        }),
                    ])]),
                    stream_cursors: vec![],
                    budget: budget.clone(),
                };
                let mut cfg = base_config(None);
                cfg.mode = mode;
                cfg.once = true;
                cfg.deadline = Some(Instant::now() + Duration::from_secs(10));
                cfg.exec_timeout = Duration::from_secs(10);
                cfg.exec_mode = exec_mode;
                cfg.callback_argv = Some(vec![python.clone(), "-c".into(),
                    "import pathlib,sys,time\nsys.stdin.readline()\npathlib.Path(sys.argv[1]).write_text('ready')\nwhile not pathlib.Path(sys.argv[2]).exists(): time.sleep(.01)\nprint('{\"ok\":true}',flush=True)".into(),
                    ready.to_string_lossy().into_owned(), release.to_string_lossy().into_owned()]);
                let run = run_stream(
                    &mut transport,
                    &cfg,
                    fast_timings(),
                    CancellationToken::new(),
                    None,
                );
                tokio::pin!(run);
                tokio::select! {
                    result = &mut run => panic!("callback returned before release: {result:?}"),
                    observed = timeout(Duration::from_secs(5), async {
                        while !ready.exists() { sleep(Duration::from_millis(10)).await; }
                    }) => observed.expect("callback must receive the event"),
                }
                assert!(
                    budget.used() >= data.len(),
                    "event lost its lease while callback waited: {mode:?}/{exec_mode:?}"
                );
                std::fs::write(&release, "continue").unwrap();
                run.await.unwrap();
                assert_eq!(budget.used(), 0, "payload lease remained after delivery");
            }
        }
    }
    struct DelayedTransport {
        inner: MockTransport,
        phase: StreamMode,
        delay: Duration,
    }

    impl EventTransport for DelayedTransport {
        fn list_events(&mut self, budget: Duration) -> BoxFuture<'_, Result<Leased<Vec<Value>>>> {
            let wait = (self.phase == StreamMode::Auto).then_some(self.delay);
            let future = self.inner.list_events(budget);
            Box::pin(async move {
                let result = future.await;
                if let Some(wait) = wait {
                    sleep(wait).await;
                }
                result
            })
        }

        fn poll_events(
            &mut self,
            params: Leased<PollEventsParams>,
            budget: Duration,
        ) -> BoxFuture<'_, Result<Leased<Value>>> {
            let wait = (self.phase == StreamMode::Poll).then_some(self.delay);
            let future = self.inner.poll_events(params, budget);
            Box::pin(async move {
                let result = future.await;
                if let Some(wait) = wait {
                    sleep(wait).await;
                }
                result
            })
        }

        fn open_stream(
            &mut self,
            params: Leased<StreamEventsParams>,
            budget: Duration,
        ) -> BoxFuture<'_, Result<Box<dyn EventStreamChannel>>> {
            let wait = (self.phase == StreamMode::Push).then_some(self.delay);
            let future = self.inner.open_stream(params, budget);
            Box::pin(async move {
                let result = future.await;
                if let Some(wait) = wait {
                    sleep(wait).await;
                }
                result
            })
        }
    }

    #[tokio::test]
    async fn complete_transport_waits_obey_deadline_and_cancel_before_delivery() {
        for mode in [StreamMode::Auto, StreamMode::Poll, StreamMode::Push] {
            for interrupted in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let store = CursorStore::new(dir.path().join("cursor"));
                store.write("before").unwrap();
                let budget = ByteBudget::new(1 << 20);
                let mut transport = DelayedTransport {
                    inner: MockTransport {
                        listed: vec![json!({"name": "pr.merged", "delivery": ["poll"]})],
                        polls: VecDeque::from([json!({
                            "events": [{"eventId": "one", "name": "pr.merged", "data": {}}],
                            "cursor": "after", "hasMore": false
                        })]),
                        poll_cursors: vec![],
                        streams: VecDeque::new(),
                        stream_cursors: vec![],
                        budget: budget.clone(),
                    },
                    phase: mode,
                    delay: Duration::from_secs(5),
                };
                let cancel = CancellationToken::new();
                let mut cfg = base_config(Some(store.clone()));
                cfg.mode = mode;
                cfg.once = true;
                cfg.deadline = if interrupted {
                    None
                } else {
                    Some(Instant::now() + Duration::from_millis(40))
                };
                let cancellation = async {
                    if interrupted {
                        sleep(Duration::from_millis(40)).await;
                        cancel.cancel();
                    }
                };
                let mut output = Vec::new();
                let started = Instant::now();
                let (result, ()) = tokio::join!(
                    run_stream(
                        &mut transport,
                        &cfg,
                        StreamTimings::default(),
                        cancel.clone(),
                        Some(&mut output)
                    ),
                    cancellation
                );
                assert_eq!(
                    result.unwrap_err().downcast_ref::<Failure>().unwrap().code,
                    if interrupted { 130 } else { 5 },
                    "{mode:?}"
                );
                assert!(
                    started.elapsed() < Duration::from_secs(1),
                    "{mode:?} did not stop its wait"
                );
                assert!(output.is_empty(), "{mode:?} delivered a late response");
                assert_eq!(store.read().unwrap().as_deref(), Some("before"));
                assert_eq!(budget.used(), 0, "{mode:?} retained a cancelled response");
            }
        }
    }

    #[tokio::test]
    async fn ready_results_are_rechecked_before_confirmation() {
        let cancel = CancellationToken::new();
        let error = wait_for_command(None, &cancel, async {
            cancel.cancel();
        })
        .await
        .unwrap_err();
        assert_eq!(error.downcast_ref::<Failure>().unwrap().code, 130);

        let cancel = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_millis(20);
        let error = wait_for_command(Some(deadline), &cancel, async {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        })
        .await
        .unwrap_err();
        assert_eq!(error.downcast_ref::<Failure>().unwrap().code, 5);
    }

    #[tokio::test]
    async fn blocking_output_retains_lease_until_writer_finishes_after_cancellation() {
        let budget = ByteBudget::new(1024);
        let line = "{\"data\":\"ordinary event\"}\n".to_owned();
        let bytes = line.capacity();
        let line = Leased::new(line, budget.try_acquire(bytes).unwrap());
        let cancel = CancellationToken::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let writing = write_blocking(line, None, &cancel, move |line| {
            assert!(line.ends_with('\n'));
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            Ok(())
        });
        tokio::pin!(writing);
        tokio::select! {
            result = &mut writing => panic!("write finished before release: {result:?}"),
            result = started_rx => result.unwrap(),
        }
        cancel.cancel();
        let error = writing.await.unwrap_err();
        assert_eq!(error.downcast_ref::<Failure>().unwrap().code, 130);
        assert_eq!(
            budget.used(),
            bytes,
            "cancelled waiter dropped the writer's lease"
        );
        release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), async {
            while budget.used() != 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("finished writer must release its lease");
    }

    #[tokio::test]
    async fn asynchronous_output_backpressure_does_not_commit_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let store = CursorStore::new(dir.path().join("cursor"));
        store.write("before").unwrap();
        let mut transport = MockTransport {
            listed: vec![],
            polls: VecDeque::from([json!({
                "events": [{"eventId": "one", "name": "pr.merged", "data": "normal".repeat(100)}],
                "cursor": "after", "hasMore": false
            })]),
            poll_cursors: vec![],
            streams: VecDeque::new(),
            stream_cursors: vec![],
            budget: ByteBudget::new(1 << 20),
        };
        let mut cfg = base_config(Some(store.clone()));
        cfg.mode = StreamMode::Poll;
        cfg.once = true;
        cfg.deadline = Some(Instant::now() + Duration::from_millis(40));
        let (mut output, _reader) = tokio::io::duplex(8);
        let error = run_stream(
            &mut transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            Some(&mut output),
        )
        .await
        .unwrap_err();
        assert_eq!(error.downcast_ref::<Failure>().unwrap().code, 5);
        assert_eq!(store.read().unwrap().as_deref(), Some("before"));
    }
    #[tokio::test]
    async fn confirmed_callback_success_survives_bounded_eof_cleanup() {
        let python = std::env::var("MCPCLI_TEST_PYTHON")
            .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into());
        for mode in [StreamMode::Poll, StreamMode::Push] {
            let dir = tempfile::tempdir().unwrap();
            let store = CursorStore::new(dir.path().join("cursor"));
            store.write("before").unwrap();
            let event =
                json!({"eventId": "one", "name": "pr.merged", "data": {}, "cursor": "after"});
            let mut transport = MockTransport {
                listed: vec![],
                polls: VecDeque::from([json!({
                    "events": [event.clone()], "cursor": "after", "hasMore": false
                })]),
                poll_cursors: vec![],
                streams: VecDeque::from([VecDeque::from([
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/active".into(),
                        params: Some(json!({"cursor": "before", "heartbeatMs": 1000})),
                    }),
                    MockStep::Yield(RawStreamEvent::Notification {
                        method: "notifications/events/event".into(),
                        params: Some(event),
                    }),
                ])]),
                stream_cursors: vec![],
                budget: ByteBudget::new(1 << 20),
            };
            let mut cfg = base_config(Some(store.clone()));
            cfg.mode = mode;
            cfg.once = true;
            cfg.deadline = Some(Instant::now() + Duration::from_secs(1));
            cfg.exec_mode = ExecMode::Stream;
            cfg.callback_argv = Some(vec![
                python.clone(), "-c".into(),
                "import sys,time; sys.stdin.readline(); print('{\"ok\":true}',flush=True); sys.stdin.read(); time.sleep(1.2)".into(),
            ]);
            run_stream(
                &mut transport,
                &cfg,
                fast_timings(),
                CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
            assert!(
                Instant::now() >= cfg.deadline.unwrap(),
                "cleanup must cross the business deadline"
            );
            assert_eq!(store.read().unwrap().as_deref(), Some("after"));
        }
    }

    #[tokio::test]
    async fn auto_mode_fails_fast_for_webhook_only_event() {
        let mut transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["webhook"]})],
            polls: VecDeque::new(),
            poll_cursors: Vec::new(),
            streams: VecDeque::new(),
            stream_cursors: Vec::new(),
            budget: ByteBudget::new(1 << 20),
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
        assert_eq!(failure.code, 4);
        assert_eq!(failure.kind, "mcp_protocol");
        assert!(failure.message.contains("webhook"));
        assert!(transport.stream_cursors.is_empty());
        assert!(transport.poll_cursors.is_empty());
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn auto_mode_selects_poll_or_push_when_combined_with_webhook() {
        let mut poll_transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["webhook", "poll"]})],
            polls: VecDeque::from([json!({
                "events": [{"eventId": "e1", "name": "pr.merged", "data": {}}],
                "cursor": "b:1",
                "hasMore": false,
                "nextPollMs": 100
            })]),
            poll_cursors: Vec::new(),
            streams: VecDeque::new(),
            stream_cursors: Vec::new(),
            budget: ByteBudget::new(1 << 20),
        };
        let mut cfg = base_config(None);
        cfg.once = true;
        let mut out = Vec::new();
        run_stream(
            &mut poll_transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            Some(&mut out),
        )
        .await
        .unwrap();
        assert_eq!(poll_transport.poll_cursors, vec![None]);
        assert!(poll_transport.stream_cursors.is_empty());

        let mut push_transport = MockTransport {
            listed: vec![json!({"name": "pr.merged", "delivery": ["webhook", "push"]})],
            polls: VecDeque::new(),
            poll_cursors: Vec::new(),
            streams: VecDeque::from([VecDeque::from([
                MockStep::Yield(RawStreamEvent::Notification {
                    method: "notifications/events/active".into(),
                    params: Some(json!({"cursor": "b:1", "heartbeatMs": 50})),
                }),
                MockStep::Yield(RawStreamEvent::Notification {
                    method: "notifications/events/event".into(),
                    params: Some(
                        json!({"eventId": "e2", "name": "pr.merged", "data": {}, "cursor": "b:2"}),
                    ),
                }),
            ])]),
            stream_cursors: Vec::new(),
            budget: ByteBudget::new(1 << 20),
        };
        let mut out = Vec::new();
        run_stream(
            &mut push_transport,
            &cfg,
            fast_timings(),
            CancellationToken::new(),
            Some(&mut out),
        )
        .await
        .unwrap();
        assert_eq!(push_transport.stream_cursors, vec![None]);
        assert!(push_transport.poll_cursors.is_empty());

        assert_eq!(
            select_auto_mode(&[json!({"name": "pr.merged", "delivery": []})], "pr.merged").unwrap(),
            StreamMode::Push
        );
        assert_eq!(
            select_auto_mode(
                &[json!({"name": "other", "delivery": ["webhook"]})],
                "pr.merged"
            )
            .unwrap(),
            StreamMode::Push
        );
    }
}
