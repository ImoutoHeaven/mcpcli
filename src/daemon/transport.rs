//! Daemon IPC framing, event stream forwarding, connection handling, and accept loop.
use super::dispatch::{ExecuteContext, ExecutionOutcome};
use super::error::mcp_error_for_server;
use super::state::{
    App, CallCancelSlot, DUAL_CLOCK_OUTER_GRACE, DaemonOptions, ExecuteDeadlines, MAX_CONNECTIONS,
    MAX_CONTROL_CONNECTIONS, MAX_EVENT_STREAMS, MAX_IN_FLIGHT, MAX_TASK_WAITERS, ServerState,
};
use crate::budget::{Leased, format_bytes};
use crate::config::LoadedConfig;
use crate::ipc::{self, Failure, Operation, Request, StreamFrameKind};
use crate::mcp::{McpEventsClient, StreamEventsParams};
use crate::platform::{
    self,
    local::{Conn, Listener},
};
use crate::runtime::{Lock, Paths, START_WAIT, State};
use anyhow::{Context, Result};
use serde_json::json;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

pub(super) async fn send<W: AsyncWrite + Unpin>(
    stream: &mut W,
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

async fn event_session_lost(client: &McpEventsClient, server: &str) -> Failure {
    let mut failure = if client.is_closed() {
        mcp_error_for_server(client.closed_error().await, false, Some(server))
    } else {
        Failure::new(
            6,
            "session_lost",
            format!("server {server:?} was reset or disconnected"),
        )
    };
    failure.code = 6;
    failure.kind = "session_lost".into();
    failure
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
        _ = server_cancel.cancelled() => Err(event_session_lost(&client, server).await),
        _ = reader.read(&mut extra) => return,
        res = client.open_event_stream(params, open_budget) => {
            if client.is_closed() {
                app.mark_server_lost_if_closed(server).await;
            }
            res.map_err(|e| mcp_error_for_server(e, false, Some(server)))
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
                        error: event_session_lost(&client, &server).await,
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
                        error: event_session_lost(&client, &server).await,
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

pub(super) async fn handle(
    mut stream: Conn,
    app: Arc<App>,
    control: bool,
    conn_permit: OwnedSemaphorePermit,
) {
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
    let mut conn_permit = Some(conn_permit);
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
    let uses_dual_clock = request.operation.uses_dual_clock();
    let unbounded_dual_clock = uses_dual_clock && request.max_timeout_ms.is_none();
    let total_cap_ms = if uses_dual_clock {
        request.max_timeout_ms.unwrap_or(request.timeout_ms)
    } else {
        request.timeout_ms
    };
    let remaining_total = request.deadline_ms.saturating_sub(now_ms).min(total_cap_ms);
    let remaining_admission = request
        .admission_deadline_ms()
        .saturating_sub(now_ms)
        .min(request.timeout_ms);
    if remaining_admission == 0 {
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
    let mut hard_deadline = request.explicit_deadline_ms.map(|exp_ms| {
        admitted + Duration::from_millis(exp_ms.saturating_sub(now_ms).min(total_cap_ms))
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
    let deadlines = ExecuteDeadlines {
        pre_dispatch: admitted + Duration::from_millis(remaining_admission),
        call_idle: Duration::from_millis(request.timeout_ms),
        call_hard: if uses_dual_clock {
            request
                .max_timeout_ms
                .map(|_| admitted + Duration::from_millis(remaining_total))
        } else {
            None
        },
    };
    let dispatched = AtomicBool::new(false);
    let polling_started = AtomicBool::new(false);
    let mut extra = [0u8; 1];

    if uses_dual_clock {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let client_cancel = CancellationToken::new();
        let grace_notify = Notify::new();
        let owns_task = request.operation.is_call();
        let mut sentinel_received = false;
        let mut grace_granted = false;
        let outer_deadline =
            (!unbounded_dual_clock).then(|| admitted + Duration::from_millis(remaining_total));
        let mut outer_sleep = outer_deadline.map(|d| Box::pin(tokio::time::sleep_until(d)));

        let exec_fut = app.execute(
            &request.operation,
            ExecuteContext {
                deadlines,
                dispatched: &dispatched,
                client_cancel: Some(&client_cancel),
                polling_started: &polling_started,
                grace_notify: (!unbounded_dual_clock).then_some(&grace_notify),
            },
            &mut conn_permit,
        );
        tokio::pin!(exec_fut);

        let result = loop {
            tokio::select! {
                biased;
                _ = app.shutdown.cancelled() => {
                    break Err(Failure::new(
                        4,
                        "stopping",
                        "daemon is stopping; interrupted calls are not replayed",
                    )
                    .uncertain(dispatched.load(Ordering::Acquire)));
                }
                result = &mut exec_fut => break result,
                () = grace_notify.notified(), if !unbounded_dual_clock && !grace_granted => {
                    grace_granted = true;
                    let _ = writer.write_all(&[0u8; 4]).await;
                    let _ = writer.flush().await;
                    if let Some(d) = outer_deadline {
                        outer_sleep = Some(Box::pin(tokio::time::sleep_until(
                            d + DUAL_CLOCK_OUTER_GRACE,
                        )));
                    }
                    hard_deadline = hard_deadline.map(|d| d + DUAL_CLOCK_OUTER_GRACE);
                }
                () = async {
                    if let Some(sleep) = outer_sleep.as_mut() {
                        sleep.await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    break Err(Failure::new(
                        5,
                        "timeout",
                        "request deadline exceeded, including queue and connection time; cancellation is not rollback",
                    )
                    .uncertain(dispatched.load(Ordering::Acquire)));
                }
                read_res = reader.read(&mut extra), if !sentinel_received => {
                    match read_res {
                        Ok(1) => {
                            // 1-byte cancel sentinel from client: trigger cooperative cancellation
                            // and wait for the structured cancelled response frame.
                            sentinel_received = true;
                            client_cancel.cancel();
                        }
                        Ok(0) | Err(_) => {
                            // Real client disconnect: best-effort cancel if we own a running task.
                            if owns_task && polling_started.load(Ordering::Acquire) {
                                client_cancel.cancel();
                                let _ = timeout(Duration::from_millis(2_500), &mut exec_fut).await;
                            }
                            return;
                        }
                        Ok(_) => unreachable!(),
                    }
                }
            }
        };
        let send_deadline = hard_deadline.filter(|_| result.is_ok());
        send(
            &mut writer,
            &app,
            result,
            dispatched.load(Ordering::Acquire),
            send_deadline,
        )
        .await;
        return;
    }

    let execute = timeout(
        Duration::from_millis(remaining_total),
        app.execute(
            &request.operation,
            ExecuteContext {
                deadlines,
                dispatched: &dispatched,
                client_cancel: None,
                polling_started: &polling_started,
                grace_notify: None,
            },
            &mut conn_permit,
        ),
    );
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
    let _lifetime = Lock::acquire(&paths.daemon_lock(), START_WAIT)
        .await
        .context("cannot acquire daemon lifetime lock")?;
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
    let call_cancels = config
        .config
        .servers
        .keys()
        .map(|name| (name.clone(), CallCancelSlot::default()))
        .collect();
    let app = Arc::new(App {
        paths: paths.clone(),
        config,
        state: state.clone(),
        options,
        servers,
        call_cancels,
        work: Semaphore::new(MAX_IN_FLIGHT),
        event_streams: Semaphore::new(MAX_EVENT_STREAMS),
        task_waiters: Arc::new(Semaphore::new(MAX_TASK_WAITERS)),
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
