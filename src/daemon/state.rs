//! Daemon configuration options, server/cancellation state, and `App` lifecycle helpers.
use super::error::mcp_error_for_server;
use crate::budget::ByteBudget;
use crate::config::LoadedConfig;
use crate::ipc::{self, Failure, Response, StreamFrame, StreamFrameKind};
use crate::mcp::{MAX_CONNECT, McpConnection, McpEventsClient, McpLimits};
use crate::media::MediaStore;
use crate::runtime::{Paths, State};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(super) const MAX_CONNECTIONS: usize = 32;
pub(super) const MAX_CONTROL_CONNECTIONS: usize = 8;
pub(super) const MAX_IN_FLIGHT: usize = 8;
pub(super) const MAX_EVENT_STREAMS: usize = 32;
pub(super) const MAX_TASK_WAITERS: usize = 24;
pub(super) const DUAL_CLOCK_OUTER_GRACE: Duration = Duration::from_secs(3);
pub(super) const FRAME_WAIT: Duration = Duration::from_secs(5);

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

pub(super) struct ServerState {
    pub(super) connection: Option<McpConnection>,
    pub(super) conn_id: u64,
    pub(super) lost: bool,
    pub(super) calls: u64,
    pub(super) streams_cancel: CancellationToken,
}

impl ServerState {
    pub(super) fn mark_lost(&mut self) {
        self.lost = true;
        self.streams_cancel.cancel();
    }

    pub(super) async fn session_lost(&mut self, server: &str) -> Failure {
        self.mark_lost();
        let mut failure = if let Some(connection) = &self.connection {
            mcp_error_for_server(connection.closed_error().await, false, Some(server))
        } else {
            Failure::new(
                6,
                "session_lost",
                format!(
                    "server {server:?} disconnected; state may be lost. Run reset {server} before opening a new connection"
                ),
            )
        };
        failure.code = 6;
        failure.kind = "session_lost".into();
        failure
    }
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            connection: None,
            conn_id: 0,
            lost: false,
            calls: 0,
            streams_cancel: CancellationToken::new(),
        }
    }
}

#[derive(Default)]
pub(super) struct CallCancelState {
    next_id: u64,
    active: HashMap<u64, CancellationToken>,
    force_resets: usize,
}

pub(super) type CallCancelSlot = Arc<std::sync::Mutex<CallCancelState>>;

pub(super) struct CallCancelRegistration {
    slot: CallCancelSlot,
    id: u64,
}

impl CallCancelRegistration {
    pub(super) fn register(
        slot: &CallCancelSlot,
        token: CancellationToken,
    ) -> (Self, CancellationToken) {
        let mut state = slot.lock().unwrap_or_else(|p| p.into_inner());
        state.next_id = state.next_id.wrapping_add(1);
        let id = state.next_id;
        if state.force_resets > 0 {
            token.cancel();
        }
        state.active.insert(id, token.clone());
        (
            Self {
                slot: slot.clone(),
                id,
            },
            token,
        )
    }
}

impl Drop for CallCancelRegistration {
    fn drop(&mut self) {
        self.slot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active
            .remove(&self.id);
    }
}

pub(super) struct ForceResetCancellation {
    slot: CallCancelSlot,
}

impl ForceResetCancellation {
    pub(super) fn begin(slot: &CallCancelSlot) -> Self {
        let mut state = slot.lock().unwrap_or_else(|p| p.into_inner());
        state.force_resets += 1;
        for token in state.active.values() {
            token.cancel();
        }
        Self { slot: slot.clone() }
    }
}

impl Drop for ForceResetCancellation {
    fn drop(&mut self) {
        self.slot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .force_resets -= 1;
    }
}

#[derive(Clone, Copy)]
pub(super) struct ExecuteDeadlines {
    pub(super) pre_dispatch: Instant,
    pub(super) call_idle: Duration,
    pub(super) call_hard: Option<Instant>,
}

pub(super) struct App {
    pub(super) paths: Paths,
    pub(super) config: LoadedConfig,
    pub(super) state: State,
    pub(super) options: DaemonOptions,
    pub(super) servers: BTreeMap<String, Arc<Mutex<ServerState>>>,
    pub(super) call_cancels: BTreeMap<String, CallCancelSlot>,
    pub(super) work: Semaphore,
    pub(super) event_streams: Semaphore,
    pub(super) task_waiters: Arc<Semaphore>,
    pub(super) shutdown: CancellationToken,
}

impl App {
    pub(super) fn response(&self, result: std::result::Result<Value, Failure>) -> Response {
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
    pub(super) fn status(&self) -> Value {
        let servers: BTreeMap<_, _> = self
            .servers
            .iter()
            .map(|(name, state)| {
                let value = if self
                    .config
                    .config
                    .servers
                    .get(name)
                    .is_some_and(|cfg| !cfg.enabled())
                {
                    json!({"state": "disabled", "calls": 0})
                } else {
                    match state.try_lock() {
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
                    }
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

    pub(super) fn stream_frame(&self, kind: StreamFrameKind) -> StreamFrame {
        StreamFrame {
            protocol: ipc::PROTOCOL,
            namespace: self.paths.namespace.clone(),
            build: self.paths.build.clone(),
            epoch: self.state.epoch.clone(),
            kind,
        }
    }

    pub(super) async fn ensure_events_client(
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
            return Err(state.session_lost(server).await);
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
                    .map_err(|e| mcp_error_for_server(e, false, Some(server)))?,
            );
            state.conn_id = state.conn_id.wrapping_add(1);
        }
        let client = state
            .connection
            .as_ref()
            .expect("connection was established")
            .events_client();
        let cancel = state.streams_cancel.child_token();
        Ok((client, cancel))
    }

    pub(super) async fn mark_server_lost_if_closed(&self, server: &str) {
        if let Some(server_state) = self.servers.get(server) {
            let mut state = server_state.lock().await;
            if state
                .connection
                .as_ref()
                .is_some_and(McpConnection::is_closed)
            {
                state.mark_lost();
            }
        }
    }
}
