//! Shared leaf types, limits, failure representations, and protocol helpers for `mcp`.
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use rmcp::{
    RoleClient,
    model::{
        CancelledNotification, CancelledNotificationParam, ClientJsonRpcMessage,
        ClientNotification, ClientRequest, CreateTaskResult, ProtocolVersion, ServerPeerInfo,
        TASKS_EXTENSION_ID, TaskStatus,
    },
    service::{Peer, PeerRequestOptions},
};
use serde_json::Value;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::budget::{ByteBudget, ByteLease, Leased, format_bytes};

pub const EVENTS_EXTENSION_ID: &str = "io.modelcontextprotocol/events";

pub const MAX_CONNECT: Duration = Duration::from_secs(30);
pub const MAX_RESPONSE_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_TOOL_CATALOG_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_STDERR_TAIL_BYTES: usize = 16 * 1024;
pub(super) const PREFERRED_PROTOCOL_VERSIONS: &[ProtocolVersion] = &[ProtocolVersion::V_2026_07_28];

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
    pub(super) fn request_options(self, started: Instant) -> Result<PeerRequestOptions> {
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

pub(super) fn failure(kind: FailureKind, message: impl Into<String>) -> anyhow::Error {
    failure_with_outcome(kind, message, false)
}

pub(super) fn failure_with_outcome(
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

/// Structured error returned by SEP-2640 (`skills`), SEP-2663 (`tasks`),
/// and schema-gated `call_with_options_leased` operations without depending on `crate::ipc`.

#[derive(Clone, Debug)]
pub struct McpOpError {
    pub code: i32,
    pub kind: String,
    pub message: String,
    pub outcome_unknown: bool,
    pub rpc_error: Option<Arc<Value>>,
    pub auth: Option<Box<crate::auth::AuthChallengeInfo>>,
    pub stderr_tail: Option<Box<str>>,
    pub data: Option<Box<Value>>,
    pub(crate) lease: Option<Arc<ByteLease>>,
}

impl McpOpError {
    pub fn new(code: i32, kind: &str, message: impl Into<String>) -> Self {
        Self {
            code,
            kind: kind.into(),
            message: message.into(),
            outcome_unknown: false,
            rpc_error: None,
            auth: None,
            stderr_tail: None,
            data: None,
            lease: None,
        }
    }

    pub fn uncertain(mut self, uncertain: bool) -> Self {
        self.outcome_unknown = uncertain;
        self
    }

    pub fn from_error(error: anyhow::Error, calling: bool) -> Self {
        let error = match error.downcast::<Self>() {
            Ok(op_err) => return op_err,
            Err(other) => other,
        };
        if let Ok(error) = error.downcast::<McpFailure>() {
            let (code, kind) = match error.kind {
                FailureKind::Timeout => (5, "timeout"),
                FailureKind::Cancelled => (4, "cancelled"),
                FailureKind::Protocol => (4, "mcp_protocol"),
                FailureKind::Transport => (4, "mcp_transport"),
                FailureKind::AuthRequired => (4, "auth_required"),
                FailureKind::InsufficientScope => (4, "insufficient_scope"),
            };
            Self {
                code,
                kind: kind.into(),
                message: error.message,
                outcome_unknown: error.outcome_unknown,
                rpc_error: error.rpc_error,
                auth: error.auth.map(Box::new),
                stderr_tail: error.stderr_tail.map(String::into_boxed_str),
                data: None,
                lease: error.lease,
            }
        } else {
            Self::new(4, "mcp_error", "MCP operation failed").uncertain(calling)
        }
    }
}

impl fmt::Display for McpOpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for McpOpError {}

/// Optional parameters for `McpConnection::call_with_options_leased`.
#[derive(Clone, Copy, Debug, Default)]
pub struct CallOptions<'a> {
    pub expected_schema: Option<&'a crate::schema::ExpectedSchema>,
}

/// Outcome of a `tools/call` dispatch supporting both synchronous results and SEP-2663 tasks.
pub enum CallOutcome {
    Complete {
        value: Value,
        lease: ByteLease,
    },
    Task {
        seed: TaskSeed,
        value: Value,
        lease: ByteLease,
    },
}

/// Upper bound in bytes for a single `Mcp-Name` HTTP routing header value (with Base64 expansion).

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStatusKind {
    Working,
    InputRequired,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatusKind {
    pub(super) fn from_rmcp(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Working => Self::Working,
            TaskStatus::InputRequired => Self::InputRequired,
            TaskStatus::Completed => Self::Completed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Cancelled => Self::Cancelled,
            _ => Self::Working,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::InputRequired => "input_required",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSeed {
    pub task_id: String,
    pub status: TaskStatusKind,
    pub poll_interval_ms: Option<u64>,
    pub ttl_ms: Option<u64>,
}

impl TaskSeed {
    pub(super) fn from_create_task_result(res: &CreateTaskResult) -> Self {
        Self {
            task_id: res.task.task_id.clone(),
            status: TaskStatusKind::from_rmcp(res.task.status),
            poll_interval_ms: res.task.poll_interval_ms,
            ttl_ms: res.task.ttl_ms,
        }
    }
}

pub(super) fn check_tasks_capability(
    info: Option<&ServerPeerInfo>,
) -> std::result::Result<(), McpOpError> {
    let Some(info) = info else {
        return Err(McpOpError::new(
            4,
            "mcp_protocol",
            "MCP server info is missing",
        ));
    };
    if info.protocol_version < ProtocolVersion::V_2026_07_28 {
        return Err(McpOpError::new(
            4,
            "mcp_protocol",
            format!(
                "MCP server protocol version {} does not support tasks (requires >= 2026-07-28)",
                info.protocol_version
            ),
        ));
    }
    if !info.capabilities.supports_tasks() {
        return Err(McpOpError::new(
            4,
            "mcp_protocol",
            "MCP server does not advertise the io.modelcontextprotocol/tasks extension",
        ));
    }
    Ok(())
}

pub(super) fn routing_header_payload_bound(value: &str) -> usize {
    value.len().div_ceil(3).saturating_mul(4).saturating_add(10)
}

/// Strip 2026-07-28 extensions (`events`, `tasks`) from legacy `<= 2025-11-25` `initialize` requests.
pub(super) fn strip_modern_extensions_for_legacy_initialize(message: &mut ClientJsonRpcMessage) {
    if let ClientJsonRpcMessage::Request(request) = message
        && let ClientRequest::InitializeRequest(init) = &mut request.request
        && init.params.protocol_version <= ProtocolVersion::V_2025_11_25
        && let Some(extensions) = init.params.capabilities.extensions.as_mut()
    {
        extensions.remove(EVENTS_EXTENSION_ID);
        extensions.remove(TASKS_EXTENSION_ID);
        if extensions.is_empty() {
            init.params.capabilities.extensions = None;
        }
    }
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
pub(super) struct CleanupRecord {
    pub(super) failure: McpFailure,
    pub(super) local_confirmed: bool,
}

pub(super) type CleanupState = Arc<Mutex<Option<CleanupRecord>>>;

pub(super) fn record_cleanup_failure(
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

pub(super) struct CancelOnDropGuard {
    peer: Peer<RoleClient>,
    request_id: Option<rmcp::model::RequestId>,
}

impl CancelOnDropGuard {
    pub(super) fn new(peer: Peer<RoleClient>, request_id: rmcp::model::RequestId) -> Self {
        Self {
            peer,
            request_id: Some(request_id),
        }
    }

    pub(super) fn disarm(&mut self) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IngressError {
    FrameTooLarge { limit: usize },
    MemoryExhausted,
    InvalidMetadata,
}

pub(super) fn encode_mcp_message(
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

pub(super) fn is_standard_notification(method: &str) -> bool {
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
            | "notifications/tasks"
            | "notifications/tools/list_changed"
    )
}

pub(super) fn is_standard_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "ping"
            | "server/discover"
            | "prompts/get"
            | "prompts/list"
            | "resources/directory/read"
            | "resources/list"
            | "resources/read"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "resources/templates/list"
            | "skills/get"
            | "skills/list"
            | "subscriptions/listen"
            | "tasks/cancel"
            | "tasks/get"
            | "tasks/update"
            | "tools/call"
            | "tools/list"
            | "completion/complete"
            | "elicitation/create"
            | "logging/setLevel"
            | "roots/list"
            | "sampling/createMessage"
    ) || is_standard_notification(method)
}

pub(super) fn should_ignore_notification(json_value: &Value, method: &str) -> bool {
    if method.starts_with("notifications/events/") {
        return false;
    }
    if json_value.get("id").is_none() && !is_standard_method(method) {
        return true;
    }
    method.starts_with("notifications/") && !is_standard_notification(method)
}
