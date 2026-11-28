//! MCP Tasks client (SEP-2663): capability gating, single-step task RPCs,
//! and the shared polling convergence state machine used by both daemon and oneshot modes.
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rmcp::model::{CancelTaskParams, GetTaskParams, InputResponses, UpdateTaskParams};
use serde_json::{Map, Value};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

pub use super::types::{TaskSeed, TaskStatusKind};
use super::{
    connection::McpConnection,
    ingress::{busy_capture, observed_result, service_failure},
    types::{McpOpError as Failure, check_tasks_capability, routing_header_payload_bound},
};
use crate::budget::ByteLease;

pub const MIN_POLL_INTERVAL_MS: u64 = 50;
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;
pub const CANCEL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub status: TaskStatusKind,
    pub poll_interval_ms: Option<u64>,
    pub value: Value,
    pub lease: ByteLease,
}

#[derive(Clone, Copy, Debug)]
pub struct TaskPollBudgets {
    pub step_idle: Duration,
    pub hard_deadline: Option<Instant>,
}

pub async fn get_task(
    conn: &McpConnection,
    task_id: &str,
    budget: Duration,
) -> std::result::Result<TaskSnapshot, Failure> {
    check_tasks_capability(conn.peer.peer_info().as_deref())?;
    let copies = if conn.http { 2 } else { 1 };
    let header_bytes = if conn.http {
        routing_header_payload_bound(task_id)
    } else {
        0
    };
    let _param_lease = conn
        .limits
        .budget
        .try_acquire(
            task_id
                .len()
                .saturating_mul(copies)
                .saturating_add(header_bytes.saturating_mul(5)),
        )
        .ok_or_else(|| {
            Failure::new(
                1,
                "memory_limit_exceeded",
                "Task identifier and routing headers exceed the active memory limit",
            )
        })?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    let req = async {
        let fut = conn.peer.get_task(GetTaskParams::new(task_id.to_owned()));
        let res = tokio::select! {
            biased;
            () = conn.termination.cancelled() => {
                conn.abort_service();
                return Err(Failure::from_error(conn.termination.failure(false), false));
            }
            res = fut => res.map_err(|e| {
                Failure::from_error(
                    service_failure(e, false, &conn.ingress, &conn.termination),
                    false,
                )
            })?,
        };
        let (_, value, lease) = observed_result(conn.ingress.take(), Some("tasks/get"), false)
            .map_err(|e| Failure::from_error(e, false))?;
        Ok(TaskSnapshot {
            task_id: res.task.task.task_id,
            status: TaskStatusKind::from_rmcp(res.task.task.status),
            poll_interval_ms: res.task.task.poll_interval_ms,
            value,
            lease,
        })
    };

    match timeout(budget, req).await {
        Ok(res) => res,
        Err(_) => Err(Failure::new(5, "timeout", "MCP tasks/get timed out")),
    }
}

pub async fn update_task(
    conn: &McpConnection,
    task_id: &str,
    input_responses: Map<String, Value>,
    budget: Duration,
    dispatched: &AtomicBool,
) -> std::result::Result<Value, Failure> {
    check_tasks_capability(conn.peer.peer_info().as_deref())?;
    if input_responses.is_empty() {
        return Err(Failure::new(
            2,
            "arguments",
            "tasks update requires a non-empty JSON object for inputResponses",
        ));
    }
    let payload_bytes = crate::json::serialized_len(&input_responses)
        .map_err(|e| Failure::new(2, "arguments", format!("Invalid inputResponses: {e}")))?
        .saturating_add(task_id.len());
    let copies = if conn.http { 2 } else { 1 };
    let header_bytes = if conn.http {
        routing_header_payload_bound(task_id)
    } else {
        0
    };
    let _param_lease = conn
        .limits
        .budget
        .try_acquire(
            payload_bytes
                .saturating_mul(copies)
                .saturating_add(header_bytes.saturating_mul(5)),
        )
        .ok_or_else(|| {
            Failure::new(
                1,
                "memory_limit_exceeded",
                "Task update payload exceeds the active memory limit",
            )
        })?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    let responses: InputResponses = input_responses.into_iter().collect();
    dispatched.store(true, Ordering::Release);
    let req = async {
        let fut = conn
            .peer
            .update_task(UpdateTaskParams::new(task_id.to_owned(), responses));
        tokio::select! {
            biased;
            () = conn.termination.cancelled() => {
                conn.abort_service();
                return Err(Failure::from_error(conn.termination.failure(true), true));
            }
            res = fut => res.map_err(|e| {
                Failure::from_error(
                    service_failure(e, true, &conn.ingress, &conn.termination),
                    true,
                )
            })?,
        };
        let _ = conn.ingress.take();
        Ok(serde_json::json!({
            "taskId": task_id,
            "acknowledged": true,
        }))
    };

    match timeout(budget, req).await {
        Ok(res) => res,
        Err(_) => Err(Failure::new(
            5,
            "timeout",
            "MCP tasks/update timed out; outcome unknown, the request was not retried",
        )
        .uncertain(true)),
    }
}

pub async fn cancel_task(
    conn: &McpConnection,
    task_id: &str,
    budget: Duration,
    dispatched: &AtomicBool,
) -> std::result::Result<Value, Failure> {
    check_tasks_capability(conn.peer.peer_info().as_deref())?;
    let copies = if conn.http { 2 } else { 1 };
    let header_bytes = if conn.http {
        routing_header_payload_bound(task_id)
    } else {
        0
    };
    let _param_lease = conn
        .limits
        .budget
        .try_acquire(
            task_id
                .len()
                .saturating_mul(copies)
                .saturating_add(header_bytes.saturating_mul(5)),
        )
        .ok_or_else(|| {
            Failure::new(
                1,
                "memory_limit_exceeded",
                "Task cancel payload exceeds the active memory limit",
            )
        })?;
    if conn.termination.is_terminated() {
        conn.abort_service();
        let err = conn
            .attach_stderr_tail(conn.termination.failure(false))
            .await;
        return Err(Failure::from_error(err, false));
    }
    conn.termination.clear_transient();
    let _capture = conn
        .ingress
        .capture()
        .ok_or_else(|| Failure::from_error(busy_capture(), false))?;

    dispatched.store(true, Ordering::Release);
    let req = async {
        let fut = conn
            .peer
            .cancel_task(CancelTaskParams::new(task_id.to_owned()));
        tokio::select! {
            biased;
            () = conn.termination.cancelled() => {
                conn.abort_service();
                return Err(Failure::from_error(conn.termination.failure(true), true));
            }
            res = fut => res.map_err(|e| {
                Failure::from_error(
                    service_failure(e, true, &conn.ingress, &conn.termination),
                    true,
                )
            })?,
        };
        let _ = conn.ingress.take();
        Ok(serde_json::json!({
            "taskId": task_id,
            "acknowledged": true,
        }))
    };

    match timeout(budget, req).await {
        Ok(res) => res,
        Err(_) => Err(Failure::new(
            5,
            "timeout",
            "MCP tasks/cancel timed out; outcome unknown, the request was not retried",
        )
        .uncertain(true)),
    }
}

fn task_data(task_id: &str, status: Option<TaskStatusKind>) -> Value {
    let mut map = Map::new();
    map.insert("taskId".into(), Value::String(task_id.to_owned()));
    if let Some(status) = status {
        map.insert("status".into(), Value::String(status.as_str().to_owned()));
    }
    Value::Object(map)
}

fn decorate_task_failure(
    mut err: Failure,
    task_id: &str,
    owns_task: bool,
    last_status: TaskStatusKind,
) -> Failure {
    if owns_task {
        err.outcome_unknown = true;
    }
    let mut map = match err.data.take().map(|b| *b) {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    map.entry("taskId".to_owned())
        .or_insert_with(|| Value::String(task_id.to_owned()));
    if !owns_task {
        map.entry("status".to_owned())
            .or_insert_with(|| Value::String(last_status.as_str().to_owned()));
    }
    err.data = Some(Box::new(Value::Object(map)));
    err
}

fn make_cancelled_failure(task_id: &str, owns_task: bool, last_status: TaskStatusKind) -> Failure {
    let suffix = if owns_task {
        "; outcome unknown, the call was not retried"
    } else {
        ""
    };
    let mut fail = Failure::new(
        4,
        "cancelled",
        format!("MCP task {task_id:?} cancelled{suffix}"),
    )
    .uncertain(owns_task);
    fail.data = Some(Box::new(task_data(
        task_id,
        (!owns_task).then_some(last_status),
    )));
    fail
}

fn make_timeout_failure(task_id: &str, owns_task: bool, last_status: TaskStatusKind) -> Failure {
    let suffix = if owns_task {
        "; outcome unknown, the call was not retried"
    } else {
        ""
    };
    let mut fail = Failure::new(
        5,
        "timeout",
        format!("MCP task {task_id:?} timed out{suffix}"),
    )
    .uncertain(owns_task);
    fail.data = Some(Box::new(task_data(
        task_id,
        (!owns_task).then_some(last_status),
    )));
    fail
}

async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Shared task convergence loop used by both daemon and `--oneshot` execution paths.
///
/// The caller releases any connection lock or work permit between `poll_once` calls so
/// the sleep window never blocks concurrent operations on the same server.
// shortcut: 采用受 pollIntervalMs 驱动的无锁轮询获取任务终态, 待上游 rmcp SubscriptionFilter 正式支持 taskIds 路由后再评估推送唤醒
pub async fn poll_task_to_completion<PollFn, PollFut, CancelFn, CancelFut>(
    seed: Option<TaskSeed>,
    task_id: String,
    owns_task: bool,
    budgets: TaskPollBudgets,
    cancel: CancellationToken,
    mut poll_once: PollFn,
    mut cancel_once: CancelFn,
) -> std::result::Result<TaskSnapshot, Failure>
where
    PollFn: FnMut(Duration) -> PollFut,
    PollFut: Future<Output = std::result::Result<TaskSnapshot, Failure>>,
    CancelFn: FnMut(Duration) -> CancelFut,
    CancelFut: Future<Output = ()>,
{
    let mut last_status = seed.as_ref().map_or(TaskStatusKind::Working, |s| s.status);
    let mut pending_sleep_ms = match &seed {
        Some(s) if s.status == TaskStatusKind::Working => Some(
            s.poll_interval_ms
                .unwrap_or(DEFAULT_POLL_INTERVAL_MS)
                .max(MIN_POLL_INTERVAL_MS),
        ),
        // When `owns_task` is true and the initial CreateTaskResult is already non-working,
        // immediately issue one `tasks/get` to retrieve the full payload.
        _ => None,
    };

    loop {
        if cancel.is_cancelled() {
            if owns_task {
                let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
            }
            return Err(make_cancelled_failure(&task_id, owns_task, last_status));
        }

        let remaining_hard = budgets
            .hard_deadline
            .map(|d| d.saturating_duration_since(Instant::now()));
        if remaining_hard.is_some_and(|d| d.is_zero()) {
            if owns_task {
                let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
            }
            return Err(make_timeout_failure(&task_id, owns_task, last_status));
        }

        if let Some(delay_ms) = pending_sleep_ms.take() {
            let delay = Duration::from_millis(delay_ms.max(MIN_POLL_INTERVAL_MS));
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    if owns_task {
                        let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
                    }
                    return Err(make_cancelled_failure(&task_id, owns_task, last_status));
                }
                () = sleep_until_optional(budgets.hard_deadline) => {
                    if owns_task {
                        let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
                    }
                    return Err(make_timeout_failure(&task_id, owns_task, last_status));
                }
                () = tokio::time::sleep(delay) => {}
            }
        }

        let remaining_hard = budgets
            .hard_deadline
            .map(|d| d.saturating_duration_since(Instant::now()));
        if remaining_hard.is_some_and(|d| d.is_zero()) {
            if owns_task {
                let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
            }
            return Err(make_timeout_failure(&task_id, owns_task, last_status));
        }
        let step_budget = remaining_hard.map_or(budgets.step_idle, |h| budgets.step_idle.min(h));

        let step_result = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                if owns_task {
                    let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
                }
                return Err(make_cancelled_failure(&task_id, owns_task, last_status));
            }
            () = sleep_until_optional(budgets.hard_deadline) => {
                if owns_task {
                    let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
                }
                return Err(make_timeout_failure(&task_id, owns_task, last_status));
            }
            res = poll_once(step_budget) => res,
        };

        let snapshot = match step_result {
            Ok(snapshot) => snapshot,
            Err(err) => {
                if owns_task && err.code == 5 {
                    let _ = timeout(CANCEL_TIMEOUT, cancel_once(CANCEL_TIMEOUT)).await;
                }
                return Err(decorate_task_failure(err, &task_id, owns_task, last_status));
            }
        };

        last_status = snapshot.status;
        match snapshot.status {
            TaskStatusKind::Completed => return Ok(snapshot),
            TaskStatusKind::Failed => {
                let rpc_error = snapshot.value.get("error").cloned().map(Arc::new);
                let message = rpc_error
                    .as_deref()
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .map(|msg| format!("MCP task {task_id:?} failed: {msg}"))
                    .unwrap_or_else(|| format!("MCP task {task_id:?} failed"));
                let mut fail = Failure::new(4, "task_failed", message).uncertain(owns_task);
                fail.rpc_error = rpc_error;
                fail.data = Some(Box::new(task_data(&task_id, None)));
                fail.lease = Some(Arc::new(snapshot.lease));
                return Err(fail);
            }
            TaskStatusKind::Cancelled => {
                let mut fail = Failure::new(
                    4,
                    "cancelled",
                    format!("MCP task {task_id:?} was cancelled"),
                )
                .uncertain(owns_task);
                fail.data = Some(Box::new(task_data(&task_id, None)));
                return Err(fail);
            }
            TaskStatusKind::InputRequired => {
                // Never send tasks/cancel on input_required: the caller may resume via tasks update.
                let input_requests = snapshot
                    .value
                    .get("inputRequests")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Map::new()));
                let mut data = Map::new();
                data.insert("taskId".into(), Value::String(task_id.clone()));
                data.insert("status".into(), Value::String("input_required".into()));
                data.insert("inputRequests".into(), input_requests);
                let mut fail = Failure::new(
                    4,
                    "input_required",
                    format!("MCP task {task_id:?} requires client input"),
                )
                .uncertain(owns_task);
                fail.data = Some(Box::new(Value::Object(data)));
                fail.lease = Some(Arc::new(snapshot.lease));
                return Err(fail);
            }
            TaskStatusKind::Working => {
                pending_sleep_ms = Some(
                    snapshot
                        .poll_interval_ms
                        .unwrap_or(DEFAULT_POLL_INTERVAL_MS)
                        .max(MIN_POLL_INTERVAL_MS),
                );
            }
        }
    }
}

impl McpConnection {
    pub fn check_tasks_support(&self) -> std::result::Result<(), Failure> {
        check_tasks_capability(self.peer.peer_info().as_deref())
    }

    pub async fn get_task(
        &self,
        task_id: &str,
        budget: Duration,
    ) -> std::result::Result<TaskSnapshot, Failure> {
        get_task(self, task_id, budget).await
    }

    pub async fn update_task(
        &self,
        task_id: &str,
        input_responses: Map<String, Value>,
        budget: Duration,
        dispatched: &AtomicBool,
    ) -> std::result::Result<Value, Failure> {
        update_task(self, task_id, input_responses, budget, dispatched).await
    }

    pub async fn cancel_task(
        &self,
        task_id: &str,
        budget: Duration,
        dispatched: &AtomicBool,
    ) -> std::result::Result<Value, Failure> {
        cancel_task(self, task_id, budget, dispatched).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::ByteBudget;
    use rmcp::model::{Implementation, InitializeResult, ServerCapabilities};
    use rmcp::model::{ProtocolVersion, ServerPeerInfo};
    use std::sync::atomic::AtomicUsize;

    fn snapshot(task_id: &str, status: TaskStatusKind, value: Value) -> TaskSnapshot {
        TaskSnapshot {
            task_id: task_id.to_owned(),
            status,
            poll_interval_ms: Some(50),
            value,
            lease: ByteBudget::new(4096).empty_lease(),
        }
    }

    fn caps_with_tasks() -> ServerCapabilities {
        let mut caps = ServerCapabilities::default();
        let mut exts = rmcp::model::ExtensionCapabilities::new();
        exts.insert(rmcp::model::TASKS_EXTENSION_ID.to_owned(), Map::new());
        caps.extensions = Some(exts);
        caps
    }

    #[test]
    fn capability_gate_requires_2026_07_28_and_tasks_extension() {
        assert!(check_tasks_capability(None).is_err());

        let mut legacy = InitializeResult::new(caps_with_tasks());
        legacy.protocol_version = ProtocolVersion::V_2025_11_25;
        legacy.server_info = Implementation::new("srv", "1.0");
        let legacy: ServerPeerInfo = legacy.into();
        let err = check_tasks_capability(Some(&legacy)).unwrap_err();
        assert_eq!(err.code, 4);
        assert_eq!(err.kind, "mcp_protocol");
        assert!(!err.outcome_unknown);

        let mut no_tasks = InitializeResult::new(ServerCapabilities::default());
        no_tasks.protocol_version = ProtocolVersion::V_2026_07_28;
        let no_tasks: ServerPeerInfo = no_tasks.into();
        assert_eq!(check_tasks_capability(Some(&no_tasks)).unwrap_err().code, 4);

        let mut ok = InitializeResult::new(caps_with_tasks());
        ok.protocol_version = ProtocolVersion::V_2026_07_28;
        let ok: ServerPeerInfo = ok.into();
        assert!(check_tasks_capability(Some(&ok)).is_ok());
    }

    #[tokio::test]
    async fn owns_task_non_working_seed_still_fetches_payload_and_preserves_uncertain_on_error() {
        let polls = Arc::new(AtomicUsize::new(0));
        let seed = TaskSeed {
            task_id: "t-ttl0".into(),
            status: TaskStatusKind::Completed,
            poll_interval_ms: None,
            ttl_ms: Some(0),
        };
        let polls_c = polls.clone();
        let err = poll_task_to_completion(
            Some(seed),
            "t-ttl0".into(),
            true,
            TaskPollBudgets {
                step_idle: Duration::from_secs(5),
                hard_deadline: None,
            },
            CancellationToken::new(),
            move |_| {
                let polls = polls_c.clone();
                async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    Err(Failure::new(4, "mcp_protocol", "task expired"))
                }
            },
            |_| async {},
        )
        .await
        .unwrap_err();

        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(err.code, 4);
        assert!(err.outcome_unknown);
        assert_eq!(err.data.unwrap()["taskId"], "t-ttl0");
    }

    #[tokio::test]
    async fn terminal_states_enforce_outcome_unknown_and_no_cancel_on_input_required() {
        for owns_task in [true, false] {
            // Completed
            let ok = poll_task_to_completion(
                None,
                "t-ok".into(),
                owns_task,
                TaskPollBudgets {
                    step_idle: Duration::from_secs(5),
                    hard_deadline: None,
                },
                CancellationToken::new(),
                |_| async {
                    Ok(snapshot(
                        "t-ok",
                        TaskStatusKind::Completed,
                        serde_json::json!({"taskId": "t-ok", "status": "completed", "result": {"content": []}}),
                    ))
                },
                |_| async {},
            )
            .await
            .unwrap();
            assert_eq!(ok.status, TaskStatusKind::Completed);

            // Failed
            let failed = poll_task_to_completion(
                None,
                "t-fail".into(),
                owns_task,
                TaskPollBudgets {
                    step_idle: Duration::from_secs(5),
                    hard_deadline: None,
                },
                CancellationToken::new(),
                |_| async {
                    Ok(snapshot(
                        "t-fail",
                        TaskStatusKind::Failed,
                        serde_json::json!({"taskId": "t-fail", "status": "failed", "error": {"code": -32000, "message": "boom"}}),
                    ))
                },
                |_| async {},
            )
            .await
            .unwrap_err();
            assert_eq!(failed.code, 4);
            assert_eq!(failed.kind, "task_failed");
            assert_eq!(failed.outcome_unknown, owns_task);
            assert_eq!(failed.data.unwrap()["taskId"], "t-fail");
            assert_eq!(failed.rpc_error.unwrap()["message"], "boom");

            // Cancelled
            let cancelled = poll_task_to_completion(
                None,
                "t-can".into(),
                owns_task,
                TaskPollBudgets {
                    step_idle: Duration::from_secs(5),
                    hard_deadline: None,
                },
                CancellationToken::new(),
                |_| async {
                    Ok(snapshot(
                        "t-can",
                        TaskStatusKind::Cancelled,
                        serde_json::json!({"taskId": "t-can", "status": "cancelled"}),
                    ))
                },
                |_| async {},
            )
            .await
            .unwrap_err();
            assert_eq!(cancelled.code, 4);
            assert_eq!(cancelled.kind, "cancelled");
            assert_eq!(cancelled.outcome_unknown, owns_task);
            assert_eq!(cancelled.data.unwrap()["taskId"], "t-can");

            // InputRequired must NOT call cancel_once
            let cancel_calls = Arc::new(AtomicUsize::new(0));
            let cancel_calls_c = cancel_calls.clone();
            let input_req = poll_task_to_completion(
                None,
                "t-in".into(),
                owns_task,
                TaskPollBudgets {
                    step_idle: Duration::from_secs(5),
                    hard_deadline: None,
                },
                CancellationToken::new(),
                |_| async {
                    Ok(snapshot(
                        "t-in",
                        TaskStatusKind::InputRequired,
                        serde_json::json!({"taskId": "t-in", "status": "input_required", "inputRequests": {"q1": {"type": "confirm"}}}),
                    ))
                },
                move |_| {
                    let c = cancel_calls_c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await
            .unwrap_err();
            assert_eq!(cancel_calls.load(Ordering::SeqCst), 0);
            assert_eq!(input_req.code, 4);
            assert_eq!(input_req.kind, "input_required");
            assert_eq!(input_req.outcome_unknown, owns_task);
            let data = input_req.data.unwrap();
            assert_eq!(data["taskId"], "t-in");
            assert_eq!(data["status"], "input_required");
            assert_eq!(data["inputRequests"]["q1"]["type"], "confirm");
        }
    }

    #[tokio::test]
    async fn timeout_and_cancel_invoke_cancel_once_only_when_owning_task() {
        for owns_task in [true, false] {
            let cancel_calls = Arc::new(AtomicUsize::new(0));
            let cancel_calls_c = cancel_calls.clone();
            let err = poll_task_to_completion(
                Some(TaskSeed {
                    task_id: "t-to".into(),
                    status: TaskStatusKind::Working,
                    poll_interval_ms: Some(500),
                    ttl_ms: None,
                }),
                "t-to".into(),
                owns_task,
                TaskPollBudgets {
                    step_idle: Duration::from_secs(5),
                    hard_deadline: Some(Instant::now() + Duration::from_millis(20)),
                },
                CancellationToken::new(),
                |_| async {
                    Ok(snapshot(
                        "t-to",
                        TaskStatusKind::Working,
                        serde_json::json!({"taskId": "t-to", "status": "working"}),
                    ))
                },
                move |_| {
                    let c = cancel_calls_c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.code, 5);
            assert_eq!(err.kind, "timeout");
            assert_eq!(err.outcome_unknown, owns_task);
            assert_eq!(cancel_calls.load(Ordering::SeqCst), usize::from(owns_task));
            let data = err.data.unwrap();
            assert_eq!(data["taskId"], "t-to");
            if !owns_task {
                assert_eq!(data["status"], "working");
            }

            let cancel_calls = Arc::new(AtomicUsize::new(0));
            let cancel_calls_c = cancel_calls.clone();
            let token = CancellationToken::new();
            let token_c = token.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                token_c.cancel();
            });
            let err = poll_task_to_completion(
                Some(TaskSeed {
                    task_id: "t-int".into(),
                    status: TaskStatusKind::Working,
                    poll_interval_ms: Some(500),
                    ttl_ms: None,
                }),
                "t-int".into(),
                owns_task,
                TaskPollBudgets {
                    step_idle: Duration::from_secs(5),
                    hard_deadline: None,
                },
                token,
                |_| async {
                    Ok(snapshot(
                        "t-int",
                        TaskStatusKind::Working,
                        serde_json::json!({"taskId": "t-int", "status": "working"}),
                    ))
                },
                move |_| {
                    let c = cancel_calls_c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.code, 4);
            assert_eq!(err.kind, "cancelled");
            assert_eq!(err.outcome_unknown, owns_task);
            assert_eq!(cancel_calls.load(Ordering::SeqCst), usize::from(owns_task));
            let data = err.data.unwrap();
            assert_eq!(data["taskId"], "t-int");
            if !owns_task {
                assert_eq!(data["status"], "working");
            }
        }
    }
}
