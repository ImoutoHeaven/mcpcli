//! Daemon RPC validation and operation dispatch handlers.
use super::error::{mcp_error_for_server, mcp_op_error};
use super::state::{App, CallCancelRegistration, ExecuteDeadlines, ForceResetCancellation};
use crate::budget::ByteLease;
use crate::ipc::{self, Failure, Operation, Request};
use crate::mcp::{
    CallBudgets, CallOptions, CallOutcome, MAX_CONNECT, McpConnection, McpOpError,
    PollEventsParams,
    tasks::{TaskPollBudgets, TaskSeed, TaskSnapshot, TaskStatusKind, poll_task_to_completion},
};
use crate::media::InflightBatch;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Notify, OwnedSemaphorePermit};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

pub(super) type ExecutionOutcome =
    std::result::Result<(Value, ByteLease, Option<InflightBatch>), Failure>;

pub(super) struct ExecuteContext<'a> {
    pub(super) deadlines: ExecuteDeadlines,
    pub(super) dispatched: &'a AtomicBool,
    pub(super) client_cancel: Option<&'a CancellationToken>,
    pub(super) polling_started: &'a AtomicBool,
    pub(super) grace_notify: Option<&'a Notify>,
}

pub(super) struct DaemonTaskPoll<'a> {
    pub(super) server: &'a str,
    pub(super) initial_conn_id: u64,
    pub(super) seed: Option<TaskSeed>,
    pub(super) task_id: String,
    pub(super) owns_task: bool,
    pub(super) deadlines: ExecuteDeadlines,
    pub(super) cancel: CancellationToken,
    pub(super) polling_started: &'a AtomicBool,
    pub(super) grace_notify: Option<&'a Notify>,
}

impl App {
    pub(super) fn validate(&self, request: &Request) -> std::result::Result<(), Failure> {
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
        if let Some(max_timeout_ms) = request.max_timeout_ms {
            if !request.operation.uses_dual_clock() {
                return Err(Failure::new(
                    2,
                    "invalid_timeout",
                    "max_timeout_ms is only valid for non-detached tool calls and waiting task queries",
                ));
            }
            if max_timeout_ms < request.timeout_ms || max_timeout_ms > 86_400_000 {
                return Err(Failure::new(
                    2,
                    "invalid_timeout",
                    "max_timeout_ms must be between timeout_ms and 24 hours",
                ));
            }
        }
        if let Operation::Server { server }
        | Operation::Tools { server }
        | Operation::Call { server, .. }
        | Operation::Reset { server, .. }
        | Operation::EventsList { server }
        | Operation::EventsPoll { server, .. }
        | Operation::EventsStream { server, .. }
        | Operation::SkillsList { server }
        | Operation::SkillsGet { server, .. }
        | Operation::SkillsFiles { server, .. }
        | Operation::SkillsRead { server, .. }
        | Operation::TasksGet { server, .. }
        | Operation::TasksUpdate { server, .. }
        | Operation::TasksCancel { server, .. } = &request.operation
        {
            let Some(cfg) = self.config.config.servers.get(server) else {
                return Err(Failure::new(
                    2,
                    "unknown_server",
                    format!("unknown server {server:?}"),
                ));
            };
            if !cfg.enabled() {
                return Err(Failure::new(
                    2,
                    "server_disabled",
                    format!("server {server:?} is disabled"),
                ));
            }
        }
        if let Operation::Call {
            tool,
            arguments,
            expected_schema,
            ..
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
            if let Some(schema) = expected_schema
                && (!crate::schema::schema_depth_ok(
                    &schema.input_schema,
                    crate::schema::MAX_SCHEMA_DEPTH,
                ) || schema.output_schema.as_ref().is_some_and(|s| {
                    !crate::schema::schema_depth_ok(s, crate::schema::MAX_SCHEMA_DEPTH)
                }))
            {
                return Err(Failure::new(
                    2,
                    "schema",
                    format!(
                        "expected schema exceeds maximum nesting depth of {}",
                        crate::schema::MAX_SCHEMA_DEPTH
                    ),
                ));
            }
        }
        if let Operation::SkillsGet { selector, .. }
        | Operation::SkillsFiles { selector, .. }
        | Operation::SkillsRead { selector, .. } = &request.operation
            && (selector.is_empty()
                || selector.len() > 2048
                || selector.bytes().any(|b| b < 0x20 || b == 0x7f))
        {
            return Err(Failure::new(
                2,
                "invalid_skill_uri",
                "invalid remote skill selector",
            ));
        }
        if let Operation::SkillsFiles { dir: Some(dir), .. } = &request.operation
            && (dir.len() > 2048 || dir.bytes().any(|b| b < 0x20 || b == 0x7f))
        {
            return Err(Failure::new(
                2,
                "invalid_skill_path",
                "invalid remote skill directory",
            ));
        }
        if let Operation::SkillsRead { path, .. } = &request.operation
            && (path.is_empty() || path.len() > 2048 || path.bytes().any(|b| b < 0x20 || b == 0x7f))
        {
            return Err(Failure::new(
                2,
                "invalid_skill_path",
                "invalid remote skill path",
            ));
        }
        if let Operation::TasksGet { task_id, .. }
        | Operation::TasksUpdate { task_id, .. }
        | Operation::TasksCancel { task_id, .. } = &request.operation
            && (task_id.is_empty()
                || task_id.len() > 512
                || task_id.bytes().any(|b| b < 0x20 || b == 0x7f))
        {
            return Err(Failure::new(2, "invalid_task_id", "invalid task_id"));
        }
        if let Operation::TasksUpdate {
            input_responses, ..
        } = &request.operation
        {
            if input_responses.is_empty() {
                return Err(Failure::new(
                    2,
                    "invalid_arguments",
                    "input_responses must be a non-empty JSON object",
                ));
            }
            let size = crate::json::serialized_len(input_responses).map_err(|_| {
                Failure::new(2, "invalid_arguments", "cannot encode input_responses")
            })?;
            if size > ipc::ARGUMENT_LIMIT {
                return Err(Failure::new(
                    2,
                    "invalid_arguments",
                    "input_responses exceed the 1 MiB limit",
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

    pub(super) fn event_arguments_lease(
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

    pub(super) async fn poll_task_in_daemon(
        &self,
        params: DaemonTaskPoll<'_>,
        conn_permit: &mut Option<OwnedSemaphorePermit>,
    ) -> std::result::Result<TaskSnapshot, Failure> {
        let DaemonTaskPoll {
            server,
            initial_conn_id,
            seed,
            task_id,
            owns_task,
            deadlines,
            cancel,
            polling_started,
            grace_notify,
        } = params;
        let _waiter_permit = self.task_waiters.try_acquire().map_err(|_| {
            let mut fail =
                Failure::new(4, "overload", "too many concurrent waiting tasks on daemon")
                    .uncertain(owns_task);
            if owns_task {
                fail.data = Some(json!({"taskId": task_id}));
            }
            fail
        })?;
        // Release the MAX_CONNECTIONS inbound permit once the task_waiters slot is held.
        drop(conn_permit.take());
        polling_started.store(true, Ordering::Release);
        if let Some(notify) = grace_notify {
            notify.notify_one();
        }

        let server_state = self.servers[server].clone();
        let poll_budgets = TaskPollBudgets {
            step_idle: deadlines.call_idle,
            hard_deadline: deadlines.call_hard,
        };
        let poll_cancel = cancel.clone();
        let poll_task_id = task_id.clone();
        let cancel_task_id = task_id.clone();

        poll_task_to_completion(
            seed,
            task_id,
            owns_task,
            poll_budgets,
            cancel,
            |step_budget| {
                let server_state = server_state.clone();
                let poll_cancel = poll_cancel.clone();
                let poll_task_id = poll_task_id.clone();
                let server = server.to_owned();
                async move {
                    let step_deadline = Instant::now() + step_budget;
                    let mut state = tokio::select! {
                        biased;
                        () = poll_cancel.cancelled() => {
                            return Err(McpOpError::new(4, "cancelled", "MCP task poll cancelled"));
                        }
                        res = timeout_at(step_deadline, server_state.lock()) => match res {
                            Ok(guard) => guard,
                            Err(_) => return Err(McpOpError::new(5, "timeout", "MCP tasks/get timed out waiting for server lock")),
                        },
                    };
                    let _work = tokio::select! {
                        biased;
                        () = poll_cancel.cancelled() => {
                            return Err(McpOpError::new(4, "cancelled", "MCP task poll cancelled"));
                        }
                        res = timeout_at(step_deadline, self.work.acquire()) => match res {
                            Ok(Ok(permit)) => permit,
                            Ok(Err(_)) => return Err(McpOpError::new(1, "stopping", "daemon is stopping")),
                            Err(_) => return Err(McpOpError::new(5, "timeout", "MCP tasks/get timed out while queued")),
                        },
                    };
                    if state.conn_id != initial_conn_id || state.connection.is_none() {
                        // Zero pollution: never mark_lost() or mutate state after a reset/reconnect.
                        return Err(McpOpError::new(
                            6,
                            "session_lost",
                            format!("server {server:?} was reset while polling task {poll_task_id:?}"),
                        ));
                    }
                    if state.lost
                        || state
                            .connection
                            .as_ref()
                            .is_some_and(McpConnection::is_closed)
                    {
                        state.mark_lost();
                        return Err(McpOpError::new(
                            6,
                            "session_lost",
                            format!("server {server:?} disconnected while polling task {poll_task_id:?}"),
                        ));
                    }
                    let remaining = step_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(McpOpError::new(5, "timeout", "MCP tasks/get timed out"));
                    }
                    let conn = state.connection.as_ref().expect("checked Some");
                    let res = conn.get_task(&poll_task_id, remaining).await;
                    if conn.is_closed() {
                        state.mark_lost();
                    }
                    res
                }
            },
            |cancel_budget| {
                let server_state = server_state.clone();
                let cancel_task_id = cancel_task_id.clone();
                async move {
                    let cancel_deadline = Instant::now() + cancel_budget;
                    let Ok(mut state) = timeout_at(cancel_deadline, server_state.lock()).await
                    else {
                        return;
                    };
                    let Ok(Ok(_work)) = timeout_at(cancel_deadline, self.work.acquire()).await
                    else {
                        return;
                    };
                    if state.conn_id != initial_conn_id || state.lost {
                        return;
                    }
                    let remaining = cancel_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return;
                    }
                    if let Some(conn) = state.connection.as_ref()
                        && !conn.is_closed()
                    {
                        let cancel_dispatched = AtomicBool::new(false);
                        let _ = conn
                            .cancel_task(&cancel_task_id, remaining, &cancel_dispatched)
                            .await;
                        if conn.is_closed() {
                            state.mark_lost();
                        }
                    }
                }
            },
        )
        .await
        .map_err(|e| mcp_op_error(e, Some(server)))
    }

    pub(super) async fn execute(
        &self,
        operation: &Operation,
        ctx: ExecuteContext<'_>,
        conn_permit: &mut Option<OwnedSemaphorePermit>,
    ) -> ExecutionOutcome {
        let ExecuteContext {
            deadlines,
            dispatched,
            client_cancel,
            polling_started,
            grace_notify,
        } = ctx;
        let deadline = deadlines.pre_dispatch;
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
                let events = result.map_err(|e| mcp_error_for_server(e, false, Some(server)))?;
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
                let polled = result.map_err(|e| mcp_error_for_server(e, false, Some(server)))?;
                let (value, lease) = polled.into_parts();
                return Ok((value, lease, None));
            }
            _ => {}
        }
        let server = match operation {
            Operation::Server { server }
            | Operation::Tools { server }
            | Operation::Call { server, .. }
            | Operation::Reset { server, .. }
            | Operation::SkillsList { server }
            | Operation::SkillsGet { server, .. }
            | Operation::SkillsFiles { server, .. }
            | Operation::SkillsRead { server, .. }
            | Operation::TasksGet { server, .. }
            | Operation::TasksUpdate { server, .. }
            | Operation::TasksCancel { server, .. } => server,
            _ => unreachable!(),
        };
        let state = self.servers.get(server).ok_or_else(|| {
            Failure::new(2, "unknown_server", format!("unknown server {server:?}"))
        })?;
        let _force_reset = if matches!(operation, Operation::Reset { force: true, .. }) {
            Some(ForceResetCancellation::begin(&self.call_cancels[server]))
        } else {
            None
        };
        let mut state = if let Some(cancel) = client_cancel {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    return Err(Failure::new(
                        4,
                        "cancelled",
                        "MCP operation cancelled while queued",
                    ));
                }
                res = timeout_at(deadline, state.lock()) => match res {
                    Ok(guard) => guard,
                    Err(_) => {
                        return Err(Failure::new(
                            5,
                            "timeout",
                            "request deadline exceeded while queued",
                        ));
                    }
                },
            }
        } else {
            match timeout_at(deadline, state.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    return Err(Failure::new(
                        5,
                        "timeout",
                        "request deadline exceeded while queued",
                    ));
                }
            }
        };
        let _call_cancel_reg;
        let call_cancel = if operation.is_call() || operation.uses_dual_clock() {
            let base_token = client_cancel
                .map(CancellationToken::child_token)
                .unwrap_or_default();
            let (reg, token) =
                CallCancelRegistration::register(&self.call_cancels[server], base_token);
            _call_cancel_reg = Some(reg);
            Some(token)
        } else {
            _call_cancel_reg = None;
            None
        };
        let _work = match timeout_at(deadline, async {
            if let Some(cancel) = &call_cancel {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => Err(Failure::new(
                        4,
                        "cancelled",
                        "MCP call cancelled while queued",
                    )),
                    res = self.work.acquire() => res.map_err(|_| Failure::new(1, "stopping", "daemon is stopping")),
                }
            } else {
                self.work
                    .acquire()
                    .await
                    .map_err(|_| Failure::new(1, "stopping", "daemon is stopping"))
            }
        })
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(failure)) => return Err(failure),
            Err(_) => {
                return Err(Failure::new(
                    5,
                    "timeout",
                    "request deadline exceeded while queued",
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(Failure::new(
                5,
                "timeout",
                "request deadline exceeded while queued",
            ));
        }
        if let Operation::Reset { force, .. } = operation {
            state.mark_lost();
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
            let connect =
                McpConnection::connect_with_limits(config, budget, self.options.mcp_limits.clone());
            let connection = if let Some(cancel) = &call_cancel {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {
                        return Err(Failure::new(
                            4,
                            "cancelled",
                            "MCP call cancelled before connection completed",
                        ));
                    }
                    res = connect => res.map_err(|e| mcp_error_for_server(e, false, Some(server)))?,
                }
            } else {
                connect
                    .await
                    .map_err(|e| mcp_error_for_server(e, false, Some(server)))?
            };
            state.connection = Some(connection);
            state.conn_id = state.conn_id.wrapping_add(1);
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
        let initial_conn_id = state.conn_id;
        let connection = state
            .connection
            .as_ref()
            .expect("connection was established");
        match operation {
            Operation::Server { .. } => {
                let result = connection.server_info();
                let closed = connection.is_closed();
                if closed {
                    state.mark_lost();
                }
                result
                    .map(|value| {
                        let (value, lease) = value.into_parts();
                        (value, lease, None)
                    })
                    .map_err(|e| mcp_error_for_server(e, false, Some(server)))
            }
            Operation::Tools { .. } => {
                let result = connection.list_tool_pages(budget).await.map(|pages| {
                    let (pages, lease) = pages.into_parts();
                    let mut value = serde_json::Map::new();
                    value.insert("pages".into(), Value::Array(pages));
                    (Value::Object(value), lease)
                });
                let closed = connection.is_closed();
                if closed {
                    state.mark_lost();
                }
                result
                    .map(|(value, lease)| (value, lease, None))
                    .map_err(|e| mcp_error_for_server(e, false, Some(server)))
            }
            Operation::Call {
                tool,
                arguments,
                inline_media,
                detach,
                expected_schema,
                ..
            } => {
                if *detach {
                    connection
                        .check_tasks_support()
                        .map_err(|e| mcp_op_error(e, Some(server)))?;
                }
                let remaining_hard = deadlines
                    .call_hard
                    .map(|hard| hard.saturating_duration_since(Instant::now()));
                if remaining_hard.is_some_and(|d| d.is_zero()) {
                    return Err(Failure::new(
                        5,
                        "timeout",
                        "request deadline exceeded before dispatch",
                    ));
                }
                let budgets = CallBudgets {
                    pre_dispatch_deadline: deadline,
                    idle: deadlines.call_idle,
                    hard: remaining_hard,
                };
                let cancel = call_cancel.expect("call cancellation token registered");
                let result = connection
                    .call_with_options_leased(
                        tool,
                        arguments,
                        CallOptions {
                            expected_schema: expected_schema.as_ref(),
                        },
                        budgets,
                        dispatched,
                        cancel.clone(),
                    )
                    .await;
                let closed = connection.is_closed();
                if closed {
                    state.mark_lost();
                }
                let outcome = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                match outcome {
                    CallOutcome::Complete {
                        mut value,
                        mut lease,
                    } => {
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
                    CallOutcome::Task { seed, value, lease } => {
                        if *detach {
                            return Ok((value, lease, None));
                        }
                        let task_id = seed.task_id.clone();
                        drop(value);
                        drop(lease);
                        drop(_work);
                        drop(state);
                        let mut snapshot = self
                            .poll_task_in_daemon(
                                DaemonTaskPoll {
                                    server,
                                    initial_conn_id,
                                    seed: Some(seed),
                                    task_id: task_id.clone(),
                                    owns_task: true,
                                    deadlines,
                                    cancel,
                                    polling_started,
                                    grace_notify,
                                },
                                conn_permit,
                            )
                            .await?;
                        let Some(mut result_val) = snapshot
                            .value
                            .as_object_mut()
                            .and_then(|o| o.remove("result"))
                        else {
                            let mut fail = Failure::new(
                                4,
                                "mcp_protocol",
                                format!("completed task {task_id:?} response missing 'result'"),
                            )
                            .uncertain(true);
                            fail.data = Some(json!({"taskId": task_id}));
                            return Err(fail);
                        };
                        let mut lease = snapshot.lease;
                        let inflight = if *inline_media {
                            None
                        } else {
                            self.options
                                .media
                                .extract_and_save(&mut result_val, &mut lease, &self.options.budget)
                                .await?
                        };
                        Ok((result_val, lease, inflight))
                    }
                }
            }
            Operation::SkillsList { .. } => {
                let result = connection.skills_list(server, budget).await;
                if connection.is_closed() {
                    state.mark_lost();
                }
                let (value, lease) = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                Ok((value, lease, None))
            }
            Operation::SkillsGet { selector, .. } => {
                let result = connection.skills_show(server, selector, budget).await;
                if connection.is_closed() {
                    state.mark_lost();
                }
                let (value, lease) = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                Ok((value, lease, None))
            }
            Operation::SkillsFiles { selector, dir, .. } => {
                let result = connection
                    .skills_files(server, selector, dir.as_deref(), budget)
                    .await;
                if connection.is_closed() {
                    state.mark_lost();
                }
                let (value, lease) = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                Ok((value, lease, None))
            }
            Operation::SkillsRead {
                selector,
                path,
                inline_media,
                ..
            } => {
                let result = connection
                    .skills_read(
                        server,
                        selector,
                        path,
                        *inline_media,
                        &self.options.media,
                        budget,
                    )
                    .await;
                if connection.is_closed() {
                    state.mark_lost();
                }
                result.map_err(|e| mcp_op_error(e, Some(server)))
            }
            Operation::TasksGet {
                task_id,
                wait,
                inline_media,
                ..
            } => {
                if !*wait {
                    let result = connection.get_task(task_id, budget).await;
                    if connection.is_closed() {
                        state.mark_lost();
                    }
                    let mut snapshot = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                    let inflight = if snapshot.status == TaskStatusKind::Completed && !*inline_media
                    {
                        self.options
                            .media
                            .extract_task_result_media(
                                &mut snapshot.value,
                                &mut snapshot.lease,
                                &self.options.budget,
                            )
                            .await?
                    } else {
                        None
                    };
                    Ok((snapshot.value, snapshot.lease, inflight))
                } else {
                    connection
                        .check_tasks_support()
                        .map_err(|e| mcp_op_error(e, Some(server)))?;
                    drop(_work);
                    drop(state);
                    let cancel = call_cancel.expect("task wait cancellation token registered");
                    let mut snapshot = self
                        .poll_task_in_daemon(
                            DaemonTaskPoll {
                                server,
                                initial_conn_id,
                                seed: None,
                                task_id: task_id.clone(),
                                owns_task: false,
                                deadlines,
                                cancel,
                                polling_started,
                                grace_notify,
                            },
                            conn_permit,
                        )
                        .await?;
                    let inflight = if !*inline_media {
                        self.options
                            .media
                            .extract_task_result_media(
                                &mut snapshot.value,
                                &mut snapshot.lease,
                                &self.options.budget,
                            )
                            .await?
                    } else {
                        None
                    };
                    Ok((snapshot.value, snapshot.lease, inflight))
                }
            }
            Operation::TasksUpdate {
                task_id,
                input_responses,
                ..
            } => {
                let result = connection
                    .update_task(task_id, input_responses.clone(), budget, dispatched)
                    .await;
                if connection.is_closed() {
                    state.mark_lost();
                }
                let value = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                Ok((value, empty(), None))
            }
            Operation::TasksCancel { task_id, .. } => {
                let result = connection.cancel_task(task_id, budget, dispatched).await;
                if connection.is_closed() {
                    state.mark_lost();
                }
                let value = result.map_err(|e| mcp_op_error(e, Some(server)))?;
                Ok((value, empty(), None))
            }
            _ => unreachable!(),
        }
    }
}
