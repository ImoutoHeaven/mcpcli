use super::args::{
    AuthCommand, Cli, Command, DaemonCommand, SkillCommand, TasksCommand, load, usage,
    validate_call_max_timeout,
};
use super::input::{
    arguments, arguments_cancellable, load_expected_schema, parse_kv_cli_arguments,
    validate_task_id,
};
use super::offline;
use super::output::{
    Output, RemoteOutput, listed_tools, project_tools, server_view, take_listed_tool,
};
use anyhow::{Context, Result};
use mcpcli::{
    auth,
    budget::{ByteBudget, Leased},
    config::{self, LoadedConfig, ServerConfig},
    daemon,
    discovery::safe_text,
    ipc::{Failure, Operation},
    mcp::{
        CallBudgets, CallOptions, CallOutcome, McpConnection, PollEventsParams,
        tasks::{TaskPollBudgets, TaskStatusKind, poll_task_to_completion},
    },
    media::{InflightBatch, MediaStore},
    runtime::{self, Paths},
    skills,
};
use serde_json::{Map, Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{Instant, timeout, timeout_at};

async fn close_after_error(connection: &mut McpConnection, error: &mut anyhow::Error) {
    if let Err(cleanup) = connection.close().await
        && let Some(failure) = error.downcast_mut::<Failure>()
    {
        failure.message = format!(
            "{}; additionally: connection cleanup failed: {cleanup}",
            failure.message
        );
    }
}

pub(super) async fn remote(
    cli: &Cli,
    loaded: &LoadedConfig,
    operation: Leased<Operation>,
    max_timeout: Option<u64>,
    uncertain: &AtomicBool,
    interrupt: Option<&tokio_util::sync::CancellationToken>,
) -> Result<RemoteOutput> {
    Box::pin(remote_inner(
        cli,
        loaded,
        operation,
        max_timeout,
        uncertain,
        interrupt,
    ))
    .await
}

async fn remote_inner(
    cli: &Cli,
    loaded: &LoadedConfig,
    operation: Leased<Operation>,
    max_timeout: Option<u64>,
    uncertain: &AtomicBool,
    interrupt: Option<&tokio_util::sync::CancellationToken>,
) -> Result<RemoteOutput> {
    let server = match &*operation {
        Operation::Server { server }
        | Operation::Tools { server }
        | Operation::Call { server, .. }
        | Operation::Reset { server, .. }
        | Operation::EventsList { server }
        | Operation::EventsPoll { server, .. }
        | Operation::SkillsList { server }
        | Operation::SkillsGet { server, .. }
        | Operation::SkillsFiles { server, .. }
        | Operation::SkillsRead { server, .. }
        | Operation::TasksGet { server, .. }
        | Operation::TasksUpdate { server, .. }
        | Operation::TasksCancel { server, .. } => server,
        _ => unreachable!(),
    };
    let config = loaded
        .config
        .servers
        .get(server)
        .ok_or_else(|| usage("unknown_server", format!("unknown server {server:?}")))?;
    if !config.enabled() {
        return Err(usage(
            "server_disabled",
            format!("server {server:?} is disabled"),
        ));
    }
    cli.validate_request_timeout()?;
    let timeout_secs = cli.timeout_secs_for(config);
    let uses_dual_clock = operation.uses_dual_clock();
    let hard = if uses_dual_clock {
        validate_call_max_timeout(timeout_secs, max_timeout)?
    } else {
        None
    };
    let budget = Duration::from_secs(timeout_secs);
    let started = Instant::now();
    if cli.oneshot {
        let cancel_token = interrupt.cloned().unwrap_or_default();
        let oneshot_fut = async {
            if matches!(&*operation, Operation::Reset { .. }) {
                return Err(usage("mode", "reset cannot be used with --oneshot"));
            }
            let connect_fut = McpConnection::connect(config, budget.min(Duration::from_secs(30)));
            let mut connection = if interrupt.is_some() {
                tokio::select! {
                    biased;
                    () = cancel_token.cancelled() => {
                        return Err(Failure::new(
                            130,
                            "cancelled",
                            "interrupted before connecting to MCP server",
                        )
                        .into());
                    }
                    res = connect_fut => res.map_err(|error| daemon::mcp_error_for_server(error, false, Some(server)))?,
                }
            } else {
                connect_fut
                    .await
                    .map_err(|error| daemon::mcp_error_for_server(error, false, Some(server)))?
            };
            let remaining = budget.saturating_sub(started.elapsed());
            let process_budget = ByteBudget::global();
            let media_store = MediaStore::default();

            let exec_result: Result<(Value, mcpcli::budget::ByteLease, Option<InflightBatch>)> =
                async {
                    match &*operation {
                    Operation::Server { .. } => connection
                        .server_info()
                        .map(|value| {
                            let (v, l) = value.into_parts();
                            (v, l, None)
                        })
                        .map_err(|e| daemon::mcp_error_for_server(e, false, Some(server)).into()),
                    Operation::Tools { .. } => connection
                        .list_tool_pages(remaining)
                        .await
                        .map(|pages| {
                            let (pages, lease) = pages.into_parts();
                            let mut value = Map::new();
                            value.insert("pages".into(), Value::Array(pages));
                            (Value::Object(value), lease, None)
                        })
                        .map_err(|e| daemon::mcp_error_for_server(e, false, Some(server)).into()),
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
                                .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                        }
                        let remaining_hard = hard.map(|h| h.saturating_sub(started.elapsed()));
                        if remaining_hard.is_some_and(|d| d.is_zero()) {
                            return Err(Failure::new(
                                5,
                                "timeout",
                                "request deadline exceeded before dispatch",
                            )
                            .into());
                        }
                        let budgets = CallBudgets {
                            pre_dispatch_deadline: started + budget,
                            idle: budget,
                            hard: remaining_hard,
                        };
                        let outcome = connection
                            .call_with_options_leased(
                                tool,
                                arguments,
                                CallOptions {
                                    expected_schema: expected_schema.as_ref(),
                                },
                                budgets,
                                uncertain,
                                cancel_token.clone(),
                            )
                            .await
                            .map_err(|e| {
                                anyhow::Error::from(daemon::mcp_op_error(e, Some(server)))
                            });
                        match outcome {
                            Ok(CallOutcome::Complete {
                                mut value,
                                mut lease,
                            }) => {
                                let inflight = if *inline_media {
                                    None
                                } else {
                                    media_store
                                        .extract_and_save(&mut value, &mut lease, process_budget)
                                        .await
                                        .map_err(anyhow::Error::from)?
                                };
                                Ok((value, lease, inflight))
                            }
                            Ok(CallOutcome::Task { seed, value, lease }) => {
                                if *detach {
                                    Ok((value, lease, None))
                                } else {
                                    let task_id = seed.task_id.clone();
                                    drop(value);
                                    drop(lease);
                                    let poll_budgets = TaskPollBudgets {
                                        step_idle: budget,
                                        hard_deadline: hard.map(|h| started + h),
                                    };
                                    let conn_ref = &connection;
                                    let poll_id = task_id.clone();
                                    let cancel_id = task_id.clone();
                                    let mut snapshot = Box::pin(poll_task_to_completion(
                                        Some(seed),
                                        task_id.clone(),
                                        true,
                                        poll_budgets,
                                        cancel_token.clone(),
                                        |step_budget| conn_ref.get_task(&poll_id, step_budget),
                                        |cancel_budget| {
                                            let cancel_id = cancel_id.clone();
                                            async move {
                                                let d = AtomicBool::new(false);
                                                let _ = conn_ref
                                                    .cancel_task(&cancel_id, cancel_budget, &d)
                                                    .await;
                                            }
                                        },
                                    ))
                                    .await
                                    .map_err(|e| {
                                        anyhow::Error::from(daemon::mcp_op_error(e, Some(server)))
                                    })?;
                                    let Some(mut result_val) = snapshot
                                        .value
                                        .as_object_mut()
                                        .and_then(|o| o.remove("result"))
                                    else {
                                        let mut fail = Failure::new(
                                            4,
                                            "mcp_protocol",
                                            format!(
                                                "completed task {task_id:?} response missing 'result'"
                                            ),
                                        )
                                        .uncertain(true);
                                        fail.data = Some(json!({"taskId": task_id}));
                                        return Err(fail.into());
                                    };
                                    let mut lease = snapshot.lease;
                                    let inflight = if *inline_media {
                                        None
                                    } else {
                                        media_store
                                            .extract_and_save(
                                                &mut result_val,
                                                &mut lease,
                                                process_budget,
                                            )
                                            .await
                                            .map_err(anyhow::Error::from)?
                                    };
                                    Ok((result_val, lease, inflight))
                                }
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Operation::SkillsList { .. } => connection
                        .skills_list(server, remaining)
                        .await
                        .map(|(v, l)| (v, l, None))
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)).into()),
                    Operation::SkillsGet { selector, .. } => connection
                        .skills_show(server, selector, remaining)
                        .await
                        .map(|(v, l)| (v, l, None))
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)).into()),
                    Operation::SkillsFiles { selector, dir, .. } => connection
                        .skills_files(server, selector, dir.as_deref(), remaining)
                        .await
                        .map(|(v, l)| (v, l, None))
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)).into()),
                    Operation::SkillsRead {
                        selector,
                        path,
                        inline_media,
                        ..
                    } => connection
                        .skills_read(
                            server,
                            selector,
                            path,
                            *inline_media,
                            &media_store,
                            remaining,
                        )
                        .await
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)).into()),
                    Operation::TasksGet {
                        task_id,
                        wait,
                        inline_media,
                        ..
                    } => {
                        if !*wait {
                            let mut snapshot = connection
                                .get_task(task_id, remaining)
                                .await
                                .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                            let inflight =
                                if snapshot.status == TaskStatusKind::Completed && !*inline_media {
                                    media_store
                                        .extract_task_result_media(
                                            &mut snapshot.value,
                                            &mut snapshot.lease,
                                            process_budget,
                                        )
                                        .await
                                        .map_err(anyhow::Error::from)?
                                } else {
                                    None
                                };
                            Ok((snapshot.value, snapshot.lease, inflight))
                        } else {
                            connection
                                .check_tasks_support()
                                .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                            let poll_budgets = TaskPollBudgets {
                                step_idle: budget,
                                hard_deadline: hard.map(|h| started + h),
                            };
                            let conn_ref = &connection;
                            let mut snapshot = Box::pin(poll_task_to_completion(
                                None,
                                task_id.clone(),
                                false,
                                poll_budgets,
                                cancel_token.clone(),
                                |step_budget| conn_ref.get_task(task_id, step_budget),
                                |_| async {},
                            ))
                            .await
                            .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                            let inflight = if !*inline_media {
                                media_store
                                    .extract_task_result_media(
                                        &mut snapshot.value,
                                        &mut snapshot.lease,
                                        process_budget,
                                    )
                                    .await
                                    .map_err(anyhow::Error::from)?
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
                    } => connection
                        .update_task(task_id, input_responses.clone(), remaining, uncertain)
                        .await
                        .map(|v| (v, process_budget.empty_lease(), None))
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)).into()),
                    Operation::TasksCancel { task_id, .. } => connection
                        .cancel_task(task_id, remaining, uncertain)
                        .await
                        .map(|v| (v, process_budget.empty_lease(), None))
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)).into()),
                    Operation::EventsList { .. } => connection
                        .list_events(remaining)
                        .await
                        .map(|events| {
                            let (events, lease) = events.into_parts();
                            let mut value = Map::new();
                            value.insert("events".into(), Value::Array(events));
                            (Value::Object(value), lease, None)
                        })
                        .map_err(|e| daemon::mcp_error_for_server(e, false, Some(server)).into()),
                    Operation::EventsPoll {
                        event,
                        arguments,
                        cursor,
                        max_age_ms,
                        max_events,
                        ..
                    } => {
                        let params = PollEventsParams {
                            name: event.clone(),
                            arguments: arguments.clone(),
                            cursor: cursor.clone(),
                            max_age_ms: *max_age_ms,
                            max_events: *max_events,
                        };
                        connection
                            .poll_events(params, remaining)
                            .await
                            .map(|value| {
                                let (v, l) = value.into_parts();
                                (v, l, None)
                            })
                            .map_err(|e| {
                                daemon::mcp_error_for_server(e, false, Some(server)).into()
                            })
                    }
                    _ => unreachable!(),
                    }
                }
                .await;
            match exec_result {
                Ok((value, lease, inflight)) => {
                    uncertain.store(false, Ordering::Release);
                    Ok(RemoteOutput {
                        value,
                        connection: Some(connection),
                        inflight,
                        lease: Some(lease),
                    })
                }
                Err(mut error) => {
                    close_after_error(&mut connection, &mut error).await;
                    Err(error)
                }
            }
        };
        let oneshot_fut = Box::pin(oneshot_fut);
        if uses_dual_clock {
            if let Some(h) = hard {
                return timeout(h + Duration::from_secs(10), oneshot_fut)
                    .await
                    .map_err(|_| {
                        Failure::new(
                            5,
                            "timeout",
                            "request deadline exceeded; the call is not replayed",
                        )
                        .uncertain(uncertain.load(Ordering::Acquire))
                    })?;
            }
            return oneshot_fut.await;
        }
        return timeout(budget + Duration::from_secs(10), oneshot_fut)
            .await
            .map_err(|_| {
                Failure::new(
                    5,
                    "timeout",
                    "request deadline exceeded; the call is not replayed",
                )
                .uncertain(uncertain.load(Ordering::Acquire))
            })?;
    }

    let paths = Paths::new(loaded.path.clone(), &cli.session)?;
    let ensure_fut = timeout(
        budget.saturating_sub(started.elapsed()),
        runtime::ensure(&paths, loaded, false),
    );
    if let Some(token) = interrupt {
        tokio::select! {
            biased;
            () = token.cancelled() => {
                return Err(Failure::new(
                    130,
                    "cancelled",
                    "interrupted while starting the daemon; tool was not dispatched",
                )
                .into());
            }
            res = ensure_fut => {
                res.map_err(|_| {
                    Failure::new(
                        5,
                        "timeout",
                        "deadline elapsed while starting the daemon; tool was not dispatched",
                    )
                })??;
            }
        }
    } else {
        ensure_fut.await.map_err(|_| {
            Failure::new(
                5,
                "timeout",
                "deadline elapsed while starting the daemon; tool was not dispatched",
            )
        })??;
    }
    let elapsed = started.elapsed();
    let (remaining, _) = check_dispatch_budgets(budget, hard, elapsed)?;
    let (input_lease, operation) = {
        let (operation, lease) = operation.into_parts();
        (lease, operation)
    };
    let request = if uses_dual_clock {
        paths.request_call_with_budgets(&loaded.digest, operation, budget, hard, elapsed)?
    } else {
        paths.request_with_explicit(
            &loaded.digest,
            operation,
            remaining,
            cli.has_explicit_timeout(),
        )?
    };
    let request = Leased::new(request, input_lease);
    let (response_lease, response) = if let Some(token) = interrupt {
        Box::pin(runtime::exchange_cancellable(
            &paths, &request, uncertain, token,
        ))
        .await
    } else {
        Box::pin(runtime::exchange_tracked(&paths, &request, uncertain)).await
    }
    .map(|(response, lease)| (lease, response))?;
    Ok(RemoteOutput {
        value: response.result.context("daemon did not return a result")?,
        connection: None,
        inflight: None,
        lease: Some(response_lease),
    })
}

pub(super) fn check_dispatch_budgets(
    budget: Duration,
    hard: Option<Duration>,
    elapsed: Duration,
) -> std::result::Result<(Duration, Option<Duration>), Failure> {
    let remaining = budget.saturating_sub(elapsed);
    let remaining_hard = hard.map(|h| h.saturating_sub(elapsed));
    if remaining < Duration::from_millis(1)
        || remaining_hard.is_some_and(|d| d < Duration::from_millis(1))
    {
        return Err(Failure::new(
            5,
            "timeout",
            "deadline elapsed before dispatch",
        ));
    }
    Ok((remaining, remaining_hard))
}

async fn notify_daemon_reset(cli: &Cli, loaded: &LoadedConfig, server: &str, deadline: Instant) {
    if cli.oneshot || Instant::now() >= deadline {
        return;
    }
    let Ok(paths) = Paths::new(loaded.path.clone(), &cli.session) else {
        return;
    };
    if !paths.running().unwrap_or(false) {
        return;
    }
    let deadline = deadline.min(Instant::now() + Duration::from_secs(5));
    let budget = deadline.saturating_duration_since(Instant::now());
    if budget < Duration::from_millis(1) {
        return;
    }
    if let Ok(req) = paths.request(
        &loaded.digest,
        Operation::Reset {
            server: server.to_owned(),
            force: true,
        },
        budget,
    ) {
        let _ = timeout_at(deadline, runtime::exchange(&paths, &req)).await;
    }
}

fn empty_leased(op: Operation) -> Leased<Operation> {
    Leased::new(op, ByteBudget::global().empty_lease())
}

fn enabled_server<'a>(loaded: &'a LoadedConfig, server: &str) -> Result<&'a ServerConfig> {
    let cfg = loaded
        .config
        .servers
        .get(server)
        .ok_or_else(|| usage("unknown_server", format!("unknown server {server:?}")))?;
    if !cfg.enabled() {
        return Err(usage(
            "server_disabled",
            format!("server {server:?} is disabled"),
        ));
    }
    Ok(cfg)
}

pub(super) async fn run_daemon(cli: &Cli, command: &DaemonCommand) -> Result<Output> {
    if cli.oneshot {
        return Err(usage(
            "mode",
            "daemon commands cannot be used with --oneshot",
        ));
    }
    let locator = config::locate(&cli.config).map_err(|error| usage("configuration", error))?;
    let paths = Paths::new(locator, &cli.session)?;
    match command {
        DaemonCommand::Status => {
            let offline = || {
                Output::json(
                    json!({"running":false,"session":cli.session,"namespace":paths.namespace}),
                )
            };
            if !paths.running()? {
                Ok(offline())
            } else {
                match runtime::exchange(
                    &paths,
                    &paths.request("", Operation::Status, Duration::from_secs(2))?,
                )
                .await
                {
                    Ok((response, lease)) => {
                        Ok(Output::json(response.result.unwrap()).with_lease(Some(lease)))
                    }
                    Err(_) if !paths.running()? => Ok(offline()),
                    Err(error) => Err(error),
                }
            }
        }
        DaemonCommand::Stop => Ok(Output::json(runtime::stop(&paths).await?)),
        DaemonCommand::Run => {
            let loaded = load(cli)?;
            daemon::run(paths, loaded).await?;
            Ok(Output::json(json!({"running":false,"stopped":true})))
        }
        DaemonCommand::Start | DaemonCommand::Restart => {
            let loaded = load(cli)?;
            if matches!(command, DaemonCommand::Restart)
                && let Err(error) = runtime::stop(&paths).await
            {
                if paths.running()? {
                    return Err(error);
                }
                eprintln!("mcpcli: {}", safe_text(&format!("{error:#}")));
            }
            runtime::ensure(&paths, &loaded, true).await?;
            let (response, response_lease) = runtime::exchange(
                &paths,
                &paths.request("", Operation::Status, Duration::from_secs(2))?,
            )
            .await?;
            let status = response.result.unwrap();
            if status["config_digest"].as_str() != Some(&loaded.digest) {
                return Err(Failure::new(
                    6,
                    "daemon_mismatch",
                    "daemon configuration changed; run daemon restart",
                )
                .into());
            }
            Ok(Output::json(status).with_lease(Some(response_lease)))
        }
    }
}

pub(super) async fn run_online(
    cli: &Cli,
    loaded: &LoadedConfig,
    uncertain: &AtomicBool,
    interrupt: Option<&tokio_util::sync::CancellationToken>,
) -> Result<Output> {
    match &cli.command {
        Command::Server { server } => {
            let op = empty_leased(Operation::Server {
                server: server.clone(),
            });
            let remote = remote(cli, loaded, op, None, uncertain, interrupt).await?;
            Ok(remote.into_output(|value| {
                if cli.text {
                    server_view(&value)?;
                }
                Ok(Output::json(value))
            }))
        }
        Command::Tools {
            server,
            filter,
            summary,
        } => {
            if let Some(substr) = filter
                && substr.is_empty()
            {
                return Err(usage("query", "filter must not be empty"));
            }
            let op = empty_leased(Operation::Tools {
                server: server.clone(),
            });
            let remote = remote(cli, loaded, op, None, uncertain, interrupt).await?;
            Ok(remote.into_output(|value| {
                if filter.is_some() || *summary {
                    let (value, lease) =
                        project_tools(value, filter.as_deref(), *summary, ByteBudget::global())?
                            .into_parts();
                    Ok(Output::json(value).with_lease(Some(lease)))
                } else {
                    if cli.text {
                        let _ = listed_tools(&value)?;
                    }
                    Ok(Output::json(value))
                }
            }))
        }
        Command::Inspect {
            server,
            tool,
            schema,
            schema_mode,
        } => {
            let expected = if let Some(path) = schema {
                Some(load_expected_schema(path, (*schema_mode).into(), ByteBudget::global()).await?)
            } else {
                None
            };
            let op = empty_leased(Operation::Tools {
                server: server.clone(),
            });
            let remote = remote(cli, loaded, op, None, uncertain, interrupt).await?;
            Ok(remote.into_output(|mut value| {
                let live_tool = take_listed_tool(&mut value, tool)?;
                if let Some(expected) = &expected {
                    let live_input = live_tool.get("inputSchema").unwrap_or(&Value::Null);
                    let live_output = live_tool.get("outputSchema").filter(|v| !v.is_null());
                    if let Err(reason) =
                        mcpcli::schema::verify_tool_schema(expected, live_input, live_output)
                    {
                        return Err(Failure::new(
                            2,
                            "schema_mismatch",
                            format!("Tool {tool:?} schema mismatch: {reason}"),
                        )
                        .into());
                    }
                }
                Ok(Output::json(live_tool))
            }))
        }
        Command::Call {
            server,
            tool,
            args,
            args_file,
            stdin,
            detach,
            schema,
            schema_mode,
            inline_media,
            max_timeout,
            kv_args,
        } => {
            if *detach && max_timeout.is_some() {
                return Err(usage("usage", "--max-timeout cannot be used with --detach"));
            }
            if tool.is_empty() || tool.len() > 512 || tool.chars().any(char::is_control) {
                return Err(usage("tool", "invalid tool name"));
            }
            let server_config = enabled_server(loaded, server)?;
            cli.validate_request_timeout()?;
            let timeout_secs = cli.timeout_secs_for(server_config);
            if !*detach {
                let _ = validate_call_max_timeout(timeout_secs, *max_timeout)?;
            }
            let (expected_schema, schema_lease) = if let Some(path) = schema {
                let leased =
                    load_expected_schema(path, (*schema_mode).into(), ByteBudget::global()).await?;
                let (s, l) = leased.into_parts();
                (Some(s), Some(l))
            } else {
                (None, None)
            };
            let args = if !kv_args.is_empty() {
                parse_kv_cli_arguments(kv_args, ByteBudget::global())?
            } else {
                arguments_cancellable(
                    args,
                    args_file,
                    *stdin,
                    Duration::from_secs(timeout_secs),
                    ByteBudget::global(),
                    interrupt,
                )
                .await?
            };
            let (args, mut input_lease) = args.into_parts();
            if let Some(lease) = schema_lease {
                input_lease.merge(lease);
            }
            let remote = remote(
                cli,
                loaded,
                Leased::new(
                    Operation::Call {
                        server: server.clone(),
                        tool: tool.clone(),
                        arguments: args,
                        inline_media: *inline_media,
                        detach: *detach,
                        expected_schema,
                    },
                    input_lease,
                ),
                if *detach { None } else { *max_timeout },
                uncertain,
                interrupt,
            )
            .await?;
            let is_detached_task =
                remote.value.get("resultType").and_then(Value::as_str) == Some("task");
            let code = if is_detached_task {
                0
            } else {
                let result = if *inline_media {
                    &remote.value
                } else {
                    &remote.value["result"]
                };
                if result["isError"].as_bool() == Some(true) {
                    3
                } else {
                    0
                }
            };
            let mut out = Output::json(remote.value)
                .with_connection(remote.connection)
                .with_inflight(remote.inflight)
                .with_lease(remote.lease);
            out.code = code;
            Ok(out)
        }
        Command::Tasks { command } => match command {
            TasksCommand::Get {
                server,
                task_id,
                wait,
                inline_media,
                max_timeout,
            } => {
                if !*wait && max_timeout.is_some() {
                    return Err(usage("usage", "--max-timeout requires --wait on tasks get"));
                }
                validate_task_id(task_id)?;
                let op = empty_leased(Operation::TasksGet {
                    server: server.clone(),
                    task_id: task_id.clone(),
                    wait: *wait,
                    inline_media: *inline_media,
                });
                let max_t = if *wait { *max_timeout } else { None };
                let remote = remote(cli, loaded, op, max_t, uncertain, interrupt).await?;
                let code = if *wait
                    && remote
                        .value
                        .get("result")
                        .and_then(|r| r.get("isError"))
                        .and_then(Value::as_bool)
                        == Some(true)
                {
                    3
                } else {
                    0
                };
                let mut out = Output::json(remote.value)
                    .with_connection(remote.connection)
                    .with_inflight(remote.inflight)
                    .with_lease(remote.lease);
                out.code = code;
                Ok(out)
            }
            TasksCommand::Update {
                server,
                task_id,
                args,
                args_file,
                stdin,
            } => {
                validate_task_id(task_id)?;
                if args.is_none() && args_file.is_none() && !*stdin {
                    return Err(usage(
                        "arguments",
                        "tasks update requires one of --args, --args-file, or --stdin",
                    ));
                }
                let server_config = enabled_server(loaded, server)?;
                cli.validate_request_timeout()?;
                let timeout_secs = cli.timeout_secs_for(server_config);
                let parsed = arguments(
                    args,
                    args_file,
                    *stdin,
                    Duration::from_secs(timeout_secs),
                    ByteBudget::global(),
                )
                .await?;
                let (input_responses, input_lease) = parsed.into_parts();
                if input_responses.is_empty() {
                    return Err(usage(
                        "arguments",
                        "tasks update requires a non-empty JSON object",
                    ));
                }
                let op = Leased::new(
                    Operation::TasksUpdate {
                        server: server.clone(),
                        task_id: task_id.clone(),
                        input_responses,
                    },
                    input_lease,
                );
                let remote = remote(cli, loaded, op, None, uncertain, interrupt).await?;
                Ok(remote.into_output(|value| Ok(Output::json(value))))
            }
            TasksCommand::Cancel { server, task_id } => {
                validate_task_id(task_id)?;
                let op = empty_leased(Operation::TasksCancel {
                    server: server.clone(),
                    task_id: task_id.clone(),
                });
                let remote = remote(cli, loaded, op, None, uncertain, interrupt).await?;
                Ok(remote.into_output(|value| Ok(Output::json(value))))
            }
        },
        Command::Reset { server, force } => {
            let op = empty_leased(Operation::Reset {
                server: server.clone(),
                force: *force,
            });
            let remote = remote(cli, loaded, op, None, uncertain, interrupt).await?;
            if let Some(warning) = remote.value.get("cleanup_warning").and_then(Value::as_str) {
                eprintln!("mcpcli: {}", safe_text(warning));
            }
            Ok(Output::json(remote.value)
                .with_connection(remote.connection)
                .with_inflight(remote.inflight)
                .with_lease(remote.lease))
        }
        Command::Skills { command } => {
            let remote_server = match command {
                SkillCommand::List { server }
                | SkillCommand::Show { server, .. }
                | SkillCommand::Files { server, .. }
                | SkillCommand::Read { server, .. } => server.as_deref(),
                SkillCommand::Builtin => unreachable!("handled before config load"),
            };
            let Some(server) = remote_server else {
                return offline::run_local_skills(loaded, command);
            };
            let op = match command {
                SkillCommand::List { .. } => Operation::SkillsList {
                    server: server.to_owned(),
                },
                SkillCommand::Show { name, .. } => {
                    mcpcli::mcp::skills::validate_selector(name)
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                    Operation::SkillsGet {
                        server: server.to_owned(),
                        selector: name.clone(),
                    }
                }
                SkillCommand::Files { name, dir, .. } => {
                    mcpcli::mcp::skills::validate_selector(name)
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                    if let Some(d) = dir
                        && !d.is_empty()
                        && d != "."
                    {
                        skills::validate_clean_skill_rel_path(d)
                            .map_err(|err| usage("invalid_skill_path", err))?;
                    }
                    Operation::SkillsFiles {
                        server: server.to_owned(),
                        selector: name.clone(),
                        dir: dir.clone(),
                    }
                }
                SkillCommand::Read {
                    name,
                    path,
                    inline_media,
                    ..
                } => {
                    mcpcli::mcp::skills::validate_selector(name)
                        .map_err(|e| daemon::mcp_op_error(e, Some(server)))?;
                    let rel_path = path.clone().into_string().map_err(|_| {
                        usage(
                            "invalid_skill_path",
                            "remote skill path must be valid UTF-8",
                        )
                    })?;
                    if rel_path.contains('\\') {
                        return Err(usage(
                            "invalid_skill_path",
                            "remote skill path must use '/' separators",
                        ));
                    }
                    skills::validate_clean_skill_rel_path(&rel_path)
                        .map_err(|err| usage("invalid_skill_path", err))?;
                    Operation::SkillsRead {
                        server: server.to_owned(),
                        selector: name.clone(),
                        path: rel_path,
                        inline_media: *inline_media,
                    }
                }
                SkillCommand::Builtin => unreachable!(),
            };
            let remote = remote(cli, loaded, empty_leased(op), None, uncertain, interrupt).await?;
            Ok(remote.into_output(|value| Ok(Output::json(value))))
        }
        Command::Auth { command } => match command {
            AuthCommand::Status { server } => {
                let (value, text) = auth::status(&loaded.config.servers, server.as_deref())?;
                Ok(Output::with_text(value, text))
            }
            AuthCommand::Login {
                server,
                scopes,
                client_id,
                callback_port,
                callback_path,
                client_name,
                no_browser,
            } => {
                cli.validate_request_timeout()?;
                let cfg = enabled_server(loaded, server)?;
                let ServerConfig::Http { url, oauth, .. } = cfg else {
                    return Err(usage(
                        "invalid_transport",
                        format!(
                            "server {server:?} uses stdio transport; OAuth applies only to http servers"
                        ),
                    ));
                };
                let budget = Duration::from_secs(cli.timeout_secs_for(cfg));
                let deadline = Instant::now() + budget;
                let configured_scopes = oauth.as_ref().map_or(&[][..], |o| o.scopes.as_slice());
                let (value, text) = auth::login(auth::LoginOptions {
                    server_name: server,
                    server_url: url,
                    scopes,
                    configured_scopes,
                    client_id: client_id.as_deref(),
                    configured_client_id: oauth.as_ref().and_then(|o| o.client_id.as_deref()),
                    client_secret: oauth.as_ref().and_then(|o| o.client_secret.as_deref()),
                    client_name: client_name.as_deref(),
                    configured_client_name: oauth.as_ref().and_then(|o| o.client_name.as_deref()),
                    callback_port: (*callback_port)
                        .or_else(|| oauth.as_ref().and_then(|o| o.callback_port)),
                    callback_path: callback_path
                        .as_deref()
                        .or_else(|| oauth.as_ref().and_then(|o| o.callback_path.as_deref())),
                    auth_server_metadata_url: oauth
                        .as_ref()
                        .and_then(|o| o.auth_server_metadata_url.as_deref()),
                    account: oauth.as_ref().and_then(|o| o.account.as_deref()),
                    no_browser: *no_browser,
                    budget,
                })
                .await?;
                notify_daemon_reset(cli, loaded, server, deadline).await;
                Ok(Output::with_text(value, text))
            }
            AuthCommand::Logout { server } => {
                cli.validate_request_timeout()?;
                let cfg =
                    loaded.config.servers.get(server).ok_or_else(|| {
                        usage("unknown_server", format!("unknown server {server:?}"))
                    })?;
                let ServerConfig::Http {
                    url,
                    enabled,
                    oauth,
                    ..
                } = cfg
                else {
                    return Err(usage(
                        "invalid_transport",
                        format!(
                            "server {server:?} uses stdio transport; OAuth applies only to http servers"
                        ),
                    ));
                };
                let budget = Duration::from_secs(cli.timeout_secs_for(cfg));
                let deadline = Instant::now() + budget;
                let (value, text) =
                    auth::logout(server, url, oauth.as_ref(), *enabled, budget).await?;
                if *enabled {
                    notify_daemon_reset(cli, loaded, server, deadline).await;
                }
                Ok(Output::with_text(value, text))
            }
        },
        _ => unreachable!(),
    }
}
