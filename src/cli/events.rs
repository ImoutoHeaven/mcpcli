use super::args::{Cli, Command, EventsCommand, load, usage};
use super::input::arguments;
use super::output::Output;
use super::remote::remote;
use anyhow::Result;
use mcpcli::{
    budget::{ByteBudget, Leased},
    config::LoadedConfig,
    daemon,
    events_exec::{CursorStore, DEFAULT_EXEC_TIMEOUT},
    events_stream::{self, DaemonTransport, DirectTransport, StreamConfig, StreamTimings},
    ipc::Operation,
    mcp::McpConnection,
    runtime::{self, Paths},
};
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::time::Instant;

pub(super) async fn run_events(
    cli: &Cli,
    loaded: &LoadedConfig,
    command: &EventsCommand,
    uncertain: &AtomicBool,
    interrupt: Option<&tokio_util::sync::CancellationToken>,
) -> Result<Output> {
    match command {
        EventsCommand::List { server } => {
            let remote = remote(
                cli,
                loaded,
                Leased::new(
                    Operation::EventsList {
                        server: server.clone(),
                    },
                    ByteBudget::global().empty_lease(),
                ),
                None,
                uncertain,
                interrupt,
            )
            .await?;
            Ok(remote.into_output(|value| Ok(Output::json(value))))
        }
        EventsCommand::Poll {
            server,
            event,
            args,
            args_file,
            stdin,
            cursor,
            max_age_ms,
            max_events,
        } => {
            if event.is_empty() || event.len() > 512 || event.chars().any(char::is_control) {
                return Err(usage("invalid_event", "invalid event name"));
            }
            let server_config = loaded
                .config
                .servers
                .get(server)
                .ok_or_else(|| usage("unknown_server", format!("unknown server {server:?}")))?;
            if !server_config.enabled() {
                return Err(usage(
                    "server_disabled",
                    format!("server {server:?} is disabled"),
                ));
            }
            cli.validate_request_timeout()?;
            let timeout_secs = cli.timeout_secs_for(server_config);
            let (input_lease, parsed_args) = if args.is_some() || args_file.is_some() || *stdin {
                let leased = arguments(
                    args,
                    args_file,
                    *stdin,
                    Duration::from_secs(timeout_secs),
                    ByteBudget::global(),
                )
                .await?;
                let (map, lease) = leased.into_parts();
                (lease, Some(map))
            } else {
                (ByteBudget::global().empty_lease(), None)
            };
            let remote = remote(
                cli,
                loaded,
                Leased::new(
                    Operation::EventsPoll {
                        server: server.clone(),
                        event: event.clone(),
                        arguments: parsed_args,
                        cursor: cursor.clone(),
                        max_age_ms: *max_age_ms,
                        max_events: *max_events,
                    },
                    input_lease,
                ),
                None,
                uncertain,
                interrupt,
            )
            .await?;
            Ok(remote.into_output(|value| Ok(Output::json(value))))
        }
        EventsCommand::Stream { .. } => unreachable!("handled by run_events_stream"),
    }
}

pub(crate) async fn run_events_stream(
    cli: &Cli,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<Output> {
    let deadline = cli
        .timeout
        .map(|ms| {
            Instant::now()
                .checked_add(Duration::from_millis(ms))
                .ok_or_else(|| usage("timeout", "event stream timeout is too large"))
        })
        .transpose()?;
    let Command::Events {
        command:
            EventsCommand::Stream {
                server,
                event,
                args,
                args_file,
                stdin,
                mode,
                cursor,
                cursor_file,
                max_age_ms,
                heartbeat_ms,
                once,
                exec_mode,
                exec_timeout,
                callback,
            },
    } = &cli.command
    else {
        unreachable!();
    };
    if event.is_empty() || event.len() > 512 || event.chars().any(char::is_control) {
        return Err(usage("invalid_event", "invalid event name"));
    }
    let loaded = load(cli)?;
    let server_config = loaded
        .config
        .servers
        .get(server)
        .ok_or_else(|| usage("unknown_server", format!("unknown server {server:?}")))?;
    if !server_config.enabled() {
        return Err(usage(
            "server_disabled",
            format!("server {server:?} is disabled"),
        ));
    }
    let (parsed_args, input_lease) = if args.is_some() || args_file.is_some() || *stdin {
        let leased = events_stream::wait_with_budget(
            Duration::from_secs(30),
            deadline,
            &cancel,
            arguments(
                args,
                args_file,
                *stdin,
                Duration::from_secs(30),
                ByteBudget::global(),
            ),
        )
        .await?;
        let (map, lease) = leased.into_parts();
        (Some(map), lease)
    } else {
        (None, ByteBudget::global().empty_lease())
    };
    let stream_cfg = Leased::new(
        StreamConfig {
            server: server.clone(),
            event: event.clone(),
            arguments: parsed_args,
            mode: (*mode).into(),
            initial_cursor: cursor.clone(),
            cursor_file: cursor_file.as_ref().map(CursorStore::new),
            max_age_ms: *max_age_ms,
            heartbeat_ms: *heartbeat_ms,
            once: *once,
            deadline,
            callback_argv: (!callback.is_empty()).then(|| callback.clone()),
            exec_mode: (*exec_mode).into(),
            exec_timeout: exec_timeout
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_EXEC_TIMEOUT),
        },
        input_lease,
    );

    if cli.oneshot {
        let connection =
            events_stream::wait_with_budget(Duration::from_secs(30), deadline, &cancel, async {
                McpConnection::connect(server_config, Duration::from_secs(30))
                    .await
                    .map_err(|error| {
                        daemon::mcp_error_for_server(error, false, Some(server)).into()
                    })
            })
            .await?;
        let mut transport = DirectTransport::with_server(connection, server.clone());
        let outcome = events_stream::run_stream(
            &mut transport,
            &stream_cfg,
            StreamTimings::default(),
            cancel,
            None,
        )
        .await;
        let connection = transport.into_connection();
        return match outcome {
            Ok(()) => Ok(Output {
                value: None,
                text: None,
                code: 0,
                diagnostic: None,
                connection: Some(connection),
                inflight: None,
                lease: None,
            }),
            Err(error) => Ok(Output::failure(error).with_connection(Some(connection))),
        };
    }

    let paths = Paths::new(loaded.path.clone(), &cli.session)?;
    events_stream::wait_with_budget(
        runtime::START_WAIT,
        deadline,
        &cancel,
        runtime::ensure(&paths, &loaded, false),
    )
    .await?;
    let mut transport = DaemonTransport::new(paths, loaded, server.clone());
    events_stream::run_stream(
        &mut transport,
        &stream_cfg,
        StreamTimings::default(),
        cancel,
        None,
    )
    .await?;
    Ok(Output {
        value: None,
        text: None,
        code: 0,
        diagnostic: None,
        connection: None,
        inflight: None,
        lease: None,
    })
}
