mod cli;

use crate::cli::args::{
    Cli, Command, DaemonCommand, EventsCommand, SkillCommand, TasksCommand, TopLevelHelpProbe,
};
use crate::cli::events::run_events_stream;
use crate::cli::input::retain_cli_input;
use crate::cli::output::{
    Output, write_diagnostic, write_server_text, write_text, write_tool_text, write_tools_text,
};
use crate::cli::run;
use clap::Parser;
use mcpcli::{
    agents,
    budget::ByteBudget,
    config::LoadedConfig,
    discovery::{self, safe_text, write_safe_text, write_text_json},
    ipc::Failure,
};
use serde_json::Value;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

async fn entry(cli: Cli) -> i32 {
    let cli = match retain_cli_input(cli, ByteBudget::global()) {
        Ok(cli) => cli,
        Err(error) => {
            let _ = write_diagnostic(&mut std::io::stderr().lock(), &error);
            return error.code;
        }
    };
    let foreground = matches!(
        cli.command,
        Command::Daemon {
            command: DaemonCommand::Run
        }
    );
    let is_event_stream = matches!(
        cli.command,
        Command::Events {
            command: EventsCommand::Stream { .. }
        }
    );
    let uses_dual_clock = cli.uses_dual_clock();
    let uncertain = AtomicBool::new(false);
    let signal = mcpcli::platform::interrupted();
    tokio::pin!(signal);
    let mut signal_consumed = false;
    let result = if foreground {
        Box::pin(run(&cli, &uncertain, None)).await
    } else if is_event_stream {
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut stream_fut = Box::pin(run_events_stream(&cli, cancel.clone()));
        let mut interrupted_code: Option<i32> = None;
        let res = loop {
            tokio::select! {
                biased;
                res = &mut stream_fut => break res,
                sig = &mut signal, if !signal_consumed => {
                    signal_consumed = true;
                    match sig {
                        Ok(code) => {
                            interrupted_code = Some(code);
                            cancel.cancel();
                        }
                        Err(error) => {
                            cancel.cancel();
                            let _ = stream_fut.await;
                            break Err(error.into());
                        }
                    }
                }
            }
        };
        match (res, interrupted_code) {
            (Ok(mut out), Some(code))
                if out
                    .diagnostic
                    .as_ref()
                    .is_some_and(|d| d.kind == "cancelled") =>
            {
                out.code = code;
                if let Some(diag) = out.diagnostic.as_mut() {
                    diag.code = code;
                }
                Ok(out)
            }
            (Err(mut err), Some(code)) => {
                if let Some(failure) = err.downcast_mut::<Failure>()
                    && failure.kind == "cancelled"
                {
                    failure.code = code;
                }
                Err(err)
            }
            (other, _) => other,
        }
    } else if uses_dual_clock {
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut run_fut = Box::pin(run(&cli, &uncertain, Some(&cancel)));
        let mut interrupted_code: Option<i32> = None;
        let mut fallback_sleep: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
        let res = loop {
            tokio::select! {
                biased;
                res = &mut run_fut => break res,
                () = async {
                    if let Some(sleep) = fallback_sleep.as_mut() {
                        sleep.await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    let code = interrupted_code.unwrap_or(130);
                    break Err(Failure::new(
                        code,
                        "cancelled",
                        "interrupted; cancellation is not rollback",
                    )
                    .uncertain(uncertain.load(Ordering::Acquire))
                    .into());
                }
                sig = &mut signal => {
                    signal_consumed = true;
                    match sig {
                        Ok(code) => {
                            if interrupted_code.is_none() {
                                interrupted_code = Some(code);
                                cancel.cancel();
                                fallback_sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(3))));
                                signal.set(mcpcli::platform::interrupted());
                            } else {
                                break Err(Failure::new(
                                    code,
                                    "cancelled",
                                    "interrupted; cancellation is not rollback",
                                )
                                .uncertain(uncertain.load(Ordering::Acquire))
                                .into());
                            }
                        }
                        Err(error) => {
                            cancel.cancel();
                            break Err(error.into());
                        }
                    }
                }
            }
        };
        match (res, interrupted_code) {
            (Ok(mut out), Some(code))
                if out
                    .diagnostic
                    .as_ref()
                    .is_some_and(|d| d.kind == "cancelled") =>
            {
                out.code = code;
                if let Some(diag) = out.diagnostic.as_mut() {
                    diag.code = code;
                }
                Ok(out)
            }
            (Err(mut err), Some(code)) => {
                if let Some(failure) = err.downcast_mut::<Failure>()
                    && failure.kind == "cancelled"
                {
                    failure.code = code;
                }
                Err(err)
            }
            // Natural completion during sentinel race preserves its 0/3 exit code.
            (other, _) => other,
        }
    } else {
        tokio::select! {
            biased;
            result = Box::pin(run(&cli, &uncertain, None)) => result,
            signal = &mut signal => {
                signal_consumed = true;
                match signal {
                    Ok(code) => Err(Failure::new(code, "cancelled", "interrupted; cancellation is not rollback")
                        .uncertain(uncertain.load(Ordering::Acquire))
                        .into()),
                    Err(error) => Err(error.into()),
                }
            },
        }
    };

    let mut output = result.unwrap_or_else(Output::failure);
    if let Some(mut connection) = output.connection.take() {
        let cleanup = tokio::select! {
            biased;
            result = connection.close() => result.err().map(|error|
                Failure::new(1, "cleanup", format!("MCP operation finished; connection cleanup failed: {error}; do not replay the operation"))),
            interrupted = &mut signal, if !signal_consumed => Some(match interrupted {
                Ok(code) => Failure::new(code, "cancelled", "MCP result received; connection cleanup interrupted; do not replay the operation"),
                Err(error) => Failure::new(1, "cleanup", format!("MCP result received; signal handling failed during cleanup: {error}")),
            }),
        };
        output = output.cleanup(cleanup);
    }
    if let Some(error) = &output.diagnostic {
        let mut stderr = std::io::stderr().lock();
        let _ = write_diagnostic(&mut stderr, error);
    }
    let Some(value) = output.value else {
        drop(output.lease.take());
        if let Some(inflight) = output.inflight.take()
            && let Err(error) = inflight.abort().await
        {
            eprintln!("mcpcli: media cache error: {error:#}");
            return 1;
        }
        return output.code;
    };
    if let Some(inflight) = &output.inflight
        && let Err(error) = inflight.begin_writeback()
    {
        eprintln!("mcpcli: media cache error: {error:#}; the operation was not replayed");
        if let Some(inflight) = output.inflight.take()
            && let Err(error) = inflight.abort().await
        {
            eprintln!("mcpcli: media cache error: {error:#}");
        }
        return 1;
    }
    let written = {
        let mut stdout = std::io::stdout().lock();
        let res = if cli.text {
            match &cli.command {
                Command::Call { inline_media, .. } => {
                    if value.get("resultType").and_then(Value::as_str) == Some("task") {
                        let task = value.get("task").unwrap_or(&value);
                        let task_id = task.get("taskId").and_then(Value::as_str).unwrap_or("");
                        let status = task.get("status").and_then(Value::as_str).unwrap_or("");
                        let msg = task.get("statusMessage").and_then(Value::as_str);
                        discovery::write_task_status(&mut stdout, task_id, status, msg)
                    } else {
                        write_tool_text(&mut stdout, &value, *inline_media)
                    }
                }
                Command::Tasks { command } => match command {
                    TasksCommand::Get { .. } => discovery::write_task_get(&mut stdout, &value),
                    TasksCommand::Update { task_id, .. } => {
                        discovery::write_task_ack(&mut stdout, task_id)
                    }
                    TasksCommand::Cancel { task_id, .. } => {
                        discovery::write_task_cancelled(&mut stdout, task_id)
                    }
                },
                Command::Skills { command } => match command {
                    SkillCommand::List { server: Some(_) } => {
                        let items = value
                            .get("skills")
                            .and_then(Value::as_array)
                            .map_or(&[][..], Vec::as_slice);
                        discovery::write_remote_skills_list(&mut stdout, items)
                    }
                    SkillCommand::Show {
                        server: Some(_), ..
                    } => {
                        let srv = value.get("server").and_then(Value::as_str).unwrap_or("");
                        let uri = value.get("uri").and_then(Value::as_str).unwrap_or("");
                        let dynamic = value
                            .get("dynamic")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let content = value.get("content").and_then(Value::as_str).unwrap_or("");
                        discovery::write_remote_skill_show(&mut stdout, srv, uri, dynamic, content)
                    }
                    SkillCommand::Files {
                        server: Some(_), ..
                    } => {
                        let entries = value
                            .get("resources")
                            .and_then(Value::as_array)
                            .map_or(&[][..], Vec::as_slice);
                        discovery::write_remote_skill_files(&mut stdout, entries)
                    }
                    SkillCommand::Read {
                        server: Some(_), ..
                    } => {
                        if let Some(content) = value.get("content").and_then(Value::as_str) {
                            write_text(&mut stdout, content)
                        } else if let Some(saved) = value
                            .get("savedMedia")
                            .and_then(Value::as_array)
                            .and_then(|arr| arr.first())
                        {
                            (|| -> std::io::Result<()> {
                                let kind = saved["kind"].as_str().unwrap_or("media");
                                let mime = saved["mimeType"]
                                    .as_str()
                                    .unwrap_or("application/octet-stream");
                                let bytes = saved["bytes"].as_u64().unwrap_or(0);
                                let path = saved["savedPath"].as_str().unwrap_or("");
                                stdout.write_all(b"Saved ")?;
                                write_safe_text(&mut stdout, kind)?;
                                stdout.write_all(b" [")?;
                                write_safe_text(&mut stdout, mime)?;
                                write!(stdout, "] ({bytes} B): ")?;
                                write_safe_text(&mut stdout, path)?;
                                stdout.write_all(b"\n")
                            })()
                        } else {
                            write_text_json(&mut stdout, &value).and_then(|_| writeln!(stdout))
                        }
                    }
                    SkillCommand::Builtin => {
                        stdout.write_all(discovery::BUILTIN_SKILL_MD.as_bytes())
                    }
                    _ => {
                        if let Some(text) = output.text {
                            write_text(&mut stdout, &text)
                        } else {
                            write_text_json(&mut stdout, &value).and_then(|_| writeln!(stdout))
                        }
                    }
                },
                Command::Server { .. } => write_server_text(&mut stdout, &value),
                Command::Tools { .. } => write_tools_text(&mut stdout, &value),
                Command::Inspect { .. } => discovery::write_tool(&mut stdout, &value),
                _ => {
                    if let Some(text) = output.text {
                        write_text(&mut stdout, &text)
                    } else {
                        write_text_json(&mut stdout, &value).and_then(|_| writeln!(stdout))
                    }
                }
            }
        } else {
            serde_json::to_writer(&mut stdout, &value)
                .map_err(std::io::Error::other)
                .and_then(|_| writeln!(stdout))
        };
        res.and_then(|()| stdout.flush())
    };
    drop(value);
    drop(output.lease.take());
    let finalized = if let Some(inflight) = output.inflight.take() {
        if written.is_ok() {
            inflight.complete().await
        } else {
            inflight.abort().await
        }
    } else {
        Ok(())
    };
    if let Err(error) = finalized {
        eprintln!("mcpcli: media cache error: {error:#}; the operation was not replayed");
        return 1;
    }
    if let Err(error) = written {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            eprintln!("mcpcli: stdout failed: {}", safe_text(&error.to_string()));
        }
        1
    } else {
        output.code
    }
}

fn main() -> std::process::ExitCode {
    if let Err(error) = mcpcli::platform::initialize_standard_handles() {
        eprintln!("mcpcli: cannot initialize standard handles: {error}");
        return std::process::ExitCode::FAILURE;
    }
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let is_help = error.kind() == clap::error::ErrorKind::DisplayHelp;
            if error.print().is_err() {
                return std::process::ExitCode::FAILURE;
            }
            if is_help
                && let Ok(probe) = TopLevelHelpProbe::try_parse()
                && probe.help > 0
                && let Ok(loaded) = LoadedConfig::load(&probe.config)
                && let Ok(Some(doc)) = agents::load(loaded.config.agents.as_deref())
            {
                let path = safe_text(&doc.path.display().to_string());
                let rendered = safe_text(&doc.content);
                let body = rendered.trim_end_matches('\n');
                let mut stdout = std::io::stdout().lock();
                let written = if body.is_empty() {
                    writeln!(stdout, "\nGlobal AGENTS.md ({path})")
                } else {
                    writeln!(stdout, "\nGlobal AGENTS.md ({path}):\n\n{body}")
                };
                if let Err(io_error) = written
                    && io_error.kind() != std::io::ErrorKind::BrokenPipe
                {
                    eprintln!(
                        "mcpcli: stdout failed: {}",
                        safe_text(&io_error.to_string())
                    );
                    return std::process::ExitCode::FAILURE;
                }
            }
            return std::process::ExitCode::from(error.exit_code() as u8);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("mcpcli: cannot start async runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(Box::pin(entry(cli)));
    // Drop async transports before exit, while bounding an interrupted stdin blocking reader.
    runtime.shutdown_timeout(Duration::from_secs(3));
    std::process::ExitCode::from(code as u8)
}
