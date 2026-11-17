use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcpcli::{
    agents,
    config::{self, LoadedConfig},
    daemon,
    discovery::{self, safe_text},
    ipc::{self, Failure, Operation},
    json as input,
    mcp::McpConnection,
    runtime::{self, Paths},
    skills,
};
use serde_json::{Map, Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::{Instant, timeout};

#[derive(Parser)]
#[command(
    version,
    about = "Call MCP tools and read global AGENTS.md and local Agent Skills",
    disable_help_subcommand = true
)]
struct Cli {
    #[arg(
        short,
        long,
        global = true,
        env = "MCPCLI_CONFIG",
        default_value = "mcpcli.json"
    )]
    config: PathBuf,
    #[arg(long, global = true, env = "MCPCLI_SESSION", default_value = "default")]
    session: String,
    #[arg(long, global = true, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=3600), help = "Request deadline in seconds, including queue and connection time")]
    timeout: u64,
    #[arg(
        long,
        global = true,
        help = "Open and close a fresh connection for this command"
    )]
    oneshot: bool,
    #[arg(
        long,
        global = true,
        help = "Print readable text instead of the default JSON"
    )]
    text: bool,
    #[command(subcommand)]
    command: Command,
}
#[allow(dead_code)]
#[derive(Parser)]
#[command(
    disable_help_flag = true,
    disable_version_flag = true,
    disable_help_subcommand = true
)]
struct TopLevelHelpProbe {
    #[arg(short, long, env = "MCPCLI_CONFIG", default_value = "mcpcli.json")]
    config: PathBuf,
    #[arg(long, env = "MCPCLI_SESSION", default_value = "default")]
    session: String,
    #[arg(long, default_value_t = 60)]
    timeout: u64,
    #[arg(long)]
    oneshot: bool,
    #[arg(long)]
    text: bool,
    #[arg(short = 'h', long = "help", action = clap::ArgAction::Count)]
    help: u8,
}
#[derive(Subcommand)]
enum Command {
    /// Read global AGENTS.md, configured server descriptions, and skill metadata without starting servers.
    Catalog,
    /// Read the configured global AGENTS.md document without starting servers.
    Agents,
    /// Search global AGENTS.md and configured server and skill metadata, offline.
    Search { query: String },
    /// List current tool names and descriptions for a server.
    Tools { server: String },
    /// Read complete tool metadata and JSON schemas.
    Inspect { server: String, tool: String },
    /// Invoke one tool. Arguments must be a JSON object.
    Call {
        server: String,
        tool: String,
        #[arg(long, conflicts_with_all = ["args_file", "stdin"])]
        args: Option<String>,
        #[arg(long, conflicts_with_all = ["args", "stdin"])]
        args_file: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["args", "args_file"])]
        stdin: bool,
    },
    /// Close a server connection and discard its session state.
    Reset {
        server: String,
        #[arg(
            long,
            help = "Discard client session state even if remote HTTP session deletion failed"
        )]
        force: bool,
    },
    /// Discover and read explicitly configured local skill directories.
    #[command(alias = "skill")]
    Skills {
        #[command(subcommand)]
        command: SkillCommand,
    },
    /// Manage the daemon for the selected config and session.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}
#[derive(Subcommand)]
enum SkillCommand {
    List,
    Show { name: String },
    Files { name: String },
    Read { name: String, path: PathBuf },
}
#[derive(Subcommand)]
enum DaemonCommand {
    Start,
    /// Run in the foreground for a process supervisor.
    Run,
    Status,
    Stop,
    /// Stop, discard all session state, and start with current config/environment.
    Restart,
}

struct Output {
    value: Option<Value>,
    text: Option<String>,
    code: i32,
    diagnostic: Option<Failure>,
    connection: Option<McpConnection>,
}
impl Output {
    fn json(value: Value) -> Self {
        Self {
            value: Some(value),
            text: None,
            code: 0,
            diagnostic: None,
            connection: None,
        }
    }
    fn with_text(value: Value, text: String) -> Self {
        Self {
            value: Some(value),
            text: Some(text),
            code: 0,
            diagnostic: None,
            connection: None,
        }
    }
    fn failure(error: anyhow::Error) -> Self {
        let failure = error
            .downcast::<Failure>()
            .unwrap_or_else(|error| Failure::new(1, "runtime", format!("{error:#}")));
        Self {
            value: None,
            text: None,
            code: failure.code,
            diagnostic: Some(failure),
            connection: None,
        }
    }
    fn with_connection(mut self, connection: Option<McpConnection>) -> Self {
        self.connection = connection;
        self
    }
    fn cleanup(mut self, error: Option<Failure>) -> Self {
        if let Some(error) = error {
            if let Some(original) = &mut self.diagnostic {
                original.message = format!("{}; additionally: {}", original.message, error.message);
            } else {
                self.code = error.code;
                self.diagnostic = Some(error);
            }
        }
        self
    }
}
struct RemoteOutput {
    value: Value,
    connection: Option<McpConnection>,
}
impl RemoteOutput {
    fn into_output(self, render: impl FnOnce(Value) -> Result<Output>) -> Output {
        render(self.value)
            .unwrap_or_else(Output::failure)
            .with_connection(self.connection)
    }
}
fn usage(kind: &str, error: impl std::fmt::Display) -> anyhow::Error {
    Failure::new(2, kind, error.to_string()).into()
}
fn brief(value: &str) -> String {
    let line = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = line.chars();
    let mut result: String = chars.by_ref().take(240).collect();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}
fn load(cli: &Cli) -> Result<LoadedConfig> {
    LoadedConfig::load(&cli.config).map_err(|error| usage("configuration", format!("{error:#}")))
}
fn catalog(loaded: &LoadedConfig, query: Option<&str>) -> Result<Output> {
    let agents_doc = agents::load(loaded.config.agents.as_deref())
        .map_err(|error| usage("agents", format!("{error:#}")))?;
    let skill_catalog = skills::discover(&loaded.config.skills)
        .map_err(|error| usage("skills", format!("{error:#}")))?;
    for warning in &skill_catalog.warnings {
        eprintln!("mcpcli: {}", safe_text(warning));
    }
    let query = query.map(str::to_lowercase);
    let matches = |name: &str, description: &str| {
        query
            .as_ref()
            .is_none_or(|q| format!("{name} {description}").to_lowercase().contains(q))
    };
    let selected_agents = agents_doc.filter(|doc| {
        query.as_ref().is_none_or(|q| {
            format!("agents {} {}", doc.path.display(), doc.content)
                .to_lowercase()
                .contains(q)
        })
    });
    let servers: Vec<_> = loaded
        .config
        .servers
        .iter()
        .filter(|(name, server)| matches(name, server.description().unwrap_or("")))
        .map(|(name, server)| {
            json!({
                "name": name,
                "transport": server.transport(),
                "description": server.description().unwrap_or("")
            })
        })
        .collect();
    let selected_skills: Vec<_> = skill_catalog
        .skills
        .into_iter()
        .filter(|s| matches(&s.name, &s.description))
        .collect();
    let mut lines = Vec::new();
    for server in &servers {
        lines.push(format!(
            "server {}\t{}",
            server["name"].as_str().unwrap(),
            brief(server["description"].as_str().unwrap())
        ));
    }
    for skill in &selected_skills {
        lines.push(format!(
            "skill {}\t{}",
            skill.name,
            brief(&skill.description)
        ));
    }
    let mut sections = Vec::new();
    if let Some(doc) = &selected_agents {
        let body = doc.content.trim_end_matches(['\r', '\n']);
        if body.is_empty() {
            sections.push(format!("agents {}", doc.path.display()));
        } else {
            sections.push(format!("agents {}\n\n{body}", doc.path.display()));
        }
    }
    if !lines.is_empty() {
        sections.push(lines.join("\n"));
    }
    Ok(Output::with_text(
        json!({
            "agents": selected_agents,
            "servers": servers,
            "skills": selected_skills,
            "warnings": skill_catalog.warnings,
        }),
        sections.join("\n\n"),
    ))
}

async fn arguments(
    args: &Option<String>,
    file: &Option<PathBuf>,
    stdin: bool,
    budget: Duration,
) -> Result<Map<String, Value>> {
    let bytes = if let Some(args) = args {
        args.as_bytes().to_vec()
    } else if let Some(file) = file {
        input::read_file(file, ipc::ARGUMENT_LIMIT)
            .map_err(|e| usage("arguments", format!("{e:#}")))?
    } else if stdin {
        let mut bytes = Vec::new();
        let mut reader = tokio::io::stdin().take(ipc::ARGUMENT_LIMIT as u64 + 1);
        timeout(budget, reader.read_to_end(&mut bytes))
            .await
            .map_err(|_| Failure::new(5, "timeout", "timed out reading JSON arguments from stdin"))?
            .map_err(|e| usage("arguments", e))?;
        bytes
    } else {
        b"{}".to_vec()
    };
    if bytes.len() > ipc::ARGUMENT_LIMIT {
        return Err(usage("arguments", "arguments exceed the 1 MiB limit"));
    }
    let value = input::parse(&bytes).map_err(|e| usage("arguments", format!("{e:#}")))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| usage("arguments", "tool arguments must be a JSON object"))
}

async fn remote(
    cli: &Cli,
    loaded: &LoadedConfig,
    operation: Operation,
    uncertain: &AtomicBool,
) -> Result<RemoteOutput> {
    let server = match &operation {
        Operation::Tools { server }
        | Operation::Call { server, .. }
        | Operation::Reset { server, .. } => server,
        _ => unreachable!(),
    };
    let config = loaded
        .config
        .servers
        .get(server)
        .ok_or_else(|| usage("unknown_server", format!("unknown server {server:?}")))?;
    let budget = Duration::from_secs(cli.timeout);
    let started = Instant::now();
    let future = async {
        if cli.oneshot {
            if matches!(operation, Operation::Reset { .. }) {
                return Err(usage("mode", "reset cannot be used with --oneshot"));
            }
            let mut connection =
                McpConnection::connect(config, budget.min(Duration::from_secs(30)))
                    .await
                    .map_err(|error| daemon::mcp_error(error, false))?;
            let remaining = budget.saturating_sub(started.elapsed());
            let result = match &operation {
                Operation::Tools { .. } => connection
                    .list_tools(remaining)
                    .await
                    .map(|tools| json!(tools)),
                Operation::Call {
                    tool, arguments, ..
                } => {
                    connection
                        .call_with_dispatch(tool, arguments.clone(), remaining, uncertain)
                        .await
                }
                _ => unreachable!(),
            }
            .map_err(|error| {
                anyhow::Error::from(daemon::mcp_error(error, uncertain.load(Ordering::Acquire)))
            });
            match result {
                Ok(value) => {
                    uncertain.store(false, Ordering::Release);
                    Ok(RemoteOutput {
                        value,
                        connection: Some(connection),
                    })
                }
                Err(mut error) => {
                    if let Err(cleanup) = connection.close().await
                        && let Some(failure) = error.downcast_mut::<Failure>()
                    {
                        failure.message = format!(
                            "{}; additionally: connection cleanup failed: {cleanup}",
                            failure.message
                        );
                    }
                    Err(error)
                }
            }
        } else {
            let paths = Paths::new(loaded.path.clone(), &cli.session)?;
            timeout(
                budget.saturating_sub(started.elapsed()),
                runtime::ensure(&paths, loaded, false),
            )
            .await
            .map_err(|_| {
                Failure::new(
                    5,
                    "timeout",
                    "deadline elapsed while starting the daemon; tool was not dispatched",
                )
            })??;
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(Failure::new(5, "timeout", "deadline elapsed before dispatch").into());
            }
            let response = runtime::exchange_tracked(
                &paths,
                &paths.request(&loaded.digest, operation, remaining)?,
                uncertain,
            )
            .await?;
            Ok(RemoteOutput {
                value: response.result.context("daemon did not return a result")?,
                connection: None,
            })
        }
    };
    timeout(budget + Duration::from_secs(10), future)
        .await
        .map_err(|_| {
            Failure::new(
                5,
                "timeout",
                "request deadline exceeded; the call is not replayed",
            )
            .uncertain(uncertain.load(Ordering::Acquire))
        })?
}

fn tool_text(result: &Value) -> String {
    let mut texts = Vec::new();
    let mut omitted = 0;
    if let Some(content) = result.get("content").and_then(Value::as_array) {
        for block in content {
            if block["type"] == "text" {
                if let Some(text) = block["text"].as_str() {
                    texts.push(text.to_owned());
                }
            } else {
                omitted += 1;
            }
        }
    }
    if omitted > 0 {
        eprintln!(
            "mcpcli: {omitted} non-text content block(s) are available in default JSON output"
        );
    }
    if let Some(structured) = result.get("structuredContent") {
        if texts.is_empty() {
            texts.push(serde_json::to_string_pretty(structured).unwrap());
        } else {
            eprintln!("mcpcli: structuredContent is available in default JSON output");
        }
    }
    texts.join("\n")
}

async fn run(cli: &Cli, uncertain: &AtomicBool) -> Result<Output> {
    if let Command::Daemon { command } = &cli.command {
        if cli.oneshot {
            return Err(usage(
                "mode",
                "daemon commands cannot be used with --oneshot",
            ));
        }
        let locator = config::locate(&cli.config).map_err(|error| usage("configuration", error))?;
        let paths = Paths::new(locator, &cli.session)?;
        return match command {
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
                        Ok(response) => Ok(Output::json(response.result.unwrap())),
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
                let response = runtime::exchange(
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
                Ok(Output::json(status))
            }
        };
    }
    let loaded = load(cli)?;
    match &cli.command {
        Command::Catalog => catalog(&loaded, None),
        Command::Agents => {
            let doc = agents::load(loaded.config.agents.as_deref())
                .map_err(|error| usage("agents", format!("{error:#}")))?;
            let text = doc
                .as_ref()
                .map(|doc| doc.content.clone())
                .unwrap_or_default();
            Ok(Output::with_text(json!({ "agents": doc }), text))
        }
        Command::Search { query } => {
            if query.is_empty() || query.len() > 1024 {
                return Err(usage("query", "query must contain 1-1024 bytes"));
            }
            catalog(&loaded, Some(query))
        }
        Command::Tools { server } => {
            let remote = remote(
                cli,
                &loaded,
                Operation::Tools {
                    server: server.clone(),
                },
                uncertain,
            )
            .await?;
            Ok(remote.into_output(|value| {
                let list = value.as_array().context("invalid tool list")?;
                let compact: Vec<_> = list.iter().map(|tool| json!({
                    "name":tool["name"],"description":brief(tool["description"].as_str().unwrap_or(""))
                })).collect();
                let text = list
                    .iter()
                    .map(discovery::tool_summary)
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(Output::with_text(json!(compact), text))
            }))
        }
        Command::Inspect { server, tool } => {
            let remote = remote(
                cli,
                &loaded,
                Operation::Tools {
                    server: server.clone(),
                },
                uncertain,
            )
            .await?;
            Ok(remote.into_output(|value| {
                let found = value
                    .as_array()
                    .context("invalid tool list")?
                    .iter()
                    .find(|t| t["name"].as_str() == Some(tool))
                    .ok_or_else(|| usage("unknown_tool", format!("unknown tool {tool:?}")))?;
                Ok(Output::with_text(
                    found.clone(),
                    discovery::render_tool(found),
                ))
            }))
        }
        Command::Call {
            server,
            tool,
            args,
            args_file,
            stdin,
        } => {
            if tool.is_empty() || tool.len() > 512 || tool.chars().any(char::is_control) {
                return Err(usage("tool", "invalid tool name"));
            }
            let args = arguments(args, args_file, *stdin, Duration::from_secs(cli.timeout)).await?;
            let remote = remote(
                cli,
                &loaded,
                Operation::Call {
                    server: server.clone(),
                    tool: tool.clone(),
                    arguments: args,
                },
                uncertain,
            )
            .await?;
            let code = if remote.value["isError"].as_bool() == Some(true) {
                3
            } else {
                0
            };
            let text = if cli.text {
                Some(tool_text(&remote.value))
            } else {
                None
            };
            Ok(Output {
                value: Some(remote.value),
                text,
                code,
                diagnostic: None,
                connection: None,
            }
            .with_connection(remote.connection))
        }
        Command::Reset { server, force } => {
            let remote = remote(
                cli,
                &loaded,
                Operation::Reset {
                    server: server.clone(),
                    force: *force,
                },
                uncertain,
            )
            .await?;
            if let Some(warning) = remote.value.get("cleanup_warning").and_then(Value::as_str) {
                eprintln!("mcpcli: {}", safe_text(warning));
            }
            Ok(Output::json(remote.value).with_connection(remote.connection))
        }
        Command::Skills { command } => {
            let catalog = skills::discover(&loaded.config.skills)
                .map_err(|e| usage("skills", format!("{e:#}")))?;
            for warning in &catalog.warnings {
                eprintln!("mcpcli: {}", safe_text(warning));
            }
            if matches!(command, SkillCommand::List) {
                let text = catalog
                    .skills
                    .iter()
                    .map(|s| format!("{}\t{}", s.name, brief(&s.description)))
                    .collect::<Vec<_>>()
                    .join("\n");
                return Ok(Output::with_text(json!(catalog), text));
            }
            let name = match command {
                SkillCommand::Show { name }
                | SkillCommand::Files { name }
                | SkillCommand::Read { name, .. } => name,
                _ => unreachable!(),
            };
            let skill = catalog
                .skills
                .iter()
                .find(|s| s.name == *name)
                .ok_or_else(|| usage("unknown_skill", format!("unknown skill {name:?}")))?;
            match command {
                SkillCommand::Show { .. } => {
                    let content =
                        skills::show(skill).map_err(|e| usage("skill_read", format!("{e:#}")))?;
                    let text = format!("Base directory: {}\n\n{content}", skill.root.display());
                    Ok(Output::with_text(
                        json!({"name":name,"root":skill.root,"path":skill.path,"content":content}),
                        text,
                    ))
                }
                SkillCommand::Files { .. } => {
                    let files =
                        skills::files(skill).map_err(|e| usage("skill_files", format!("{e:#}")))?;
                    let text = files.join("\n");
                    Ok(Output::with_text(
                        json!({"name":name,"root":skill.root,"files":files}),
                        text,
                    ))
                }
                SkillCommand::Read { path, .. } => {
                    let content = skills::read(skill, Path::new(path))
                        .map_err(|e| usage("skill_read", format!("{e:#}")))?;
                    Ok(Output::with_text(
                        json!({"name":name,"root":skill.root,"path":path,"content":content}),
                        content,
                    ))
                }
                _ => unreachable!(),
            }
        }
        Command::Daemon { .. } => unreachable!(),
    }
}

async fn entry(cli: Cli) -> i32 {
    let foreground = matches!(
        cli.command,
        Command::Daemon {
            command: DaemonCommand::Run
        }
    );
    let uncertain = AtomicBool::new(false);
    let signal = mcpcli::platform::interrupted();
    tokio::pin!(signal);
    let result = if foreground {
        run(&cli, &uncertain).await
    } else {
        tokio::select! {
            biased;
            result = run(&cli, &uncertain) => result,
            signal = &mut signal => match signal {
                Ok(code) => Err(Failure::new(code, "cancelled", "interrupted; cancellation is not rollback")
                    .uncertain(uncertain.load(Ordering::Acquire))
                    .into()),
                Err(error) => Err(error.into()),
            },
        }
    };

    let mut output = result.unwrap_or_else(Output::failure);
    if let Some(mut connection) = output.connection.take() {
        let cleanup = tokio::select! {
            biased;
            result = connection.close() => result.err().map(|error|
                Failure::new(1, "cleanup", format!("MCP operation finished; connection cleanup failed: {error}; do not replay the operation"))),
            interrupted = &mut signal => Some(match interrupted {
                Ok(code) => Failure::new(code, "cancelled", "MCP result received; connection cleanup interrupted; do not replay the operation"),
                Err(error) => Failure::new(1, "cleanup", format!("MCP result received; signal handling failed during cleanup: {error}")),
            }),
        };
        output = output.cleanup(cleanup);
    }
    if let Some(error) = &output.diagnostic {
        let mut stderr = std::io::stderr().lock();
        let _ = serde_json::to_writer(&mut stderr, &json!({"error":error}));
        let _ = writeln!(stderr);
    }
    let Some(value) = output.value else {
        return output.code;
    };
    let mut stdout = std::io::stdout().lock();
    let written = if cli.text {
        let text = output
            .text
            .unwrap_or_else(|| serde_json::to_string_pretty(&value).unwrap());
        let rendered = safe_text(&text);
        if rendered.is_empty() {
            Ok(())
        } else if rendered.ends_with('\n') {
            stdout.write_all(rendered.as_bytes())
        } else {
            writeln!(stdout, "{rendered}")
        }
    } else {
        serde_json::to_writer(&mut stdout, &value)
            .map_err(std::io::Error::other)
            .and_then(|_| writeln!(stdout))
    };
    if let Err(error) = written {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            eprintln!("mcpcli: stdout failed: {error}");
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
                let rendered = safe_text(&doc.content);
                let body = rendered.trim_end_matches('\n');
                let mut stdout = std::io::stdout().lock();
                let _ = if body.is_empty() {
                    writeln!(stdout, "\nGlobal AGENTS.md ({})", doc.path.display())
                } else {
                    writeln!(
                        stdout,
                        "\nGlobal AGENTS.md ({}):\n\n{body}",
                        doc.path.display()
                    )
                };
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
    let code = runtime.block_on(entry(cli));
    // Drop async transports before exit, while bounding an interrupted stdin blocking reader.
    runtime.shutdown_timeout(Duration::from_secs(3));
    std::process::ExitCode::from(code as u8)
}
