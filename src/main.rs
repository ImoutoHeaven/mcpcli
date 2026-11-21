use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcpcli::{
    agents,
    budget::{ByteBudget, Leased},
    config::{self, LoadedConfig},
    daemon,
    discovery::{self, safe_text, write_safe_text, write_text_json},
    ipc::{self, Failure, Operation},
    json as input,
    mcp::McpConnection,
    media::{InflightBatch, MediaStore},
    runtime::{self, Paths},
    skills,
};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
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
    #[arg(
        long,
        global = true,
        value_parser = clap::value_parser!(u64).range(1..=3600),
        help = "Request deadline in seconds, including queue and connection time [default: 60]"
    )]
    timeout: Option<u64>,
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

impl Cli {
    fn timeout_secs(&self) -> u64 {
        self.timeout.unwrap_or(60)
    }

    fn has_explicit_timeout(&self) -> bool {
        self.timeout.is_some()
    }
}

#[allow(dead_code)]
#[derive(Parser)]
#[command(
    ignore_errors = true,
    disable_help_flag = true,
    disable_version_flag = true,
    disable_help_subcommand = true
)]
struct TopLevelHelpProbe {
    #[arg(short, long, env = "MCPCLI_CONFIG", default_value = "mcpcli.json")]
    config: PathBuf,
    #[arg(long)]
    session: Option<std::ffi::OsString>,
    #[arg(long)]
    timeout: Option<std::ffi::OsString>,
    #[arg(long, action = clap::ArgAction::Count)]
    oneshot: u8,
    #[arg(long, action = clap::ArgAction::Count)]
    text: u8,
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
    /// Read live server information, capabilities, and instructions.
    Server { server: String },
    /// List current tool definitions for a server.
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
        #[arg(
            long,
            help = "Keep base64 media inline instead of saving to the local media cache"
        )]
        inline_media: bool,
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
    inflight: Option<InflightBatch>,
    lease: Option<mcpcli::budget::ByteLease>,
}
impl Output {
    fn json(value: Value) -> Self {
        Self {
            value: Some(value),
            text: None,
            code: 0,
            diagnostic: None,
            connection: None,
            inflight: None,
            lease: None,
        }
    }
    fn with_text(value: Value, text: String) -> Self {
        Self {
            value: Some(value),
            text: Some(text),
            code: 0,
            diagnostic: None,
            connection: None,
            inflight: None,
            lease: None,
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
            inflight: None,
            lease: None,
        }
    }
    fn with_connection(mut self, connection: Option<McpConnection>) -> Self {
        self.connection = connection;
        self
    }
    fn with_inflight(mut self, inflight: Option<InflightBatch>) -> Self {
        self.inflight = inflight;
        self
    }
    fn with_lease(mut self, lease: Option<mcpcli::budget::ByteLease>) -> Self {
        self.lease = lease;
        self
    }
    fn cleanup(mut self, error: Option<Failure>) -> Self {
        if let Some(error) = error {
            if let Some(original) = &mut self.diagnostic {
                original.message = format!("{}; additionally: {}", original.message, error.message);
            } else {
                if self.code == 0 {
                    self.code = error.code;
                }
                self.diagnostic = Some(error);
            }
        }
        self
    }
}

struct RemoteOutput {
    value: Value,
    connection: Option<McpConnection>,
    inflight: Option<InflightBatch>,
    lease: Option<mcpcli::budget::ByteLease>,
}
impl RemoteOutput {
    fn into_output(self, render: impl FnOnce(Value) -> Result<Output>) -> Output {
        render(self.value)
            .unwrap_or_else(Output::failure)
            .with_connection(self.connection)
            .with_inflight(self.inflight)
            .with_lease(self.lease)
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
            let file_name = doc
                .path
                .file_name()
                .map(|name| name.to_string_lossy())
                .unwrap_or_default();
            format!("{file_name} {}", doc.content)
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
        let path = safe_text(&doc.path.display().to_string());
        let rendered = safe_text(&doc.content);
        let body = rendered.trim_end_matches('\n');
        if body.is_empty() {
            sections.push(format!("agents {path}"));
        } else {
            sections.push(format!("agents {path}\n\n{body}"));
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

fn argument_memory_failure() -> anyhow::Error {
    Failure::new(
        1,
        "memory_limit_exceeded",
        "Tool arguments exceed the active memory limit",
    )
    .into()
}

async fn read_argument_bytes(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    process_budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<Vec<u8>>> {
    let mut lease = process_budget.empty_lease();
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut chunk)
            .await
            .map_err(|error| usage("arguments", error))?;
        if count == 0 {
            break;
        }
        let needed = bytes.len().saturating_add(count);
        if needed > ipc::ARGUMENT_LIMIT {
            return Err(usage("arguments", "arguments exceed the 1 MiB limit"));
        }
        if !lease.reserve_vec(&mut bytes, needed, ipc::ARGUMENT_LIMIT) {
            return Err(argument_memory_failure());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Ok(Leased::new(bytes, lease))
}

async fn arguments(
    args: &Option<String>,
    file: &Option<PathBuf>,
    stdin: bool,
    budget: Duration,
    process_budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<Map<String, Value>>> {
    let bytes = if let Some(args) = args {
        read_argument_bytes(args.as_bytes(), process_budget).await?
    } else if let Some(path) = file {
        let file = mcpcli::platform::open_input(path)
            .with_context(|| format!("cannot read {}", path.display()))
            .map_err(|error| usage("arguments", error))?;
        if !file
            .metadata()
            .map_err(|error| usage("arguments", error))?
            .is_file()
        {
            return Err(usage("arguments", "input must be a regular file"));
        }
        read_argument_bytes(tokio::fs::File::from_std(file), process_budget).await?
    } else if stdin {
        timeout(
            budget,
            read_argument_bytes(tokio::io::stdin(), process_budget),
        )
        .await
        .map_err(|_| Failure::new(5, "timeout", "timed out reading JSON arguments from stdin"))??
    } else {
        read_argument_bytes(&b"{}"[..], process_budget).await?
    };
    let value_lease = process_budget
        .try_acquire(bytes.len())
        .ok_or_else(argument_memory_failure)?;
    let scratch = process_budget
        .try_acquire(input::scratch_bytes(&bytes))
        .ok_or_else(argument_memory_failure)?;
    let value = input::parse(&bytes).map_err(|error| usage("arguments", format!("{error:#}")))?;
    drop(scratch);
    drop(bytes);
    match value {
        Value::Object(arguments) => Ok(Leased::new(arguments, value_lease)),
        _ => Err(usage("arguments", "tool arguments must be a JSON object")),
    }
}

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

async fn remote(
    cli: &Cli,
    loaded: &LoadedConfig,
    operation: Leased<Operation>,
    uncertain: &AtomicBool,
) -> Result<RemoteOutput> {
    let server = match &*operation {
        Operation::Server { server }
        | Operation::Tools { server }
        | Operation::Call { server, .. }
        | Operation::Reset { server, .. } => server,
        _ => unreachable!(),
    };
    let config = loaded
        .config
        .servers
        .get(server)
        .ok_or_else(|| usage("unknown_server", format!("unknown server {server:?}")))?;
    let budget = Duration::from_secs(cli.timeout_secs());
    let started = Instant::now();
    if cli.oneshot {
        let oneshot_fut = async {
            if matches!(&*operation, Operation::Reset { .. }) {
                return Err(usage("mode", "reset cannot be used with --oneshot"));
            }
            let mut connection =
                McpConnection::connect(config, budget.min(Duration::from_secs(30)))
                    .await
                    .map_err(|error| daemon::mcp_error(error, false))?;
            let remaining = budget.saturating_sub(started.elapsed());
            let process_budget = ByteBudget::global();
            let result: std::result::Result<(Value, mcpcli::budget::ByteLease), anyhow::Error> =
                match &*operation {
                    Operation::Server { .. } => {
                        connection.server_info().map(|value| value.into_parts())
                    }
                    Operation::Tools { .. } => {
                        connection.list_tool_pages(remaining).await.map(|pages| {
                            let (pages, lease) = pages.into_parts();
                            let mut value = Map::new();
                            value.insert("pages".into(), Value::Array(pages));
                            (Value::Object(value), lease)
                        })
                    }
                    Operation::Call {
                        tool, arguments, ..
                    } => {
                        connection
                            .call_with_dispatch_leased(tool, arguments, remaining, uncertain)
                            .await
                    }
                    _ => unreachable!(),
                }
                .map_err(|error| {
                    anyhow::Error::from(daemon::mcp_error(error, uncertain.load(Ordering::Acquire)))
                });
            match result.map(|(value, lease)| (lease, value)) {
                Ok((mut lease, mut value)) => {
                    uncertain.store(false, Ordering::Release);
                    let inflight = match &*operation {
                        Operation::Call {
                            inline_media: false,
                            ..
                        } => match MediaStore::default()
                            .extract_and_save(&mut value, &mut lease, process_budget)
                            .await
                        {
                            Ok(batch) => batch,
                            Err(failure) => {
                                drop(value);
                                drop(lease);
                                let mut error = anyhow::Error::from(failure);
                                close_after_error(&mut connection, &mut error).await;
                                return Err(error);
                            }
                        },
                        _ => None,
                    };
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
    let (input_lease, operation) = {
        let (operation, lease) = operation.into_parts();
        (lease, operation)
    };
    let request = paths.request_with_explicit(
        &loaded.digest,
        operation,
        remaining,
        cli.has_explicit_timeout(),
    )?;
    let request = Leased::new(request, input_lease);
    let (response_lease, response) = runtime::exchange_tracked(&paths, &request, uncertain)
        .await
        .map(|(response, lease)| (lease, response))?;
    Ok(RemoteOutput {
        value: response.result.context("daemon did not return a result")?,
        connection: None,
        inflight: None,
        lease: Some(response_lease),
    })
}

struct ServerView<'a> {
    root: &'a Map<String, Value>,
    result: &'a Map<String, Value>,
}

impl serde::Serialize for ServerView<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.root.len()))?;
        for (key, value) in self.root {
            if key == "result" {
                map.serialize_entry(
                    key,
                    &discovery::ObjectWithout(self.result, &["instructions"]),
                )?;
            } else {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

fn server_view(value: &Value) -> Result<ServerView<'_>> {
    let root = value.as_object().context("invalid server information")?;
    let result = root
        .get("result")
        .and_then(Value::as_object)
        .context("invalid MCP server result")?;
    Ok(ServerView { root, result })
}

fn write_server_text(stdout: &mut impl Write, value: &Value) -> std::io::Result<()> {
    let view = server_view(value).map_err(std::io::Error::other)?;
    write_text_json(stdout, &view)?;
    if let Some(instructions) = view.result.get("instructions").and_then(Value::as_str) {
        stdout.write_all(b"\n\nInstructions:\n")?;
        write_safe_text(stdout, instructions)?;
        if !instructions.is_empty() && !instructions.ends_with('\n') {
            stdout.write_all(b"\n")?;
        }
    } else {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

fn listed_tools(value: &Value) -> Result<impl Iterator<Item = &Value>> {
    let pages = value["pages"]
        .as_array()
        .context("invalid tool list pages")?;
    anyhow::ensure!(
        pages.iter().all(|page| page["tools"].is_array()),
        "invalid tool list page"
    );
    Ok(pages
        .iter()
        .flat_map(|page| page["tools"].as_array().unwrap()))
}

fn write_tools_text(stdout: &mut impl Write, value: &Value) -> std::io::Result<()> {
    let tools = listed_tools(value).map_err(std::io::Error::other)?;
    let parts = tools.enumerate().flat_map(|(index, tool)| {
        let name = tool["name"].as_str().unwrap_or("<unnamed>");
        let description = tool["description"]
            .as_str()
            .filter(|description| !description.is_empty());
        [
            (index > 0).then_some("\n\n"),
            Some(name),
            description.map(|_| "\n"),
            description,
        ]
        .into_iter()
        .flatten()
    });
    discovery::write_text_parts(stdout, parts)
}

fn take_listed_tool(value: &mut Value, name: &str) -> Result<Value> {
    let _ = listed_tools(value)?;
    for page in value["pages"].as_array_mut().expect("validated pages") {
        for tool in page["tools"].as_array_mut().expect("validated tools") {
            if tool["name"].as_str() == Some(name) {
                return Ok(tool.take());
            }
        }
    }
    Err(usage("unknown_tool", format!("unknown tool {name:?}")))
}

fn write_tool_text(
    stdout: &mut impl Write,
    result: &Value,
    inline_media: bool,
) -> std::io::Result<()> {
    let mut saved_by_index = BTreeMap::new();
    if !inline_media && let Some(entries) = result.get("savedMedia").and_then(Value::as_array) {
        for entry in entries {
            if let Some(index) = entry.get("contentIndex").and_then(Value::as_u64) {
                saved_by_index.insert(index as usize, entry);
            }
        }
    }
    let remote = if inline_media {
        result
    } else {
        &result["result"]
    };
    enum Part<'a> {
        Text(&'a str),
        Saved(&'a Value),
    }
    let mut omitted = 0;
    let mut emitted = false;
    let mut ends_with_lf = false;
    let content = remote
        .get("content")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut parts = content
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            if block["type"] == "text" {
                block["text"]
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .map(Part::Text)
            } else if let Some(saved) = saved_by_index.get(&index) {
                Some(Part::Saved(saved))
            } else {
                omitted += 1;
                None
            }
        })
        .peekable();
    while let Some(part) = parts.next() {
        if emitted {
            stdout.write_all(b"\n")?;
        }
        emitted = true;
        let followed_by_lf = parts.peek().is_some();
        let tail = match part {
            Part::Text(text) => text,
            Part::Saved(saved) => {
                let kind = saved["kind"].as_str().unwrap_or("media");
                let mime = saved["mimeType"]
                    .as_str()
                    .unwrap_or("application/octet-stream");
                let bytes = saved["bytes"].as_u64().unwrap_or(0);
                stdout.write_all(b"Saved ")?;
                write_safe_text(stdout, kind)?;
                stdout.write_all(b" [")?;
                write_safe_text(stdout, mime)?;
                write!(stdout, "] ({bytes} B): ")?;
                saved["savedPath"].as_str().unwrap_or("")
            }
        };
        // Joining text blocks inserts LF. Preserve CRLF normalization even when
        // the CR belongs to the preceding block and the LF is the separator.
        let tail = if followed_by_lf {
            tail.strip_suffix('\r').unwrap_or(tail)
        } else {
            tail
        };
        write_safe_text(stdout, tail)?;
        ends_with_lf = tail.ends_with('\n');
    }
    if omitted > 0 {
        eprintln!(
            "mcpcli: {omitted} non-text content block(s) are available in default JSON output"
        );
    }
    if let Some(structured) = remote.get("structuredContent") {
        if !emitted {
            write_text_json(stdout, structured)?;
            emitted = true;
            ends_with_lf = false;
        } else {
            eprintln!("mcpcli: structuredContent is available in default JSON output");
        }
    }
    if emitted && !ends_with_lf {
        stdout.write_all(b"\n")?;
    }
    Ok(())
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
                let (response_lease, response) = runtime::exchange(
                    &paths,
                    &paths.request("", Operation::Status, Duration::from_secs(2))?,
                )
                .await
                .map(|(response, lease)| (lease, response))?;
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
        Command::Server { server } => {
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Server {
                        server: server.clone(),
                    },
                    ByteBudget::global().empty_lease(),
                ),
                uncertain,
            )
            .await?;
            Ok(remote.into_output(|value| {
                if cli.text {
                    server_view(&value)?;
                }
                Ok(Output::json(value))
            }))
        }
        Command::Tools { server } => {
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Tools {
                        server: server.clone(),
                    },
                    ByteBudget::global().empty_lease(),
                ),
                uncertain,
            )
            .await?;
            Ok(remote.into_output(|value| {
                if cli.text {
                    let _ = listed_tools(&value)?;
                }
                Ok(Output::json(value))
            }))
        }
        Command::Inspect { server, tool } => {
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Tools {
                        server: server.clone(),
                    },
                    ByteBudget::global().empty_lease(),
                ),
                uncertain,
            )
            .await?;
            Ok(remote
                .into_output(|mut value| Ok(Output::json(take_listed_tool(&mut value, tool)?))))
        }
        Command::Call {
            server,
            tool,
            args,
            args_file,
            stdin,
            inline_media,
        } => {
            if tool.is_empty() || tool.len() > 512 || tool.chars().any(char::is_control) {
                return Err(usage("tool", "invalid tool name"));
            }
            let args = arguments(
                args,
                args_file,
                *stdin,
                Duration::from_secs(cli.timeout_secs()),
                ByteBudget::global(),
            )
            .await?;
            let (input_lease, args) = {
                let (args, lease) = args.into_parts();
                (lease, args)
            };
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Call {
                        server: server.clone(),
                        tool: tool.clone(),
                        arguments: args,
                        inline_media: *inline_media,
                    },
                    input_lease,
                ),
                uncertain,
            )
            .await?;
            let result = if *inline_media {
                &remote.value
            } else {
                &remote.value["result"]
            };
            let code = if result["isError"].as_bool() == Some(true) {
                3
            } else {
                0
            };
            Ok(Output {
                value: Some(remote.value),
                text: None,
                code,
                diagnostic: None,
                connection: None,
                inflight: None,
                lease: None,
            }
            .with_connection(remote.connection)
            .with_inflight(remote.inflight)
            .with_lease(remote.lease))
        }
        Command::Reset { server, force } => {
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Reset {
                        server: server.clone(),
                        force: *force,
                    },
                    ByteBudget::global().empty_lease(),
                ),
                uncertain,
            )
            .await?;
            if let Some(warning) = remote.value.get("cleanup_warning").and_then(Value::as_str) {
                eprintln!("mcpcli: {}", safe_text(warning));
            }
            Ok(Output::json(remote.value)
                .with_connection(remote.connection)
                .with_inflight(remote.inflight)
                .with_lease(remote.lease))
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
                    let root = safe_text(&skill.root.display().to_string());
                    let text = format!("Base directory: {root}\n\n{content}");
                    Ok(Output::with_text(
                        json!({"name":name,"root":skill.root,"path":skill.path,"content":content}),
                        text,
                    ))
                }
                SkillCommand::Files { .. } => {
                    let files =
                        skills::files(skill).map_err(|e| usage("skill_files", format!("{e:#}")))?;
                    let text = files
                        .iter()
                        .map(|path| safe_text(path))
                        .collect::<Vec<_>>()
                        .join("\n");
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

/// Terminal boundary for text-mode stdout.
fn write_text(stdout: &mut impl Write, text: &str) -> std::io::Result<()> {
    write_safe_text(stdout, text)?;
    if !text.is_empty() && !text.ends_with('\n') {
        stdout.write_all(b"\n")?;
    }
    Ok(())
}

fn write_diagnostic(stderr: &mut impl Write, error: &Failure) -> std::io::Result<()> {
    #[derive(serde::Serialize)]
    struct Diagnostic<'a> {
        error: &'a Failure,
    }
    serde_json::to_writer(&mut *stderr, &Diagnostic { error }).map_err(std::io::Error::other)?;
    writeln!(stderr)
}

fn retain_cli_input(
    cli: Cli,
    budget: &std::sync::Arc<ByteBudget>,
) -> std::result::Result<Leased<Cli>, Failure> {
    let bytes = match &cli.command {
        Command::Call {
            args: Some(args), ..
        } => {
            if args.len() > ipc::ARGUMENT_LIMIT {
                return Err(Failure::new(
                    2,
                    "arguments",
                    "arguments exceed the 1 MiB limit",
                ));
            }
            args.capacity()
        }
        _ => 0,
    };
    let lease = budget.try_acquire(bytes).ok_or_else(|| {
        Failure::new(
            1,
            "memory_limit_exceeded",
            "Tool arguments exceed the active memory limit",
        )
    })?;
    Ok(Leased::new(cli, lease))
}

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
                    write_tool_text(&mut stdout, &value, *inline_media)
                }
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
    let code = runtime.block_on(entry(cli));
    // Drop async transports before exit, while bounding an interrupted stdin blocking reader.
    runtime.shutdown_timeout(Duration::from_secs(3));
    std::process::ExitCode::from(code as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn server_text(value: &Value) -> Result<String> {
        let mut bytes = Vec::new();
        write_server_text(&mut bytes, value)?;
        Ok(String::from_utf8(bytes)?)
    }

    fn tools_text(value: &Value) -> Result<String> {
        let mut bytes = Vec::new();
        write_tools_text(&mut bytes, value)?;
        Ok(String::from_utf8(bytes)?)
    }

    fn tool_text(result: &Value, inline_media: bool) -> String {
        let mut output = Vec::new();
        write_tool_text(&mut output, result, inline_media).unwrap();
        String::from_utf8(output).unwrap()
    }

    #[test]
    fn tool_text_streams_large_borrowed_spans_and_normalizes_joined_crlf() {
        struct BorrowedWriter {
            start: usize,
            end: usize,
            borrowed: usize,
            written: usize,
        }
        impl Write for BorrowedWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > 1024 {
                    let ptr = bytes.as_ptr() as usize;
                    assert!(
                        ptr >= self.start && ptr + bytes.len() <= self.end,
                        "large text was copied"
                    );
                    self.borrowed += bytes.len();
                }
                self.written += bytes.len();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let value = json!({"content": [{"type":"text", "text":"x".repeat(128 << 10)}]});
        let text = value["content"][0]["text"].as_str().unwrap();
        let mut writer = BorrowedWriter {
            start: text.as_ptr() as usize,
            end: text.as_ptr() as usize + text.len(),
            borrowed: 0,
            written: 0,
        };
        write_tool_text(&mut writer, &value, true).unwrap();
        assert_eq!(writer.borrowed, text.len());
        assert_eq!(writer.written, text.len() + 1);
        assert_eq!(
            tool_text(
                &json!({"content": [
                    {"type":"text", "text":"first\r"}, {"type":"text", "text":"second\r\n"}
                ]}),
                true
            ),
            "first\nsecond\n"
        );
    }

    #[test]
    fn diagnostic_streams_shared_rpc_error_without_copying_its_value() {
        struct BorrowedWriter {
            start: usize,
            end: usize,
            borrowed: usize,
        }
        impl Write for BorrowedWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > 1024 {
                    let ptr = bytes.as_ptr() as usize;
                    assert!(
                        ptr >= self.start && ptr + bytes.len() <= self.end,
                        "large diagnostic was copied"
                    );
                    self.borrowed += bytes.len();
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut error = Failure::new(4, "mcp_protocol", "failed");
        error.rpc_error = Some(std::sync::Arc::new(
            json!({"data":{"text":"x".repeat(128 << 10)}}),
        ));
        let text = error.rpc_error.as_ref().unwrap()["data"]["text"]
            .as_str()
            .unwrap();
        let mut writer = BorrowedWriter {
            start: text.as_ptr() as usize,
            end: text.as_ptr() as usize + text.len(),
            borrowed: 0,
        };
        write_diagnostic(&mut writer, &error).unwrap();
        assert_eq!(writer.borrowed, text.len());
    }

    #[test]
    fn structured_and_fallback_text_streams_escape_c1_without_changing_json() {
        let structured = json!({"key\u{85}": "normal é😀\u{85}\u{9b}\n"});
        let expected = safe_text(&serde_json::to_string_pretty(&structured).unwrap());
        assert_eq!(
            tool_text(&json!({"structuredContent": structured}), true),
            format!("{expected}\n")
        );
        let mut bytes = Vec::new();
        write_text_json(&mut bytes, &structured).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), expected);
        let json = serde_json::to_vec(&structured).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&json).unwrap(), structured);
        assert_eq!(structured["key\u{85}"], "normal é😀\u{85}\u{9b}\n");
    }

    #[test]
    fn empty_tool_text_blocks_preserve_structured_output_without_blank_lines() {
        for count in [1, 2] {
            let mut result = json!({
                "content": vec![json!({"type": "text", "text": ""}); count]
            });
            assert_eq!(tool_text(&result, true), "");
            result["structuredContent"] = json!({"answer": 42});
            assert_eq!(
                tool_text(&result, true),
                format!(
                    "{}\n",
                    serde_json::to_string_pretty(&result["structuredContent"]).unwrap()
                )
            );
        }
        assert_eq!(
            tool_text(&json!({"content": [{"type": "text", "text": " \n"}]}), true),
            " \n"
        );
    }

    #[test]
    fn tool_text_renders_saved_media_summary_and_ignores_remote_field_when_inline() {
        let saved = json!({
            "content": [
                {"type": "text", "text": "Read video"},
                {"type": "resource", "resource": {"uri": "file:///x.mp4", "mimeType": "video/mp4"}}
            ],
            "savedMedia": [{
                "contentIndex": 1,
                "kind": "resource",
                "savedPath": "/tmp/media/abc.mp4",
                "mimeType": "video/mp4",
                "bytes": 1234
            }]
        });
        let local = json!({"result": saved, "savedMedia": saved["savedMedia"]});
        assert_eq!(
            tool_text(&local, false),
            "Read video\nSaved resource [video/mp4] (1234 B): /tmp/media/abc.mp4\n"
        );
        // When --inline-media is set, any remote savedMedia field is ignored.
        assert_eq!(tool_text(&saved, true), "Read video\n");
    }

    #[test]
    fn server_text_preserves_full_instruction_body() {
        let value = json!({
            "source":"initialize",
            "protocolVersion":"2025-11-25",
            "result":{
                "serverInfo":{"name":"sample","version":"1.0"},
                "capabilities":{"tools":{}},
                "instructions":"    first line\nsecond line  \n\n"
            }
        });
        let rendered = server_text(&value).unwrap();
        assert!(rendered.contains("\"capabilities\""));
        assert!(rendered.ends_with("\n\nInstructions:\n    first line\nsecond line  \n\n"));
        assert_eq!(
            value["result"]["instructions"],
            "    first line\nsecond line  \n\n"
        );
    }

    #[test]
    fn tools_text_keeps_multiline_and_long_descriptions() {
        let description = format!("    first\n{}  ", "x".repeat(300));
        let tools = json!({"pages":[{"tools":[{"name":"sample","description":description,"inputSchema":{}}]}]});
        let rendered = tools_text(&tools).unwrap();
        assert_eq!(rendered, format!("sample\n{description}\n"));
    }

    #[test]
    fn text_stdout_sanitizes_server_tools_and_call_results_without_changing_json() {
        let raw = " \tindented  \nnext\r\nbare\r\u{1b}\u{9b}\u{0}  \n\n";
        let server = json!({"result":{"instructions":raw}});
        let tools = json!({"pages":[{"tools":[{"name":"sample","description":raw}]}]});
        let call = json!({"content":[{"type":"text","text":raw}]});
        for rendered in [
            server_text(&server).unwrap(),
            tools_text(&tools).unwrap(),
            tool_text(&call, true),
        ] {
            let mut bytes = Vec::new();
            write_text(&mut bytes, &rendered).unwrap();
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.ends_with(" \tindented  \nnext\nbare\\r\\u{1b}\\u{9b}\\u{0}  \n\n"));
            assert!(
                !text
                    .chars()
                    .any(|c| c.is_control() && !matches!(c, '\n' | '\t'))
            );
        }
        assert_eq!(server["result"]["instructions"], raw);
        assert_eq!(tools["pages"][0]["tools"][0]["description"], raw);
        assert_eq!(call["content"][0]["text"], raw);
    }

    #[tokio::test]
    async fn argument_reads_and_parse_retain_the_source_value_owner() {
        let input = serde_json::to_string(&json!({"text":"ordinary ".repeat(256) + "\n"})).unwrap();
        let peak = input.len() * 2 + input::scratch_bytes(input.as_bytes());
        for limit in [peak - 1, peak] {
            let budget = ByteBudget::new(limit);
            let result = arguments(
                &Some(input.clone()),
                &None,
                false,
                Duration::from_secs(1),
                &budget,
            )
            .await;
            if limit < peak {
                assert_eq!(
                    result.unwrap_err().downcast_ref::<Failure>().unwrap().kind,
                    "memory_limit_exceeded"
                );
            } else {
                let arguments = result.unwrap();
                assert_eq!(arguments["text"], "ordinary ".repeat(256) + "\n");
                assert_eq!(budget.used(), input.len());
                drop(arguments);
            }
            assert_eq!(budget.used(), 0);
        }
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("arguments.json");
        std::fs::write(&file, &input).unwrap();
        let budget = ByteBudget::new(peak);
        let result = arguments(&None, &Some(file), false, Duration::from_secs(1), &budget)
            .await
            .unwrap();
        assert_eq!(budget.used(), input.len());
        drop(result);
        assert_eq!(budget.used(), 0);

        let budget = ByteBudget::new(64);
        let (mut sender, reader) = tokio::io::duplex(64);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"part")
            .await
            .unwrap();
        let mut reading = Box::pin(read_argument_bytes(reader, &budget));
        assert!(
            timeout(Duration::from_millis(10), &mut reading)
                .await
                .is_err()
        );
        assert_eq!(budget.used(), 4);
        drop(reading);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn noncall_text_borrows_payloads_and_inspect_moves_the_selected_value() {
        struct BorrowedWriter {
            ranges: Vec<std::ops::Range<usize>>,
            borrowed: usize,
        }
        impl Write for BorrowedWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > 1024 {
                    let start = bytes.as_ptr() as usize;
                    assert!(
                        self.ranges
                            .iter()
                            .any(|range| range.start <= start && start + bytes.len() <= range.end),
                        "large text was copied"
                    );
                    self.borrowed += bytes.len();
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        fn ranges(value: &Value, output: &mut Vec<std::ops::Range<usize>>) {
            match value {
                Value::String(text) if text.len() > 1024 => {
                    let start = text.as_ptr() as usize;
                    output.push(start..start + text.len());
                }
                Value::Array(items) => {
                    for item in items {
                        ranges(item, output);
                    }
                }
                Value::Object(fields) => {
                    for value in fields.values() {
                        ranges(value, output);
                    }
                }
                _ => {}
            }
        }
        let server =
            json!({"result":{"instructions":"i".repeat(128 << 10), "extra":"m".repeat(128 << 10)}});
        let tools =
            json!({"pages":[{"tools":[{"name":"sample", "description":"d".repeat(128 << 10)}]}]});
        let tool = json!({"name":"sample", "description":"d".repeat(128 << 10),
            "inputSchema":{"description":"s".repeat(128 << 10)}, "_meta":{"long":"m".repeat(128 << 10)}});
        for (kind, value) in [(0, &server), (1, &tools), (2, &tool)] {
            let mut writer = BorrowedWriter {
                ranges: Vec::new(),
                borrowed: 0,
            };
            ranges(value, &mut writer.ranges);
            let expected: usize = writer.ranges.iter().map(|range| range.len()).sum();
            match kind {
                0 => write_server_text(&mut writer, value).unwrap(),
                1 => write_tools_text(&mut writer, value).unwrap(),
                _ => discovery::write_tool(&mut writer, value).unwrap(),
            }
            assert_eq!(writer.borrowed, expected);
        }
        let mut pages = json!({"pages":[{"tools":[{"name":"other"}, tool]}]});
        let pointer = pages["pages"][0]["tools"][1]["description"]
            .as_str()
            .unwrap()
            .as_ptr();
        let selected = take_listed_tool(&mut pages, "sample").unwrap();
        assert_eq!(selected["description"].as_str().unwrap().as_ptr(), pointer);
        assert!(pages["pages"][0]["tools"][1].is_null());
        assert_eq!(selected["_meta"]["long"].as_str().unwrap().len(), 128 << 10);
    }

    #[test]
    fn noncall_text_keeps_control_and_separator_semantics() {
        let tools = json!({"pages":[{"tools":[
            {"name":"first\r","description":"body\u{85}é\r"},
            {"name":"next"}
        ]}]});
        assert_eq!(tools_text(&tools).unwrap(), "first\nbody\\u{85}é\n\nnext\n");
        assert_eq!(
            tools_text(&json!({"pages":[{"tools":[{"name":""},{"name":""}]}]})).unwrap(),
            "\n\n"
        );
        assert_eq!(tools_text(&json!({"pages":[{"tools":[]}]})).unwrap(), "");
        let server = json!({"result":{"instructions":"","extra":"é\u{85}"}});
        assert!(
            server_text(&server)
                .unwrap()
                .ends_with("\n\nInstructions:\n")
        );
        assert!(server_text(&server).unwrap().contains("é\\u{85}"));
        assert_eq!(server["result"]["extra"], "é\u{85}");
        let tool = json!({"name":"  name\tpart ", "description":"description\r",
            "inputSchema":{"title":"é\u{9b}"}, "extra":null});
        let mut text = Vec::new();
        discovery::write_tool(&mut text, &tool).unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(text.starts_with("name part\n\ndescription\\r\n\nInput schema:\n"));
        assert!(text.contains("é\\u{9b}"));
        assert!(text.contains("\"extra\": null"));
        assert_eq!(tool["inputSchema"]["title"], "é\u{9b}");
    }

    #[tokio::test]
    async fn cli_argument_string_capacity_survives_reader_and_value_handoffs() {
        for exact in [false, true] {
            let mut cli = Cli::try_parse_from(["mcpcli", "call", "srv", "sample"]).unwrap();
            let mut text = String::with_capacity(512);
            text.push_str("{\"text\":\"ordinary\\ntext\"}");
            let capacity = text.capacity();
            let length = text.len();
            let peak = capacity + 2 * length + input::scratch_bytes(text.as_bytes());
            let Command::Call { args, .. } = &mut cli.command else {
                unreachable!()
            };
            *args = Some(text);
            let budget = ByteBudget::new(if exact { peak } else { peak - 1 });
            let cli = retain_cli_input(cli, &budget).unwrap();
            assert_eq!(budget.used(), capacity);
            let Command::Call { args, .. } = &cli.command else {
                unreachable!()
            };
            let parsed = arguments(args, &None, false, Duration::from_secs(1), &budget).await;
            if exact {
                let parsed = parsed.unwrap();
                assert_eq!(parsed["text"], "ordinary\ntext");
                assert_eq!(budget.used(), capacity + length);
                drop(parsed);
            } else {
                assert_eq!(
                    parsed.unwrap_err().downcast_ref::<Failure>().unwrap().kind,
                    "memory_limit_exceeded"
                );
            }
            assert_eq!(budget.used(), capacity);
            drop(cli);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn cleanup_failure_does_not_replace_known_tool_error_exit() {
        let mut output = Output::json(json!({"isError":true}));
        output.code = 3;
        let output = output.cleanup(Some(Failure::new(1, "cleanup", "cleanup failed")));
        assert_eq!(output.code, 3);
        assert_eq!(output.value, Some(json!({"isError":true})));
        assert_eq!(output.diagnostic.unwrap().kind, "cleanup");
    }

    #[test]
    fn cli_and_help_probe_definitions_are_consistent() {
        Cli::command().debug_assert();
        TopLevelHelpProbe::command().debug_assert();
        let parsed =
            Cli::try_parse_from(["mcpcli", "call", "srv", "tool", "--inline-media"]).unwrap();
        assert!(!parsed.has_explicit_timeout());
        assert_eq!(parsed.timeout_secs(), 60);
        assert!(matches!(
            parsed.command,
            Command::Call {
                inline_media: true,
                ..
            }
        ));
    }
}
