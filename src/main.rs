use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mcpcli::{
    agents, auth,
    budget::{ByteBudget, Leased},
    config::{self, LoadedConfig, ServerConfig},
    daemon,
    discovery::{self, safe_text, write_safe_text, write_text_json},
    events_exec::{CursorStore, DEFAULT_EXEC_TIMEOUT, ExecMode},
    events_stream::{
        self, DaemonTransport, DirectTransport, StreamConfig, StreamMode, StreamTimings,
    },
    ipc::{self, Failure, Operation},
    json as input,
    mcp::{
        CallBudgets, CallOptions, CallOutcome, McpConnection, PollEventsParams,
        tasks::{TaskPollBudgets, TaskStatusKind, poll_task_to_completion},
    },
    media::{InflightBatch, MediaStore},
    runtime::{self, Paths},
    schema::{ExpectedSchema, MAX_SCHEMA_DEPTH, SchemaMode},
    skills,
};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::{Instant, timeout, timeout_at};

#[derive(Parser)]
#[command(
    version,
    about = "Call MCP tools and read global AGENTS.md and local Agent Skills",
    after_help = "Agent skill guide: mcpcli --text skills builtin",
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
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Request timeout in seconds (1-3600; server default or 60); call inactivity timeout; events stream total milliseconds"
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
    fn timeout_secs_for(&self, server: &ServerConfig) -> u64 {
        self.timeout
            .or_else(|| server.timeout())
            .unwrap_or(60)
            .min(3600)
    }

    fn validate_request_timeout(&self) -> Result<()> {
        if let Some(secs) = self.timeout
            && secs > 3600
        {
            return Err(usage(
                "invalid_timeout",
                "timeout must be between 1 and 3600 seconds",
            ));
        }
        Ok(())
    }

    fn has_explicit_timeout(&self) -> bool {
        self.timeout.is_some()
    }

    fn uses_dual_clock(&self) -> bool {
        matches!(
            self.command,
            Command::Call { detach: false, .. }
                | Command::Tasks {
                    command: TasksCommand::Get { wait: true, .. }
                }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
enum CliSchemaMode {
    Compatible,
    Exact,
}

impl From<CliSchemaMode> for SchemaMode {
    fn from(mode: CliSchemaMode) -> Self {
        match mode {
            CliSchemaMode::Compatible => Self::Compatible,
            CliSchemaMode::Exact => Self::Exact,
        }
    }
}

fn validate_call_max_timeout(
    timeout_secs: u64,
    max_timeout: Option<u64>,
) -> Result<Option<Duration>> {
    let Some(max_secs) = max_timeout else {
        return Ok(None);
    };
    if max_secs == 0 || max_secs > 86_400 {
        return Err(usage(
            "invalid_timeout",
            "max-timeout must be between 1 and 86400 seconds",
        ));
    }
    if max_secs < timeout_secs {
        return Err(usage(
            "invalid_timeout",
            "max-timeout must be greater than or equal to timeout",
        ));
    }
    Ok(Some(Duration::from_secs(max_secs)))
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
    Tools {
        server: String,
        #[arg(
            long,
            help = "Filter tools by case-insensitive substring match on name or description"
        )]
        filter: Option<String>,
        #[arg(
            long,
            help = "Return compact tool summaries (name, first-line description, annotations)"
        )]
        summary: bool,
    },
    /// Read complete tool metadata and JSON schemas.
    Inspect {
        server: String,
        tool: String,
        #[arg(
            long,
            value_name = "FILE",
            help = "Verify live tool schema against expected JSON schema file"
        )]
        schema: Option<PathBuf>,
        #[arg(
            long,
            value_enum,
            default_value_t = CliSchemaMode::Compatible,
            requires = "schema",
            help = "Schema verification mode (compatible or exact)"
        )]
        schema_mode: CliSchemaMode,
    },
    /// Invoke one tool. Arguments must be a JSON object or KEY:=VALUE pairs.
    Call {
        server: String,
        tool: String,
        #[arg(long, conflicts_with_all = ["args_file", "stdin", "kv_args"])]
        args: Option<String>,
        #[arg(long, conflicts_with_all = ["args", "stdin", "kv_args"])]
        args_file: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["args", "args_file", "kv_args"])]
        stdin: bool,
        #[arg(
            long,
            help = "Return asynchronous task handles immediately instead of waiting for completion"
        )]
        detach: bool,
        #[arg(
            long,
            value_name = "FILE",
            help = "Verify live tool schema against expected JSON schema file before invoking"
        )]
        schema: Option<PathBuf>,
        #[arg(
            long,
            value_enum,
            default_value_t = CliSchemaMode::Compatible,
            requires = "schema",
            help = "Schema verification mode (compatible or exact)"
        )]
        schema_mode: CliSchemaMode,
        #[arg(
            long,
            help = "Keep base64 media inline instead of saving to the local media cache"
        )]
        inline_media: bool,
        #[arg(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Optional hard wall-clock limit in seconds (1-86400; default unbounded while progress continues)"
        )]
        max_timeout: Option<u64>,
        #[arg(
            value_name = "KEY:=VALUE",
            conflicts_with_all = ["args", "args_file", "stdin"]
        )]
        kv_args: Vec<String>,
    },
    /// Inspect, update, or cancel asynchronous MCP tasks.
    #[command(visible_alias = "task")]
    Tasks {
        #[command(subcommand)]
        command: TasksCommand,
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
    /// List, poll, or stream MCP events from a server.
    Events {
        #[command(subcommand)]
        command: EventsCommand,
    },
    /// Manage OAuth 2.1 credentials for HTTP MCP servers.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Validate configuration, AGENTS.md, and configured skill directories offline.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Manage the daemon for the selected config and session.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Validate configuration, AGENTS.md, and skills offline without starting a daemon.
    Validate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
enum CliStreamMode {
    Auto,
    Push,
    Poll,
}

impl From<CliStreamMode> for StreamMode {
    fn from(mode: CliStreamMode) -> Self {
        match mode {
            CliStreamMode::Auto => Self::Auto,
            CliStreamMode::Push => Self::Push,
            CliStreamMode::Poll => Self::Poll,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
enum CliExecMode {
    Oneshot,
    Stream,
}

impl From<CliExecMode> for ExecMode {
    fn from(mode: CliExecMode) -> Self {
        match mode {
            CliExecMode::Oneshot => Self::Oneshot,
            CliExecMode::Stream => Self::Stream,
        }
    }
}

#[derive(Subcommand)]
enum EventsCommand {
    /// List available event types on a server.
    List { server: String },
    /// Poll once for new events from a server.
    Poll {
        server: String,
        event: String,
        #[arg(long, conflicts_with_all = ["args_file", "stdin"])]
        args: Option<String>,
        #[arg(long, conflicts_with_all = ["args", "stdin"])]
        args_file: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["args", "args_file"])]
        stdin: bool,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        max_age_ms: Option<u64>,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        max_events: Option<u64>,
    },
    /// Subscribe to events in the foreground (NDJSON stdout or callback command).
    Stream {
        server: String,
        event: String,
        #[arg(long, conflicts_with_all = ["args_file", "stdin"])]
        args: Option<String>,
        #[arg(long, conflicts_with_all = ["args", "stdin"])]
        args_file: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["args", "args_file"])]
        stdin: bool,
        #[arg(long, value_enum, default_value_t = CliStreamMode::Auto)]
        mode: CliStreamMode,
        #[arg(long, conflicts_with = "cursor_file")]
        cursor: Option<String>,
        #[arg(long, conflicts_with = "cursor")]
        cursor_file: Option<PathBuf>,
        #[arg(long)]
        max_age_ms: Option<u64>,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        heartbeat_ms: Option<u64>,
        #[arg(long)]
        once: bool,
        #[arg(long, value_enum, default_value_t = CliExecMode::Oneshot)]
        exec_mode: CliExecMode,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        exec_timeout: Option<u64>,
        #[arg(last = true)]
        callback: Vec<String>,
    },
}

#[derive(Subcommand)]
enum TasksCommand {
    /// Fetch current status or wait for completion of an MCP task.
    Get {
        server: String,
        task_id: String,
        #[arg(
            long,
            help = "Wait until the task reaches a terminal state or requires input"
        )]
        wait: bool,
        #[arg(
            long,
            help = "Keep base64 media inline instead of saving to the local media cache"
        )]
        inline_media: bool,
        #[arg(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Optional hard wall-clock limit in seconds (requires --wait)"
        )]
        max_timeout: Option<u64>,
    },
    /// Submit input responses to an input_required MCP task.
    Update {
        server: String,
        task_id: String,
        #[arg(long, conflicts_with_all = ["args_file", "stdin"])]
        args: Option<String>,
        #[arg(long, conflicts_with_all = ["args", "stdin"])]
        args_file: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["args", "args_file"])]
        stdin: bool,
    },
    /// Request cancellation of an MCP task.
    Cancel { server: String, task_id: String },
}

#[derive(Subcommand)]
enum SkillCommand {
    List {
        #[arg(long, help = "List skills exposed by a remote MCP server")]
        server: Option<String>,
    },
    Show {
        #[arg(long, help = "Read a skill from a remote MCP server")]
        server: Option<String>,
        name: String,
    },
    Files {
        #[arg(long, help = "List files of a skill on a remote MCP server")]
        server: Option<String>,
        #[arg(long, help = "Relative directory to read on a remote MCP server")]
        dir: Option<String>,
        name: String,
    },
    Read {
        #[arg(long, help = "Read a skill file from a remote MCP server")]
        server: Option<String>,
        #[arg(
            long,
            help = "Keep base64 binary media inline instead of saving to the local media cache"
        )]
        inline_media: bool,
        name: String,
        path: std::ffi::OsString,
    },
    /// Print the built-in mcpcli agent skill guide offline.
    Builtin,
}

#[derive(Subcommand)]
enum AuthCommand {
    /// Authenticate with an HTTP MCP server via OAuth 2.1 PKCE.
    Login {
        server: String,
        #[arg(long = "scope")]
        scopes: Vec<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=65535))]
        callback_port: Option<u16>,
        #[arg(long)]
        callback_path: Option<String>,
        #[arg(long)]
        client_name: Option<String>,
        #[arg(long)]
        no_browser: bool,
    },
    /// Inspect stored OAuth status for one or all HTTP servers.
    Status { server: Option<String> },
    /// Revoke and delete stored OAuth credentials for an HTTP server.
    Logout { server: String },
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
        match (&mut self.lease, lease) {
            (Some(current), Some(extra)) => current.merge(extra),
            (None, lease) => self.lease = lease,
            _ => {}
        }
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
                "enabled": server.enabled(),
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
        let name = server["name"].as_str().unwrap();
        let desc = brief(server["description"].as_str().unwrap());
        if server["enabled"].as_bool() == Some(false) {
            lines.push(format!("server {name} [disabled]\t{desc}"));
        } else {
            lines.push(format!("server {name}\t{desc}"));
        }
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

const MAX_SCHEMA_FILE_BYTES: usize = 1 << 20;

fn validate_task_id(task_id: &str) -> Result<()> {
    if task_id.is_empty() || task_id.len() > 512 || task_id.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(usage(
            "invalid_task_id",
            "task_id must be 1..=512 bytes without ASCII control characters",
        ));
    }
    Ok(())
}

async fn load_expected_schema(
    path: &Path,
    mode: SchemaMode,
    process_budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<ExpectedSchema>> {
    let file = mcpcli::platform::open_input(path)
        .with_context(|| format!("cannot read schema file {}", path.display()))
        .map_err(|error| usage("schema", error))?;
    if !file
        .metadata()
        .map_err(|error| usage("schema", error))?
        .is_file()
    {
        return Err(usage("schema", "schema input must be a regular file"));
    }
    let mut reader = tokio::fs::File::from_std(file);
    let mut lease = process_budget.empty_lease();
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut chunk)
            .await
            .map_err(|error| usage("schema", error))?;
        if count == 0 {
            break;
        }
        let needed = bytes.len().saturating_add(count);
        if needed > MAX_SCHEMA_FILE_BYTES {
            return Err(usage("schema", "schema file exceeds the 1 MiB limit"));
        }
        if !lease.reserve_vec(&mut bytes, needed, MAX_SCHEMA_FILE_BYTES) {
            return Err(Failure::new(
                1,
                "memory_limit_exceeded",
                "Schema file exceeds the active memory limit",
            )
            .into());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let value_lease = process_budget.try_acquire(bytes.len()).ok_or_else(|| {
        Failure::new(
            1,
            "memory_limit_exceeded",
            "Schema file exceeds the active memory limit",
        )
    })?;
    let scratch = process_budget
        .try_acquire(input::scratch_bytes(&bytes))
        .ok_or_else(|| {
            Failure::new(
                1,
                "memory_limit_exceeded",
                "Schema file exceeds the active memory limit",
            )
        })?;
    let value = input::parse(&bytes).map_err(|error| usage("schema", format!("{error:#}")))?;
    drop(scratch);
    drop(bytes);
    let parsed =
        mcpcli::schema::parse_expected_schema(value, mode).map_err(|err| usage("schema", err))?;
    if !mcpcli::schema::schema_depth_ok(&parsed.input_schema, MAX_SCHEMA_DEPTH)
        || parsed
            .output_schema
            .as_ref()
            .is_some_and(|s| !mcpcli::schema::schema_depth_ok(s, MAX_SCHEMA_DEPTH))
    {
        return Err(usage(
            "schema",
            format!("schema exceeds maximum nesting depth of {MAX_SCHEMA_DEPTH}"),
        ));
    }
    Ok(Leased::new(parsed, value_lease))
}

fn parse_kv_cli_arguments(
    items: &[String],
    process_budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<Map<String, Value>>> {
    match input::parse_kv_arguments(items, process_budget) {
        Ok(map) => {
            let encoded = input::serialized_len(&*map)
                .map_err(|error| usage("arguments", format!("{error:#}")))?;
            if encoded > ipc::ARGUMENT_LIMIT {
                return Err(usage("arguments", "arguments exceed the 1 MiB limit"));
            }
            Ok(map)
        }
        Err(error) => {
            if let Some(io_err) = error.downcast_ref::<std::io::Error>()
                && io_err.kind() == std::io::ErrorKind::OutOfMemory
            {
                return Err(argument_memory_failure());
            }
            Err(usage("arguments", format!("{error:#}")))
        }
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
                                    let mut snapshot = poll_task_to_completion(
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
                                    )
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
                            let mut snapshot = poll_task_to_completion(
                                None,
                                task_id.clone(),
                                false,
                                poll_budgets,
                                cancel_token.clone(),
                                |step_budget| conn_ref.get_task(task_id, step_budget),
                                |_| async {},
                            )
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
                };
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
        runtime::exchange_cancellable(&paths, &request, uncertain, token).await
    } else {
        runtime::exchange_tracked(&paths, &request, uncertain).await
    }
    .map(|(response, lease)| (lease, response))?;
    Ok(RemoteOutput {
        value: response.result.context("daemon did not return a result")?,
        connection: None,
        inflight: None,
        lease: Some(response_lease),
    })
}

fn check_dispatch_budgets(
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
    fn render_tools<'a>(
        stdout: &mut impl Write,
        tools: impl Iterator<Item = &'a Value>,
        first_line: bool,
    ) -> std::io::Result<()> {
        let parts = tools.enumerate().flat_map(|(index, tool)| {
            let name = tool["name"].as_str().unwrap_or("<unnamed>");
            let description = tool["description"]
                .as_str()
                .filter(|description| !description.is_empty())
                .and_then(|description| {
                    if first_line {
                        description
                            .lines()
                            .map(str::trim)
                            .find(|line| !line.is_empty())
                    } else {
                        Some(description)
                    }
                });
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

    if let Some(tools) = value.get("tools").and_then(Value::as_array) {
        render_tools(stdout, tools.iter(), true)
    } else {
        let tools = listed_tools(value).map_err(std::io::Error::other)?;
        render_tools(stdout, tools, false)
    }
}

fn project_tools(
    mut value: Value,
    filter: Option<&str>,
    summary: bool,
    budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<Value>> {
    let _ = listed_tools(&value)?;
    let mut projection_lease = budget.empty_lease();
    let needle = filter.map(str::to_lowercase);
    let mut selected = Vec::new();
    let Value::Array(pages) = value["pages"].take() else {
        unreachable!("validated pages")
    };
    for mut page in pages {
        let Value::Array(tools) = page["tools"].take() else {
            unreachable!("validated tools")
        };
        for tool in tools {
            let mut matched = needle.is_none();
            if let Some(needle) = &needle {
                for field in ["name", "description"] {
                    if let Some(text) = tool.get(field).and_then(Value::as_str) {
                        // Cover UTF-8 lowercasing and its allocation growth before copying.
                        let _scratch = budget
                            .try_acquire(text.len().saturating_mul(4))
                            .ok_or_else(|| {
                                Failure::new(
                                    1,
                                    "memory_limit_exceeded",
                                    "Tool filtering exceeds the active memory limit",
                                )
                            })?;
                        if text.to_lowercase().contains(needle) {
                            matched = true;
                            break;
                        }
                    }
                }
            }
            if matched {
                let tool = if summary {
                    let mut description_lease =
                        budget.try_acquire(2 * 4 * 257).ok_or_else(|| {
                            Failure::new(
                                1,
                                "memory_limit_exceeded",
                                "Tool summary exceeds the active memory limit",
                            )
                        })?;
                    let tool = discovery::summarize_tool(tool);
                    let bytes = match tool.get("description") {
                        Some(Value::String(description)) => description.capacity(),
                        _ => 0,
                    };
                    description_lease.shrink_to(bytes);
                    projection_lease.merge(description_lease);
                    tool
                } else {
                    tool
                };
                selected.push(tool);
            }
        }
    }
    let mut output = Map::new();
    output.insert("tools".into(), Value::Array(selected));
    Ok(Leased::new(Value::Object(output), projection_lease))
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
        ResourceText {
            uri: &'a str,
            mime: Option<&'a str>,
            text: &'a str,
        },
        ResourceLink {
            name: Option<&'a str>,
            mime: Option<&'a str>,
            uri: &'a str,
        },
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
            } else if block["type"] == "resource"
                && let Some(res) = block.get("resource").and_then(Value::as_object)
                && res.get("blob").is_none_or(Value::is_null)
                && let Some(text) = res.get("text").and_then(Value::as_str)
            {
                let uri = res.get("uri").and_then(Value::as_str).unwrap_or("");
                let mime = res
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .filter(|m| !m.is_empty());
                if uri.is_empty() && text.is_empty() {
                    None
                } else {
                    Some(Part::ResourceText { uri, mime, text })
                }
            } else if block["type"] == "resource_link"
                && let Some(uri) = block
                    .get("uri")
                    .and_then(Value::as_str)
                    .filter(|u| !u.is_empty())
            {
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|n| !n.is_empty());
                let mime = block
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .filter(|m| !m.is_empty());
                Some(Part::ResourceLink { name, mime, uri })
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
            Part::ResourceText { uri, mime, text } => {
                if !uri.is_empty() {
                    stdout.write_all(b"Resource ")?;
                    if let Some(mime) = mime {
                        stdout.write_all(b"[")?;
                        write_safe_text(stdout, mime)?;
                        stdout.write_all(b"] ")?;
                    }
                    write_safe_text(stdout, uri)?;
                    if !text.is_empty() {
                        stdout.write_all(b":\n")?;
                        text
                    } else {
                        ""
                    }
                } else {
                    text
                }
            }
            Part::ResourceLink { name, mime, uri } => {
                stdout.write_all(b"Resource link")?;
                if let Some(mime) = mime {
                    stdout.write_all(b" [")?;
                    write_safe_text(stdout, mime)?;
                    stdout.write_all(b"]")?;
                }
                if let Some(name) = name {
                    stdout.write_all(b" ")?;
                    write_safe_text(stdout, name)?;
                }
                stdout.write_all(b": ")?;
                uri
            }
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

async fn run(
    cli: &Cli,
    uncertain: &AtomicBool,
    interrupt: Option<&tokio_util::sync::CancellationToken>,
) -> Result<Output> {
    if matches!(
        &cli.command,
        Command::Skills {
            command: SkillCommand::Builtin
        }
    ) {
        return Ok(Output::with_text(
            json!({
                "name": discovery::BUILTIN_SKILL_NAME,
                "version": discovery::BUILTIN_SKILL_VERSION,
                "content": discovery::BUILTIN_SKILL_MD,
            }),
            discovery::BUILTIN_SKILL_MD.to_owned(),
        ));
    }
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
        Command::Config {
            command: ConfigCommand::Validate,
        } => {
            if loaded.config.agents.is_some() {
                agents::load(loaded.config.agents.as_deref())
                    .map_err(|error| usage("agents", format!("{error:#}")))?;
            }
            let skill_catalog = if loaded.config.skills.is_empty() {
                skills::SkillCatalog::default()
            } else {
                skills::discover(&loaded.config.skills)
                    .map_err(|error| usage("skills", format!("{error:#}")))?
            };
            let servers = loaded.config.servers.len();
            let enabled_servers = loaded
                .config
                .servers
                .values()
                .filter(|server| server.enabled())
                .count();
            let skills_count = skill_catalog.skills.len();
            let mut lines = vec![
                "valid: true".to_owned(),
                format!("servers: {servers} ({enabled_servers} enabled)"),
                format!("skills: {skills_count}"),
            ];
            for warning in &skill_catalog.warnings {
                lines.push(format!("warning: {}", safe_text(warning)));
            }
            Ok(Output::with_text(
                json!({
                    "valid": true,
                    "servers": servers,
                    "enabledServers": enabled_servers,
                    "skills": skills_count,
                    "warnings": skill_catalog.warnings,
                }),
                lines.join("\n"),
            ))
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
                None,
                uncertain,
                interrupt,
            )
            .await?;
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
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Tools {
                        server: server.clone(),
                    },
                    ByteBudget::global().empty_lease(),
                ),
                None,
                uncertain,
                interrupt,
            )
            .await?;
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
            let remote = remote(
                cli,
                &loaded,
                Leased::new(
                    Operation::Tools {
                        server: server.clone(),
                    },
                    ByteBudget::global().empty_lease(),
                ),
                None,
                uncertain,
                interrupt,
            )
            .await?;
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
                arguments(
                    args,
                    args_file,
                    *stdin,
                    Duration::from_secs(timeout_secs),
                    ByteBudget::global(),
                )
                .await?
            };
            let (mut input_lease, args) = {
                let (args, lease) = args.into_parts();
                (lease, args)
            };
            if let Some(lease) = schema_lease {
                input_lease.merge(lease);
            }
            let remote = remote(
                cli,
                &loaded,
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
                let remote = remote(
                    cli,
                    &loaded,
                    Leased::new(
                        Operation::TasksGet {
                            server: server.clone(),
                            task_id: task_id.clone(),
                            wait: *wait,
                            inline_media: *inline_media,
                        },
                        ByteBudget::global().empty_lease(),
                    ),
                    if *wait { *max_timeout } else { None },
                    uncertain,
                    interrupt,
                )
                .await?;
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
                let server_config =
                    loaded.config.servers.get(server).ok_or_else(|| {
                        usage("unknown_server", format!("unknown server {server:?}"))
                    })?;
                if !server_config.enabled() {
                    return Err(usage(
                        "server_disabled",
                        format!("server {server:?} is disabled"),
                    ));
                }
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
                let remote = remote(
                    cli,
                    &loaded,
                    Leased::new(
                        Operation::TasksUpdate {
                            server: server.clone(),
                            task_id: task_id.clone(),
                            input_responses,
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
            TasksCommand::Cancel { server, task_id } => {
                validate_task_id(task_id)?;
                let remote = remote(
                    cli,
                    &loaded,
                    Leased::new(
                        Operation::TasksCancel {
                            server: server.clone(),
                            task_id: task_id.clone(),
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
        },
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
                None,
                uncertain,
                interrupt,
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
            let remote_server = match command {
                SkillCommand::List { server }
                | SkillCommand::Show { server, .. }
                | SkillCommand::Files { server, .. }
                | SkillCommand::Read { server, .. } => server.as_deref(),
                SkillCommand::Builtin => unreachable!("handled before config load"),
            };
            if let Some(server) = remote_server {
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
                let remote = remote(
                    cli,
                    &loaded,
                    Leased::new(op, ByteBudget::global().empty_lease()),
                    None,
                    uncertain,
                    interrupt,
                )
                .await?;
                return Ok(remote.into_output(|value| Ok(Output::json(value))));
            }
            match command {
                SkillCommand::Files { dir: Some(_), .. } => {
                    return Err(usage("usage", "--dir requires --server"));
                }
                SkillCommand::Read {
                    inline_media: true, ..
                } => {
                    return Err(usage("usage", "--inline-media requires --server"));
                }
                _ => {}
            }
            let catalog = skills::discover(&loaded.config.skills)
                .map_err(|e| usage("skills", format!("{e:#}")))?;
            for warning in &catalog.warnings {
                eprintln!("mcpcli: {}", safe_text(warning));
            }
            if matches!(command, SkillCommand::List { .. }) {
                let text = catalog
                    .skills
                    .iter()
                    .map(|s| format!("{}\t{}", s.name, brief(&s.description)))
                    .collect::<Vec<_>>()
                    .join("\n");
                return Ok(Output::with_text(json!(catalog), text));
            }
            let name = match command {
                SkillCommand::Show { name, .. }
                | SkillCommand::Files { name, .. }
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
                    let local_path = PathBuf::from(path);
                    let content = skills::read(skill, &local_path)
                        .map_err(|e| usage("skill_read", format!("{e:#}")))?;
                    Ok(Output::with_text(
                        json!({"name":name,"root":skill.root,"path":local_path,"content":content}),
                        content,
                    ))
                }
                _ => unreachable!(),
            }
        }
        Command::Events { command } => match command {
            EventsCommand::List { server } => {
                let remote = remote(
                    cli,
                    &loaded,
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
                let server_config =
                    loaded.config.servers.get(server).ok_or_else(|| {
                        usage("unknown_server", format!("unknown server {server:?}"))
                    })?;
                if !server_config.enabled() {
                    return Err(usage(
                        "server_disabled",
                        format!("server {server:?} is disabled"),
                    ));
                }
                cli.validate_request_timeout()?;
                let timeout_secs = cli.timeout_secs_for(server_config);
                let (input_lease, parsed_args) = if args.is_some() || args_file.is_some() || *stdin
                {
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
                    &loaded,
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
        },
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
                let cfg =
                    loaded.config.servers.get(server).ok_or_else(|| {
                        usage("unknown_server", format!("unknown server {server:?}"))
                    })?;
                if !cfg.enabled() {
                    return Err(usage(
                        "server_disabled",
                        format!("server {server:?} is disabled"),
                    ));
                }
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
                notify_daemon_reset(cli, &loaded, server, deadline).await;
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
                    notify_daemon_reset(cli, &loaded, server, deadline).await;
                }
                Ok(Output::with_text(value, text))
            }
        },
        Command::Daemon { .. } => unreachable!(),
    }
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
        }
        | Command::Tasks {
            command: TasksCommand::Update {
                args: Some(args), ..
            },
        }
        | Command::Events {
            command:
                EventsCommand::Poll {
                    args: Some(args), ..
                }
                | EventsCommand::Stream {
                    args: Some(args), ..
                },
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
        Command::Call { kv_args, .. } if !kv_args.is_empty() => {
            let mut total_len = 0usize;
            let mut total_cap = 0usize;
            for item in kv_args {
                total_len = total_len.saturating_add(item.len());
                total_cap = total_cap.saturating_add(item.capacity());
            }
            if total_len > ipc::ARGUMENT_LIMIT {
                return Err(Failure::new(
                    2,
                    "arguments",
                    "arguments exceed the 1 MiB limit",
                ));
            }
            total_len.saturating_add(total_cap)
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

async fn run_events_stream(
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
        run(&cli, &uncertain, None).await
    } else if is_event_stream {
        let cancel = tokio_util::sync::CancellationToken::new();
        let stream_fut = run_events_stream(&cli, cancel.clone());
        tokio::pin!(stream_fut);
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
        let run_fut = run(&cli, &uncertain, Some(&cancel));
        tokio::pin!(run_fut);
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
            result = run(&cli, &uncertain, None) => result,
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
    fn tool_text_renders_inline_text_resources_and_resource_links_without_omitted_warning() {
        let payload = json!({
            "content": [
                {"type": "text", "text": "Header"},
                {"type": "resource", "resource": {
                    "uri": "file:///notes/todo.txt",
                    "mimeType": "text/plain",
                    "text": "line 1\r\nline 2"
                }},
                {"type": "resource_link", "uri": "https://example.test/spec.md", "name": "spec.md", "mimeType": "text/markdown"}
            ]
        });
        assert_eq!(
            tool_text(&payload, true),
            "Header\nResource [text/plain] file:///notes/todo.txt:\nline 1\nline 2\nResource link [text/markdown] spec.md: https://example.test/spec.md\n"
        );

        let auth_login = Cli::try_parse_from([
            "mcpcli",
            "auth",
            "login",
            "remote",
            "--scope",
            "mcp:read",
            "--scope",
            "mcp:write",
            "--client-id",
            "my-client",
            "--callback-port",
            "8765",
            "--callback-path",
            "/oauth/cb",
            "--client-name",
            "my-cli",
            "--no-browser",
        ])
        .unwrap();
        assert!(matches!(
            auth_login.command,
            Command::Auth {
                command: AuthCommand::Login {
                    ref server,
                    ref scopes,
                    ref client_id,
                    callback_port: Some(8765),
                    ref callback_path,
                    ref client_name,
                    no_browser: true,
                }
            } if server == "remote"
                && scopes == &["mcp:read", "mcp:write"]
                && client_id.as_deref() == Some("my-client")
                && callback_path.as_deref() == Some("/oauth/cb")
                && client_name.as_deref() == Some("my-cli")
        ));
        assert!(
            Cli::try_parse_from(["mcpcli", "auth", "login", "remote", "--callback-port", "0"])
                .is_err()
        );
        assert!(Cli::try_parse_from(["mcpcli", "auth", "status"]).is_ok());
        assert!(Cli::try_parse_from(["mcpcli", "auth", "status", "remote"]).is_ok());
        assert!(Cli::try_parse_from(["mcpcli", "auth", "logout", "remote"]).is_ok());
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
        let parsed = Cli::try_parse_from([
            "mcpcli",
            "call",
            "srv",
            "tool",
            "--inline-media",
            "--max-timeout",
            "300",
        ])
        .unwrap();
        assert!(!parsed.has_explicit_timeout());
        assert!(matches!(
            parsed.command,
            Command::Call {
                inline_media: true,
                max_timeout: Some(300),
                ..
            }
        ));
        assert!(Cli::try_parse_from(["mcpcli", "config", "validate"]).is_ok());
        let tools_cmd =
            Cli::try_parse_from(["mcpcli", "tools", "srv", "--filter", "search", "--summary"])
                .unwrap();
        assert!(matches!(
            tools_cmd.command,
            Command::Tools {
                ref server,
                ref filter,
                summary: true,
            } if server == "srv" && filter.as_deref() == Some("search")
        ));
    }

    #[test]
    fn validate_call_max_timeout_enforces_bounds_and_order() {
        assert_eq!(validate_call_max_timeout(60, None).unwrap(), None);
        assert_eq!(
            validate_call_max_timeout(60, Some(60)).unwrap(),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            validate_call_max_timeout(60, Some(86_400)).unwrap(),
            Some(Duration::from_secs(86_400))
        );
        let too_small = validate_call_max_timeout(60, Some(59))
            .unwrap_err()
            .downcast::<Failure>()
            .unwrap();
        assert_eq!(too_small.code, 2);
        assert_eq!(too_small.kind, "invalid_timeout");
        let too_large = validate_call_max_timeout(60, Some(86_401))
            .unwrap_err()
            .downcast::<Failure>()
            .unwrap();
        assert_eq!(too_large.code, 2);
        assert_eq!(too_large.kind, "invalid_timeout");
    }

    #[test]
    fn write_tools_text_renders_flattened_tools_array() {
        let flat = json!({
            "tools": [
                {"name": "alpha", "description": "First line"},
                {"name": "beta"}
            ]
        });
        assert_eq!(tools_text(&flat).unwrap(), "alpha\nFirst line\n\nbeta\n");
    }

    #[test]
    fn tools_projection_moves_payloads_and_retains_new_description_lease() {
        for summary in [false, true] {
            let value = json!({"pages":[{"tools":[{
                "name":"sample", "description":"First line\nSecond line",
                "inputSchema":{"type":"object", "description":"s".repeat(64 << 10)},
                "annotations":{"note":"a".repeat(64 << 10)}
            }]}]});
            let annotation = value["pages"][0]["tools"][0]["annotations"]["note"]
                .as_str()
                .unwrap()
                .as_ptr();
            let schema = value["pages"][0]["tools"][0]["inputSchema"]["description"]
                .as_str()
                .unwrap()
                .as_ptr();
            let bytes = input::serialized_len(&value).unwrap();
            let budget = ByteBudget::new(bytes + 2 * 4 * 257);
            let source_lease = budget.try_acquire(bytes).unwrap();
            let remote = RemoteOutput {
                value,
                connection: None,
                inflight: None,
                lease: Some(source_lease),
            };
            let output = remote.into_output(|value| {
                let (value, lease) =
                    project_tools(value, Some("SAMPLE"), summary, &budget)?.into_parts();
                Ok(Output::json(value).with_lease(Some(lease)))
            });
            assert_eq!(output.code, 0);
            let selected = &output.value.as_ref().unwrap()["tools"][0];
            assert_eq!(
                selected["annotations"]["note"].as_str().unwrap().as_ptr(),
                annotation
            );
            if summary {
                assert_eq!(selected["description"], "First line");
                assert!(selected.get("inputSchema").is_none());
                assert!(budget.used() > bytes);
            } else {
                assert_eq!(
                    selected["inputSchema"]["description"]
                        .as_str()
                        .unwrap()
                        .as_ptr(),
                    schema
                );
                assert_eq!(selected["description"], "First line\nSecond line");
                assert_eq!(budget.used(), bytes);
            }
            assert_eq!(output.lease.as_ref().unwrap().bytes(), budget.used());
            drop(output);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn filtered_text_uses_first_nonempty_line_without_changing_json() {
        let flat =
            json!({"tools":[{"name":"sample", "description":"\n First line \nSecond line"}]});
        assert_eq!(tools_text(&flat).unwrap(), "sample\nFirst line\n");
        assert_eq!(
            flat["tools"][0]["description"],
            "\n First line \nSecond line"
        );
        let pages = json!({"pages":[{"tools":flat["tools"].clone()}]});
        assert_eq!(
            tools_text(&pages).unwrap(),
            "sample\n\n First line \nSecond line\n"
        );
    }

    #[test]
    fn catalog_includes_enabled_flag_and_marks_disabled_in_text() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("mcpcli.json");
        std::fs::write(
            &cfg_path,
            serde_json::to_string(&json!({
                "version": 1,
                "servers": {
                    "active": {
                        "transport": "stdio",
                        "command": "echo",
                        "description": "Active server",
                        "timeout": 15
                    },
                    "off": {
                        "transport": "stdio",
                        "command": "~/nonexistent/bin",
                        "cwd": "/definitely/missing/dir",
                        "env": {"TOKEN": "${MISSING_SECRET_VAR}"},
                        "enabled": false,
                        "description": "Disabled server"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let loaded = LoadedConfig::load(&cfg_path).unwrap();
        let out = catalog(&loaded, None).unwrap();
        let servers = out.value.as_ref().unwrap()["servers"].as_array().unwrap();
        assert_eq!(servers.len(), 2);
        let active = servers.iter().find(|s| s["name"] == "active").unwrap();
        let off = servers.iter().find(|s| s["name"] == "off").unwrap();
        assert_eq!(active["enabled"], true);
        assert_eq!(off["enabled"], false);
        let text = out.text.unwrap();
        assert!(text.contains("server active\tActive server"));
        assert!(text.contains("server off [disabled]\tDisabled server"));

        let cli = Cli::try_parse_from(["mcpcli", "tools", "active"]).unwrap();
        assert_eq!(cli.timeout_secs_for(&loaded.config.servers["active"]), 15);
        let cli_override =
            Cli::try_parse_from(["mcpcli", "--timeout", "9", "tools", "active"]).unwrap();
        assert_eq!(
            cli_override.timeout_secs_for(&loaded.config.servers["active"]),
            9
        );
    }

    #[test]
    fn events_cli_commands_parse_all_flags_and_reject_conflicts() {
        let list = Cli::try_parse_from(["mcpcli", "events", "list", "github"]).unwrap();
        assert!(matches!(
            list.command,
            Command::Events {
                command: EventsCommand::List { ref server }
            } if server == "github"
        ));

        let poll = Cli::try_parse_from([
            "mcpcli",
            "events",
            "poll",
            "github",
            "pr.merged",
            "--args",
            r#"{"repo":"org/app"}"#,
            "--cursor",
            "1:5",
            "--max-age-ms",
            "60000",
            "--max-events",
            "10",
        ])
        .unwrap();
        assert!(matches!(
            poll.command,
            Command::Events {
                command: EventsCommand::Poll {
                    ref server,
                    ref event,
                    args: Some(_),
                    cursor: Some(_),
                    max_age_ms: Some(60_000),
                    max_events: Some(10),
                    ..
                }
            } if server == "github" && event == "pr.merged"
        ));

        let stream = Cli::try_parse_from([
            "mcpcli",
            "--oneshot",
            "events",
            "stream",
            "github",
            "pr.merged",
            "--args",
            r#"{"repo":"org/app"}"#,
            "--mode",
            "poll",
            "--cursor-file",
            "/tmp/pr.cursor",
            "--max-age-ms",
            "120000",
            "--heartbeat-ms",
            "15000",
            "--once",
            "--timeout",
            "300000",
            "--exec-mode",
            "stream",
            "--exec-timeout",
            "5000",
            "--",
            "fastexec",
            "daemon",
            "emit",
            "--stream",
        ])
        .unwrap();
        assert!(stream.oneshot);
        assert_eq!(stream.timeout, Some(300_000));
        assert!(matches!(
            stream.command,
            Command::Events {
                command: EventsCommand::Stream {
                    ref server,
                    ref event,
                    mode: CliStreamMode::Poll,
                    cursor_file: Some(_),
                    max_age_ms: Some(120_000),
                    heartbeat_ms: Some(15_000),
                    once: true,
                    exec_mode: CliExecMode::Stream,
                    exec_timeout: Some(5_000),
                    ref callback,
                    ..
                }
            } if server == "github" && event == "pr.merged" && callback == &["fastexec", "daemon", "emit", "--stream"]
        ));

        // --cursor and --cursor-file must conflict.
        assert!(
            Cli::try_parse_from([
                "mcpcli",
                "events",
                "stream",
                "github",
                "pr.merged",
                "--cursor",
                "1:1",
                "--cursor-file",
                "/tmp/c",
            ])
            .is_err()
        );
    }

    #[test]
    fn sub_millisecond_dispatch_budget_returns_exit_5_timeout() {
        let err = check_dispatch_budgets(
            Duration::from_millis(1000),
            None,
            Duration::from_micros(999_500),
        )
        .unwrap_err();
        assert_eq!(err.code, 5);
        assert_eq!(err.kind, "timeout");

        let hard_err = check_dispatch_budgets(
            Duration::from_secs(5),
            Some(Duration::from_millis(1000)),
            Duration::from_micros(999_999),
        )
        .unwrap_err();
        assert_eq!(hard_err.code, 5);
        assert_eq!(hard_err.kind, "timeout");

        let zero_err = check_dispatch_budgets(Duration::from_secs(1), None, Duration::from_secs(2))
            .unwrap_err();
        assert_eq!(zero_err.code, 5);
        assert_eq!(zero_err.kind, "timeout");

        let (rem, rem_hard) = check_dispatch_budgets(
            Duration::from_millis(1000),
            Some(Duration::from_millis(2000)),
            Duration::from_millis(999),
        )
        .unwrap();
        assert_eq!(rem, Duration::from_millis(1));
        assert_eq!(rem_hard, Some(Duration::from_millis(1001)));
    }
}
