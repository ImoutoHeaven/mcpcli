use anyhow::Result;
use clap::{Parser, Subcommand};
use mcpcli::{
    config::{LoadedConfig, ServerConfig},
    events_exec::ExecMode,
    events_stream::StreamMode,
    ipc::Failure,
    schema::SchemaMode,
};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(
    version,
    about = "Call MCP tools and read global AGENTS.md and local Agent Skills",
    after_help = "Agent skill guide: mcpcli --text skills builtin",
    disable_help_subcommand = true
)]
pub(crate) struct Cli {
    #[arg(
        short,
        long,
        global = true,
        env = "MCPCLI_CONFIG",
        default_value = "mcpcli.json"
    )]
    pub(crate) config: PathBuf,
    #[arg(long, global = true, env = "MCPCLI_SESSION", default_value = "default")]
    pub(crate) session: String,
    #[arg(
        long,
        global = true,
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Request timeout in seconds (1-3600; server default or 60); call inactivity timeout; events stream total milliseconds"
    )]
    pub(crate) timeout: Option<u64>,
    #[arg(
        long,
        global = true,
        help = "Open and close a fresh connection for this command"
    )]
    pub(crate) oneshot: bool,
    #[arg(
        long,
        global = true,
        help = "Print readable text instead of the default JSON"
    )]
    pub(crate) text: bool,
    #[command(subcommand)]
    pub(crate) command: Command,
}

impl Cli {
    pub(crate) fn timeout_secs_for(&self, server: &ServerConfig) -> u64 {
        self.timeout
            .or_else(|| server.timeout())
            .unwrap_or(60)
            .min(3600)
    }

    pub(crate) fn validate_request_timeout(&self) -> Result<()> {
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

    pub(crate) fn has_explicit_timeout(&self) -> bool {
        self.timeout.is_some()
    }

    pub(crate) fn uses_dual_clock(&self) -> bool {
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
pub(crate) enum CliSchemaMode {
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

pub(crate) fn validate_call_max_timeout(
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

#[derive(Parser)]
#[command(
    ignore_errors = true,
    disable_help_flag = true,
    disable_version_flag = true,
    disable_help_subcommand = true
)]
pub(crate) struct TopLevelHelpProbe {
    #[arg(short, long, env = "MCPCLI_CONFIG", default_value = "mcpcli.json")]
    pub(crate) config: PathBuf,
    #[arg(long = "session")]
    _session: Option<std::ffi::OsString>,
    #[arg(long = "timeout")]
    _timeout: Option<std::ffi::OsString>,
    #[arg(long = "oneshot", action = clap::ArgAction::Count)]
    _oneshot: u8,
    #[arg(long = "text", action = clap::ArgAction::Count)]
    _text: u8,
    #[arg(short = 'h', long = "help", action = clap::ArgAction::Count)]
    pub(crate) help: u8,
}

#[derive(Subcommand)]
pub(crate) enum Command {
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
pub(crate) enum ConfigCommand {
    /// Validate configuration, AGENTS.md, and skills offline without starting a daemon.
    Validate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub(crate) enum CliStreamMode {
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
pub(crate) enum CliExecMode {
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
pub(crate) enum EventsCommand {
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
pub(crate) enum TasksCommand {
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
pub(crate) enum SkillCommand {
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
pub(crate) enum AuthCommand {
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
pub(crate) enum DaemonCommand {
    Start,
    /// Run in the foreground for a process supervisor.
    Run,
    Status,
    Stop,
    /// Stop, discard all session state, and start with current config/environment.
    Restart,
}

pub(crate) fn usage(kind: &str, error: impl std::fmt::Display) -> anyhow::Error {
    Failure::new(2, kind, error.to_string()).into()
}

pub(crate) fn load(cli: &Cli) -> Result<LoadedConfig> {
    LoadedConfig::load(&cli.config).map_err(|error| usage("configuration", format!("{error:#}")))
}
