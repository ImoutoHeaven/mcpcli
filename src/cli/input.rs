use super::args::{Cli, Command, EventsCommand, TasksCommand, usage};
use anyhow::{Context, Result};
use mcpcli::{
    budget::{ByteBudget, Leased},
    ipc::{self, Failure},
    json as input,
    schema::{ExpectedSchema, MAX_SCHEMA_DEPTH, SchemaMode},
};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::timeout;

fn argument_memory_failure() -> anyhow::Error {
    Failure::new(
        1,
        "memory_limit_exceeded",
        "Tool arguments exceed the active memory limit",
    )
    .into()
}

pub(super) async fn read_argument_bytes(
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

pub(super) async fn arguments(
    args: &Option<String>,
    file: &Option<PathBuf>,
    stdin: bool,
    budget: Duration,
    process_budget: &std::sync::Arc<ByteBudget>,
) -> Result<Leased<Map<String, Value>>> {
    arguments_cancellable(args, file, stdin, budget, process_budget, None).await
}

pub(super) async fn arguments_cancellable(
    args: &Option<String>,
    file: &Option<PathBuf>,
    stdin: bool,
    budget: Duration,
    process_budget: &std::sync::Arc<ByteBudget>,
    interrupt: Option<&tokio_util::sync::CancellationToken>,
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
        let read_fut = timeout(
            budget,
            read_argument_bytes(tokio::io::stdin(), process_budget),
        );
        if let Some(token) = interrupt {
            tokio::select! {
                biased;
                () = token.cancelled() => {
                    return Err(Failure::new(
                        130,
                        "cancelled",
                        "interrupted while reading JSON arguments from stdin",
                    )
                    .into());
                }
                res = read_fut => res
                    .map_err(|_| Failure::new(5, "timeout", "timed out reading JSON arguments from stdin"))??,
            }
        } else {
            read_fut.await.map_err(|_| {
                Failure::new(5, "timeout", "timed out reading JSON arguments from stdin")
            })??
        }
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

pub(super) fn validate_task_id(task_id: &str) -> Result<()> {
    if task_id.is_empty() || task_id.len() > 512 || task_id.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(usage(
            "invalid_task_id",
            "task_id must be 1..=512 bytes without ASCII control characters",
        ));
    }
    Ok(())
}

pub(super) async fn load_expected_schema(
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

pub(super) fn parse_kv_cli_arguments(
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

pub(crate) fn retain_cli_input(
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
