pub(crate) mod args;
pub(crate) mod events;
pub(crate) mod input;
mod offline;
pub(crate) mod output;
mod remote;
#[cfg(test)]
mod tests;

use anyhow::Result;
use args::{Cli, Command, ConfigCommand, SkillCommand, load};
use output::Output;
use std::sync::atomic::AtomicBool;

pub(crate) async fn run(
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
        return Ok(offline::builtin_skill());
    }
    if let Command::Daemon { command } = &cli.command {
        return Box::pin(remote::run_daemon(cli, command)).await;
    }
    let loaded = load(cli)?;
    match &cli.command {
        Command::Catalog => offline::catalog(&loaded, None),
        Command::Agents => offline::run_agents(&loaded),
        Command::Search { query } => offline::run_search(&loaded, query),
        Command::Config {
            command: ConfigCommand::Validate,
        } => offline::run_config_validate(&loaded),
        Command::Events { command } => {
            Box::pin(events::run_events(
                cli, &loaded, command, uncertain, interrupt,
            ))
            .await
        }
        Command::Server { .. }
        | Command::Tools { .. }
        | Command::Inspect { .. }
        | Command::Call { .. }
        | Command::Tasks { .. }
        | Command::Reset { .. }
        | Command::Skills { .. }
        | Command::Auth { .. } => {
            Box::pin(remote::run_online(cli, &loaded, uncertain, interrupt)).await
        }
        Command::Daemon { .. } => unreachable!(),
    }
}
