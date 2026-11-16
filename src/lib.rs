#[cfg(not(target_os = "linux"))]
compile_error!("mcpcli 0.1.0 supports Linux only");

pub mod config;
pub mod daemon;
pub mod discovery;
pub mod ipc;
pub mod json;
pub mod mcp;
pub mod runtime;
pub mod skills;
