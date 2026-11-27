#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    windows
)))]
compile_error!("mcpcli supports Linux, FreeBSD, macOS and Windows");

pub mod agents;
pub mod auth;
pub mod budget;
pub mod config;
pub mod daemon;
pub mod discovery;
pub mod events_exec;
pub mod events_stream;
pub mod ipc;
pub mod json;
pub mod mcp;
pub mod media;
pub mod oauth_url;
pub mod runtime;
pub mod schema;
pub mod skills;

pub mod platform;
