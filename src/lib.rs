#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    windows
)))]
compile_error!("mcpcli supports Linux, FreeBSD, macOS and Windows");

pub mod config;
pub mod daemon;
pub mod discovery;
pub mod ipc;
pub mod json;
pub mod mcp;
pub mod runtime;
pub mod skills;

pub mod platform;
