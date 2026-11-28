//! Local MCP daemon managing persistent server connections and bounded IPC dispatch.
mod dispatch;
mod error;
mod state;
mod transport;

#[cfg(test)]
mod tests;

pub use error::{mcp_error, mcp_error_for_server, mcp_op_error};
pub use state::DaemonOptions;
pub use transport::{run, run_with_options};
