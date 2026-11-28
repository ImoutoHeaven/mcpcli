//! MCP lifecycle and transports. Tool calls are sent once, never replayed.
mod connection;
mod events;
mod events_client;
mod http;
mod ingress;
pub mod skills;
mod sse;
mod stdio;
pub mod tasks;
mod types;

#[cfg(test)]
mod tests;

pub use connection::McpConnection;
pub use events::{
    EVENTS_EXTENSION_ID, EventNotification, EventStreamHandle, EventStreamReceiver,
    MAX_EVENT_NOTIFICATION_BYTES, MAX_EVENT_PAGES, PollEventsParams, StreamEventsParams,
};
pub use events_client::McpEventsClient;
pub use ingress::SKILL_RESOURCE_MAX_FRAME_BYTES;
pub use rmcp::model::{SKILLS_EXTENSION_ID, TASKS_EXTENSION_ID};
pub use types::{
    CallBudgets, CallOptions, CallOutcome, DEFAULT_STDERR_TAIL_BYTES, FailureKind, MAX_CONNECT,
    MAX_RESPONSE_BYTES, MAX_TOOL_CATALOG_BYTES, McpFailure, McpLimits, McpOpError,
};
