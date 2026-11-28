//! Daemon error classification and mapping from MCP failures to IPC failures.
use crate::ipc::Failure;
use crate::mcp::{FailureKind, McpFailure, McpOpError};
use serde_json::Value;

pub fn mcp_op_error(error: McpOpError, server: Option<&str>) -> Failure {
    let message = if let Some(auth) = &error.auth {
        let mut msg = auth.format_message(server);
        if error.outcome_unknown {
            msg.push_str("; outcome unknown, the call was not retried");
        }
        msg
    } else {
        error.message
    };
    let mut failure =
        Failure::new(error.code, &error.kind, message).uncertain(error.outcome_unknown);
    failure.rpc_error = error.rpc_error;
    let mut data_map = match error.data.map(|b| *b) {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    if let Some(auth) = &error.auth
        && let Value::Object(auth_map) = auth.to_data_value(server)
    {
        for (k, v) in auth_map {
            data_map.entry(k).or_insert(v);
        }
    }
    if let Some(stderr_tail) = error.stderr_tail {
        data_map
            .entry("stderr_tail")
            .or_insert_with(|| Value::String(stderr_tail.into_string()));
    }
    if !data_map.is_empty() {
        failure.data = Some(Value::Object(data_map));
    }
    failure.lease = error.lease;
    failure
}

pub fn mcp_error(error: anyhow::Error, calling: bool) -> Failure {
    mcp_error_for_server(error, calling, None)
}

pub fn mcp_error_for_server(error: anyhow::Error, calling: bool, server: Option<&str>) -> Failure {
    if let Ok(error) = error.downcast::<McpFailure>() {
        let (code, kind) = match error.kind {
            FailureKind::Timeout => (5, "timeout"),
            FailureKind::Cancelled => (4, "cancelled"),
            FailureKind::Protocol => (4, "mcp_protocol"),
            FailureKind::Transport => (4, "mcp_transport"),
            FailureKind::AuthRequired => (4, "auth_required"),
            FailureKind::InsufficientScope => (4, "insufficient_scope"),
        };
        let message = if let Some(auth) = &error.auth {
            let mut msg = auth.format_message(server);
            if error.outcome_unknown {
                msg.push_str("; outcome unknown, the call was not retried");
            }
            msg
        } else {
            error.message
        };
        let mut failure = Failure::new(code, kind, message).uncertain(error.outcome_unknown);
        failure.rpc_error = error.rpc_error;
        let mut data_map = match error.auth.as_ref().map(|auth| auth.to_data_value(server)) {
            Some(Value::Object(map)) => map,
            _ => serde_json::Map::new(),
        };
        if let Some(stderr_tail) = error.stderr_tail {
            data_map.insert("stderr_tail".into(), Value::String(stderr_tail));
        }
        if !data_map.is_empty() {
            failure.data = Some(Value::Object(data_map));
        }
        failure.lease = error.lease;
        failure
    } else {
        Failure::new(4, "mcp_error", "MCP operation failed").uncertain(calling)
    }
}
