//! Capture verified raw JSON-RPC responses alongside dedicated byte leases.
use crate::budget::{ByteBudget, ByteLease, Leased};
use rmcp::model::{
    ClientJsonRpcMessage, ErrorCode, ErrorData, RequestId, ServerJsonRpcMessage, ServerResult,
};
use serde::Deserialize;
use serde_json::Value;
use sse_stream::Sse;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(super) enum IngressError {
    MemoryExhausted,
    InvalidMetadata,
}

pub(super) enum ObserveOutcome {
    NotMatched,
    InvalidMessage(serde_json::Error, bool),
    Captured(Leased<Box<ServerJsonRpcMessage>>),
}

/// End local accounting only at the return into the official SDK transport API.
/// The SDK does not expose ownership hooks for its later queues or clones.
pub(super) fn into_sdk(message: Leased<Box<ServerJsonRpcMessage>>) -> ServerJsonRpcMessage {
    let (message, lease) = message.into_parts();
    let message = *message;
    drop(lease);
    message
}

#[derive(Clone, Default)]
pub(super) struct Ingress(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    active: bool,
    pending: Option<Pending>,
}

struct Pending {
    method: &'static str,
    id: RequestId,
    response: Option<(anyhow::Result<Value>, ByteLease)>,
}

impl Ingress {
    pub(super) fn track(&self, message: &ClientJsonRpcMessage) {
        let ClientJsonRpcMessage::Request(request) = message else {
            return;
        };
        let method = match request.request.method() {
            "initialize" => "initialize",
            "server/discover" => "server/discover",
            "tools/list" => "tools/list",
            "tools/call" => "tools/call",
            _ => return,
        };
        let mut state = self.0.lock().expect("response observation mutex");
        if state.active {
            state.pending = Some(Pending {
                method,
                id: request.id.clone(),
                response: None,
            });
        }
    }

    pub(super) fn capture(&self) -> Option<impl Drop + use<>> {
        struct Clear(Ingress);
        impl Drop for Clear {
            fn drop(&mut self) {
                let mut state = self.0.0.lock().expect("response observation mutex");
                state.active = false;
                state.pending = None;
            }
        }
        let mut state = self.0.lock().expect("response observation mutex");
        if state.active {
            return None;
        }
        state.active = true;
        Some(Clear(self.clone()))
    }

    #[cfg(test)]
    pub(super) fn observe_bytes(
        &self,
        bytes: &[u8],
        limit: usize,
        budget: &Arc<ByteBudget>,
        error_only: bool,
    ) -> Result<ObserveOutcome, IngressError> {
        let lease = budget
            .try_acquire(bytes.len())
            .ok_or(IngressError::MemoryExhausted)?;
        self.observe_owned(
            Leased::new(bytes.to_vec(), lease),
            limit,
            budget,
            error_only,
        )
    }

    pub(super) fn observe_owned(
        &self,
        raw: Leased<Vec<u8>>,
        limit: usize,
        budget: &Arc<ByteBudget>,
        error_only: bool,
    ) -> Result<ObserveOutcome, IngressError> {
        let wire_len = raw.len();
        if wire_len > limit {
            return Ok(ObserveOutcome::NotMatched);
        }
        let value_lease = budget
            .try_acquire(wire_len)
            .ok_or(IngressError::MemoryExhausted)?;
        let scratch = budget
            .try_acquire(crate::json::scratch_bytes(&raw))
            .ok_or(IngressError::MemoryExhausted)?;
        let response = match crate::json::parse(&raw) {
            Ok(value) => value,
            Err(error) => {
                let fallback = match serde_json::from_slice::<Value>(&raw) {
                    Ok(value) => value,
                    Err(error) => return Ok(ObserveOutcome::InvalidMessage(error, false)),
                };
                let correlated = (fallback.get("error").is_some()
                    || (!error_only && fallback.get("result").is_some()))
                    && self
                        .0
                        .lock()
                        .expect("response observation mutex")
                        .pending
                        .as_ref()
                        .is_some_and(|pending| {
                            pending.response.is_none()
                                && match &pending.id {
                                    RequestId::Number(id) => {
                                        fallback.get("id").and_then(Value::as_i64) == Some(*id)
                                    }
                                    RequestId::String(id) => {
                                        fallback.get("id").and_then(Value::as_str)
                                            == Some(id.as_ref())
                                    }
                                }
                        });
                if correlated {
                    return Err(IngressError::InvalidMetadata);
                }
                let ignore = ignore_notification(&fallback);
                return Ok(ObserveOutcome::InvalidMessage(
                    <serde_json::Error as serde::de::Error>::custom(error),
                    ignore,
                ));
            }
        };
        drop(scratch);
        drop(raw);
        let response = Leased::new(response, value_lease);
        // &Value yields borrowed strings to Serde's untagged/flatten buffers.
        // The official SDK may create temporary owned strings while converting
        // protocol IDs or validating subscription metadata. Reserve two additional
        // payload-sized copies for that synchronous conversion only.
        let _sdk_scratch = budget
            .try_acquire(wire_len.saturating_mul(2))
            .ok_or(IngressError::MemoryExhausted)?;
        let sdk_lease = budget
            .try_acquire(wire_len)
            .ok_or(IngressError::MemoryExhausted)?;
        let mut message = match ServerJsonRpcMessage::deserialize(&*response) {
            Ok(message) => message,
            Err(error) => {
                return Ok(ObserveOutcome::InvalidMessage(
                    error,
                    ignore_notification(&response),
                ));
            }
        };
        let id = match &mut message {
            ServerJsonRpcMessage::Request(_) | ServerJsonRpcMessage::Notification(_)
                if !error_only =>
            {
                return Ok(ObserveOutcome::Captured(Leased::new(
                    Box::new(message),
                    sdk_lease,
                )));
            }
            ServerJsonRpcMessage::Response(response) if !error_only => &response.id,
            ServerJsonRpcMessage::Error(error) => {
                let Some(id) = error.id.as_ref() else {
                    return Ok(ObserveOutcome::NotMatched);
                };
                id
            }
            _ => return Ok(ObserveOutcome::NotMatched),
        };
        let pending_method = {
            let state = self.0.lock().expect("response observation mutex");
            match state.pending.as_ref() {
                Some(pending) if &pending.id == id && pending.response.is_none() => pending.method,
                _ => return Ok(ObserveOutcome::NotMatched),
            }
        };
        let request_id = id.clone();
        match &mut message {
            ServerJsonRpcMessage::Response(response) => {
                match (pending_method, &mut response.result) {
                    ("tools/call", ServerResult::CallToolResult(result)) => {
                        result.content = Vec::new();
                        result.structured_content = None;
                        result.meta = None;
                    }
                    ("initialize", ServerResult::InitializeResult(_))
                    | ("server/discover", ServerResult::DiscoverResult(_))
                    | ("tools/list", ServerResult::ListToolsResult(_)) => {}
                    ("tools/call", ServerResult::InputRequiredResult(_)) => {
                        message = ServerJsonRpcMessage::error(
                            ErrorData::new(
                                ErrorCode::INVALID_REQUEST,
                                "The tool requires unsupported interactive input; side effects may have occurred",
                                None,
                            ),
                            Some(response.id.clone()),
                        );
                    }
                    ("tools/call", ServerResult::CreateTaskResult(_)) => {
                        message = ServerJsonRpcMessage::error(
                            ErrorData::new(
                                ErrorCode::INVALID_REQUEST,
                                "The server returned an unsupported asynchronous task; side effects may have occurred",
                                None,
                            ),
                            Some(response.id.clone()),
                        );
                    }
                    _ => response.result = ServerResult::empty(()),
                }
            }
            ServerJsonRpcMessage::Error(error) => {
                error.error.data = if error.error.code == ErrorCode::UNSUPPORTED_PROTOCOL_VERSION {
                    negotiation_data(error.error.data.take())
                } else {
                    None
                };
                if error.error.message.len() > 1024 {
                    let mut end = 1024;
                    while !error.error.message.is_char_boundary(end) {
                        end -= 1;
                    }
                    error.error.message = format!("{}…", &error.error.message[..end]).into();
                }
            }
            _ => unreachable!("only correlated responses reach capture"),
        }
        let mut state = self.0.lock().expect("response observation mutex");
        if let Some(pending) = state.pending.as_mut()
            && pending.id == request_id
            && pending.method == pending_method
            && pending.response.is_none()
        {
            let (response, value_lease) = response.into_parts();
            pending.response = Some((Ok(response), value_lease));
            return Ok(ObserveOutcome::Captured(Leased::new(
                Box::new(message),
                sdk_lease,
            )));
        }
        Ok(ObserveOutcome::NotMatched)
    }

    pub(super) fn observe_sse(
        &self,
        sse: &mut Sse,
        event_lease: &mut ByteLease,
        limit: usize,
        budget: &Arc<ByteBudget>,
    ) -> Result<(), IngressError> {
        if !matches!(sse.event.as_deref(), None | Some("") | Some("message")) {
            return Ok(());
        }
        // JSON-RPC messages are objects. The SDK ignores other SSE data events.
        if !sse
            .data
            .as_ref()
            .is_some_and(|data| data.trim_start().starts_with('{'))
        {
            return Ok(());
        }
        let data = sse.data.take().expect("checked data");
        let lease = event_lease
            .split(data.capacity())
            .expect("SSE data capacity is charged");
        let observed =
            self.observe_owned(Leased::new(data.into_bytes(), lease), limit, budget, false)?;
        if let ObserveOutcome::Captured(message) = observed {
            // The official SDK accepts raw SSE events. Encode the already
            // validated message while this application still owns its payload.
            // Matched call/error responses are small stubs; lossless data stays
            // in the capture. The SDK's subsequent parse/queue is outside this
            // local owner's lifetime.
            let (bytes, lease) = crate::ipc::encode_json_leased(&**message, limit, budget)
                .map_err(|_| IngressError::MemoryExhausted)?;
            sse.data = Some(String::from_utf8(bytes).expect("JSON is UTF-8"));
            event_lease.merge(lease);
        }
        Ok(())
    }

    pub(super) fn take(&self) -> Option<(&'static str, anyhow::Result<Value>, ByteLease)> {
        let mut pending = self
            .0
            .lock()
            .expect("response observation mutex")
            .pending
            .take()?;
        let (response, lease) = pending.response.take()?;
        Some((pending.method, response, lease))
    }
}

fn ignore_notification(value: &Value) -> bool {
    value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| super::should_ignore_notification(value, method))
}

fn negotiation_data(data: Option<Value>) -> Option<Value> {
    let supported = data.as_ref()?.get("supported")?.as_array()?;
    if supported.iter().any(|version| !version.is_string()) {
        return None;
    }
    let versions = super::PREFERRED_PROTOCOL_VERSIONS
        .iter()
        .filter(|version| {
            supported
                .iter()
                .any(|value| value.as_str() == Some(version.as_str()))
        })
        .map(|version| Value::String(version.as_str().to_owned()))
        .collect();
    let mut projected = serde_json::Map::new();
    projected.insert("supported".into(), Value::Array(versions));
    Some(Value::Object(projected))
}

#[cfg(test)]
mod tests {
    use super::super::sse::sse_retained_capacity;
    use super::*;

    fn request(id: u64, method: &str) -> ClientJsonRpcMessage {
        serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": {"name": "demo"}
        }))
        .unwrap()
    }

    #[test]
    fn escaped_text_reserves_scratch_before_parse_and_releases_raw_before_sdk() {
        for text in ["x".repeat(64 << 10), "x".repeat(64 << 10) + "\n"] {
            let raw = serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":text}]}})).unwrap();
            let len = raw.len();
            let scratch = crate::json::scratch_bytes(&raw);
            let peak = (raw.capacity() + len + scratch).max(4 * len);
            for fits in [false, true] {
                let budget = ByteBudget::new(peak - usize::from(!fits));
                let ingress = Ingress::default();
                let _capture = ingress.capture().unwrap();
                ingress.track(&request(2, "tools/call"));
                let lease = budget.try_acquire(raw.capacity()).unwrap();
                let mut owned = Vec::with_capacity(raw.capacity());
                owned.extend_from_slice(&raw);
                let observed =
                    ingress.observe_owned(Leased::new(owned, lease), 1 << 20, &budget, false);
                if fits {
                    assert!(matches!(observed, Ok(ObserveOutcome::Captured(_))));
                    assert_eq!(budget.used(), 2 * len);
                    drop(observed);
                    let (_, result, lease) = ingress.take().unwrap();
                    let result = result.unwrap();
                    assert_eq!(result["result"]["content"][0]["text"], text);
                    assert_eq!(budget.used(), len);
                    drop(result);
                    drop(lease);
                } else {
                    assert!(matches!(observed, Err(IngressError::MemoryExhausted)));
                }
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[test]
    fn ordinary_decimal_tail_zeros_reserve_float_workspace() {
        let source = br#"{"jsonrpc":"2.0","id":2,"result":{"content":[],"value":0.1000000000000000000000000000000000000000}}"#;
        let peak = (2 * source.len() + crate::json::scratch_bytes(source)).max(4 * source.len());
        assert!(peak > 2 * source.len());
        for fits in [false, true] {
            let budget = ByteBudget::new(peak - usize::from(!fits));
            let ingress = Ingress::default();
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(2, "tools/call"));
            let lease = budget.try_acquire(source.len()).unwrap();
            let result =
                ingress.observe_owned(Leased::new(source.to_vec(), lease), 4096, &budget, false);
            if fits {
                assert!(matches!(result, Ok(ObserveOutcome::Captured(_))));
                drop(result);
                let (_, value, lease) = ingress.take().unwrap();
                let value = value.unwrap();
                assert_eq!(value["result"]["value"], serde_json::json!(0.1));
                drop(value);
                drop(lease);
            } else {
                assert!(matches!(result, Err(IngressError::MemoryExhausted)));
            }
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn sdk_string_id_conversion_reserves_its_temporary_workspace() {
        for token in [false, true] {
            let id = "ordinary-".to_owned() + &"x".repeat(4096);
            let raw = if token {
                serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":id,"progress":1}})).unwrap()
            } else {
                serde_json::to_vec(
                    &serde_json::json!({"jsonrpc":"2.0","id":id,"method":"local/unsupported"}),
                )
                .unwrap()
            };
            let budget = ByteBudget::new((raw.capacity() + raw.len()).max(4 * raw.len()));
            let len = raw.len();
            let lease = budget.try_acquire(raw.capacity()).unwrap();
            let outcome = Ingress::default()
                .observe_owned(Leased::new(raw, lease), 1 << 20, &budget, false)
                .unwrap();
            assert!(matches!(&outcome, ObserveOutcome::Captured(_)));
            assert_eq!(budget.used(), len);
            drop(outcome);
            assert_eq!(budget.used(), 0);
        }
        for (raw, valid) in [
            ("1", true),
            ("1.0", true),
            ("1e2", true),
            ("-0.0", true),
            ("1.5", false),
            ("18446744073709551615", false),
            ("9007199254740992.0", false),
        ] {
            assert_eq!(
                serde_json::from_str::<RequestId>(raw).is_ok(),
                valid,
                "{raw}"
            );
        }
    }

    #[test]
    fn metadata_messages_stay_charged_in_the_application_queue() {
        for (method, result) in [
            (
                "initialize",
                serde_json::json!({
                    "protocolVersion":"2025-11-25", "capabilities":{},
                    "serverInfo":{"name":"sample","version":"1"}, "instructions":"ordinary"
                }),
            ),
            (
                "tools/list",
                serde_json::json!({
                    "tools":[{"name":"sample", "inputSchema":{"type":"object"}}],
                    "nextCursor":"next-page"
                }),
            ),
        ] {
            let raw = serde_json::to_vec(&serde_json::json!({
                "jsonrpc":"2.0", "id":12, "result":result
            }))
            .unwrap();
            let budget = ByteBudget::new(8 * raw.len());
            let ingress = Ingress::default();
            let capture = ingress.capture().unwrap();
            ingress.track(&request(12, method));
            let ObserveOutcome::Captured(message) =
                ingress.observe_bytes(&raw, 4096, &budget, false).unwrap()
            else {
                panic!("expected response")
            };
            assert_eq!(budget.used(), 2 * raw.len());
            drop(capture);
            assert_eq!(budget.used(), raw.len());
            let mut queue = std::collections::VecDeque::from([message]);
            let message = queue.pop_front().unwrap();
            drop(queue);
            assert!(matches!(&**message, ServerJsonRpcMessage::Response(_)));
            assert_eq!(budget.used(), raw.len());
            drop(message);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn error_stub_preserves_sdk_version_negotiation() {
        let preferred = super::super::PREFERRED_PROTOCOL_VERSIONS[0].as_str();
        for supported in [
            serde_json::json!([preferred, "unknown", preferred]),
            serde_json::json!(["unknown"]),
            serde_json::json!([preferred, 1]),
        ] {
            let budget = ByteBudget::new(1 << 20);
            let ingress = Ingress::default();
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(12, "server/discover"));
            let raw = serde_json::to_vec(&serde_json::json!({
                "jsonrpc":"2.0", "id":12, "error": {
                    "code":-32022, "message":"retry", "data": {
                        "supported": supported, "extra":"x".repeat(64 << 10)
                    }
                }
            }))
            .unwrap();
            let ObserveOutcome::Captured(message) = ingress
                .observe_bytes(&raw, 1 << 20, &budget, false)
                .unwrap()
            else {
                panic!("expected captured version error")
            };
            let ServerJsonRpcMessage::Error(sdk_error) = &**message else {
                panic!("expected error")
            };
            let projected = &sdk_error.error.data;
            let original: Option<Vec<rmcp::model::ProtocolVersion>> =
                serde_json::from_value(supported.clone()).ok();
            let expected: Vec<_> = super::super::PREFERRED_PROTOCOL_VERSIONS
                .iter()
                .filter(|version| original.as_ref().is_some_and(|all| all.contains(version)))
                .map(|version| version.as_str())
                .collect();
            let actual: Vec<&str> = projected
                .as_ref()
                .and_then(|data| data.get("supported"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|value| value.as_str().unwrap())
                .collect();
            assert_eq!(actual, expected);
            assert!(serde_json::to_vec(&projected).unwrap().len() < 128);
            let (_, value, lease) = ingress.take().unwrap();
            let value = value.unwrap();
            assert_eq!(value["error"]["data"]["supported"], supported);
            assert_eq!(
                value["error"]["data"]["extra"].as_str().unwrap().len(),
                64 << 10
            );
            drop(value);
            drop(lease);
            drop(message);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn error_sse_stubs_sdk_copies_but_retains_lossless_error_and_lease() {
        let budget = ByteBudget::new(1 << 20);
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        ingress.track(&request(12, "tools/call"));
        let raw = serde_json::to_string(&serde_json::json!({
            "jsonrpc":"2.0", "id":12, "error": {
                "code":-32603, "message":"é".repeat(32 << 10),
                "data":{"text":"x".repeat(128 << 10)}, "extra":null
            }
        }))
        .unwrap();
        let value_bytes = raw.len();
        let mut lease = budget.try_acquire(raw.capacity()).unwrap();
        let mut event = Sse {
            data: Some(raw),
            ..Sse::default()
        };
        ingress
            .observe_sse(&mut event, &mut lease, 1 << 20, &budget)
            .unwrap();
        let sdk: ServerJsonRpcMessage =
            serde_json::from_str(event.data.as_deref().unwrap()).unwrap();
        let ServerJsonRpcMessage::Error(sdk) = sdk else {
            panic!("expected error")
        };
        assert!(sdk.error.data.is_none());
        assert!(sdk.error.message.len() <= 1027);
        assert_eq!(lease.bytes(), sse_retained_capacity(&event));
        assert_eq!(budget.used(), value_bytes + lease.bytes());
        let (_, value, value_lease) = ingress.take().unwrap();
        let value = value.unwrap();
        assert_eq!(value["error"]["message"].as_str().unwrap().len(), 64 << 10);
        assert_eq!(
            value["error"]["data"]["text"].as_str().unwrap().len(),
            128 << 10
        );
        assert_eq!(value["error"].get("extra"), Some(&Value::Null));
        drop(value);
        drop(value_lease);
        drop(event);
        drop(lease);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn charges_overlap_window_stubs_tool_call_and_rejects_lossy_metadata() {
        let budget = ByteBudget::new(1024);
        let ingress = Ingress::default();
        {
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(2, "tools/call"));
            let line = br#"{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"payload"}],"structuredContent":{"big":"data"},"_meta":{"m":1},"unknown":null}}"#;
            let raw_lease = budget.try_acquire(line.len()).unwrap();
            let outcome = ingress
                .observe_bytes(line, 1024, &budget, false)
                .expect("application decode workspace fits");
            // The external raw bytes, local typed message and lossless capture
            // remain independently charged.
            assert_eq!(budget.used(), line.len() * 3);
            drop(raw_lease);
            assert_eq!(budget.used(), line.len() * 2);

            let ObserveOutcome::Captured(boxed) = outcome else {
                panic!("expected captured response");
            };
            let ServerJsonRpcMessage::Response(stub) = &**boxed else {
                panic!("expected captured response");
            };
            let ServerResult::CallToolResult(call_stub) = &stub.result else {
                panic!("expected CallToolResult stub");
            };
            assert!(call_stub.content.is_empty());
            assert!(call_stub.structured_content.is_none());
            assert!(call_stub.meta.is_none());

            let (method, response, handed) = ingress.take().unwrap();
            assert_eq!(method, "tools/call");
            let response = response.unwrap();
            assert_eq!(response["result"].get("unknown"), Some(&Value::Null));
            assert_eq!(response["result"]["content"][0]["text"], "payload");
            drop(response);
            drop(handed);
            drop(boxed);
            assert_eq!(budget.used(), 0);
        }
        {
            // Overlap exhaustion fails cleanly without leaking when budget < 2 * line.len().
            let tight = ByteBudget::new(100);
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(7, "tools/list"));
            let line = br#"{"jsonrpc":"2.0","id":7,"result":{"tools":[],"pad":"0123456789"}}"#;
            let raw_lease = tight.try_acquire(line.len()).unwrap();
            assert!(ingress.observe_bytes(line, 1024, &tight, false).is_err());
            assert_eq!(tight.used(), line.len());
            drop(raw_lease);
            assert_eq!(tight.used(), 0);
        }
        {
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(2, "tools/list"));
            let lossy = br#"{"jsonrpc":"2.0","id":2,"result":{"tools":[],"x":1,"x":2}}"#;
            let raw_lease = budget.try_acquire(lossy.len()).unwrap();
            assert!(matches!(
                ingress.observe_bytes(lossy, 1024, &budget, false),
                Err(IngressError::InvalidMetadata)
            ));
            drop(raw_lease);
            assert!(ingress.take().is_none());
            assert_eq!(budget.used(), 0);
        }
        ingress.track(&request(3, "tools/list"));
        let line = br#"{"jsonrpc":"2.0","id":3,"result":{"tools":[]}}"#;
        let raw_lease = budget.try_acquire(line.len()).unwrap();
        assert!(matches!(
            ingress.observe_bytes(line, 1024, &budget, false),
            Ok(ObserveOutcome::NotMatched)
        ));
        assert!(ingress.take().is_none());
        drop(raw_lease);
        assert_eq!(budget.used(), 0);
    }
}
