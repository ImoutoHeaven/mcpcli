//! Capture verified raw JSON-RPC responses alongside dedicated byte leases.
use crate::budget::{ByteBudget, ByteLease};
use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage, ServerResult};
use serde::Deserialize;
use serde_json::Value;
use sse_stream::Sse;
use std::sync::{Arc, Mutex};

use super::sse::sse_retained_len;

#[derive(Debug)]
pub(super) struct IngressMemoryExhausted;

pub(super) enum ObserveOutcome {
    NotMatched,
    InvalidMessage(serde_json::Error),
    Captured(Box<ServerJsonRpcMessage>),
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
    id: Value,
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
        let Ok(id) = serde_json::to_value(&request.id) else {
            return;
        };
        let mut state = self.0.lock().expect("response observation mutex");
        if state.active {
            state.pending = Some(Pending {
                method,
                id,
                response: None,
            });
        }
    }

    pub(super) fn capture(&self) -> Option<impl Drop + use<>> {
        struct Clear(Ingress);
        impl Drop for Clear {
            fn drop(&mut self) {
                let mut state = self.0.0.lock().expect("response observation mutex");
                *state = State::default();
            }
        }
        let mut state = self.0.lock().expect("response observation mutex");
        if state.active {
            return None;
        }
        state.active = true;
        Some(Clear(self.clone()))
    }

    pub(super) fn awaiting_response(&self) -> bool {
        self.0
            .lock()
            .expect("response observation mutex")
            .pending
            .as_ref()
            .is_some_and(|pending| pending.response.is_none())
    }

    pub(super) fn observe_bytes(
        &self,
        bytes: &[u8],
        limit: usize,
        budget: &Arc<ByteBudget>,
        error_only: bool,
    ) -> Result<ObserveOutcome, IngressMemoryExhausted> {
        if bytes.len() > limit || !self.awaiting_response() {
            return Ok(ObserveOutcome::NotMatched);
        }
        #[derive(Deserialize)]
        struct EnvelopeId {
            id: Value,
        }
        let Ok(probe) = serde_json::from_slice::<EnvelopeId>(bytes) else {
            return Ok(ObserveOutcome::NotMatched);
        };
        let pending_method = {
            let state = self.0.lock().expect("response observation mutex");
            match state.pending.as_ref() {
                Some(pending) if pending.id == probe.id && pending.response.is_none() => {
                    pending.method
                }
                _ => return Ok(ObserveOutcome::NotMatched),
            }
        };

        // Account for the lossless Value allocation while the caller's raw buffer is still live.
        let value_lease = budget
            .try_acquire(bytes.len())
            .ok_or(IngressMemoryExhausted)?;

        let mut message = match serde_json::from_slice::<ServerJsonRpcMessage>(bytes) {
            Ok(msg) => msg,
            Err(error) => return Ok(ObserveOutcome::InvalidMessage(error)),
        };
        let accepted = match &mut message {
            ServerJsonRpcMessage::Response(resp) if !error_only => {
                // Free SDK payload copies immediately once ContentBlock and variant validation pass.
                if pending_method == "tools/call"
                    && let ServerResult::CallToolResult(call_res) = &mut resp.result
                {
                    call_res.content = Vec::new();
                    call_res.structured_content = None;
                    call_res.meta = None;
                }
                true
            }
            ServerJsonRpcMessage::Error(_) => true,
            _ => false,
        };
        if !accepted {
            return Ok(ObserveOutcome::NotMatched);
        }

        let response = crate::json::parse(bytes);
        let mut state = self.0.lock().expect("response observation mutex");
        if let Some(pending) = state.pending.as_mut()
            && pending.id == probe.id
            && pending.response.is_none()
        {
            pending.response = Some((response, value_lease));
            return Ok(ObserveOutcome::Captured(Box::new(message)));
        }
        Ok(ObserveOutcome::NotMatched)
    }

    pub(super) fn observe_sse(
        &self,
        sse: &mut Sse,
        event_lease: &mut ByteLease,
        limit: usize,
        budget: &Arc<ByteBudget>,
    ) -> Result<(), IngressMemoryExhausted> {
        let Some(data) = sse.data.as_deref() else {
            return Ok(());
        };
        if data.trim().is_empty() {
            return Ok(());
        }
        if let ObserveOutcome::Captured(message) =
            self.observe_bytes(data.as_bytes(), limit, budget, false)?
            && let ServerJsonRpcMessage::Response(ref resp) = *message
            && matches!(resp.result, ServerResult::CallToolResult(_))
        {
            let stub = serde_json::to_string(&message).expect("CallToolResult stub serializes");
            sse.data = Some(stub);
            event_lease.shrink_to(sse_retained_len(sse));
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

#[cfg(test)]
mod tests {
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
    fn charges_overlap_window_stubs_tool_call_and_rejects_lossy_metadata() {
        let budget = ByteBudget::new(512);
        let ingress = Ingress::default();
        {
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(2, "tools/call"));
            let line = br#"{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"payload"}],"structuredContent":{"big":"data"},"_meta":{"m":1},"unknown":null}}"#;
            let raw_lease = budget.try_acquire(line.len()).unwrap();
            let outcome = ingress
                .observe_bytes(line, 1024, &budget, false)
                .expect("overlap fits in 2x budget");
            // Both raw_lease and value_lease are accounted while raw buffer is still live.
            assert_eq!(budget.used(), line.len() * 2);
            drop(raw_lease);
            assert_eq!(budget.used(), line.len());

            let ObserveOutcome::Captured(boxed) = outcome else {
                panic!("expected captured response");
            };
            let ServerJsonRpcMessage::Response(stub) = *boxed else {
                panic!("expected captured response");
            };
            let ServerResult::CallToolResult(call_stub) = stub.result else {
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
            drop(handed);
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
                Ok(ObserveOutcome::Captured(_))
            ));
            drop(raw_lease);
            let (_, result, handed) = ingress.take().unwrap();
            assert!(result.is_err());
            drop(handed);
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
