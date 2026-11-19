//! Observe bounded response bodies without replacing the SDK transport.
use super::MAX_RESPONSE_BYTES;
use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
use serde::Deserialize;
use serde_json::Value;
use sse_stream::Sse;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

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
    response: Option<anyhow::Result<Value>>,
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

    fn awaiting_response(&self) -> bool {
        self.0
            .lock()
            .expect("response observation mutex")
            .pending
            .as_ref()
            .is_some_and(|pending| pending.response.is_none())
    }

    pub(super) fn observe(&self, bytes: &[u8]) {
        if bytes.len() > MAX_RESPONSE_BYTES || !self.awaiting_response() {
            return;
        }
        #[derive(Deserialize)]
        struct EnvelopeId {
            id: Value,
        }
        let Ok(probe) = serde_json::from_slice::<EnvelopeId>(bytes) else {
            return;
        };
        let matches_pending =
            |pending: &Pending| pending.id == probe.id && pending.response.is_none();
        if !self
            .0
            .lock()
            .expect("response observation mutex")
            .pending
            .as_ref()
            .is_some_and(matches_pending)
        {
            return;
        }
        // The SDK must accept this as a response before it can satisfy the pending request.
        if !matches!(
            serde_json::from_slice::<ServerJsonRpcMessage>(bytes),
            Ok(ServerJsonRpcMessage::Response(_) | ServerJsonRpcMessage::Error(_))
        ) {
            return;
        }
        let response = crate::json::parse(bytes);
        if let Some(pending) = self
            .0
            .lock()
            .expect("response observation mutex")
            .pending
            .as_mut()
            && matches_pending(pending)
        {
            pending.response = Some(response);
        }
    }

    pub(super) fn observe_sse(&self, sse: &Sse) {
        if let Some(data) = &sse.data {
            self.observe(data.as_bytes());
        }
    }
    pub(super) fn take(&self) -> Option<(&'static str, anyhow::Result<Value>)> {
        let mut pending = self
            .0
            .lock()
            .expect("response observation mutex")
            .pending
            .take()?;
        Some((pending.method, pending.response.take()?))
    }
}

pub(super) struct ObservedRead<R> {
    reader: R,
    ingress: Ingress,
    line: Vec<u8>,
    discarding: bool,
}
impl<R> ObservedRead<R> {
    pub(super) fn new(reader: R, ingress: Ingress) -> Self {
        Self {
            reader,
            ingress,
            line: Vec::new(),
            discarding: false,
        }
    }
}
impl<R: AsyncRead + Unpin> AsyncRead for ObservedRead<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let start = buf.filled().len();
        let result = Pin::new(&mut self.reader).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            for segment in buf.filled()[start..].split_inclusive(|byte| *byte == b'\n') {
                if (self.line.is_empty() && !self.ingress.awaiting_response())
                    || self.line.len().saturating_add(segment.len()) > MAX_RESPONSE_BYTES
                {
                    self.line = Vec::new();
                    self.discarding = true;
                }
                if !self.discarding {
                    self.line.extend_from_slice(segment);
                }
                if segment.last() == Some(&b'\n') {
                    if !self.discarding {
                        // AsyncRwTransport ignores a UTF-8 BOM on complete lines.
                        self.ingress.observe(
                            self.line
                                .strip_prefix(b"\xef\xbb\xbf")
                                .unwrap_or(&self.line),
                        );
                    }
                    // A quiet persistent connection must not retain a large response buffer.
                    if self.line.capacity() > 64 * 1024 {
                        self.line = Vec::new();
                    } else {
                        self.line.clear();
                    }
                    self.discarding = false;
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn request(id: u64) -> ClientJsonRpcMessage {
        serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/list"}))
            .unwrap()
    }

    #[tokio::test]
    async fn tracks_only_matching_complete_lines_without_changing_the_stream() {
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        ingress.track(&request(2));
        let (mut writer, reader) = tokio::io::duplex(1024);
        let mut reader = ObservedRead::new(reader, ingress.clone());
        let frames=b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[]}}\r\n\xef\xbb\xbf{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[],\"unknown\":null}}\r\n";
        writer.write_all(frames).await.unwrap();
        drop(writer);
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, frames);
        let (method, response) = ingress.take().unwrap();
        assert_eq!(method, "tools/list");
        assert_eq!(
            response.unwrap()["result"].get("unknown"),
            Some(&Value::Null)
        );
    }

    #[test]
    fn discards_inactive_and_rejects_lossy_metadata() {
        let ingress = Ingress::default();
        {
            let _capture = ingress.capture().unwrap();
            ingress.track(&request(2));
            ingress.observe(br#"{"jsonrpc":"2.0","id":2,"result":{"tools":[],"x":1,"x":2}}"#);
            assert!(ingress.take().unwrap().1.is_err());
        }
        ingress.track(&request(3));
        ingress.observe(br#"{"jsonrpc":"2.0","id":3,"result":{"tools":[]}}"#);
        assert!(ingress.take().is_none());
    }
}
