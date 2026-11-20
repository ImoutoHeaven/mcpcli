//! Custom per-event bounded SSE parser integrated with the process byte budget.
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::{Stream, StreamExt, stream::BoxStream};
use rmcp::transport::streamable_http_client::SseError;
use sse_stream::Sse;

use super::{FailureKind, TransportTermination, ingress::Ingress};
use crate::budget::{ByteBudget, ByteLease, format_bytes};

const LINE_SHRINK_CAP: usize = 64 * 1024;

pub(super) fn sse_retained_len(sse: &Sse) -> usize {
    sse.event.as_ref().map_or(0, String::len)
        + sse.data.as_ref().map_or(0, String::len)
        + sse.id.as_ref().map_or(0, String::len)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BomState {
    Start,
    SawEf,
    SawEfBb,
    Done,
}

fn strip_bom_prefix<'a>(
    bom: &mut BomState,
    bytes: &'a [u8],
    fallback: &mut [u8; 2],
) -> (&'a [u8], usize) {
    let mut idx = 0usize;
    while idx < bytes.len() && *bom != BomState::Done {
        match *bom {
            BomState::Start => {
                if bytes[idx] == 0xEF {
                    *bom = BomState::SawEf;
                    idx += 1;
                } else {
                    *bom = BomState::Done;
                }
            }
            BomState::SawEf => {
                if bytes[idx] == 0xBB {
                    *bom = BomState::SawEfBb;
                    idx += 1;
                } else {
                    *bom = BomState::Done;
                    fallback[0] = 0xEF;
                    return (&bytes[idx..], 1);
                }
            }
            BomState::SawEfBb => {
                if bytes[idx] == 0xBF {
                    *bom = BomState::Done;
                    idx += 1;
                } else {
                    *bom = BomState::Done;
                    fallback[0] = 0xEF;
                    fallback[1] = 0xBB;
                    return (&bytes[idx..], 2);
                }
            }
            BomState::Done => break,
        }
    }
    (&bytes[idx..], 0)
}

struct BoundedSseStream<S> {
    source: S,
    limit: usize,
    budget: Arc<ByteBudget>,
    ingress: Ingress,
    termination: Arc<TransportTermination>,
    bom: BomState,
    skip_leading_lf: bool,
    line_buf: Vec<u8>,
    line_lease: ByteLease,
    current: Option<Sse>,
    current_raw_bytes: usize,
    current_lease: ByteLease,
    parsed: VecDeque<(Sse, ByteLease)>,
    yielded_lease: ByteLease,
    done: bool,
}

impl<S> BoundedSseStream<S> {
    fn new(
        source: S,
        limit: usize,
        budget: Arc<ByteBudget>,
        ingress: Ingress,
        termination: Arc<TransportTermination>,
    ) -> Self {
        let line_lease = budget.empty_lease();
        let current_lease = budget.empty_lease();
        let yielded_lease = budget.empty_lease();
        Self {
            source,
            limit,
            budget,
            ingress,
            termination,
            bom: BomState::Start,
            skip_leading_lf: false,
            line_buf: Vec::new(),
            line_lease,
            current: None,
            current_raw_bytes: 0,
            current_lease,
            parsed: VecDeque::new(),
            yielded_lease,
            done: false,
        }
    }

    fn clear_line_buf(&mut self) {
        self.line_buf.clear();
        if self.line_buf.capacity() > LINE_SHRINK_CAP {
            self.line_buf = Vec::new();
        }
        self.line_lease.release();
    }

    fn clear_all(&mut self) {
        self.clear_line_buf();
        self.current = None;
        self.current_raw_bytes = 0;
        self.current_lease.release();
        self.parsed.clear();
        self.yielded_lease.release();
    }

    fn fail_too_large(&mut self) -> SseError {
        self.done = true;
        self.clear_all();
        let msg = format!(
            "SSE event exceeds the {} response limit",
            format_bytes(self.limit)
        );
        self.termination
            .terminate(FailureKind::Transport, msg.clone());
        SseError::Body(Box::new(std::io::Error::other(msg)))
    }

    fn fail_memory_limit(&mut self) -> SseError {
        self.done = true;
        self.clear_all();
        let msg = format!(
            "SSE event exceeded the {} active memory limit",
            format_bytes(self.budget.limit())
        );
        self.termination
            .terminate(FailureKind::Transport, msg.clone());
        SseError::Body(Box::new(std::io::Error::other(msg)))
    }

    fn dispatch_event(&mut self) -> Result<(), SseError> {
        self.current_raw_bytes = 0;
        if let Some(mut sse) = self.current.take() {
            if let Some(data) = sse.data.as_mut()
                && data.capacity() > data.len().saturating_add(LINE_SHRINK_CAP)
            {
                data.shrink_to_fit();
            }
            if let Some(ev) = sse.event.as_mut()
                && ev.capacity() > ev.len().saturating_add(LINE_SHRINK_CAP)
            {
                ev.shrink_to_fit();
            }
            if let Some(id) = sse.id.as_mut()
                && id.capacity() > id.len().saturating_add(LINE_SHRINK_CAP)
            {
                id.shrink_to_fit();
            }
            let mut lease = self.current_lease.take();
            lease.shrink_to(sse_retained_len(&sse));
            if self
                .ingress
                .observe_sse(&mut sse, &mut lease, self.limit, &self.budget)
                .is_err()
            {
                drop(sse);
                drop(lease);
                return Err(self.fail_memory_limit());
            }
            self.parsed.push_back((sse, lease));
        } else {
            self.current_lease.release();
        }
        Ok(())
    }

    fn parse_borrowed_field(&mut self, line: &[u8]) -> Result<(), SseError> {
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return Err(SseError::InvalidLine);
        };
        let field = &line[..colon];
        let value = &line[colon + 1..];
        let value = value.strip_prefix(b" ").unwrap_or(value);

        match field {
            b"data" => {
                let text = std::str::from_utf8(value).map_err(SseError::Utf8Parse)?;
                let has_prev = self.current.as_ref().is_some_and(|ev| ev.data.is_some());
                let grow = text.len() + usize::from(has_prev);
                if !self.current_lease.try_grow(grow) {
                    return Err(self.fail_memory_limit());
                }
                let event = self.current.get_or_insert_default();
                if let Some(existing) = event.data.as_mut() {
                    existing.push('\n');
                    existing.push_str(text);
                } else {
                    event.data = Some(text.to_owned());
                }
            }
            b"event" => {
                let text = std::str::from_utf8(value).map_err(SseError::Utf8Parse)?;
                if self.current.as_ref().is_some_and(|ev| ev.event.is_some()) {
                    return Err(SseError::DuplicatedEventLine);
                }
                if !self.current_lease.try_grow(text.len()) {
                    return Err(self.fail_memory_limit());
                }
                let event = self.current.get_or_insert_default();
                event.event = Some(text.to_owned());
            }
            b"id" => {
                if value.contains(&0u8) {
                    return Ok(());
                }
                let text = std::str::from_utf8(value).map_err(SseError::Utf8Parse)?;
                if self.current.as_ref().is_some_and(|ev| ev.id.is_some()) {
                    return Err(SseError::DuplicatedIdLine);
                }
                if !self.current_lease.try_grow(text.len()) {
                    return Err(self.fail_memory_limit());
                }
                let event = self.current.get_or_insert_default();
                event.id = Some(text.to_owned());
            }
            b"retry" => {
                let retry = std::str::from_utf8(value)
                    .map_err(SseError::Utf8Parse)?
                    .trim_ascii()
                    .parse::<u64>()
                    .map_err(SseError::IntParse)?;
                let event = self.current.get_or_insert_default();
                if event.retry.is_some() {
                    return Err(SseError::DuplicatedRetry);
                }
                event.retry = Some(retry);
            }
            b"" => {}
            _ => return Err(SseError::InvalidLine),
        }
        Ok(())
    }

    fn parse_owned_field(
        &mut self,
        mut line: Vec<u8>,
        mut line_lease: ByteLease,
    ) -> Result<(), SseError> {
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return Err(SseError::InvalidLine);
        };
        if &line[..colon] == b"data" && self.current.as_ref().is_none_or(|ev| ev.data.is_none()) {
            let val_start = if line.get(colon + 1) == Some(&b' ') {
                colon + 2
            } else {
                colon + 1
            };
            std::str::from_utf8(&line[val_start..]).map_err(SseError::Utf8Parse)?;
            line.drain(..val_start);
            if line.capacity() > line.len().saturating_add(LINE_SHRINK_CAP) {
                line.shrink_to_fit();
            }
            let data = String::from_utf8(line).expect("validated UTF-8");
            line_lease.shrink_to(data.len());
            self.current_lease.merge(line_lease);
            let event = self.current.get_or_insert_default();
            event.data = Some(data);
            return Ok(());
        }
        let res = self.parse_borrowed_field(&line);
        drop(line);
        drop(line_lease);
        res
    }

    fn feed_bytes(&mut self, mut bytes: &[u8]) -> Result<(), SseError> {
        if self.skip_leading_lf {
            if bytes.is_empty() {
                return Ok(());
            }
            self.skip_leading_lf = false;
            if bytes[0] == b'\n' {
                bytes = &bytes[1..];
            }
        }

        while !bytes.is_empty() {
            let Some(pos) = bytes.iter().position(|&b| matches!(b, b'\r' | b'\n')) else {
                let next_line_len = self.line_buf.len().saturating_add(bytes.len());
                if self.current_raw_bytes.saturating_add(next_line_len) > self.limit {
                    return Err(self.fail_too_large());
                }
                if !self.line_lease.try_grow(bytes.len()) {
                    return Err(self.fail_memory_limit());
                }
                self.line_buf.extend_from_slice(bytes);
                return Ok(());
            };

            let fragment = &bytes[..pos];
            let delimiter = bytes[pos];
            bytes = &bytes[pos + 1..];
            if delimiter == b'\r' {
                if bytes.first() == Some(&b'\n') {
                    bytes = &bytes[1..];
                } else if bytes.is_empty() {
                    self.skip_leading_lf = true;
                }
            }

            if self.line_buf.is_empty() {
                if fragment.is_empty() {
                    self.dispatch_event()?;
                } else if fragment[0] == b':' {
                    if self.current_raw_bytes.saturating_add(fragment.len()) > self.limit {
                        return Err(self.fail_too_large());
                    }
                    let Some(comment_lease) = self.budget.try_acquire(fragment.len()) else {
                        return Err(self.fail_memory_limit());
                    };
                    drop(comment_lease);
                } else {
                    let next_raw = self
                        .current_raw_bytes
                        .saturating_add(fragment.len())
                        .saturating_add(1);
                    if next_raw > self.limit {
                        return Err(self.fail_too_large());
                    }
                    self.current_raw_bytes = next_raw;
                    self.parse_borrowed_field(fragment)?;
                }
            } else {
                let total_len = self.line_buf.len().saturating_add(fragment.len());
                if self.line_buf[0] == b':' {
                    if self.current_raw_bytes.saturating_add(total_len) > self.limit {
                        return Err(self.fail_too_large());
                    }
                    if !self.line_lease.try_grow(fragment.len()) {
                        return Err(self.fail_memory_limit());
                    }
                    self.clear_line_buf();
                } else {
                    let next_raw = self
                        .current_raw_bytes
                        .saturating_add(total_len)
                        .saturating_add(1);
                    if next_raw > self.limit {
                        return Err(self.fail_too_large());
                    }
                    if !self.line_lease.try_grow(fragment.len()) {
                        return Err(self.fail_memory_limit());
                    }
                    self.line_buf.extend_from_slice(fragment);
                    self.current_raw_bytes = next_raw;
                    let owned_line = std::mem::take(&mut self.line_buf);
                    let owned_lease = self.line_lease.take();
                    self.parse_owned_field(owned_line, owned_lease)?;
                }
            }
        }
        Ok(())
    }
}

impl<S> Drop for BoundedSseStream<S> {
    fn drop(&mut self) {
        self.clear_all();
    }
}

impl<S, B, E> Stream for BoundedSseStream<S>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
{
    type Item = Result<Sse, SseError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        // Release the lease of the previously yielded event now that downstream moved on.
        this.yielded_lease.release();
        if let Some((sse, lease)) = this.parsed.pop_front() {
            this.yielded_lease = lease;
            return Poll::Ready(Some(Ok(sse)));
        }
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.source).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.done = true;
                    let mut fallback = [0u8; 2];
                    let prefix_len = match this.bom {
                        BomState::SawEf => {
                            fallback[0] = 0xEF;
                            1
                        }
                        BomState::SawEfBb => {
                            fallback[0] = 0xEF;
                            fallback[1] = 0xBB;
                            2
                        }
                        BomState::Start | BomState::Done => 0,
                    };
                    this.bom = BomState::Done;
                    if prefix_len > 0
                        && let Err(err) = this.feed_bytes(&fallback[..prefix_len])
                    {
                        this.clear_all();
                        return Poll::Ready(Some(Err(err)));
                    }
                    this.clear_all();
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(_))) => {
                    this.done = true;
                    this.clear_all();
                    return Poll::Ready(Some(Err(SseError::Body(Box::new(
                        std::io::Error::other("HTTP response body failed"),
                    )))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    let raw = chunk.as_ref();
                    let mut fallback = [0u8; 2];
                    let (rest, prefix_len) = strip_bom_prefix(&mut this.bom, raw, &mut fallback);
                    if prefix_len > 0
                        && let Err(err) = this.feed_bytes(&fallback[..prefix_len])
                    {
                        this.done = true;
                        this.clear_all();
                        return Poll::Ready(Some(Err(err)));
                    }
                    if !rest.is_empty()
                        && let Err(err) = this.feed_bytes(rest)
                    {
                        this.done = true;
                        this.clear_all();
                        return Poll::Ready(Some(Err(err)));
                    }
                    if let Some((sse, lease)) = this.parsed.pop_front() {
                        this.yielded_lease = lease;
                        return Poll::Ready(Some(Ok(sse)));
                    }
                }
            }
        }
    }
}

pub(super) fn bounded_stream(
    response: reqwest::Response,
    limit: usize,
    budget: Arc<ByteBudget>,
    ingress: Ingress,
    termination: Arc<TransportTermination>,
) -> BoxStream<'static, Result<Sse, SseError>> {
    BoundedSseStream::new(response.bytes_stream(), limit, budget, ingress, termination).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ClientJsonRpcMessage;

    fn test_stream(
        chunks: Vec<&'static [u8]>,
        limit: usize,
        budget: Arc<ByteBudget>,
        ingress: Ingress,
        termination: Arc<TransportTermination>,
    ) -> BoundedSseStream<impl Stream<Item = Result<&'static [u8], std::io::Error>> + Unpin> {
        BoundedSseStream::new(
            futures_util::stream::iter(chunks.into_iter().map(Ok)),
            limit,
            budget,
            ingress,
            termination,
        )
    }

    #[tokio::test]
    async fn bounds_unfinished_lines_and_retained_data_across_chunks() {
        let budget = ByteBudget::new(1024);
        let term = Arc::new(TransportTermination::default());
        let mut s = test_stream(
            vec![b"data: a\r", b"\ndata: b\n", b"x"],
            16,
            budget.clone(),
            Ingress::default(),
            term.clone(),
        );
        assert!(s.next().await.unwrap().is_err());
        assert!(term.is_terminated());
        assert_eq!(budget.used(), 0);

        let term2 = Arc::new(TransportTermination::default());
        let mut s2 = test_stream(
            vec![b"data: ab", b"c"],
            8,
            budget.clone(),
            Ingress::default(),
            term2.clone(),
        );
        assert!(s2.next().await.unwrap().is_err());
        assert!(term2.is_terminated());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn handles_bom_comment_null_id_all_blank_lines_and_shrinks_buffers() {
        let budget = ByteBudget::new(256 * 1024);
        let term = Arc::new(TransportTermination::default());
        // Split UTF-8 BOM across chunks followed by comment, null-byte id event (ignored),
        // then a >64 KiB multi-chunk line, then a small event: line_buf must shrink <= 64 KiB
        // and unmatched event lease must stay held while yielded!
        let big_payload = "z".repeat(80 * 1024);
        let big_part2: &'static [u8] =
            Box::leak(format!("{big_payload}\n\n").into_bytes().into_boxed_slice());
        let mut s = test_stream(
            vec![
                b"\xEF\xBB",
                b"\xBF: keepalive\r\n\r\n",
                b"id: bad\0id\n\n",
                b"id: good-1\rdata: start-",
                big_part2,
                b"data: after\r\r",
            ],
            128 * 1024,
            budget.clone(),
            Ingress::default(),
            term.clone(),
        );

        let ev1 = s.next().await.unwrap().unwrap();
        assert_eq!(ev1.id.as_deref(), Some("good-1"));
        assert_eq!(ev1.data.as_ref().unwrap().len(), 6 + 80 * 1024);
        assert!(s.line_buf.capacity() <= LINE_SHRINK_CAP);
        // Unmatched SSE event keeps its lease active while downstream holds ev1!
        assert_eq!(budget.used(), sse_retained_len(&ev1));
        drop(ev1);

        let ev2 = s.next().await.unwrap().unwrap();
        assert_eq!(ev2.data.as_deref(), Some("after"));
        // Previous event lease was released on next poll; only ev2 is charged now.
        assert_eq!(budget.used(), 5);
        drop(ev2);

        assert!(s.next().await.is_none());
        assert!(!term.is_terminated());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn stubs_captured_tool_call_sse_data_and_enforces_memory_limit() {
        let budget = ByteBudget::new(200 * 1024);
        let ingress = Ingress::default();
        let _capture = ingress.capture().unwrap();
        let req: ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "big"}
        }))
        .unwrap();
        ingress.track(&req);

        let pad = "p".repeat(70 * 1024);
        let raw_event = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"id\":5,\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{pad}\"}}]}}}}\n\n"
        );
        let leaked: &'static [u8] = Box::leak(raw_event.into_bytes().into_boxed_slice());
        let term = Arc::new(TransportTermination::default());
        let mut s = test_stream(
            vec![leaked],
            128 * 1024,
            budget.clone(),
            ingress.clone(),
            term,
        );

        let yielded = s.next().await.unwrap().unwrap();
        // Yielded SSE data was shrunk to a tiny stub (~48 B), freeing the 70 KiB string immediately!
        assert!(yielded.data.as_ref().unwrap().len() < 128);
        let (_, val, val_lease) = ingress.take().unwrap();
        assert_eq!(
            val.unwrap()["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .len(),
            70 * 1024
        );
        drop(val_lease);
        drop(s);
        assert_eq!(budget.used(), 0);

        // Tight budget fails cleanly with active memory limit error.
        let tight = ByteBudget::new(10);
        let term_tight = Arc::new(TransportTermination::default());
        let mut s_tight = test_stream(
            vec![b"data: abcdef\n", b"data: ghijkl\n\n"],
            64,
            tight.clone(),
            Ingress::default(),
            term_tight.clone(),
        );
        assert!(s_tight.next().await.unwrap().is_err());
        assert!(term_tight.is_terminated());
        assert!(
            term_tight
                .failure(false)
                .to_string()
                .contains("active memory limit")
        );
        assert_eq!(tight.used(), 0);
    }
}
