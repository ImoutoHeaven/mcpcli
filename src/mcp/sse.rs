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
use crate::budget::{ByteBudget, ByteLease, Leased, format_bytes};

const MAX_SSE_ID_BYTES: usize = 8 * 1024;

#[cfg(test)]
const LINE_SHRINK_CAP: usize = 64 * 1024;

pub(super) fn sse_retained_capacity(sse: &Sse) -> usize {
    sse.event.as_ref().map_or(0, String::capacity)
        + sse.data.as_ref().map_or(0, String::capacity)
        + sse.id.as_ref().map_or(0, String::capacity)
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
    parsed: VecDeque<Leased<Sse>>,
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
            done: false,
        }
    }

    fn clear_line_buf(&mut self) {
        self.line_buf = Vec::new();
        self.line_lease.release();
    }

    fn clear_all(&mut self) {
        self.clear_line_buf();
        self.current = None;
        self.current_raw_bytes = 0;
        self.current_lease.release();
        self.parsed.clear();
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

    fn fail_id_too_large(&mut self) -> SseError {
        self.done = true;
        self.clear_all();
        let msg = format!(
            "SSE id exceeds the {} metadata limit",
            format_bytes(MAX_SSE_ID_BYTES)
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

    fn finish_parse_error(&mut self, error: SseError) -> SseError {
        if matches!(error, SseError::Utf8Parse(_)) {
            self.termination.terminate(
                FailureKind::Transport,
                format!("SSE UTF-8 decoding failed: {error}"),
            );
        }
        self.done = true;
        self.clear_all();
        error
    }

    fn dispatch_event(&mut self) -> Result<(), SseError> {
        self.current_raw_bytes = 0;
        if let Some(mut sse) = self.current.take() {
            let mut lease = self.current_lease.take();
            lease.shrink_to(sse_retained_capacity(&sse));
            match self
                .ingress
                .observe_sse(&mut sse, &mut lease, self.limit, &self.budget)
            {
                Ok(()) => {}
                Err(super::IngressError::MemoryExhausted) => {
                    drop(sse);
                    drop(lease);
                    return Err(self.fail_memory_limit());
                }
                Err(super::IngressError::InvalidMetadata) => {
                    drop(sse);
                    drop(lease);
                    self.done = true;
                    self.clear_all();
                    let message = "MCP response JSON cannot be represented losslessly";
                    self.termination
                        .terminate(FailureKind::Protocol, message.into());
                    return Err(SseError::Body(Box::new(std::io::Error::other(message))));
                }
            }
            self.parsed.push_back(Leased::new(sse, lease));
        } else {
            self.current_lease.release();
        }
        Ok(())
    }

    fn parse_borrowed_field(&mut self, line: &[u8]) -> Result<(), SseError> {
        let (field, value) = match line.iter().position(|&b| b == b':') {
            Some(colon) => (&line[..colon], &line[colon + 1..]),
            None => (line, &[][..]),
        };
        let value = value.strip_prefix(b" ").unwrap_or(value);

        match field {
            b"data" => {
                let text = std::str::from_utf8(value).map_err(SseError::Utf8Parse)?;
                let event = self.current.get_or_insert_default();
                let has_prev = event.data.is_some();
                let existing = event.data.get_or_insert_default();
                let needed = existing
                    .len()
                    .saturating_add(text.len())
                    .saturating_add(usize::from(has_prev));
                if !self
                    .current_lease
                    .reserve_string(existing, needed, self.limit)
                {
                    return Err(self.fail_memory_limit());
                }
                if has_prev {
                    existing.push('\n');
                }
                existing.push_str(text);
            }
            b"event" | b"id" => {
                if field == b"id" && value.contains(&0u8) {
                    return Ok(());
                }
                if field == b"id" && value.len() > MAX_SSE_ID_BYTES {
                    return Err(self.fail_id_too_large());
                }
                let text = std::str::from_utf8(value).map_err(SseError::Utf8Parse)?;
                let event = self.current.get_or_insert_default();
                let slot = if field == b"event" {
                    &mut event.event
                } else {
                    &mut event.id
                };
                let released = slot.take().map_or(0, |old| old.capacity());
                self.current_lease.shrink_by(released);
                let mut value = String::new();
                if !self
                    .current_lease
                    .reserve_string(&mut value, text.len(), self.limit)
                {
                    return Err(self.fail_memory_limit());
                }
                value.push_str(text);
                *slot = Some(value);
            }
            b"retry" => {
                if !value.is_empty()
                    && value.iter().all(u8::is_ascii_digit)
                    && let Ok(retry) = std::str::from_utf8(value)
                        .expect("ASCII digits")
                        .parse::<u64>()
                {
                    self.current.get_or_insert_default().retry = Some(retry);
                }
            }
            _ => {}
        }
        Ok(())
    }

    // Parameters drop in reverse order: every early return must release the
    // owned bytes before their lease, including bare fields and UTF-8 failures.
    fn parse_owned_field(
        &mut self,
        mut line_lease: ByteLease,
        mut line: Vec<u8>,
    ) -> Result<(), SseError> {
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return self.parse_borrowed_field(&line);
        };
        if &line[..colon] == b"data" && self.current.as_ref().is_none_or(|ev| ev.data.is_none()) {
            let val_start = if line.get(colon + 1) == Some(&b' ') {
                colon + 2
            } else {
                colon + 1
            };
            std::str::from_utf8(&line[val_start..]).map_err(SseError::Utf8Parse)?;
            line.drain(..val_start);
            let data = String::from_utf8(line).expect("validated UTF-8");
            line_lease.shrink_to(data.capacity());
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
                if !self
                    .line_lease
                    .reserve_vec(&mut self.line_buf, next_line_len, self.limit)
                {
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
                    self.clear_line_buf();
                } else {
                    let next_raw = self
                        .current_raw_bytes
                        .saturating_add(total_len)
                        .saturating_add(1);
                    if next_raw > self.limit {
                        return Err(self.fail_too_large());
                    }
                    if !self
                        .line_lease
                        .reserve_vec(&mut self.line_buf, total_len, self.limit)
                    {
                        return Err(self.fail_memory_limit());
                    }
                    self.line_buf.extend_from_slice(fragment);
                    self.current_raw_bytes = next_raw;
                    let owned_line = std::mem::take(&mut self.line_buf);
                    let owned_lease = self.line_lease.take();
                    self.parse_owned_field(owned_lease, owned_line)?;
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
        if let Some(sse) = this.parsed.pop_front() {
            return Poll::Ready(Some(Ok(sse.into_parts().0)));
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
                        return Poll::Ready(Some(Err(this.finish_parse_error(err))));
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
                        return Poll::Ready(Some(Err(this.finish_parse_error(err))));
                    }
                    if !rest.is_empty()
                        && let Err(err) = this.feed_bytes(rest)
                    {
                        return Poll::Ready(Some(Err(this.finish_parse_error(err))));
                    }
                    if let Some(sse) = this.parsed.pop_front() {
                        return Poll::Ready(Some(Ok(sse.into_parts().0)));
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
    async fn id_metadata_limit_counts_utf8_bytes_before_sdk_handoff() {
        for bytes in [MAX_SSE_ID_BYTES - 1, MAX_SSE_ID_BYTES, MAX_SSE_ID_BYTES + 1] {
            let id = format!(
                "{}{}",
                "é".repeat(bytes / 2),
                if bytes % 2 == 0 { "" } else { "x" }
            );
            assert_eq!(id.len(), bytes);
            for fragmented in [false, true] {
                let wire = format!("id: {id}\ndata: ok\n\n").into_bytes();
                let chunks = if fragmented {
                    vec![wire[..100].to_vec(), wire[100..].to_vec()]
                } else {
                    vec![wire]
                };
                let budget = ByteBudget::new(128 * 1024);
                let term = Arc::new(TransportTermination::default());
                let source =
                    futures_util::stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>));
                let mut stream = BoundedSseStream::new(
                    source,
                    32 * 1024,
                    budget.clone(),
                    Ingress::default(),
                    term.clone(),
                );
                let event = stream.next().await.unwrap();
                if bytes <= MAX_SSE_ID_BYTES {
                    let event = event.unwrap();
                    assert_eq!(event.id.as_deref(), Some(id.as_str()));
                    assert!(!term.is_terminated());
                    drop(event);
                } else {
                    assert!(event.is_err());
                    assert!(term.is_terminated());
                    assert!(term.failure(false).to_string().contains("SSE id exceeds"));
                }
                drop(stream);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    #[tokio::test]
    async fn local_utf8_error_keeps_its_reason_after_a_valid_event_id() {
        for fragmented in [false, true] {
            let budget = ByteBudget::new(1024);
            let termination = Arc::new(TransportTermination::default());
            let mut stream = test_stream(
                if fragmented {
                    vec![b"id: first\n\n", b"data: ", b"\xff\n\n"]
                } else {
                    vec![b"id: first\n\n", b"data: \xff\n\n"]
                },
                1024,
                budget.clone(),
                Ingress::default(),
                termination.clone(),
            );
            let first = stream.next().await.unwrap().unwrap();
            assert_eq!(first.id.as_deref(), Some("first"));
            drop(first);
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(matches!(error, SseError::Utf8Parse(_)));
            assert!(termination.is_terminated());
            assert!(
                termination
                    .failure(false)
                    .to_string()
                    .contains("SSE UTF-8 decoding failed")
            );
            assert!(stream.next().await.is_none());
            assert_eq!(budget.used(), 0);
        }
    }

    #[tokio::test]
    async fn standard_fields_allow_empty_overwritten_and_ignored_values() {
        let budget = ByteBudget::new(512);
        let term = Arc::new(TransportTermination::default());
        let mut stream = test_stream(vec![
            b"event: old\nid: old\nretry: 1\nunknown: ignored\n",
            b"event: new\nid: new\nretry: 2\nretry: +3\nretry: 1x\nretry: 18446744073709551616\nretry:  4\n",
            b"data\ndata: line\n\n", b"i", b"d\nretry\nevent\nunknown\n\n",
        ], 512, budget.clone(), Ingress::default(), term.clone());
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.event.as_deref(), Some("new"));
        assert_eq!(first.id.as_deref(), Some("new"));
        assert_eq!(first.retry, Some(2));
        assert_eq!(first.data.as_deref(), Some("\nline"));
        drop(first);
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.id.as_deref(), Some(""));
        assert_eq!(second.event.as_deref(), Some(""));
        assert_eq!(second.retry, None);
        assert_eq!(second.data, None);
        drop(second);
        assert!(stream.next().await.is_none());
        assert!(!term.is_terminated());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn replaced_metadata_releases_capacity_and_ignored_fields_keep_existing_value() {
        let budget = ByteBudget::new(100);
        let mut stream = test_stream(
            vec![],
            512,
            budget.clone(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
        );
        stream
            .parse_borrowed_field(format!("id: {}", "a".repeat(80)).as_bytes())
            .unwrap();
        assert_eq!(budget.used(), 80);
        stream
            .parse_borrowed_field(format!("id: {}", "b".repeat(90)).as_bytes())
            .unwrap();
        assert_eq!(budget.used(), 90);
        stream.parse_borrowed_field(b"id: a\0b").unwrap();
        stream.parse_borrowed_field(b"extension: unused").unwrap();
        assert_eq!(budget.used(), 90);
        stream.parse_borrowed_field(b"id").unwrap();
        assert_eq!(budget.used(), 0);
        assert_eq!(stream.current.as_ref().unwrap().id.as_deref(), Some(""));
    }

    #[test]
    fn fragmented_sse_handoffs_keep_capacity_charged() {
        let budget = ByteBudget::new(256);
        let mut stream = test_stream(
            vec![],
            128,
            budget.clone(),
            Ingress::default(),
            Arc::new(TransportTermination::default()),
        );
        stream.feed_bytes(b"data: ab").unwrap();
        stream.feed_bytes(b"cdefghijk").unwrap();
        assert_eq!(stream.line_lease.bytes(), stream.line_buf.capacity());
        assert_eq!(budget.used(), stream.line_buf.capacity());
        stream.feed_bytes(b"\n").unwrap();
        assert!(stream.line_buf.is_empty());
        assert_eq!(
            budget.used(),
            sse_retained_capacity(stream.current.as_ref().unwrap())
        );
        stream.feed_bytes(b"data: more\n").unwrap();
        assert_eq!(
            budget.used(),
            sse_retained_capacity(stream.current.as_ref().unwrap())
        );
        stream.feed_bytes(b"\n").unwrap();
        assert_eq!(budget.used(), sse_retained_capacity(&stream.parsed[0]));
        drop(stream);
        assert_eq!(budget.used(), 0);
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
        // Accounting ends when the official SDK receives the event. The
        // application's own queued events remain charged.
        assert_eq!(
            budget.used(),
            s.parsed
                .iter()
                .map(|event| sse_retained_capacity(event))
                .sum::<usize>()
        );
        drop(ev1);

        let ev2 = s.next().await.unwrap().unwrap();
        assert_eq!(ev2.data.as_deref(), Some("after"));
        // Both events have crossed the official SDK handoff.
        assert_eq!(budget.used(), 0);
        drop(ev2);

        assert!(s.next().await.is_none());
        assert!(!term.is_terminated());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn stubs_captured_tool_call_sse_data_and_enforces_memory_limit() {
        let budget = ByteBudget::new(400 * 1024);
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
        // The official SDK receives a small encoded stub. The application
        // retains the lossless result separately.
        let stub: rmcp::model::ServerJsonRpcMessage =
            serde_json::from_str(yielded.data.as_deref().unwrap()).unwrap();
        let rmcp::model::ServerJsonRpcMessage::Response(response) = &stub else {
            panic!("expected response")
        };
        let rmcp::model::ServerResult::CallToolResult(result) = &response.result else {
            panic!("expected call result")
        };
        assert!(result.content.is_empty());
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
