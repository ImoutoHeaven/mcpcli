//! Bound retained SSE event data before the parser allocates it.
use futures_util::{StreamExt, stream::BoxStream};
use rmcp::transport::streamable_http_client::SseError;
use sse_stream::{Sse, SseStream};

#[derive(Default)]
struct EventBudget {
    retained: usize,
    line: usize,
    comment: bool,
    after_cr: bool,
}

impl EventBudget {
    fn observe(&mut self, bytes: &[u8], limit: usize) -> bool {
        for &byte in bytes {
            if self.after_cr {
                self.after_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if matches!(byte, b'\r' | b'\n') {
                if self.line == 0 {
                    self.retained = 0;
                } else if !self.comment {
                    self.retained = self.retained.saturating_add(self.line).saturating_add(1);
                }
                self.line = 0;
                self.comment = false;
                self.after_cr = byte == b'\r';
            } else {
                if self.line == 0 {
                    self.comment = byte == b':';
                }
                self.line = self.line.saturating_add(1);
            }
            if self.retained.saturating_add(self.line) > limit {
                return false;
            }
        }
        true
    }
}

pub(super) fn bounded_stream(
    response: reqwest::Response,
    limit: usize,
) -> BoxStream<'static, Result<Sse, SseError>> {
    let bytes = response.bytes_stream().scan(
        (EventBudget::default(), false),
        move |(budget, failed), chunk| {
            let next = if *failed {
                None
            } else {
                match chunk {
                    Ok(bytes) if budget.observe(&bytes, limit) => Some(Ok(bytes)),
                    Ok(_) => {
                        *failed = true;
                        Some(Err(std::io::Error::other(
                            "SSE event exceeds the size limit",
                        )))
                    }
                    Err(_) => {
                        *failed = true;
                        Some(Err(std::io::Error::other("HTTP response body failed")))
                    }
                }
            };
            std::future::ready(next)
        },
    );
    SseStream::from_bytes_stream(bytes).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_unfinished_lines_and_retained_data_across_chunks() {
        let mut budget = EventBudget::default();
        assert!(budget.observe(b"data: a\r", 16));
        assert!(budget.observe(b"\ndata: b\n", 16));
        assert!(!budget.observe(b"x", 16));
        let mut budget = EventBudget::default();
        assert!(budget.observe(b"data: ab", 8));
        assert!(!budget.observe(b"c", 8));
    }

    #[test]
    fn resets_on_all_sse_blank_lines_and_discards_completed_comments() {
        for delimiter in [b"\n\n".as_slice(), b"\r\r", b"\r\n\r\n"] {
            let mut budget = EventBudget::default();
            for _ in 0..100 {
                assert!(budget.observe(b"data: a", 16));
                for byte in delimiter {
                    assert!(budget.observe(&[*byte], 16));
                }
            }
        }
        let mut budget = EventBudget::default();
        assert!(budget.observe(b"data: a\n", 16));
        for _ in 0..100 {
            assert!(budget.observe(b": ping\n", 16));
        }
        assert!(budget.observe(b"data: b\n", 16));
        assert!(!budget.observe(b"data:", 16));
    }
}
