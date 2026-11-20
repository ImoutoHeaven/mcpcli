use crate::budget::{ByteBudget, ByteLease};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::{Instant, timeout},
};

pub const PROTOCOL: u32 = 3;
pub const REQUEST_LIMIT: usize = 4 << 20;
pub const RESPONSE_LIMIT: usize = 512 << 20;
pub const ARGUMENT_LIMIT: usize = 1 << 20;
pub const TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

const WRITE_CHUNK: usize = 64 * 1024;
const LEASE_SLAB: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub protocol: u32,
    pub namespace: String,
    pub build: String,
    pub digest: String,
    pub timeout_ms: u64,
    pub deadline_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_deadline_ms: Option<u64>,
    pub operation: Operation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Status,
    Stop,
    Server {
        server: String,
    },
    Tools {
        server: String,
    },
    Call {
        server: String,
        tool: String,
        arguments: Map<String, Value>,
        inline_media: bool,
    },
    Reset {
        server: String,
        force: bool,
    },
}
impl Operation {
    pub fn management(&self) -> bool {
        matches!(self, Self::Status | Self::Stop)
    }
    pub fn is_call(&self) -> bool {
        matches!(self, Self::Call { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub code: i32,
    pub kind: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub outcome_unknown: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_error: Option<Value>,
}
impl Failure {
    pub fn new(code: i32, kind: &str, message: impl Into<String>) -> Self {
        Self {
            code,
            kind: kind.into(),
            message: message.into(),
            outcome_unknown: false,
            rpc_error: None,
        }
    }
    pub fn uncertain(mut self, uncertain: bool) -> Self {
        self.outcome_unknown = uncertain;
        self
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Failure {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub protocol: u32,
    pub namespace: String,
    pub build: String,
    pub epoch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Failure>,
}

struct LimitedVecWriter {
    buf: Vec<u8>,
    max: usize,
    lease: ByteLease,
    abort: Option<io::Error>,
}

impl LimitedVecWriter {
    fn new(max: usize, budget: &Arc<ByteBudget>) -> io::Result<Self> {
        let mut lease = budget.empty_lease();
        if !lease.try_grow(4) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "IPC frame exceeded active memory limit",
            ));
        }
        Ok(Self {
            buf: vec![0u8; 4],
            max,
            lease,
            abort: None,
        })
    }

    fn finish(mut self) -> io::Result<(Vec<u8>, ByteLease)> {
        if let Some(error) = self.abort {
            return Err(error);
        }
        let payload_len = self.buf.len().saturating_sub(4);
        if payload_len == 0 || payload_len > self.max || payload_len > u32::MAX as usize {
            self.buf = Vec::new();
            self.lease.release();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
        }
        self.lease.shrink_to(self.buf.len());
        self.buf[..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
        Ok((self.buf, self.lease))
    }
}

impl io::Write for LimitedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(payload_len) = (self.buf.len() - 4).checked_add(bytes.len()) else {
            self.buf = Vec::new();
            self.lease.release();
            self.abort = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
        };
        if payload_len > self.max || payload_len > u32::MAX as usize {
            self.buf = Vec::new();
            self.lease.release();
            self.abort = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
        }
        let needed = 4 + payload_len;
        if needed > self.lease.bytes() {
            let delta = needed - self.lease.bytes();
            let max_frame = 4usize.saturating_add(self.max);
            let slab = delta.max(LEASE_SLAB.min(max_frame.saturating_sub(self.lease.bytes())));
            if !self.lease.try_grow(slab) && !self.lease.try_grow(delta) {
                self.buf = Vec::new();
                self.lease.release();
                self.abort = Some(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "IPC frame exceeded active memory limit",
                ));
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "IPC frame exceeded active memory limit",
                ));
            }
        }
        self.buf.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_frame_leased<T: Serialize>(
    value: &T,
    max: usize,
    budget: &Arc<ByteBudget>,
) -> io::Result<(Vec<u8>, ByteLease)> {
    let mut writer = LimitedVecWriter::new(max, budget)?;
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if let Some(abort) = writer.abort.take() {
            return Err(abort);
        }
        return Err(io::Error::other(error));
    }
    writer.finish()
}

fn step_budget(idle_timeout: Duration, hard_deadline: Option<Instant>) -> io::Result<Duration> {
    let budget = match hard_deadline {
        Some(deadline) => idle_timeout.min(deadline.saturating_duration_since(Instant::now())),
        None => idle_timeout,
    };
    if budget.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "IPC transfer deadline exceeded",
        ));
    }
    Ok(budget)
}

pub async fn write_raw_idle<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
    idle_timeout: Duration,
    hard_deadline: Option<Instant>,
    started: &AtomicBool,
) -> io::Result<()> {
    let mut remaining = frame;
    while !remaining.is_empty() {
        let wait = step_budget(idle_timeout, hard_deadline)?;
        started.store(true, Ordering::Release);
        let chunk = &remaining[..remaining.len().min(WRITE_CHUNK)];
        let written = timeout(wait, writer.write(chunk))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "IPC write stalled"))??;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "IPC stream closed during write",
            ));
        }
        remaining = &remaining[written..];
    }
    let wait = step_budget(idle_timeout, hard_deadline)?;
    timeout(wait, writer.flush())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "IPC flush stalled"))?
}

pub async fn read_frame_header<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> io::Result<Option<usize>> {
    let mut header = [0u8; 4];
    if reader.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let len = u32::from_le_bytes(header) as usize;
    if len == 0 || len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC frame exceeds limit or is empty",
        ));
    }
    Ok(Some(len))
}

pub async fn read_frame_body_idle<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    len: usize,
    idle_timeout: Duration,
    hard_deadline: Option<Instant>,
    budget: &Arc<ByteBudget>,
) -> io::Result<T> {
    let _lease = budget.try_acquire(len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "IPC frame exceeded active memory limit",
        )
    })?;
    let mut body = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let wait = step_budget(idle_timeout, hard_deadline)?;
        let n = timeout(wait, reader.read(&mut body[filled..]))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "IPC read stalled"))??;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "IPC frame truncated",
            ));
        }
        filled += n;
    }
    serde_json::from_slice(&body).map_err(io::Error::other)
}

pub async fn read_frame_with_budget<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    max: usize,
    budget: &Arc<ByteBudget>,
) -> io::Result<Option<T>> {
    let Some(len) = read_frame_header(reader, max).await? else {
        return Ok(None);
    };
    let _lease = budget.try_acquire(len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "IPC frame exceeded active memory limit",
        )
    })?;
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(
        reader: &mut R,
        max: usize,
    ) -> io::Result<Option<T>> {
        read_frame_with_budget(reader, max, ByteBudget::global()).await
    }

    async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
        writer: &mut W,
        value: &T,
        max: usize,
    ) -> io::Result<()> {
        let (frame, _lease) = encode_frame_leased(value, max, ByteBudget::global())?;
        writer.write_all(&frame).await?;
        writer.flush().await
    }

    #[tokio::test]
    async fn rejects_lengths_before_reading_body() {
        let (mut tx, mut rx) = tokio::io::duplex(16);
        tx.write_all(&(1024u32).to_le_bytes()).await.unwrap();
        let err = read_frame::<_, Value>(&mut rx, 32).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn truncation_is_not_clean_eof() {
        let (mut tx, mut rx) = tokio::io::duplex(16);
        tx.write_all(&[1, 0]).await.unwrap();
        drop(tx);
        assert_eq!(
            read_frame::<_, Value>(&mut rx, 32)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[tokio::test]
    async fn round_trip_and_clean_eof() {
        let (mut tx, mut rx) = tokio::io::duplex(256);
        write_frame(&mut tx, &serde_json::json!({"key":"value"}), 100)
            .await
            .unwrap();
        drop(tx);
        assert_eq!(
            read_frame::<_, Value>(&mut rx, 100).await.unwrap().unwrap()["key"],
            "value"
        );
        assert!(
            read_frame::<_, Value>(&mut rx, 100)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn limited_vec_writer_serializes_in_one_pass_and_enforces_frame_and_budget_caps() {
        struct OncePayload<'a>(&'a AtomicUsize, &'a str);
        impl Serialize for OncePayload<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.fetch_add(1, Ordering::Relaxed);
                serializer.serialize_str(self.1)
            }
        }

        let calls = AtomicUsize::new(0);
        let budget = ByteBudget::new(64);
        let (frame, lease) =
            encode_frame_leased(&OncePayload(&calls, "hello"), 7, &budget).unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(&frame[..4], &(7u32).to_le_bytes());
        assert_eq!(&frame[4..], br#""hello""#);
        assert_eq!(lease.bytes(), 11);
        assert_eq!(budget.used(), 11);
        drop(lease);
        assert_eq!(budget.used(), 0);

        let calls = AtomicUsize::new(0);
        let err = encode_frame_leased(&OncePayload(&calls, "hello"), 6, &budget).unwrap_err();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(budget.used(), 0);

        // 4-byte header + 7-byte JSON = 11 bytes; budget of 10 must reject without leaking.
        let tiny_budget = ByteBudget::new(10);
        let err = encode_frame_leased(&OncePayload(&calls, "hello"), 64, &tiny_budget).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(tiny_budget.used(), 0);
    }
}
