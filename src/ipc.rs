use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL: u32 = 2;
pub const REQUEST_LIMIT: usize = 4 << 20;
pub const RESPONSE_LIMIT: usize = 32 << 20;
pub const ARGUMENT_LIMIT: usize = 1 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub protocol: u32,
    pub namespace: String,
    pub build: String,
    pub digest: String,
    pub timeout_ms: u64,
    pub deadline_ms: u64,
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

pub async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    max: usize,
) -> io::Result<Option<T>> {
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
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(io::Error::other)
}

pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    max: usize,
) -> io::Result<()> {
    let started = std::sync::atomic::AtomicBool::new(false);
    write_frame_tracked(writer, value, max, &started).await
}

pub async fn write_frame_tracked<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    max: usize,
    started: &std::sync::atomic::AtomicBool,
) -> io::Result<()> {
    // Count first so an oversized frame does not allocate a second large body.
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_err(io::Error::other)?;
    if count.0 > max || count.0 > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC response exceeds frame limit",
        ));
    }
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    started.store(true, std::sync::atomic::Ordering::Release);
    writer
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    writer.write_all(&bytes).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
