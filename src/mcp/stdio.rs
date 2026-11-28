//! Bounded stdio child transport and stderr tail capture.
use std::{
    collections::VecDeque,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    time::timeout,
};

use super::{
    ingress::{self, Ingress, ObserveOutcome},
    types::{
        CleanupState, FailureKind, IngressError, McpFailure, TransportTermination,
        encode_mcp_message, record_cleanup_failure, strip_modern_extensions_for_legacy_initialize,
    },
};
use crate::budget::{ByteBudget, ByteLease, Leased, format_bytes};
#[cfg(windows)]
use crate::platform::windows_job;

pub(super) const MAX_STDERR_SNAPSHOT_BYTES: usize = 4096;
const STDERR_SNAPSHOT_WAIT: Duration = Duration::from_millis(100);
#[cfg(test)]
pub(super) const LINE_SHRINK_CAP: usize = 64 * 1024;
const UTF8_BOM: &[u8; 3] = b"\xef\xbb\xbf";
const INVALID_REQUEST_REPLY: &[u8] =
    b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,\"message\":\"Invalid request\"}}\n";

pub(super) struct StderrRing {
    pub(super) buf: VecDeque<u8>,
    cap: usize,
    pub(super) lease: ByteLease,
}

impl StderrRing {
    pub(super) fn try_new(cap: usize, mut lease: ByteLease) -> Option<Self> {
        let mut buf = VecDeque::new();
        buf.try_reserve_exact(cap).ok()?;
        if !lease.try_grow(buf.capacity().saturating_sub(lease.bytes())) {
            return None;
        }
        Some(Self { buf, cap, lease })
    }

    pub(super) fn push(&mut self, bytes: &[u8]) {
        if self.cap == 0 || bytes.is_empty() {
            return;
        }
        if bytes.len() >= self.cap {
            self.buf.clear();
            self.buf.extend(&bytes[bytes.len() - self.cap..]);
            return;
        }
        let overflow = self
            .buf
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(self.cap);
        if overflow > 0 {
            self.buf.drain(..overflow);
        }
        self.buf.extend(bytes);
    }

    pub(super) fn tail_bytes(&self, max_bytes: usize) -> Vec<u8> {
        let len = self.buf.len();
        let skip = len.saturating_sub(max_bytes);
        let (a, b) = self.buf.as_slices();
        let mut out = Vec::with_capacity(len - skip);
        if skip < a.len() {
            out.extend_from_slice(&a[skip..]);
            out.extend_from_slice(b);
        } else {
            out.extend_from_slice(&b[skip - a.len()..]);
        }
        out
    }
}

pub(super) fn format_stderr_tail(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let start = bytes
        .iter()
        .position(|&b| !(0x80..=0xBF).contains(&b))
        .unwrap_or(bytes.len());
    let sliced = &bytes[start..];
    if sliced.is_empty() {
        return None;
    }
    let decoded = String::from_utf8_lossy(sliced);
    let sanitized = crate::discovery::safe_text(&decoded);
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
    }
}

pub(crate) struct StderrCapture {
    ring: Arc<Mutex<StderrRing>>,
    done_rx: tokio::sync::watch::Receiver<bool>,
    abort_handle: tokio::task::AbortHandle,
}

impl StderrCapture {
    pub(super) fn spawn(mut stderr: ChildStderr, ring: StderrRing) -> Arc<Self> {
        let ring = Arc::new(Mutex::new(ring));
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let task_ring = ring.clone();
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        task_ring
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .push(&buf[..n]);
                    }
                }
            }
            let _ = done_tx.send(true);
        });
        Arc::new(Self {
            ring,
            done_rx,
            abort_handle: handle.abort_handle(),
        })
    }

    pub(crate) async fn snapshot_tail(&self, wait: Duration) -> Option<String> {
        let mut rx = self.done_rx.clone();
        if !*rx.borrow() {
            let _ = timeout(wait, rx.wait_for(|&done| done)).await;
        }
        let bytes = self
            .ring
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tail_bytes(MAX_STDERR_SNAPSHOT_BYTES);
        format_stderr_tail(&bytes)
    }

    pub(crate) fn abort(&self) {
        self.abort_handle.abort();
        let mut ring = self.ring.lock().unwrap_or_else(|p| p.into_inner());
        ring.buf = VecDeque::new();
        ring.cap = 0;
        ring.lease.release();
    }
}

impl Drop for StderrCapture {
    fn drop(&mut self) {
        self.abort();
    }
}

pub(super) async fn attach_stderr_tail(
    mut error: anyhow::Error,
    capture: Option<&Arc<StderrCapture>>,
    is_closed: bool,
) -> anyhow::Error {
    if let Some(capture) = capture
        && let Some(failure) = error.downcast_mut::<McpFailure>()
        && failure.stderr_tail.is_none()
        && (failure.kind == FailureKind::Transport || is_closed)
    {
        failure.stderr_tail = capture.snapshot_tail(STDERR_SNAPSHOT_WAIT).await;
    }
    error
}

pub(super) async fn attach_stderr_tail_and_abort(
    mut error: anyhow::Error,
    capture: Option<Arc<StderrCapture>>,
) -> anyhow::Error {
    if let Some(capture) = capture {
        if let Some(failure) = error.downcast_mut::<McpFailure>()
            && failure.stderr_tail.is_none()
        {
            failure.stderr_tail = capture.snapshot_tail(STDERR_SNAPSHOT_WAIT).await;
        }
        capture.abort();
    }
    error
}

pub(super) enum StdioParseError {
    Ignore,
    InvalidRequest,
}

pub(super) fn classify_stdio_error(
    error: serde_json::Error,
    ignore_notification: bool,
) -> std::result::Result<Option<ServerJsonRpcMessage>, StdioParseError> {
    if ignore_notification {
        return Ok(None);
    }
    match error.classify() {
        serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
            Err(StdioParseError::Ignore)
        }
        serde_json::error::Category::Data | serde_json::error::Category::Io => {
            Err(StdioParseError::InvalidRequest)
        }
    }
}

/// Own the child and bounded stdio framing throughout shutdown.
pub(super) struct ChildTransport<R = ChildStdout, W = ChildStdin> {
    pub(super) read: BufReader<R>,
    pub(super) line_buf: Vec<u8>,
    pub(super) line_lease: ByteLease,
    max_line_bytes: usize,
    budget: Arc<ByteBudget>,
    write: Arc<tokio::sync::Mutex<Option<W>>>,
    pending_error_reply: bool,
    ingress: Ingress,
    termination: Arc<TransportTermination>,
    pub(super) child: Option<Child>,
    #[cfg(unix)]
    pub(super) group: Option<i32>,
    #[cfg(windows)]
    job: Option<windows_job::Job>,
    cleanup: CleanupState,
}

impl ChildTransport<ChildStdout, ChildStdin> {
    pub(super) fn spawn_observed(
        mut command: Command,
        cleanup: CleanupState,
        ingress: Ingress,
        termination: Arc<TransportTermination>,
        max_line_bytes: usize,
        stderr_tail_bytes: usize,
        budget: Arc<ByteBudget>,
    ) -> std::io::Result<(Self, Option<Arc<StderrCapture>>)> {
        let stderr_ring = if stderr_tail_bytes > 0 {
            budget
                .try_acquire(stderr_tail_bytes)
                .and_then(|lease| StderrRing::try_new(stderr_tail_bytes, lease))
        } else {
            None
        };
        let capture_stderr = stderr_ring.is_some();
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(if capture_stderr {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .kill_on_drop(false);
        #[cfg(unix)]
        crate::platform::configure_child_process_group(&mut command);
        #[cfg(windows)]
        command
            .creation_flags(
                windows_sys::Win32::System::Threading::CREATE_SUSPENDED
                    | windows_sys::Win32::System::Threading::CREATE_NO_WINDOW,
            )
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        #[cfg(unix)]
        let group = child.id().map(|id| id as i32);
        #[cfg(windows)]
        let job = match windows_job::Job::adopt(&child) {
            Ok(job) => Some(job),
            Err(error) => {
                let _ = child.start_kill();
                tokio::spawn(async move {
                    let _ = child.wait().await;
                });
                return Err(error);
            }
        };
        let stdout = child.stdout.take().expect("piped stdout");
        let stdin = child.stdin.take().expect("piped stdin");
        let stderr_capture = match (child.stderr.take(), stderr_ring) {
            (Some(stderr), Some(ring)) => Some(StderrCapture::spawn(stderr, ring)),
            _ => None,
        };
        let line_lease = budget.empty_lease();
        Ok((
            Self {
                read: BufReader::new(stdout),
                line_buf: Vec::new(),
                line_lease,
                max_line_bytes,
                budget,
                write: Arc::new(tokio::sync::Mutex::new(Some(stdin))),
                pending_error_reply: false,
                ingress,
                termination,
                child: Some(child),
                #[cfg(unix)]
                group,
                #[cfg(windows)]
                job,
                cleanup,
            },
            stderr_capture,
        ))
    }
}

impl<R, W> ChildTransport<R, W> {
    #[cfg(test)]
    pub(super) fn from_parts(
        read: R,
        write: W,
        ingress: Ingress,
        termination: Arc<TransportTermination>,
        max_line_bytes: usize,
        budget: Arc<ByteBudget>,
    ) -> Self
    where
        R: AsyncRead,
    {
        let line_lease = budget.empty_lease();
        Self {
            read: BufReader::new(read),
            line_buf: Vec::new(),
            line_lease,
            max_line_bytes,
            budget,
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
            pending_error_reply: false,
            ingress,
            termination,
            child: None,
            #[cfg(unix)]
            group: None,
            #[cfg(windows)]
            job: None,
            cleanup: CleanupState::default(),
        }
    }

    pub(super) fn clear_line_buf(&mut self) {
        self.line_buf = Vec::new();
        self.line_lease.release();
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) {
        if let Some(group) = self.group {
            crate::platform::signal_process_group(group, signal);
        }
    }

    #[cfg(all(unix, test))]
    pub(super) fn exited_unreaped(&mut self) -> std::io::Result<bool> {
        crate::platform::child_exited_unreaped(&mut self.group)
    }

    #[cfg(unix)]
    pub(super) async fn wait_unreaped(&mut self) -> std::io::Result<bool> {
        crate::platform::wait_child_unreaped(&mut self.group, Duration::from_secs(1)).await
    }
}

impl<R, W> ChildTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn close_writer(&mut self) -> std::io::Result<()> {
        let mut guard = self.write.lock().await;
        if let Some(mut stdin) = guard.take() {
            let _ = stdin.shutdown().await;
        }
        Ok(())
    }

    // Cancellation-safe bounded line read: fill_buf is the sole await point.
    pub(super) async fn read_line(&mut self) -> bool {
        loop {
            let (consumed, done, hard_error) = {
                let available = match self.read.fill_buf().await {
                    Ok(available) => available,
                    Err(error) => {
                        self.clear_line_buf();
                        self.termination.terminate(
                            FailureKind::Transport,
                            format!("MCP stdio read failed: {error}"),
                        );
                        return false;
                    }
                };
                if available.is_empty() {
                    self.clear_line_buf();
                    return false;
                }
                let newline = available.iter().position(|&b| b == b'\n');
                let take = newline.map_or(available.len(), |idx| idx + 1);
                let active_limit = self.ingress.active_response_limit(self.max_line_bytes);
                let next_len = self.line_buf.len().saturating_add(take);
                if next_len > active_limit {
                    (
                        take,
                        false,
                        Some(format!(
                            "MCP stdio response exceeds the {} limit",
                            format_bytes(active_limit)
                        )),
                    )
                } else if !self
                    .line_lease
                    .reserve_vec(&mut self.line_buf, next_len, active_limit)
                {
                    (
                        take,
                        false,
                        Some(format!(
                            "MCP stdio response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        )),
                    )
                } else {
                    self.line_buf.extend_from_slice(&available[..take]);
                    (take, newline.is_some(), None)
                }
            };
            self.read.consume(consumed);
            if let Some(reason) = hard_error {
                self.clear_line_buf();
                self.termination.terminate(FailureKind::Transport, reason);
                return false;
            }
            if done {
                return true;
            }
        }
    }

    #[cfg(unix)]
    pub(super) async fn shutdown(&mut self) -> std::io::Result<()> {
        self.clear_line_buf();
        // A pending SDK send can hold the writer lock while the child stops
        // reading. Kill the still-owned process before waiting on that lock.
        if !matches!(
            timeout(Duration::from_millis(500), self.close_writer()).await,
            Ok(Ok(()))
        ) {
            self.signal_group(libc::SIGKILL);
            if self.group.is_some()
                && let Some(child) = self.child.as_mut()
            {
                let _ = child.start_kill();
            }
        }
        if self.child.is_none() {
            return Ok(());
        }
        if !self.wait_unreaped().await? {
            self.signal_group(libc::SIGTERM);
            let _ = self.wait_unreaped().await?;
        }
        // The unreaped leader pins the PGID until every group signal is complete.
        self.signal_group(libc::SIGKILL);
        let waited = if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
            child.wait().await.map(|_| ())
        } else {
            Ok(())
        };
        self.group = None;
        self.child = None;
        waited
    }

    #[cfg(windows)]
    pub(super) async fn shutdown(&mut self) -> std::io::Result<()> {
        self.clear_line_buf();
        let _ = timeout(Duration::from_millis(500), self.close_writer()).await;
        if let Some(job) = &self.job {
            job.terminate()?;
        }
        // Retain the job and child if cancellation interrupts the join.
        timeout(Duration::from_secs(3), async {
            if let Some(child) = &mut self.child {
                child.wait().await?;
            }
            if let Some(job) = &self.job {
                while !job.is_empty()? {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            Ok::<_, std::io::Error>(())
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "MCP process job did not close",
            )
        })??;
        self.child = None;
        self.job = None;
        Ok(())
    }
}

impl<R, W> Drop for ChildTransport<R, W> {
    fn drop(&mut self) {
        self.clear_line_buf();
        #[cfg(unix)]
        self.signal_group(libc::SIGKILL);
        #[cfg(windows)]
        if let Some(job) = &self.job {
            let _ = job.terminate();
        }
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            if self.group.is_some() {
                let _ = child.start_kill();
            }
            #[cfg(windows)]
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

impl<R, W> Transport<RoleClient> for ChildTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Error = std::io::Error;

    fn send(
        &mut self,
        mut item: ClientJsonRpcMessage,
    ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
        strip_modern_extensions_for_legacy_initialize(&mut item);
        let terminated = self.termination.is_terminated();
        if !terminated {
            self.ingress.track(&item);
        }
        let write = self.write.clone();
        let termination = self.termination.clone();
        let budget = self.budget.clone();
        let limit = self.max_line_bytes.saturating_sub(1);
        async move {
            if terminated || termination.is_terminated() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Transport is terminated",
                ));
            }
            let bytes = encode_mcp_message(&item, limit, &budget, &termination)?;
            drop(item);
            let mut guard = write.lock().await;
            if termination.is_terminated() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Transport is terminated",
                ));
            }
            let stdin = guard.as_mut().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "Transport is closed")
            })?;
            stdin.write_all(&bytes).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        }
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        loop {
            if self.pending_error_reply {
                let mut guard = self.write.lock().await;
                let writer = guard.as_mut()?;
                if writer.write_all(INVALID_REQUEST_REPLY).await.is_err()
                    || writer.flush().await.is_err()
                {
                    return None;
                }
                self.pending_error_reply = false;
            }
            if !self.read_line().await {
                return None;
            }
            let mut raw = Leased::new(std::mem::take(&mut self.line_buf), self.line_lease.take());
            if raw.last() == Some(&b'\n') {
                raw.value_mut().pop();
            }
            if raw.last() == Some(&b'\r') {
                raw.value_mut().pop();
            }
            if raw.starts_with(UTF8_BOM) {
                raw.value_mut().drain(..UTF8_BOM.len());
            }
            let outcome = if raw.is_empty() {
                Ok(None)
            } else {
                match self
                    .ingress
                    .observe_owned(raw, self.max_line_bytes, &self.budget, false)
                {
                    Err(IngressError::MemoryExhausted) => Err((
                        FailureKind::Transport,
                        format!(
                            "MCP stdio response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        ),
                    )),
                    Err(IngressError::InvalidMetadata) => Err((
                        FailureKind::Protocol,
                        "MCP response JSON cannot be represented losslessly".into(),
                    )),
                    Err(IngressError::FrameTooLarge { limit }) => Err((
                        FailureKind::Transport,
                        format!(
                            "MCP stdio response exceeds the {} limit",
                            format_bytes(limit)
                        ),
                    )),
                    Ok(ObserveOutcome::Captured(message)) => {
                        self.clear_line_buf();
                        return Some(ingress::into_sdk(message));
                    }
                    Ok(ObserveOutcome::InvalidMessage(error, ignore)) => {
                        Ok(Some(classify_stdio_error(error, ignore)))
                    }
                    Ok(ObserveOutcome::NotMatched) => Ok(Some(Ok(None))),
                }
            };
            self.clear_line_buf();
            match outcome {
                Err((kind, reason)) => {
                    self.termination.terminate(kind, reason);
                    return None;
                }
                Ok(None) | Ok(Some(Ok(None))) | Ok(Some(Err(StdioParseError::Ignore))) => continue,
                Ok(Some(Ok(Some(message)))) => return Some(message),
                Ok(Some(Err(StdioParseError::InvalidRequest))) => {
                    self.pending_error_reply = true;
                }
            }
        }
    }

    async fn close(&mut self) -> std::io::Result<()> {
        let result = self.shutdown().await;
        if result.is_err() {
            record_cleanup_failure(
                &self.cleanup,
                FailureKind::Transport,
                "MCP child cleanup failed; local closure is not confirmed",
                false,
            );
        }
        result
    }
}
