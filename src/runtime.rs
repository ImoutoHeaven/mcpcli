//! Private runtime files and short-lived clients of the persistent daemon.
use crate::budget::{ByteBudget, ByteLease, format_bytes};
use crate::config::{LoadedConfig, valid_name};
use crate::ipc::{self, Failure, Operation, Request, Response, StreamFrame, StreamFrameKind};
use crate::platform;
use crate::platform::{private_dir, secure_file};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tokio_util::sync::CancellationToken;

pub const START_WAIT: Duration = Duration::from_secs(10);
pub const STOP_WAIT: Duration = Duration::from_secs(20);
pub const DUAL_CLOCK_GRACE: Duration = Duration::from_secs(5);
pub const CANCEL_SENTINEL_WAIT: Duration = Duration::from_millis(2_500);

#[derive(Clone)]
pub struct Paths {
    pub directory: PathBuf,
    pub socket: PathBuf,
    pub control_socket: PathBuf,
    pub namespace: String,
    pub build: String,
    pub config: PathBuf,
    pub session: String,
}

pub struct Lock(File);
impl Lock {
    pub(crate) fn file(&self) -> &File {
        &self.0
    }

    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        let file = secure_file(path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self(file))),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }
    pub async fn acquire(path: &Path, budget: Duration) -> Result<Self> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(lock) = Self::try_acquire(path)? {
                return Ok(lock);
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for runtime lock");
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn build_id() -> Result<String> {
    let mut file = File::open(platform::image_path()?)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

impl Paths {
    pub fn new(config: PathBuf, session: &str) -> Result<Self> {
        let root =
            if let Some(root) = std::env::var_os("MCPCLI_RUNTIME_DIR").filter(|v| !v.is_empty()) {
                PathBuf::from(root)
            } else {
                platform::default_runtime()?
            };
        Self::with_root(config, session, &root)
    }
    pub fn with_root(config: PathBuf, session: &str, root: &Path) -> Result<Self> {
        if !valid_name(session) {
            bail!("session must be 1-64 ASCII letters, digits, '_' or '-'");
        }
        if !root.is_absolute() {
            bail!("runtime directory must be absolute");
        }
        let parent = root
            .parent()
            .context("runtime directory must have a parent")?
            .canonicalize()?;
        let root = parent.join(
            root.file_name()
                .context("runtime directory must have a name")?,
        );
        private_dir(&root)?;
        let root = root.canonicalize()?;
        let mut key = config.as_os_str().as_encoded_bytes().to_vec();
        key.push(0);
        key.extend_from_slice(session.as_bytes());
        let namespace = platform::digest(&key)[..24].to_owned();
        let directory = root.join(&namespace);
        private_dir(&directory)?;
        let socket = platform::endpoint(&directory, false)?;
        let control_socket = platform::endpoint(&directory, true)?;
        Ok(Self {
            directory,
            socket,
            control_socket,
            namespace,
            build: build_id()?,
            config,
            session: session.into(),
        })
    }
    pub fn startup_lock(&self) -> PathBuf {
        self.directory.join("startup.lock")
    }
    pub fn daemon_lock(&self) -> PathBuf {
        self.directory.join("daemon.lock")
    }
    pub fn state_path(&self) -> PathBuf {
        self.directory.join("state.json")
    }
    pub fn running(&self) -> Result<bool> {
        Ok(Lock::try_acquire(&self.daemon_lock())?.is_none())
    }
    pub fn request(
        &self,
        digest: &str,
        operation: Operation,
        duration: Duration,
    ) -> Result<Request> {
        self.request_with_explicit(digest, operation, duration, false)
    }
    pub fn request_with_explicit(
        &self,
        digest: &str,
        operation: Operation,
        duration: Duration,
        explicit_deadline: bool,
    ) -> Result<Request> {
        let timeout_ms = duration.as_millis().min(3_600_000) as u64;
        let deadline_ms = platform::monotonic_millis()?.saturating_add(timeout_ms);
        Ok(Request {
            protocol: ipc::PROTOCOL,
            namespace: self.namespace.clone(),
            build: self.build.clone(),
            digest: digest.into(),
            timeout_ms,
            max_timeout_ms: None,
            deadline_ms,
            explicit_deadline_ms: explicit_deadline.then_some(deadline_ms),
            operation,
        })
    }
    pub fn request_call_with_budgets(
        &self,
        digest: &str,
        operation: Operation,
        timeout: Duration,
        max_timeout: Option<Duration>,
        elapsed: Duration,
    ) -> Result<Request> {
        let timeout_ms = timeout.as_millis().min(3_600_000) as u64;
        let started_ms = platform::monotonic_millis()?
            .saturating_sub(elapsed.as_millis().min(u128::from(u64::MAX)) as u64);
        let (max_timeout_ms, deadline_ms, explicit_deadline_ms) = match max_timeout {
            Some(max) => {
                let max_ms = max.as_millis().min(86_400_000) as u64;
                let deadline_ms = started_ms.saturating_add(max_ms);
                (Some(max_ms), deadline_ms, Some(deadline_ms))
            }
            None => (None, started_ms.saturating_add(timeout_ms), None),
        };
        Ok(Request {
            protocol: ipc::PROTOCOL,
            namespace: self.namespace.clone(),
            build: self.build.clone(),
            digest: digest.into(),
            timeout_ms,
            max_timeout_ms,
            deadline_ms,
            explicit_deadline_ms,
            operation,
        })
    }
    pub fn remove_socket(&self) -> Result<()> {
        for socket in [&self.socket, &self.control_socket] {
            platform::remove_endpoint(socket)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct State {
    pub epoch: String,
    pub pid: u32,
    pub clean_stop: bool,
    pub started_unix_ms: u64,
    pub cleanup_errors: Vec<String>,
}
impl State {
    pub fn fresh() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            epoch: format!("{}-{now}", std::process::id()),
            pid: std::process::id(),
            clean_stop: false,
            started_unix_ms: now,
            cleanup_errors: Vec::new(),
        }
    }
    pub fn load(paths: &Paths) -> Result<Option<Self>> {
        if !paths.state_path().try_exists()? {
            return Ok(None);
        }
        let file = secure_file(&paths.state_path())?;
        let bytes = crate::json::read_limited(file, 16 << 10)?;
        Ok(Some(
            serde_json::from_slice(&bytes).context("invalid daemon state record")?,
        ))
    }
    pub fn store(&self, paths: &Paths) -> Result<()> {
        let path = paths.directory.join("state.json.tmp");
        match std::fs::remove_file(&path) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let mut file = platform::new_private_file(&path)?;
        let result = (|| -> Result<()> {
            file.write_all(&serde_json::to_vec(self)?)?;
            file.sync_all()?;
            drop(file);
            platform::replace_file(&path, &paths.state_path())?;
            platform::sync_directory(&paths.directory)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&path);
        }
        result
    }
}

pub async fn exchange(paths: &Paths, request: &Request) -> Result<(Response, ByteLease)> {
    let dispatched = AtomicBool::new(false);
    exchange_tracked(paths, request, &dispatched).await
}

pub async fn exchange_tracked(
    paths: &Paths,
    request: &Request,
    dispatched: &AtomicBool,
) -> Result<(Response, ByteLease)> {
    exchange_tracked_with_options(
        paths,
        request,
        dispatched,
        ipc::TRANSFER_IDLE_TIMEOUT,
        ByteBudget::global(),
    )
    .await
}

pub async fn exchange_tracked_with_options(
    paths: &Paths,
    request: &Request,
    dispatched: &AtomicBool,
    idle_timeout: Duration,
    budget: &Arc<ByteBudget>,
) -> Result<(Response, ByteLease)> {
    exchange_cancellable_with_options(paths, request, dispatched, idle_timeout, budget, None).await
}

pub async fn exchange_cancellable(
    paths: &Paths,
    request: &Request,
    dispatched: &AtomicBool,
    interrupt: &CancellationToken,
) -> Result<(Response, ByteLease)> {
    exchange_cancellable_with_options(
        paths,
        request,
        dispatched,
        ipc::TRANSFER_IDLE_TIMEOUT,
        ByteBudget::global(),
        Some(interrupt),
    )
    .await
}

async fn read_header_resumable<R: AsyncRead + Unpin>(
    reader: &mut R,
    header: &mut [u8; 4],
    filled: &mut usize,
    max: usize,
    allow_zero_marker: bool,
) -> io::Result<Option<usize>> {
    while *filled < 4 {
        let n = reader.read(&mut header[*filled..]).await?;
        if n == 0 {
            if *filled == 0 {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "IPC frame header truncated",
            ));
        }
        *filled += n;
    }
    let len = u32::from_le_bytes(*header) as usize;
    if len == 0 && allow_zero_marker {
        *filled = 0;
        return Ok(Some(0));
    }
    if len == 0 || len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC frame exceeds limit or is empty",
        ));
    }
    Ok(Some(len))
}

pub async fn exchange_cancellable_with_options(
    paths: &Paths,
    request: &Request,
    dispatched: &AtomicBool,
    idle_timeout: Duration,
    budget: &Arc<ByteBudget>,
    interrupt: Option<&CancellationToken>,
) -> Result<(Response, ByteLease)> {
    let has_side_effects = request.operation.has_side_effects();
    let uses_dual_clock = request.operation.uses_dual_clock();
    let unbounded_dual_clock = uses_dual_clock && request.max_timeout_ms.is_none();
    let now_ms = platform::monotonic_millis()?;
    let total_budget_cap_ms = if uses_dual_clock {
        request.max_timeout_ms.unwrap_or(request.timeout_ms)
    } else {
        request.timeout_ms
    };
    let remaining_total = request
        .deadline_ms
        .saturating_sub(now_ms)
        .min(total_budget_cap_ms);
    let remaining_admission = request
        .admission_deadline_ms()
        .saturating_sub(now_ms)
        .min(request.timeout_ms);
    if remaining_admission == 0 {
        return Err(
            Failure::new(5, "timeout", "request expired before connecting to daemon").into(),
        );
    }
    if request
        .explicit_deadline_ms
        .is_some_and(|exp_ms| exp_ms.saturating_sub(now_ms).min(total_budget_cap_ms) == 0)
    {
        return Err(
            Failure::new(5, "timeout", "request expired before connecting to daemon").into(),
        );
    }
    if interrupt.is_some_and(CancellationToken::is_cancelled) {
        return Err(Failure::new(
            130,
            "cancelled",
            "interrupted; cancellation is not rollback",
        )
        .into());
    }

    let allow_grace = uses_dual_clock && request.max_timeout_ms.is_some();
    let started = Instant::now();
    let admission_deadline = started + Duration::from_millis(remaining_admission);
    // Strict admission/pre-task deadline: +5s DUAL_CLOCK_GRACE is granted only after
    // the daemon signals task polling with a 4-byte zero marker ([0u8; 4]).
    let mut stage1b_deadline = if unbounded_dual_clock {
        None
    } else {
        Some(started + Duration::from_millis(remaining_total))
    };
    let hard_deadline = request.explicit_deadline_ms.map(|exp_ms| {
        started + Duration::from_millis(exp_ms.saturating_sub(now_ms).min(total_budget_cap_ms))
    });

    let ignored = AtomicBool::new(false);
    let write_tracker = if has_side_effects {
        dispatched
    } else {
        &ignored
    };
    let local_cancelled = || -> anyhow::Error {
        Failure::new(
            130,
            "cancelled",
            "interrupted; cancellation is not rollback",
        )
        .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
        .into()
    };

    // Stage 1a: connect and write the request frame under the admission deadline.
    let stage1a = async {
        let socket = if request.operation.management() {
            &paths.control_socket
        } else {
            &paths.socket
        };
        platform::validate_endpoint(socket)?;
        let mut stream = platform::local::connect(socket.as_os_str())
            .await
            .context("cannot connect to daemon")?;
        platform::validate_server(&stream)?;
        if platform::monotonic_millis()? >= request.deadline_ms {
            return Err(
                Failure::new(5, "timeout", "request expired before writing to daemon").into(),
            );
        }
        let (req_lease, req_frame) = ipc::encode_frame_leased(request, ipc::REQUEST_LIMIT, budget)
            .map(|(frame, lease)| (lease, frame))
            .map_err(|error| -> anyhow::Error {
                if error.kind() == io::ErrorKind::OutOfMemory {
                    Failure::new(
                        1,
                        "memory_limit_exceeded",
                        format!(
                            "IPC request exceeded the {} active memory limit",
                            format_bytes(budget.limit())
                        ),
                    )
                    .into()
                } else {
                    error.into()
                }
            })?;
        ipc::write_raw_idle(
            &mut stream,
            &req_frame,
            idle_timeout,
            Some(admission_deadline),
            write_tracker,
        )
        .await?;
        drop(req_frame);
        drop(req_lease);
        Ok::<_, anyhow::Error>(stream)
    };

    let map_stage1_err = |error: anyhow::Error| -> anyhow::Error {
        if error.downcast_ref::<Failure>().is_some() {
            return error;
        }
        if let Some(io_err) = error.downcast_ref::<io::Error>()
            && io_err.kind() == io::ErrorKind::TimedOut
        {
            return Failure::new(
                5,
                "timeout",
                "daemon deadline exceeded; cancellation is not rollback",
            )
            .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
            .into();
        }
        Failure::new(
            4,
            "daemon_transport",
            "daemon communication failed; tool calls are never replayed",
        )
        .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
        .into()
    };

    let mut stream = match interrupt {
        Some(token) => {
            let connect_fut = timeout_at(admission_deadline, stage1a);
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(local_cancelled()),
                outcome = connect_fut => match outcome {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => return Err(map_stage1_err(error)),
                    Err(_) => {
                        return Err(Failure::new(
                            5,
                            "timeout",
                            "daemon deadline exceeded; cancellation is not rollback",
                        )
                        .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
                        .into());
                    }
                },
            }
        }
        None => match timeout_at(admission_deadline, stage1a).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => return Err(map_stage1_err(error)),
            Err(_) => {
                return Err(Failure::new(
                    5,
                    "timeout",
                    "daemon deadline exceeded; cancellation is not rollback",
                )
                .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
                .into());
            }
        },
    };

    // Stage 1b: await the 4-byte response frame header. Unbounded dual-clock requests wait
    // without a client-side wall-clock cap until the daemon replies, closes, or errors.
    let mut header = [0u8; 4];
    let mut header_read = 0usize;
    let mut cancelled_after_sentinel = false;
    let mut grace_granted = false;
    let mut stage2_deadline = hard_deadline;

    let response_len = loop {
        let allow_zero = allow_grace && !grace_granted;
        let step = match interrupt {
            Some(token) => {
                let read_header = async {
                    read_header_resumable(
                        &mut stream,
                        &mut header,
                        &mut header_read,
                        ipc::RESPONSE_LIMIT,
                        allow_zero,
                    )
                    .await?
                    .context("daemon disconnected before returning a result")
                };
                match stage1b_deadline {
                    Some(deadline) => {
                        let timed = timeout_at(deadline, read_header);
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => Err(true),
                            outcome = timed => match outcome {
                                Ok(Ok(len)) => Ok(len),
                                Ok(Err(error)) => return Err(map_stage1_err(error)),
                                Err(_) => {
                                    return Err(Failure::new(
                                        5,
                                        "timeout",
                                        "daemon deadline exceeded; cancellation is not rollback",
                                    )
                                    .uncertain(
                                        has_side_effects && write_tracker.load(Ordering::Acquire),
                                    )
                                    .into());
                                }
                            },
                        }
                    }
                    None => {
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => Err(true),
                            outcome = read_header => match outcome {
                                Ok(len) => Ok(len),
                                Err(error) => return Err(map_stage1_err(error)),
                            },
                        }
                    }
                }
            }
            None => {
                let read_header = async {
                    read_header_resumable(
                        &mut stream,
                        &mut header,
                        &mut header_read,
                        ipc::RESPONSE_LIMIT,
                        allow_zero,
                    )
                    .await?
                    .context("daemon disconnected before returning a result")
                };
                match stage1b_deadline {
                    Some(deadline) => match timeout_at(deadline, read_header).await {
                        Ok(Ok(len)) => Ok(len),
                        Ok(Err(error)) => return Err(map_stage1_err(error)),
                        Err(_) => {
                            return Err(Failure::new(
                                5,
                                "timeout",
                                "daemon deadline exceeded; cancellation is not rollback",
                            )
                            .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
                            .into());
                        }
                    },
                    None => Ok(read_header.await.map_err(map_stage1_err)?),
                }
            }
        };

        match step {
            Ok(0) => {
                grace_granted = true;
                stage1b_deadline = stage1b_deadline.map(|d| d + DUAL_CLOCK_GRACE);
                stage2_deadline = stage2_deadline.map(|d| d + DUAL_CLOCK_GRACE);
            }
            Ok(len) => break len,
            Err(true) => {
                cancelled_after_sentinel = true;
                let cancel_deadline = Instant::now() + CANCEL_SENTINEL_WAIT;
                stage2_deadline = Some(cancel_deadline);
                let _ = timeout_at(cancel_deadline, async {
                    stream.write_all(&[0u8]).await?;
                    stream.flush().await
                })
                .await;
                let len = loop {
                    match timeout_at(
                        cancel_deadline,
                        read_header_resumable(
                            &mut stream,
                            &mut header,
                            &mut header_read,
                            ipc::RESPONSE_LIMIT,
                            allow_grace && !grace_granted,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(Some(0))) => {
                            grace_granted = true;
                        }
                        Ok(Ok(Some(len))) => break len,
                        _ => return Err(local_cancelled()),
                    }
                };
                break len;
            }
            Err(false) => unreachable!(),
        }
    };

    // Stage 2: read validated response body using local 60s idle clock capped by explicit deadline.
    let body_result: io::Result<(Response, ByteLease)> = match (cancelled_after_sentinel, interrupt)
    {
        (false, Some(token)) => {
            let read_body = ipc::read_frame_body_idle(
                &mut stream,
                response_len,
                idle_timeout,
                stage2_deadline,
                budget,
            );
            tokio::pin!(read_body);
            tokio::select! {
                biased;
                res = &mut read_body => res,
                _ = token.cancelled() => {
                    cancelled_after_sentinel = true;
                    match timeout(CANCEL_SENTINEL_WAIT, &mut read_body).await {
                        Ok(res) => res,
                        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "IPC cancel wait timed out")),
                    }
                }
            }
        }
        _ => {
            ipc::read_frame_body_idle(
                &mut stream,
                response_len,
                idle_timeout,
                stage2_deadline,
                budget,
            )
            .await
        }
    };
    let (response_lease, mut response): (ByteLease, Response) = match body_result {
        Ok((response, lease)) => (lease, response),
        Err(error) if error.kind() == io::ErrorKind::OutOfMemory => {
            return Err(Failure::new(
                1,
                "memory_limit_exceeded",
                format!(
                    "daemon response exceeded the {} active memory limit; tool calls are never replayed",
                    format_bytes(budget.limit())
                ),
            )
            .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
            .into());
        }
        Err(_) if cancelled_after_sentinel => return Err(local_cancelled()),
        Err(error) if error.kind() == io::ErrorKind::TimedOut => {
            return Err(Failure::new(
                5,
                "timeout",
                "daemon deadline exceeded; cancellation is not rollback",
            )
            .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
            .into());
        }
        Err(_) => {
            return Err(Failure::new(
                4,
                "daemon_transport",
                "daemon communication failed; tool calls are never replayed",
            )
            .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
            .into());
        }
    };

    if response.protocol != ipc::PROTOCOL {
        return Err(Failure::new(
            6,
            "daemon_mismatch",
            "daemon protocol does not match; use the matching mcpcli version to run daemon stop, then use the current version to run daemon start",
        )
        .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
        .into());
    }
    if response.namespace != paths.namespace {
        return Err(Failure::new(
            4,
            "daemon_transport",
            "daemon IPC identity does not match; tool calls are never replayed",
        )
        .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
        .into());
    }
    if !request.operation.management() && response.build != paths.build {
        return Err(Failure::new(
            6,
            "daemon_mismatch",
            "daemon binary changed; run daemon restart",
        )
        .into());
    }
    if let Some(mut error) = response.error.take() {
        error.lease = Some(Arc::new(response_lease));
        return Err(error.into());
    }
    if response.result.is_none() {
        if cancelled_after_sentinel {
            return Err(local_cancelled());
        }
        return Err(Failure::new(
            4,
            "daemon_transport",
            "daemon response has no result; tool calls are never replayed",
        )
        .uncertain(has_side_effects && write_tracker.load(Ordering::Acquire))
        .into());
    }
    Ok((response, response_lease))
}

pub struct DaemonEventStream {
    stream: platform::local::Client,
    namespace: String,
    build: String,
    idle_timeout: Duration,
    budget: Arc<ByteBudget>,
}

impl DaemonEventStream {
    pub async fn next_frame(&mut self) -> Result<Option<(StreamFrameKind, ByteLease)>> {
        let Some(len) = ipc::read_frame_header(&mut self.stream, ipc::RESPONSE_LIMIT)
            .await
            .map_err(|_| {
                Failure::new(
                    4,
                    "daemon_transport",
                    "daemon event stream disconnected unexpectedly",
                )
            })?
        else {
            return Ok(None);
        };
        let (frame, lease): (StreamFrame, ByteLease) =
            ipc::read_frame_body_idle(&mut self.stream, len, self.idle_timeout, None, &self.budget)
                .await
                .map_err(|error| -> anyhow::Error {
                    if error.kind() == io::ErrorKind::TimedOut {
                        Failure::new(5, "timeout", "daemon event stream frame read stalled").into()
                    } else if error.kind() == io::ErrorKind::OutOfMemory {
                        Failure::new(
                            1,
                            "memory_limit_exceeded",
                            format!(
                                "daemon event frame exceeded the {} active memory limit",
                                format_bytes(self.budget.limit())
                            ),
                        )
                        .into()
                    } else {
                        Failure::new(
                            4,
                            "daemon_transport",
                            "daemon event stream communication failed",
                        )
                        .into()
                    }
                })?;
        if frame.protocol != ipc::PROTOCOL {
            return Err(Failure::new(
                6,
                "daemon_mismatch",
                "daemon protocol does not match; use the matching mcpcli version to run daemon stop, then use the current version to run daemon start",
            )
            .into());
        }
        if frame.namespace != self.namespace {
            return Err(
                Failure::new(4, "daemon_transport", "daemon IPC identity does not match").into(),
            );
        }
        if frame.build != self.build {
            return Err(Failure::new(
                6,
                "daemon_mismatch",
                "daemon binary changed; run daemon restart",
            )
            .into());
        }
        Ok(Some((frame.kind, lease)))
    }
}

pub async fn open_event_stream(paths: &Paths, request: &Request) -> Result<DaemonEventStream> {
    open_event_stream_with_options(
        paths,
        request,
        ipc::TRANSFER_IDLE_TIMEOUT,
        ByteBudget::global(),
    )
    .await
}

pub async fn open_event_stream_with_options(
    paths: &Paths,
    request: &Request,
    idle_timeout: Duration,
    budget: &Arc<ByteBudget>,
) -> Result<DaemonEventStream> {
    let connect_budget = Duration::from_millis(request.timeout_ms.clamp(1, 30_000));
    let connect_deadline = Instant::now() + connect_budget;
    let ignored = AtomicBool::new(false);
    let stream = timeout_at(connect_deadline, async {
        platform::validate_endpoint(&paths.socket)?;
        let mut stream = platform::local::connect(paths.socket.as_os_str())
            .await
            .context("cannot connect to daemon")?;
        platform::validate_server(&stream)?;
        let (req_frame, req_lease) = ipc::encode_frame_leased(request, ipc::REQUEST_LIMIT, budget)
            .map_err(|error| -> anyhow::Error {
                if error.kind() == io::ErrorKind::OutOfMemory {
                    Failure::new(
                        1,
                        "memory_limit_exceeded",
                        format!(
                            "IPC request exceeded the {} active memory limit",
                            format_bytes(budget.limit())
                        ),
                    )
                    .into()
                } else {
                    error.into()
                }
            })?;
        ipc::write_raw_idle(
            &mut stream,
            &req_frame,
            idle_timeout,
            Some(connect_deadline),
            &ignored,
        )
        .await?;
        drop(req_frame);
        drop(req_lease);
        Ok::<_, anyhow::Error>(stream)
    })
    .await
    .map_err(|_| Failure::new(5, "timeout", "timed out opening daemon event stream"))??;

    Ok(DaemonEventStream {
        stream,
        namespace: paths.namespace.clone(),
        build: paths.build.clone(),
        idle_timeout,
        budget: budget.clone(),
    })
}

pub async fn ensure(paths: &Paths, loaded: &LoadedConfig, explicit: bool) -> Result<()> {
    let _startup = Lock::acquire(&paths.startup_lock(), START_WAIT).await?;
    if paths.running()? {
        let (_response_lease, response) = exchange(
            paths,
            &paths.request("", Operation::Status, Duration::from_secs(2))?,
        )
        .await
        .map(|(response, lease)| (lease, response))?;
        if response.build != paths.build {
            return Err(Failure::new(
                6,
                "daemon_mismatch",
                "daemon binary changed; run daemon restart",
            )
            .into());
        }
        // Status remains usable with stale config; actual requests enforce the config digest.
        return Ok(());
    }
    if let Some(state) = State::load(paths)? {
        if !state.clean_stop && !explicit {
            return Err(Failure::new(6,"state_lost",
                "previous daemon ended unexpectedly; state may be lost. Explicitly run daemon start or daemon restart. A process supervisor may have terminated the background process; daemon run supports foreground supervision.").into());
        }
        if !state.clean_stop {
            eprintln!(
                "mcpcli: starting a new daemon after an unclean stop; previous in-memory state is lost"
            );
        }
    }
    let log = secure_file(&paths.directory.join("startup.log"))?;
    log.set_len(0)?;
    let mut command = Command::new(platform::image_path()?);
    command
        .args(["--config"])
        .arg(&loaded.path)
        .args(["--session", &paths.session, "daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    let mut child = platform::spawn_daemon(&mut command).context("cannot start daemon")?;
    let deadline = Instant::now() + START_WAIT;
    while Instant::now() < deadline {
        if paths.running()?
            && exchange(
                paths,
                &paths.request("", Operation::Status, Duration::from_millis(100))?,
            )
            .await
            .is_ok()
        {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            bail!(
                "daemon exited during startup; inspect {} (child {status})",
                paths.directory.join("startup.log").display()
            );
        }
        sleep(Duration::from_millis(25)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    bail!("daemon did not become ready before the startup deadline")
}

pub async fn stop(paths: &Paths) -> Result<serde_json::Value> {
    let _startup = Lock::acquire(&paths.startup_lock(), STOP_WAIT).await?;
    if !paths.running()? {
        return Ok(serde_json::json!({"stopped":false,"running":false}));
    }
    if let Err(error) = exchange(
        paths,
        &paths.request("", Operation::Stop, Duration::from_secs(2))?,
    )
    .await
        && paths.running()?
    {
        return Err(error);
    }
    let deadline = Instant::now() + STOP_WAIT;
    while paths.running()? {
        if Instant::now() >= deadline {
            bail!("daemon is still stopping after its shutdown deadline");
        }
        sleep(Duration::from_millis(25)).await;
    }
    if let Some(state) = State::load(paths)?
        && !state.clean_stop
    {
        bail!(
            "daemon exited without confirming complete cleanup: {}",
            state.cleanup_errors.join("; ")
        );
    }
    Ok(serde_json::json!({"stopped":true,"running":false}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    #[cfg(unix)]
    #[test]
    fn private_directory_refuses_links_and_weak_modes() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let base_path = base.path().canonicalize().unwrap();
        let real = base_path.join("real");
        private_dir(&real).unwrap();
        let link = base_path.join("link");
        symlink(&real, &link).unwrap();
        assert!(private_dir(&link).is_err());
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(private_dir(&real).is_err());
    }
    #[test]
    fn locks_remain_singletons_and_refuse_hardlinks() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let base_path = base.path().canonicalize().unwrap();
        let path = base_path.join("lock");
        let first = Lock::try_acquire(&path).unwrap().unwrap();
        assert!(Lock::try_acquire(&path).unwrap().is_none());
        drop(first);
        assert!(Lock::try_acquire(&path).unwrap().is_some());
        std::fs::hard_link(&path, base_path.join("alias")).unwrap();
        assert!(Lock::try_acquire(&path).is_err());
    }
    #[test]
    fn namespace_isolates_config_and_session() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let base_path = base.path().canonicalize().unwrap();
        let root = base_path.join("runtime");
        let a = Paths::with_root(base_path.join("a.json"), "a", &root).unwrap();
        let b = Paths::with_root(base_path.join("a.json"), "b", &root).unwrap();
        let c = Paths::with_root(base_path.join("b.json"), "a", &root).unwrap();
        assert_ne!(a.namespace, b.namespace);
        assert_ne!(a.namespace, c.namespace);
    }
    #[test]
    fn state_store_replaces_stale_temp_file_from_crashed_pid() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let base_path = base.path().canonicalize().unwrap();
        let root = base_path.join("runtime");
        let paths = Paths::with_root(base_path.join("mcpcli.json"), "default", &root).unwrap();
        std::fs::write(paths.directory.join("state.json.tmp"), b"partial").unwrap();
        let state = State::fresh();
        state.store(&paths).unwrap();
        let loaded = State::load(&paths).unwrap().unwrap();
        assert_eq!(loaded.epoch, state.epoch);
        assert!(!paths.directory.join("state.json.tmp").exists());
    }
    #[test]
    fn call_request_keeps_idle_and_shared_deadlines_after_startup() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let root = base.path().canonicalize().unwrap().join("runtime");
        let paths = Paths::with_root(root.join("config.json"), "budgets", &root).unwrap();
        for hard_ms in [None, Some(1_000), Some(5_000)] {
            let before = platform::monotonic_millis().unwrap();
            let request = paths
                .request_call_with_budgets(
                    "digest",
                    Operation::Call {
                        server: "server".into(),
                        tool: "tool".into(),
                        arguments: serde_json::Map::new(),
                        inline_media: false,
                        detach: false,
                        expected_schema: None,
                    },
                    Duration::from_millis(1_000),
                    hard_ms.map(Duration::from_millis),
                    Duration::from_millis(700),
                )
                .unwrap();
            let after = platform::monotonic_millis().unwrap();
            assert_eq!(request.timeout_ms, 1_000);
            assert_eq!(request.max_timeout_ms, hard_ms);
            let remaining_total = hard_ms.unwrap_or(1_000) - 700;
            assert!(
                (before + remaining_total..=after + remaining_total).contains(&request.deadline_ms)
            );
            assert!((before + 300..=after + 300).contains(&request.admission_deadline_ms()));
            assert_eq!(
                request.explicit_deadline_ms,
                hard_ms.map(|_| request.deadline_ms)
            );
        }
    }

    #[tokio::test]
    async fn expired_call_admission_does_not_connect_with_hard_time_remaining() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let root = base.path().canonicalize().unwrap().join("runtime");
        let paths = Paths::with_root(root.join("config.json"), "budgets", &root).unwrap();
        let request = paths
            .request_call_with_budgets(
                "digest",
                Operation::Call {
                    server: "server".into(),
                    tool: "tool".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_secs(1),
                Some(Duration::from_secs(5)),
                Duration::from_secs(2),
            )
            .unwrap();
        assert!(request.deadline_ms > platform::monotonic_millis().unwrap());
        let dispatched = AtomicBool::new(false);
        let error = exchange_tracked(&paths, &request, &dispatched)
            .await
            .unwrap_err();
        let failure = error.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.kind, "timeout");
        assert!(!failure.outcome_unknown);
        assert!(!dispatched.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn expired_request_never_connects_or_marks_dispatched() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let root = base.path().canonicalize().unwrap().join("runtime");
        let paths = Paths::with_root(root.join("config.json"), "default", &root).unwrap();
        let mut request = paths
            .request(
                "digest",
                Operation::Call {
                    server: "server".into(),
                    tool: "tool".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_secs(1),
            )
            .unwrap();
        request.deadline_ms = 0;
        let dispatched = AtomicBool::new(false);
        let error = exchange_tracked(&paths, &request, &dispatched)
            .await
            .unwrap_err();
        let failure = error.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.kind, "timeout");
        assert!(!failure.outcome_unknown);
        assert!(!dispatched.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn pre_write_transport_failure_is_not_outcome_unknown() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let base_path = base.path().canonicalize().unwrap();
        let root = base_path.join("runtime");
        let paths = Paths::with_root(base_path.join("mcpcli.json"), "default", &root).unwrap();
        let req = paths
            .request(
                "digest",
                Operation::Call {
                    server: "s".into(),
                    tool: "t".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_secs(2),
            )
            .unwrap();
        let err = exchange(&paths, &req).await.unwrap_err();
        let failure = err.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.kind, "daemon_transport");
        assert!(!failure.outcome_unknown);
    }

    #[tokio::test]
    async fn dual_clock_max_timeout_allows_grace_window_for_final_response() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let root = base.path().canonicalize().unwrap().join("runtime");
        let paths = Paths::with_root(root.join("config.json"), "grace", &root).unwrap();
        let mut listener = platform::local::Listener::bind(paths.socket.as_os_str()).unwrap();
        platform::secure_endpoint(&paths.socket).unwrap();
        let expected_ns = paths.namespace.clone();
        let expected_build = paths.build.clone();
        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let (_req, _lease) = ipc::read_frame_with_budget::<_, Request>(
                &mut conn,
                ipc::REQUEST_LIMIT,
                ByteBudget::global(),
            )
            .await
            .unwrap()
            .unwrap();
            // Signal task polling grace with [0u8; 4], then reply after remaining_total (150ms).
            conn.write_all(&[0u8; 4]).await.unwrap();
            conn.flush().await.unwrap();
            sleep(Duration::from_millis(300)).await;
            let resp = Response {
                protocol: ipc::PROTOCOL,
                namespace: expected_ns,
                build: expected_build,
                epoch: "e1".into(),
                result: Some(serde_json::json!({"ok": true})),
                error: None,
            };
            let (frame, _lease) =
                ipc::encode_frame_leased(&resp, ipc::RESPONSE_LIMIT, ByteBudget::global()).unwrap();
            conn.write_all(&frame).await.unwrap();
            conn.flush().await.unwrap();
        });

        let req = paths
            .request_call_with_budgets(
                "digest",
                Operation::TasksGet {
                    server: "s".into(),
                    task_id: "t-1".into(),
                    wait: true,
                    inline_media: false,
                },
                Duration::from_millis(150),
                Some(Duration::from_millis(150)),
                Duration::ZERO,
            )
            .unwrap();
        let (resp, _lease) = exchange(&paths, &req).await.unwrap();
        assert_eq!(resp.result.unwrap()["ok"], true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellable_exchange_sends_sentinel_and_preserves_cancelled_failure() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let root = base.path().canonicalize().unwrap().join("runtime");
        let paths = Paths::with_root(root.join("config.json"), "cancel-err", &root).unwrap();
        let mut listener = platform::local::Listener::bind(paths.socket.as_os_str()).unwrap();
        platform::secure_endpoint(&paths.socket).unwrap();
        let expected_ns = paths.namespace.clone();
        let expected_build = paths.build.clone();
        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let (_req, _lease) = ipc::read_frame_with_budget::<_, Request>(
                &mut conn,
                ipc::REQUEST_LIMIT,
                ByteBudget::global(),
            )
            .await
            .unwrap()
            .unwrap();
            let mut sentinel = [0xffu8; 1];
            let n = conn.read(&mut sentinel).await.unwrap();
            assert_eq!(n, 1);
            assert_eq!(sentinel, [0u8]);
            let mut failure = Failure::new(4, "cancelled", "cancelled on daemon").uncertain(true);
            failure.data = Some(serde_json::json!({"taskId": "task-42"}));
            let resp = Response {
                protocol: ipc::PROTOCOL,
                namespace: expected_ns,
                build: expected_build,
                epoch: "e1".into(),
                result: None,
                error: Some(failure),
            };
            let (frame, _lease) =
                ipc::encode_frame_leased(&resp, ipc::RESPONSE_LIMIT, ByteBudget::global()).unwrap();
            conn.write_all(&frame).await.unwrap();
            conn.flush().await.unwrap();
        });

        let req = paths
            .request_call_with_budgets(
                "digest",
                Operation::Call {
                    server: "s".into(),
                    tool: "t".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_secs(5),
                None,
                Duration::ZERO,
            )
            .unwrap();
        let dispatched = AtomicBool::new(false);
        let interrupt = CancellationToken::new();
        let trigger = interrupt.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });
        let err = exchange_cancellable(&paths, &req, &dispatched, &interrupt)
            .await
            .unwrap_err();
        let failure = err.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.kind, "cancelled");
        assert!(failure.outcome_unknown);
        assert_eq!(failure.data.as_ref().unwrap()["taskId"], "task-42");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellable_exchange_preserves_natural_completion_race() {
        #[cfg(unix)]
        let base = tempfile::tempdir_in("/tmp").unwrap();
        #[cfg(windows)]
        let base = tempfile::tempdir().unwrap();
        let root = base.path().canonicalize().unwrap().join("runtime");
        let paths = Paths::with_root(root.join("config.json"), "cancel-ok", &root).unwrap();
        let mut listener = platform::local::Listener::bind(paths.socket.as_os_str()).unwrap();
        platform::secure_endpoint(&paths.socket).unwrap();
        let expected_ns = paths.namespace.clone();
        let expected_build = paths.build.clone();
        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let (_req, _lease) = ipc::read_frame_with_budget::<_, Request>(
                &mut conn,
                ipc::REQUEST_LIMIT,
                ByteBudget::global(),
            )
            .await
            .unwrap()
            .unwrap();
            let mut sentinel = [0xffu8; 1];
            let _ = conn.read(&mut sentinel).await.unwrap();
            let resp = Response {
                protocol: ipc::PROTOCOL,
                namespace: expected_ns,
                build: expected_build,
                epoch: "e1".into(),
                result: Some(serde_json::json!({"content": [{"type": "text", "text": "done"}]})),
                error: None,
            };
            let (frame, _lease) =
                ipc::encode_frame_leased(&resp, ipc::RESPONSE_LIMIT, ByteBudget::global()).unwrap();
            conn.write_all(&frame).await.unwrap();
            conn.flush().await.unwrap();
        });

        let req = paths
            .request_call_with_budgets(
                "digest",
                Operation::Call {
                    server: "s".into(),
                    tool: "t".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
                Duration::from_secs(5),
                None,
                Duration::ZERO,
            )
            .unwrap();
        let dispatched = AtomicBool::new(false);
        let interrupt = CancellationToken::new();
        let trigger = interrupt.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });
        let (resp, _lease) = exchange_cancellable(&paths, &req, &dispatched, &interrupt)
            .await
            .unwrap();
        assert_eq!(resp.result.unwrap()["content"][0]["text"], "done");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellable_exchange_falls_back_to_cancelled_on_eof_never_daemon_transport() {
        for (is_call, op) in [
            (
                true,
                Operation::Call {
                    server: "s".into(),
                    tool: "t".into(),
                    arguments: serde_json::Map::new(),
                    inline_media: false,
                    detach: false,
                    expected_schema: None,
                },
            ),
            (
                false,
                Operation::TasksGet {
                    server: "s".into(),
                    task_id: "t-1".into(),
                    wait: true,
                    inline_media: false,
                },
            ),
        ] {
            #[cfg(unix)]
            let base = tempfile::tempdir_in("/tmp").unwrap();
            #[cfg(windows)]
            let base = tempfile::tempdir().unwrap();
            let root = base.path().canonicalize().unwrap().join("runtime");
            let paths = Paths::with_root(root.join("config.json"), "cancel-eof", &root).unwrap();
            let mut listener = platform::local::Listener::bind(paths.socket.as_os_str()).unwrap();
            platform::secure_endpoint(&paths.socket).unwrap();
            let server = tokio::spawn(async move {
                let mut conn = listener.accept().await.unwrap();
                let (_req, _lease) = ipc::read_frame_with_budget::<_, Request>(
                    &mut conn,
                    ipc::REQUEST_LIMIT,
                    ByteBudget::global(),
                )
                .await
                .unwrap()
                .unwrap();
                let mut sentinel = [0xffu8; 1];
                let _ = conn.read(&mut sentinel).await.unwrap();
                // Drop connection immediately (EOF) without sending a response.
            });

            let req = paths
                .request_call_with_budgets(
                    "digest",
                    op,
                    Duration::from_secs(5),
                    None,
                    Duration::ZERO,
                )
                .unwrap();
            let dispatched = AtomicBool::new(false);
            let interrupt = CancellationToken::new();
            let trigger = interrupt.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(50)).await;
                trigger.cancel();
            });
            let err = exchange_cancellable(&paths, &req, &dispatched, &interrupt)
                .await
                .unwrap_err();
            let failure = err.downcast_ref::<Failure>().unwrap();
            assert_eq!(failure.kind, "cancelled");
            assert_eq!(failure.code, 130);
            assert_eq!(failure.outcome_unknown, is_call);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn tasks_update_and_cancel_post_write_disconnect_marks_outcome_unknown() {
        let mut responses = serde_json::Map::new();
        responses.insert("answer".into(), serde_json::json!("yes"));

        for op in [
            Operation::TasksUpdate {
                server: "s".into(),
                task_id: "task-1".into(),
                input_responses: responses,
            },
            Operation::TasksCancel {
                server: "s".into(),
                task_id: "task-1".into(),
            },
        ] {
            #[cfg(unix)]
            let base = tempfile::tempdir_in("/tmp").unwrap();
            #[cfg(windows)]
            let base = tempfile::tempdir().unwrap();
            let root = base.path().canonicalize().unwrap().join("runtime");
            let paths = Paths::with_root(root.join("config.json"), "side-fx", &root).unwrap();

            // Pre-write transport failure (no listener): outcome_unknown stays false.
            let pre_req = paths
                .request("digest", op.clone(), Duration::from_secs(2))
                .unwrap();
            let pre_dispatched = AtomicBool::new(false);
            let pre_err = exchange_tracked(&paths, &pre_req, &pre_dispatched)
                .await
                .unwrap_err();
            let pre_failure = pre_err.downcast_ref::<Failure>().unwrap();
            assert_eq!(pre_failure.kind, "daemon_transport");
            assert!(!pre_failure.outcome_unknown);
            assert!(!pre_dispatched.load(Ordering::Acquire));

            // Post-write disconnect: outcome_unknown must be true.
            let mut listener = platform::local::Listener::bind(paths.socket.as_os_str()).unwrap();
            platform::secure_endpoint(&paths.socket).unwrap();
            let server = tokio::spawn(async move {
                let mut conn = listener.accept().await.unwrap();
                let (_req, _lease) = ipc::read_frame_with_budget::<_, Request>(
                    &mut conn,
                    ipc::REQUEST_LIMIT,
                    ByteBudget::global(),
                )
                .await
                .unwrap()
                .unwrap();
                // Drop connection immediately after receiving request.
            });

            let req = paths.request("digest", op, Duration::from_secs(2)).unwrap();
            let dispatched = AtomicBool::new(false);
            let err = exchange_tracked(&paths, &req, &dispatched)
                .await
                .unwrap_err();
            let failure = err.downcast_ref::<Failure>().unwrap();
            assert_eq!(failure.kind, "daemon_transport");
            assert!(failure.outcome_unknown);
            assert!(dispatched.load(Ordering::Acquire));
            server.await.unwrap();
        }
    }
}
