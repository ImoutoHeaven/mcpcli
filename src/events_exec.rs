//! Cross-platform event callback executor (at-least-once) and atomic cursor persistence.
use crate::platform;
use serde::Deserialize;
use std::{
    collections::{VecDeque, hash_map::RandomState},
    fmt,
    hash::{BuildHasher, Hasher},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    task::JoinHandle,
    time::timeout,
};

#[cfg(windows)]
use crate::platform::windows_job;

pub const DEFAULT_EXEC_TIMEOUT: Duration = Duration::from_secs(30);
pub const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_STDERR_BYTES: usize = 64 * 1024;
const MAX_CURSOR_BYTES: usize = 64 * 1024;
const MAX_ACK_LINE_BYTES: usize = 64 * 1024;
const STDERR_DRAIN_WAIT: Duration = Duration::from_millis(200);

static TMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn unique_temp_path(target: &Path) -> io::Result<(PathBuf, PathBuf)> {
    let name = target
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cursor path has no file name"))?
        .to_string_lossy();
    let parent = match target.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let pid = std::process::id();
    let seq = TMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut h = RandomState::new().build_hasher();
    h.write_u32(pid);
    h.write_u64(seq);
    h.write_u128(nanos);
    let rand = h.finish();
    let tmp_name = format!(".{name}.tmp-{pid}-{seq}-{rand:016x}");
    Ok((parent.clone(), parent.join(tmp_name)))
}

/// Atomic file-backed cursor storage.
#[derive(Clone, Debug)]
pub struct CursorStore {
    path: PathBuf,
}

impl CursorStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read(&self) -> io::Result<Option<String>> {
        let file = match platform::open_input(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cursor file must be a regular file",
            ));
        }
        let mut buf = Vec::new();
        file.take((MAX_CURSOR_BYTES + 1) as u64)
            .read_to_end(&mut buf)?;
        if buf.len() > MAX_CURSOR_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cursor file exceeds 64 KiB limit",
            ));
        }
        let text = String::from_utf8(buf)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        Ok(Some(text))
    }

    pub fn clear(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => {
                let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty());
                platform::sync_directory(parent.unwrap_or(Path::new(".")))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn write(&self, cursor: &str) -> io::Result<()> {
        if cursor.len() > MAX_CURSOR_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cursor exceeds 64 KiB limit",
            ));
        }
        let (parent, tmp_path) = unique_temp_path(&self.path)?;
        if parent != Path::new(".") {
            std::fs::create_dir_all(&parent)?;
        }
        let mut file = platform::new_private_file(&tmp_path)?;
        let outcome = (|| -> io::Result<()> {
            file.write_all(cursor.as_bytes())?;
            file.sync_all()?;
            drop(file);
            platform::replace_file_with_retry(&tmp_path, &self.path)?;
            platform::sync_directory(&parent)?;
            Ok(())
        })();
        if outcome.is_err() {
            let _ = std::fs::remove_file(&tmp_path);
        }
        outcome
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecMode {
    Oneshot,
    Stream,
}

#[derive(Debug)]
pub enum ExecError {
    InvalidConfig(String),
    Spawn(io::Error),
    Io { source: io::Error, stderr: String },
    NonZeroExit { code: Option<i32>, stderr: String },
    Timeout { timeout: Duration, stderr: String },
    StreamClosed { stderr: String },
    InvalidAck { line: String, stderr: String },
    Rejected { message: String, stderr: String },
}

impl ExecError {
    pub fn stderr(&self) -> &str {
        match self {
            Self::InvalidConfig(_) | Self::Spawn(_) => "",
            Self::Io { stderr, .. }
            | Self::NonZeroExit { stderr, .. }
            | Self::Timeout { stderr, .. }
            | Self::StreamClosed { stderr }
            | Self::InvalidAck { stderr, .. }
            | Self::Rejected { stderr, .. } => stderr,
        }
    }
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(msg) => write!(f, "invalid callback config: {msg}"),
            Self::Spawn(err) => write!(f, "failed to spawn callback: {err}"),
            Self::Io { source, stderr } => {
                write!(f, "callback I/O error: {source}")?;
                fmt_stderr(f, stderr)
            }
            Self::NonZeroExit { code, stderr } => {
                match code {
                    Some(c) => write!(f, "callback exited with status {c}")?,
                    None => write!(f, "callback terminated by signal")?,
                }
                fmt_stderr(f, stderr)
            }
            Self::Timeout { timeout, stderr } => {
                write!(f, "callback timed out after {}ms", timeout.as_millis())?;
                fmt_stderr(f, stderr)
            }
            Self::StreamClosed { stderr } => {
                write!(f, "stream callback closed stdout before ack")?;
                fmt_stderr(f, stderr)
            }
            Self::InvalidAck { line, stderr } => {
                write!(f, "stream callback returned invalid ack: {line}")?;
                fmt_stderr(f, stderr)
            }
            Self::Rejected { message, stderr } => {
                write!(f, "stream callback rejected event: {message}")?;
                fmt_stderr(f, stderr)
            }
        }
    }
}

impl std::error::Error for ExecError {}

fn fmt_stderr(f: &mut fmt::Formatter<'_>, stderr: &str) -> fmt::Result {
    let trimmed = stderr.trim();
    if !trimmed.is_empty() {
        write!(f, " (stderr: {trimmed})")?;
    }
    Ok(())
}

#[derive(Clone, Default)]
struct BoundedStderr {
    buf: Arc<Mutex<VecDeque<u8>>>,
}

impl BoundedStderr {
    fn spawn(mut stderr: ChildStderr) -> (Self, JoinHandle<()>) {
        let store = Self::default();
        let shared = store.buf.clone();
        let handle = tokio::spawn(async move {
            let mut chunk = [0u8; 4096];
            loop {
                match stderr.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let mut guard = shared.lock().unwrap_or_else(|p| p.into_inner());
                        if n >= MAX_STDERR_BYTES {
                            guard.clear();
                            guard.extend(&chunk[n - MAX_STDERR_BYTES..n]);
                        } else {
                            let overflow = guard
                                .len()
                                .saturating_add(n)
                                .saturating_sub(MAX_STDERR_BYTES);
                            if overflow > 0 {
                                guard.drain(..overflow);
                            }
                            guard.extend(&chunk[..n]);
                        }
                    }
                }
            }
        });
        (store, handle)
    }

    fn snapshot(&self) -> String {
        let guard = self.buf.lock().unwrap_or_else(|p| p.into_inner());
        let (a, b) = guard.as_slices();
        let mut bytes = Vec::with_capacity(guard.len());
        bytes.extend_from_slice(a);
        bytes.extend_from_slice(b);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

struct ProcessTree {
    child: Option<Child>,
    #[cfg(unix)]
    group: Option<i32>,
    #[cfg(windows)]
    job: Option<windows_job::Job>,
}

impl ProcessTree {
    fn spawn(argv: &[String], mode: ExecMode) -> io::Result<Self> {
        let mut command = Command::new(&argv[0]);
        command
            .args(&argv[1..])
            .stdin(Stdio::piped())
            .stdout(match mode {
                ExecMode::Oneshot => Stdio::null(),
                ExecMode::Stream => Stdio::piped(),
            })
            .stderr(Stdio::piped())
            .kill_on_drop(false);

        #[cfg(unix)]
        platform::configure_child_process_group(&mut command);

        #[cfg(windows)]
        command.creation_flags(
            windows_sys::Win32::System::Threading::CREATE_NO_WINDOW
                | windows_sys::Win32::System::Threading::CREATE_SUSPENDED,
        );

        #[cfg_attr(not(windows), allow(unused_mut))]
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

        Ok(Self {
            child: Some(child),
            #[cfg(unix)]
            group,
            #[cfg(windows)]
            job,
        })
    }

    fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.as_mut().and_then(|c| c.stdin.take())
    }

    fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.as_mut().and_then(|c| c.stdout.take())
    }

    fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.as_mut().and_then(|c| c.stderr.take())
    }

    #[cfg(test)]
    fn id(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    async fn wait_exit(&mut self) -> io::Result<std::process::ExitStatus> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("callback child already reaped"))?;
        let status = child.wait().await?;
        #[cfg(unix)]
        {
            self.group = None;
        }
        self.child = None;
        Ok(status)
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) {
        if let Some(group) = self.group {
            platform::signal_process_group(group, signal);
        }
    }

    #[cfg(unix)]
    fn exited_unreaped(&mut self) -> io::Result<bool> {
        match platform::child_exited_unreaped(&mut self.group) {
            Err(error) if error.raw_os_error() == Some(libc::ECHILD) => Ok(true),
            other => other,
        }
    }

    #[cfg(unix)]
    async fn wait_unreaped(&mut self, wait: Duration) -> bool {
        platform::wait_child_unreaped(&mut self.group, wait)
            .await
            .unwrap_or(true)
    }

    /// Kill the process tree after timeout: SIGTERM + grace + SIGKILL on Unix, TerminateJobObject on Windows.
    #[cfg(unix)]
    async fn kill_tree(&mut self, grace: Duration) {
        if self.child.is_none() {
            return;
        }
        if !matches!(self.exited_unreaped(), Ok(true)) {
            self.signal_group(libc::SIGTERM);
            let _ = self.wait_unreaped(grace).await;
        }
        // The unreaped leader still pins the PGID until SIGKILL reaches every member.
        self.signal_group(libc::SIGKILL);
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        self.group = None;
        self.child = None;
    }

    /// Close stdin, wait grace period for clean exit, then kill remaining process tree.
    #[cfg(unix)]
    async fn shutdown_graceful(&mut self, stdin: Option<ChildStdin>, grace: Duration) {
        if let Some(mut stdin) = stdin {
            let _ = timeout(Duration::from_millis(500), stdin.shutdown()).await;
            drop(stdin);
        }
        if self.child.is_none() {
            return;
        }
        if !self.wait_unreaped(grace).await {
            self.signal_group(libc::SIGTERM);
            let _ = self
                .wait_unreaped(Duration::from_millis(500).min(grace))
                .await;
        }
        self.signal_group(libc::SIGKILL);
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        self.group = None;
        self.child = None;
    }

    #[cfg(windows)]
    async fn kill_tree(&mut self, grace: Duration) {
        if let Some(child) = self.child.as_mut()
            && timeout(grace, child.wait()).await.is_ok()
        {
            self.child = None;
        }
        let need_job_terminate = self
            .job
            .as_ref()
            .is_some_and(|job| !job.is_empty().unwrap_or(true));
        if self.child.is_some() || need_job_terminate {
            if let Some(job) = &self.job {
                let _ = job.terminate();
            }
            let _ = timeout(Duration::from_secs(3), async {
                if let Some(child) = self.child.as_mut() {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
                if let Some(job) = &self.job {
                    while !job.is_empty().unwrap_or(true) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            })
            .await;
        }
        self.child = None;
        self.job = None;
    }

    #[cfg(windows)]
    async fn shutdown_graceful(&mut self, stdin: Option<ChildStdin>, grace: Duration) {
        if let Some(mut stdin) = stdin {
            let _ = timeout(Duration::from_millis(500), stdin.shutdown()).await;
            drop(stdin);
        }
        self.kill_tree(grace).await;
    }
}

impl Drop for ProcessTree {
    fn drop(&mut self) {
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

struct StreamWorker {
    tree: ProcessTree,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr: BoundedStderr,
    stderr_task: JoinHandle<()>,
}

impl StreamWorker {
    fn spawn(argv: &[String]) -> Result<Self, ExecError> {
        let mut tree = ProcessTree::spawn(argv, ExecMode::Stream).map_err(ExecError::Spawn)?;
        let stdin = tree.take_stdin().expect("piped stdin");
        let stdout = tree.take_stdout().expect("piped stdout");
        let stderr = tree.take_stderr().expect("piped stderr");
        let (stderr, stderr_task) = BoundedStderr::spawn(stderr);
        Ok(Self {
            tree,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            stderr,
            stderr_task,
        })
    }

    async fn shutdown(mut self, grace: Duration) -> String {
        let stdin = self.stdin.take();
        self.tree.shutdown_graceful(stdin, grace).await;
        let _ = timeout(STDERR_DRAIN_WAIT, &mut self.stderr_task).await;
        self.stderr_task.abort();
        self.stderr.snapshot()
    }
}

impl Drop for StreamWorker {
    fn drop(&mut self) {
        self.stderr_task.abort();
    }
}

#[derive(Deserialize)]
struct StreamAck {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

enum StreamReply {
    Ack(StreamAck),
    Invalid(String),
    Closed,
    Io(io::Error),
}

fn is_broken_pipe(err: &io::Error) -> bool {
    if err.kind() == io::ErrorKind::BrokenPipe {
        return true;
    }
    #[cfg(windows)]
    if matches!(err.raw_os_error(), Some(109 | 232)) {
        return true;
    }
    false
}

fn format_ndjson_line(event_json: &str) -> Result<std::borrow::Cow<'_, str>, ExecError> {
    if event_json.ends_with('\n') && !event_json[..event_json.len() - 1].contains(['\n', '\r']) {
        return Ok(std::borrow::Cow::Borrowed(event_json));
    }
    let trimmed = event_json.trim_end_matches(['\r', '\n']);
    if !trimmed.contains(['\n', '\r']) {
        let mut line = String::with_capacity(trimmed.len() + 1);
        line.push_str(trimmed);
        line.push('\n');
        return Ok(line.into());
    }
    let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|err| {
        ExecError::InvalidConfig(format!("invalid event JSON for stream mode: {err}"))
    })?;
    let mut line = serde_json::to_string(&value).map_err(|err| {
        ExecError::InvalidConfig(format!("failed to serialize event JSON: {err}"))
    })?;
    line.push('\n');
    Ok(line.into())
}

async fn read_bounded_line(reader: &mut BufReader<ChildStdout>) -> io::Result<Option<String>> {
    let mut buf = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(None);
            }
            break;
        }
        let newline = available.iter().position(|&b| b == b'\n');
        let take = newline.map_or(available.len(), |idx| idx + 1);
        if buf.len().saturating_add(take) > MAX_ACK_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stream callback ack line exceeds 64 KiB limit",
            ));
        }
        buf.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            break;
        }
    }
    while matches!(buf.last(), Some(b'\n' | b'\r')) {
        buf.pop();
    }
    String::from_utf8(buf)
        .map(Some)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

/// Orthogonal at-least-once callback executor (oneshot or persistent stream).
pub struct CallbackExecutor {
    argv: Vec<String>,
    mode: ExecMode,
    exec_timeout: Duration,
    shutdown_grace: Duration,
    worker: Option<StreamWorker>,
}

impl CallbackExecutor {
    pub fn new(
        argv: Vec<String>,
        mode: ExecMode,
        exec_timeout: Duration,
    ) -> Result<Self, ExecError> {
        if argv.is_empty() || argv[0].is_empty() {
            return Err(ExecError::InvalidConfig(
                "callback command argv must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            argv,
            mode,
            exec_timeout,
            shutdown_grace: GRACEFUL_SHUTDOWN_TIMEOUT,
            worker: None,
        })
    }

    pub fn mode(&self) -> ExecMode {
        self.mode
    }

    pub fn exec_timeout(&self) -> Duration {
        self.exec_timeout
    }

    #[cfg(test)]
    fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    #[cfg(test)]
    fn worker_pid(&self) -> Option<u32> {
        self.worker.as_ref().and_then(|w| w.tree.id())
    }

    pub async fn deliver(&mut self, event_json: &str) -> Result<(), ExecError> {
        match self.mode {
            ExecMode::Oneshot => self.deliver_oneshot(event_json).await,
            ExecMode::Stream => self.deliver_stream(event_json).await,
        }
    }

    pub async fn shutdown(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.shutdown(self.shutdown_grace).await;
        }
    }

    async fn deliver_oneshot(&self, event_json: &str) -> Result<(), ExecError> {
        let mut tree =
            ProcessTree::spawn(&self.argv, ExecMode::Oneshot).map_err(ExecError::Spawn)?;
        let mut stdin = tree.take_stdin().expect("piped stdin");
        let stderr = tree.take_stderr().expect("piped stderr");
        let (stderr_store, mut stderr_task) = BoundedStderr::spawn(stderr);

        let payload = if event_json.ends_with('\n') {
            std::borrow::Cow::Borrowed(event_json.as_bytes())
        } else {
            let mut owned = Vec::with_capacity(event_json.len() + 1);
            owned.extend_from_slice(event_json.as_bytes());
            owned.push(b'\n');
            std::borrow::Cow::Owned(owned)
        };

        let run = timeout(self.exec_timeout, async {
            let write_res = async {
                stdin.write_all(&payload).await?;
                stdin.shutdown().await
            }
            .await;
            drop(stdin);
            if let Err(err) = write_res
                && !is_broken_pipe(&err)
            {
                return Err(err);
            }
            tree.wait_exit().await
        })
        .await;

        match run {
            Ok(Ok(status)) => {
                let _ = timeout(STDERR_DRAIN_WAIT, &mut stderr_task).await;
                stderr_task.abort();
                if status.success() {
                    Ok(())
                } else {
                    Err(ExecError::NonZeroExit {
                        code: status.code(),
                        stderr: stderr_store.snapshot(),
                    })
                }
            }
            Ok(Err(source)) => {
                tree.kill_tree(self.shutdown_grace).await;
                let _ = timeout(STDERR_DRAIN_WAIT, &mut stderr_task).await;
                stderr_task.abort();
                Err(ExecError::Io {
                    source,
                    stderr: stderr_store.snapshot(),
                })
            }
            Err(_) => {
                tree.kill_tree(self.shutdown_grace).await;
                let _ = timeout(STDERR_DRAIN_WAIT, &mut stderr_task).await;
                stderr_task.abort();
                Err(ExecError::Timeout {
                    timeout: self.exec_timeout,
                    stderr: stderr_store.snapshot(),
                })
            }
        }
    }

    async fn deliver_stream(&mut self, event_json: &str) -> Result<(), ExecError> {
        let line = format_ndjson_line(event_json)?;
        if self.worker.is_none() {
            self.worker = Some(StreamWorker::spawn(&self.argv)?);
        }

        let worker = self.worker.as_mut().expect("worker spawned");
        let (stdin, stdout) = (&mut worker.stdin, &mut worker.stdout);
        let exchange = timeout(self.exec_timeout, async {
            let write_fut = async {
                let stdin = stdin
                    .as_mut()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "stdin closed"))?;
                stdin.write_all(line.as_bytes()).await?;
                stdin.flush().await
            };
            let read_fut = async {
                match read_bounded_line(stdout).await {
                    Ok(Some(line)) => match serde_json::from_str::<StreamAck>(&line) {
                        Ok(ack) => StreamReply::Ack(ack),
                        Err(_) => StreamReply::Invalid(line),
                    },
                    Ok(None) => StreamReply::Closed,
                    Err(source) => StreamReply::Io(source),
                }
            };
            tokio::pin!(write_fut, read_fut);
            // Poll stdin first so an already-ready write error keeps its precedence.
            tokio::select! {
                biased;
                write_res = &mut write_fut => (Some(write_res), read_fut.await),
                read_res = &mut read_fut => {
                    let write_res = if matches!(&read_res, StreamReply::Ack(ack) if ack.ok) {
                        Some(write_fut.await)
                    } else {
                        None
                    };
                    (write_res, read_res)
                }
            }
        })
        .await;

        match exchange {
            Ok((write_res, read_res)) => {
                let write_broken_pipe =
                    matches!(&write_res, Some(Err(error)) if is_broken_pipe(error));
                if let Some(Err(source)) = write_res
                    && !(write_broken_pipe
                        && matches!(&read_res, StreamReply::Ack(_) | StreamReply::Invalid(_)))
                {
                    let worker = self.worker.take().expect("worker present");
                    let stderr = worker.shutdown(self.shutdown_grace).await;
                    return Err(ExecError::Io { source, stderr });
                }
                match read_res {
                    StreamReply::Ack(ack) if ack.ok => {
                        if write_broken_pipe {
                            let worker = self.worker.take().expect("worker present");
                            let _ = worker.shutdown(self.shutdown_grace).await;
                        }
                        Ok(())
                    }
                    StreamReply::Ack(ack) => {
                        let worker = self.worker.take().expect("worker present");
                        let stderr = worker.shutdown(self.shutdown_grace).await;
                        Err(ExecError::Rejected {
                            message: ack.error.unwrap_or_else(|| "ok=false".to_owned()),
                            stderr,
                        })
                    }
                    StreamReply::Invalid(line) => {
                        let worker = self.worker.take().expect("worker present");
                        let stderr = worker.shutdown(self.shutdown_grace).await;
                        Err(ExecError::InvalidAck { line, stderr })
                    }
                    StreamReply::Closed => {
                        let worker = self.worker.take().expect("worker present");
                        let stderr = worker.shutdown(self.shutdown_grace).await;
                        Err(ExecError::StreamClosed { stderr })
                    }
                    StreamReply::Io(source) => {
                        let worker = self.worker.take().expect("worker present");
                        let stderr = worker.shutdown(self.shutdown_grace).await;
                        Err(ExecError::Io { source, stderr })
                    }
                }
            }
            Err(_) => {
                let worker = self.worker.take().expect("worker present");
                let stderr = worker.shutdown(self.shutdown_grace).await;
                Err(ExecError::Timeout {
                    timeout: self.exec_timeout,
                    stderr,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_store_reads_missing_writes_atomically_and_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("github-pr.cursor");
        let store = CursorStore::new(&path);

        assert_eq!(store.read().unwrap(), None);

        store.write("1700000000:1").unwrap();
        assert_eq!(store.read().unwrap().as_deref(), Some("1700000000:1"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        store.write("1700000000:42").unwrap();
        assert_eq!(store.read().unwrap().as_deref(), Some("1700000000:42"));

        // No leftover temp files in the cursor directory.
        let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["github-pr.cursor"]);
    }

    #[test]
    fn cursor_store_preserves_opaque_bytes_and_cleans_up_on_replace_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.cursor");
        std::fs::write(&path, "boot:99\r\n").unwrap();
        let store = CursorStore::new(&path);
        assert_eq!(store.read().unwrap().as_deref(), Some("boot:99\r\n"));
        for cursor in ["", " ", " leading and trailing ", "\nopaque\r\n", "游标 "] {
            store.write(cursor).unwrap();
            assert_eq!(store.read().unwrap().as_deref(), Some(cursor));
        }
        store.clear().unwrap();
        assert_eq!(store.read().unwrap(), None);
        store.clear().unwrap();

        // Target is a directory so rename/replace fails and temp file must be removed.
        let target_dir = dir.path().join("occupied");
        std::fs::create_dir(&target_dir).unwrap();
        let bad_store = CursorStore::new(&target_dir);
        assert!(bad_store.read().is_err());
        assert!(bad_store.write("boot:100").is_err());

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[cfg(unix)]
    fn sh(script: &str) -> Vec<String> {
        vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()]
    }

    #[test]
    fn executor_rejects_empty_argv() {
        assert!(matches!(
            CallbackExecutor::new(vec![], ExecMode::Oneshot, DEFAULT_EXEC_TIMEOUT),
            Err(ExecError::InvalidConfig(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oneshot_succeeds_and_delivers_json_over_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("event.json");
        let script = format!("cat > '{}'", out.display());
        let mut exec =
            CallbackExecutor::new(sh(&script), ExecMode::Oneshot, Duration::from_secs(5)).unwrap();

        let payload = r#"{"eventId":"evt-1","data":{"ok":true}}"#;
        exec.deliver(payload).await.unwrap();
        let saved = std::fs::read_to_string(&out).unwrap();
        assert_eq!(saved.trim(), payload);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oneshot_non_zero_exit_captures_exit_code_and_bounded_stderr() {
        let script = "cat >/dev/null; printf 'fatal error\\n' >&2; exit 7";
        let mut exec =
            CallbackExecutor::new(sh(script), ExecMode::Oneshot, Duration::from_secs(5)).unwrap();

        let err = exec.deliver(r#"{"id":1}"#).await.unwrap_err();
        match err {
            ExecError::NonZeroExit { code, stderr } => {
                assert_eq!(code, Some(7));
                assert!(stderr.contains("fatal error"), "stderr={stderr:?}");
            }
            other => panic!("expected NonZeroExit, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oneshot_catches_broken_pipe_and_reaps_real_exit_code() {
        // Exit immediately without reading stdin while writing >128 KiB payload to force EPIPE.
        let script = "printf 'early exit\\n' >&2; exit 42";
        let mut exec =
            CallbackExecutor::new(sh(script), ExecMode::Oneshot, Duration::from_secs(5)).unwrap();

        let big = format!(r#"{{"pad":"{}"}}"#, "x".repeat(256 * 1024));
        let err = exec.deliver(&big).await.unwrap_err();
        match err {
            ExecError::NonZeroExit { code, stderr } => {
                assert_eq!(code, Some(42));
                assert!(stderr.contains("early exit"), "stderr={stderr:?}");
            }
            other => panic!("expected NonZeroExit after BrokenPipe, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oneshot_timeout_reaps_entire_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("grandchild.pid");
        // Ignore SIGTERM in parent and spawn a grandchild so SIGKILL must clean up the group.
        let script = format!(
            "trap '' TERM; sleep 60 & echo $! > '{}'; wait",
            pid_file.display()
        );
        let mut exec =
            CallbackExecutor::new(sh(&script), ExecMode::Oneshot, Duration::from_millis(250))
                .unwrap()
                .with_shutdown_grace(Duration::from_millis(150));

        let err = exec.deliver(r#"{"id":1}"#).await.unwrap_err();
        assert!(matches!(err, ExecError::Timeout { .. }), "got {err:?}");

        let grandchild_pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // Give the kernel a brief moment to finish tearing down the killed process group.
        let mut alive = true;
        for _ in 0..20 {
            if unsafe { libc::kill(grandchild_pid, 0) } != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            !alive,
            "grandchild pid {grandchild_pid} survived timeout kill"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_handles_lf_and_crlf_acks_and_reuses_worker() {
        let script = r#"
while IFS= read -r line; do
  case "$line" in
    *crlf*) printf '{"ok":true}\r\n' ;;
    *) printf '{"ok":true}\n' ;;
  esac
done
"#;
        let mut exec =
            CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5)).unwrap();

        exec.deliver(r#"{"n":1}"#).await.unwrap();
        let first_pid = exec.worker_pid().expect("worker running");
        exec.deliver(r#"{"n":2,"mode":"crlf"}"#).await.unwrap();
        assert_eq!(exec.worker_pid(), Some(first_pid));

        exec.shutdown().await;
        assert_eq!(exec.worker_pid(), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_handles_abnormal_exit_and_rejection_then_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let count_file = dir.path().join("starts");
        // First invocation rejects with ok:false, second crashes without ack, third succeeds.
        let script = format!(
            r#"
n=$(cat '{f}' 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > '{f}'
IFS= read -r _line
if [ "$n" -eq 1 ]; then
  printf 'rejected reason\n' >&2
  printf '{{"ok":false,"error":"bad payload"}}\n'
  exit 0
elif [ "$n" -eq 2 ]; then
  printf 'crashed mid-stream\n' >&2
  exit 3
else
  printf '{{"ok":true}}\n'
  while IFS= read -r _line; do
    printf '{{"ok":true}}\n'
  done
fi
"#,
            f = count_file.display()
        );
        let mut exec = CallbackExecutor::new(sh(&script), ExecMode::Stream, Duration::from_secs(5))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(200));

        let err1 = exec.deliver(r#"{"a":1}"#).await.unwrap_err();
        match err1 {
            ExecError::Rejected { message, stderr } => {
                assert_eq!(message, "bad payload");
                assert!(stderr.contains("rejected reason"), "stderr={stderr:?}");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        assert_eq!(exec.worker_pid(), None);

        let err2 = exec.deliver(r#"{"a":1}"#).await.unwrap_err();
        match err2 {
            ExecError::StreamClosed { stderr } => {
                assert!(stderr.contains("crashed mid-stream"), "stderr={stderr:?}");
            }
            other => panic!("expected StreamClosed, got {other:?}"),
        }
        assert_eq!(exec.worker_pid(), None);

        exec.deliver(r#"{"a":1}"#).await.unwrap();
        assert!(exec.worker_pid().is_some());
        exec.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_drains_large_stderr_without_deadlock_and_bounds_buffer() {
        // Write >128 KiB to stderr before sending ack; would deadlock without concurrent drain.
        let script = r#"
while IFS= read -r _line; do
  i=0
  while [ "$i" -lt 200 ]; do
    printf '%0700d\n' "$i" >&2
    i=$((i + 1))
  done
  printf '{"ok":true}\n'
done
"#;
        let mut exec =
            CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(10)).unwrap();

        exec.deliver(r#"{"heavy":true}"#).await.unwrap();
        let captured_len = exec.worker.as_ref().unwrap().stderr.snapshot().len();
        assert!(
            captured_len <= MAX_STDERR_BYTES,
            "stderr snapshot exceeded cap: {captured_len}"
        );
        assert!(captured_len > 0);
        exec.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_ack_timeout_reaps_process_tree() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("stream_gc.pid");
        let script = format!(
            "trap '' TERM; sleep 60 & echo $! > '{}'; IFS= read -r _line; wait",
            pid_file.display()
        );
        let mut exec =
            CallbackExecutor::new(sh(&script), ExecMode::Stream, Duration::from_millis(250))
                .unwrap()
                .with_shutdown_grace(Duration::from_millis(150));

        let err = exec.deliver(r#"{"hang":true}"#).await.unwrap_err();
        assert!(matches!(err, ExecError::Timeout { .. }), "got {err:?}");
        assert_eq!(exec.worker_pid(), None);

        let grandchild_pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let mut alive = true;
        for _ in 0..20 {
            if unsafe { libc::kill(grandchild_pid, 0) } != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            !alive,
            "stream grandchild pid {grandchild_pid} survived timeout kill"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_rejection_does_not_wait_for_unread_stdin() {
        let script = r#"printf '{"ok":false,"error":"callback unavailable"}\n'; exec sleep 60"#;
        let mut exec = CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(200));

        let event = format!(r#"{{"data":"{}"}}"#, "x".repeat(256 * 1024));
        let err = exec.deliver(&event).await.unwrap_err();
        match err {
            ExecError::Rejected { message, .. } => assert_eq!(message, "callback unavailable"),
            other => panic!("expected Rejected, got {other:?}"),
        }
        assert_eq!(exec.worker_pid(), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_invalid_reply_and_eof_do_not_wait_for_unread_stdin() {
        for (script, expected_line) in [
            (
                r#"printf 'callback starting\n'; exec sleep 60"#,
                Some("callback starting"),
            ),
            ("exec 1>&-; exec sleep 60", None),
        ] {
            let mut exec =
                CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5))
                    .unwrap()
                    .with_shutdown_grace(Duration::from_millis(200));
            let event = format!(r#"{{"data":"{}"}}"#, "x".repeat(256 * 1024));
            let err = exec.deliver(&event).await.unwrap_err();
            match (err, expected_line) {
                (ExecError::InvalidAck { line, .. }, Some(expected)) => assert_eq!(line, expected),
                (ExecError::StreamClosed { .. }, None) => {}
                other => panic!("unexpected terminal reply: {other:?}"),
            }
            assert_eq!(exec.worker_pid(), None);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_positive_ack_still_waits_for_pending_stdin() {
        let script = r#"printf '{"ok":true}\n'; exec sleep 60"#;
        let mut exec = CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(200));

        let event = format!(r#"{{"data":"{}"}}"#, "x".repeat(256 * 1024));
        let err = exec.deliver(&event).await.unwrap_err();
        assert!(matches!(err, ExecError::Timeout { .. }), "got {err:?}");
        assert_eq!(exec.worker_pid(), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_retains_partial_ack_while_stdin_finishes() {
        let script = r#"
printf '{"ok":'
sleep 0.05
head -n 1 >/dev/null
printf 'true}\n'
while IFS= read -r _line; do
  printf '{"ok":true}\n'
done
"#;
        let mut exec =
            CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5)).unwrap();
        let event = format!(r#"{{"data":"{}"}}"#, "x".repeat(256 * 1024));
        exec.deliver(&event).await.unwrap();
        let worker = exec.worker_pid().expect("worker running");
        exec.deliver(r#"{"data":"next"}"#).await.unwrap();
        assert_eq!(exec.worker_pid(), Some(worker));
        exec.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_positive_ack_accepts_broken_pipe_and_reaps_worker() {
        let script = r#"printf '{"ok":true}\n'; exit 0"#;
        let mut exec = CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(200));

        let event = format!(r#"{{"data":"{}"}}"#, "x".repeat(256 * 1024));
        exec.deliver(&event).await.unwrap();
        assert_eq!(exec.worker_pid(), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_prefers_ack_over_broken_pipe_during_concurrent_exchange() {
        // Reject and exit before reading a 256 KiB line so stdin hits EPIPE while stdout yields ack.
        let script = r#"printf '{"ok":false,"error":"too large"}\n'; exit 0"#;
        let mut exec = CallbackExecutor::new(sh(script), ExecMode::Stream, Duration::from_secs(5))
            .unwrap()
            .with_shutdown_grace(Duration::from_millis(200));

        let big = format!(r#"{{"pad":"{}"}}"#, "x".repeat(256 * 1024));
        let err = exec.deliver(&big).await.unwrap_err();
        match err {
            ExecError::Rejected { message, .. } => assert_eq!(message, "too large"),
            other => panic!("expected Rejected, got {other:?}"),
        }
        assert_eq!(exec.worker_pid(), None);
    }

    #[cfg(windows)]
    const ONESHOT_STDIN_HELPER: &str = "events_exec::tests::windows_oneshot_stdin_helper_entry";
    #[cfg(windows)]
    const ONESHOT_FAILURE_HELPER: &str = "events_exec::tests::windows_oneshot_failure_helper_entry";

    #[cfg(windows)]
    fn selected_helper(name: &str) -> bool {
        std::env::args_os().any(|arg| arg == "--exact")
            && std::env::args_os().any(|arg| arg == name)
    }

    #[cfg(windows)]
    fn native_callback_argv(name: &str) -> Vec<String> {
        vec![
            std::env::current_exe()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
            "--exact".to_owned(),
            name.to_owned(),
            "--nocapture".to_owned(),
            "--test-threads=1".to_owned(),
        ]
    }

    #[cfg(windows)]
    #[test]
    fn windows_oneshot_stdin_helper_entry() {
        if !selected_helper(ONESHOT_STDIN_HELPER) {
            return;
        }
        let mut input = String::new();
        std::io::stdin().read_to_string(&mut input).unwrap();
        assert_eq!(
            input,
            concat!(r#"{"eventId":"win-1","data":{"ok":true}}"#, "\n")
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_oneshot_failure_helper_entry() {
        if !selected_helper(ONESHOT_FAILURE_HELPER) {
            return;
        }
        let mut input = String::new();
        std::io::stdin().read_to_string(&mut input).unwrap();
        assert_eq!(input, concat!(r#"{"id":1}"#, "\n"));
        let mut stderr = std::io::stderr().lock();
        writeln!(stderr, "win fatal").unwrap();
        stderr.flush().unwrap();
        std::process::exit(7);
    }

    #[cfg(windows)]
    fn ps(script: &str) -> Vec<String> {
        vec![
            "powershell.exe".to_owned(),
            "-NoProfile".to_owned(),
            "-NonInteractive".to_owned(),
            "-Command".to_owned(),
            script.to_owned(),
        ]
    }

    #[cfg(windows)]
    mod callback_tree_tests {
        use super::*;
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use windows_sys::Win32::{
            Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
            System::Threading::{
                OpenProcess, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, TerminateProcess,
                WaitForSingleObject,
            },
        };

        const PARENT_HELPER: &str =
            "events_exec::tests::callback_tree_tests::windows_callback_parent_helper_entry";
        const WORKER_HELPER: &str =
            "events_exec::tests::callback_tree_tests::windows_callback_worker_helper_entry";
        const WORKER_PID_PREFIX: &str = "mcpcli-callback-worker:";

        #[test]
        fn windows_callback_parent_helper_entry() {
            if !selected_helper(PARENT_HELPER) {
                return;
            }
            // The worker starts before the parent reports readiness.
            let mut worker = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", WORKER_HELPER, "--nocapture", "--test-threads=1"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("cannot start native callback worker");
            eprintln!("{WORKER_PID_PREFIX}{}", worker.id());
            assert!(
                worker
                    .wait()
                    .expect("cannot wait for callback worker")
                    .success()
            );
        }

        #[test]
        fn windows_callback_worker_helper_entry() {
            if !selected_helper(WORKER_HELPER) {
                return;
            }
            std::thread::sleep(Duration::from_secs(60));
        }

        struct Worker(OwnedHandle);

        impl Worker {
            fn open(pid: u32) -> Self {
                // SAFETY: the PID belongs to the worker just reported by this test's callback.
                let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
                assert!(
                    !raw.is_null(),
                    "cannot open callback worker: {}",
                    io::Error::last_os_error()
                );
                // SAFETY: OpenProcess returned a new handle owned by this test.
                let worker = Self(unsafe { OwnedHandle::from_raw_handle(raw) });
                assert_eq!(
                    worker.state(),
                    WAIT_TIMEOUT,
                    "callback worker exited before cleanup"
                );
                worker
            }

            fn state(&self) -> u32 {
                // SAFETY: the process handle stays live through this nonblocking wait.
                unsafe { WaitForSingleObject(self.0.as_raw_handle(), 0) }
            }

            async fn assert_stopped(&self) {
                timeout(Duration::from_secs(5), async {
                    loop {
                        match self.state() {
                            WAIT_OBJECT_0 => return,
                            WAIT_TIMEOUT => tokio::time::sleep(Duration::from_millis(10)).await,
                            other => panic!("cannot observe callback worker exit: {other}"),
                        }
                    }
                })
                .await
                .expect("callback worker survived process-tree cleanup");
            }
        }

        impl Drop for Worker {
            fn drop(&mut self) {
                if self.state() == WAIT_TIMEOUT {
                    // SAFETY: retain this exact worker handle for cleanup if an assertion fails.
                    unsafe {
                        TerminateProcess(self.0.as_raw_handle(), 1);
                        WaitForSingleObject(self.0.as_raw_handle(), 3000);
                    }
                }
            }
        }

        async fn start_callback(mode: ExecMode) -> (ProcessTree, Worker) {
            let argv = native_callback_argv(PARENT_HELPER);
            let mut tree = ProcessTree::spawn(&argv, mode).unwrap();
            let mut stderr =
                BufReader::new(tree.take_stderr().expect("piped callback stderr")).lines();
            let mut diagnostics = String::new();
            let worker = timeout(Duration::from_secs(15), async {
                while let Some(line) = stderr
                    .next_line()
                    .await
                    .expect("cannot read callback stderr")
                {
                    if let Some(pid) = line.strip_prefix(WORKER_PID_PREFIX) {
                        return Worker::open(pid.parse().expect("invalid callback worker PID"));
                    }
                    diagnostics.push_str(&line);
                    diagnostics.push('\n');
                }
                panic!(
                    "callback exited before reporting its worker: {:?}; stderr: {diagnostics}",
                    tree.child.as_mut().unwrap().try_wait().unwrap()
                );
            })
            .await
            .unwrap_or_else(|_| panic!("callback did not start its worker; stderr: {diagnostics}"));
            (tree, worker)
        }

        #[tokio::test]
        async fn windows_callback_timeout_reaps_immediately_spawned_worker() {
            for mode in [ExecMode::Oneshot, ExecMode::Stream] {
                let (mut tree, worker) = start_callback(mode).await;
                assert!(
                    timeout(Duration::from_millis(50), tree.wait_exit())
                        .await
                        .is_err()
                );
                tree.kill_tree(Duration::ZERO).await;
                worker.assert_stopped().await;
            }
        }

        #[tokio::test]
        async fn windows_callback_drop_reaps_immediately_spawned_worker() {
            for mode in [ExecMode::Oneshot, ExecMode::Stream] {
                let (tree, worker) = start_callback(mode).await;
                drop(tree);
                worker.assert_stopped().await;
            }
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_oneshot_succeeds_and_delivers_json_over_stdin() {
        let mut exec = CallbackExecutor::new(
            native_callback_argv(ONESHOT_STDIN_HELPER),
            ExecMode::Oneshot,
            Duration::from_secs(15),
        )
        .unwrap();

        let payload = r#"{"eventId":"win-1","data":{"ok":true}}"#;
        exec.deliver(payload).await.unwrap();
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_oneshot_non_zero_exit_captures_exit_code_and_stderr() {
        let mut exec = CallbackExecutor::new(
            native_callback_argv(ONESHOT_FAILURE_HELPER),
            ExecMode::Oneshot,
            Duration::from_secs(15),
        )
        .unwrap();

        let err = exec.deliver(r#"{"id":1}"#).await.unwrap_err();
        match err {
            ExecError::NonZeroExit { code, stderr } => {
                assert_eq!(code, Some(7));
                assert!(stderr.contains("win fatal"), "stderr={stderr:?}");
            }
            other => panic!("expected NonZeroExit, got {other:?}"),
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_oneshot_timeout_reaps_job() {
        let script = "Start-Sleep -Seconds 60";
        let mut exec =
            CallbackExecutor::new(ps(script), ExecMode::Oneshot, Duration::from_millis(500))
                .unwrap()
                .with_shutdown_grace(Duration::from_millis(200));

        let err = exec.deliver(r#"{"id":1}"#).await.unwrap_err();
        assert!(matches!(err, ExecError::Timeout { .. }), "got {err:?}");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_stream_handles_lf_and_crlf_acks_and_reuses_worker() {
        let script = r#"
while (($line = [Console]::In.ReadLine()) -ne $null) {
  if ($line -like '*crlf*') {
    [Console]::Out.Write("{`"ok`":true}`r`n")
  } else {
    [Console]::Out.Write("{`"ok`":true}`n")
  }
  [Console]::Out.Flush()
}
"#;
        let mut exec =
            CallbackExecutor::new(ps(script), ExecMode::Stream, Duration::from_secs(15)).unwrap();

        exec.deliver(r#"{"n":1}"#).await.unwrap();
        let first_pid = exec.worker_pid().expect("worker running");
        exec.deliver(r#"{"n":2,"mode":"crlf"}"#).await.unwrap();
        assert_eq!(exec.worker_pid(), Some(first_pid));

        exec.shutdown().await;
        assert_eq!(exec.worker_pid(), None);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_stream_ack_timeout_reaps_job() {
        let script = "[void][Console]::In.ReadLine(); Start-Sleep -Seconds 60";
        let mut exec =
            CallbackExecutor::new(ps(script), ExecMode::Stream, Duration::from_millis(500))
                .unwrap()
                .with_shutdown_grace(Duration::from_millis(200));

        let err = exec.deliver(r#"{"hang":true}"#).await.unwrap_err();
        assert!(matches!(err, ExecError::Timeout { .. }), "got {err:?}");
        assert_eq!(exec.worker_pid(), None);
    }
}
