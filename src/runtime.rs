//! Private runtime files and short-lived clients of the persistent daemon.
use crate::budget::{ByteBudget, ByteLease, format_bytes};
use crate::config::{LoadedConfig, digest, valid_name};
use crate::ipc::{self, Failure, Operation, Request, Response, StreamFrame, StreamFrameKind};
use crate::platform;
pub use crate::platform::{private_dir, secure_file};
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
use tokio::time::{Instant, sleep, timeout_at};

pub const START_WAIT: Duration = Duration::from_secs(10);
pub const STOP_WAIT: Duration = Duration::from_secs(20);

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
        let namespace = digest(&key)[..24].to_owned();
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
            deadline_ms,
            explicit_deadline_ms: explicit_deadline.then_some(deadline_ms),
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
    let is_call = request.operation.is_call();
    let now_ms = platform::monotonic_millis()?;
    let remaining = request
        .deadline_ms
        .saturating_sub(now_ms)
        .min(request.timeout_ms);
    if remaining == 0 {
        return Err(
            Failure::new(5, "timeout", "request expired before connecting to daemon").into(),
        );
    }
    let started = Instant::now();
    let exec_deadline = started + Duration::from_millis(remaining);
    let hard_deadline = request.explicit_deadline_ms.map(|exp_ms| {
        started + Duration::from_millis(exp_ms.saturating_sub(now_ms).min(request.timeout_ms))
    });
    if hard_deadline.is_some_and(|dl| Instant::now() >= dl) {
        return Err(
            Failure::new(5, "timeout", "request expired before connecting to daemon").into(),
        );
    }

    let ignored = AtomicBool::new(false);
    let write_tracker = if is_call { dispatched } else { &ignored };

    // Stage 1: connect, send request, and await validated 4-byte response header under operation budget.
    let stage1 = async {
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
            Some(exec_deadline),
            write_tracker,
        )
        .await?;
        drop(req_frame);
        drop(req_lease);
        let len = ipc::read_frame_header(&mut stream, ipc::RESPONSE_LIMIT)
            .await?
            .context("daemon disconnected before returning a result")?;
        Ok::<_, anyhow::Error>((stream, len))
    };

    let (mut stream, response_len) = match timeout_at(exec_deadline, stage1).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(error)) if error.downcast_ref::<Failure>().is_some() => return Err(error),
        Ok(Err(error)) => {
            if let Some(io_err) = error.downcast_ref::<io::Error>()
                && io_err.kind() == io::ErrorKind::TimedOut
            {
                return Err(Failure::new(
                    5,
                    "timeout",
                    "daemon deadline exceeded; cancellation is not rollback",
                )
                .uncertain(is_call && write_tracker.load(Ordering::Acquire))
                .into());
            }
            return Err(Failure::new(
                4,
                "daemon_transport",
                "daemon communication failed; tool calls are never replayed",
            )
            .uncertain(is_call && write_tracker.load(Ordering::Acquire))
            .into());
        }
        Err(_) => {
            return Err(Failure::new(
                5,
                "timeout",
                "daemon deadline exceeded; cancellation is not rollback",
            )
            .uncertain(is_call && write_tracker.load(Ordering::Acquire))
            .into());
        }
    };

    // Stage 2: read validated response body using local 60s idle clock capped by explicit deadline.
    let (response_lease, mut response): (ByteLease, Response) = match ipc::read_frame_body_idle(
        &mut stream,
        response_len,
        idle_timeout,
        hard_deadline,
        budget,
    )
    .await
    {
        Ok((response, lease)) => (lease, response),
        Err(error) if error.kind() == io::ErrorKind::TimedOut => {
            return Err(Failure::new(
                5,
                "timeout",
                "daemon deadline exceeded; cancellation is not rollback",
            )
            .uncertain(is_call && write_tracker.load(Ordering::Acquire))
            .into());
        }
        Err(error) if error.kind() == io::ErrorKind::OutOfMemory => {
            return Err(Failure::new(
                1,
                "memory_limit_exceeded",
                format!(
                    "daemon response exceeded the {} active memory limit; tool calls are never replayed",
                    format_bytes(budget.limit())
                ),
            )
            .uncertain(is_call && write_tracker.load(Ordering::Acquire))
            .into());
        }
        Err(_) => {
            return Err(Failure::new(
                4,
                "daemon_transport",
                "daemon communication failed; tool calls are never replayed",
            )
            .uncertain(is_call && write_tracker.load(Ordering::Acquire))
            .into());
        }
    };

    if response.protocol != ipc::PROTOCOL {
        return Err(Failure::new(
            6,
            "daemon_mismatch",
            "daemon protocol does not match; run daemon restart",
        )
        .uncertain(is_call && write_tracker.load(Ordering::Acquire))
        .into());
    }
    if response.namespace != paths.namespace {
        return Err(Failure::new(
            4,
            "daemon_transport",
            "daemon IPC identity does not match; tool calls are never replayed",
        )
        .uncertain(is_call && write_tracker.load(Ordering::Acquire))
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
        return Err(Failure::new(
            4,
            "daemon_transport",
            "daemon response has no result; tool calls are never replayed",
        )
        .uncertain(is_call && write_tracker.load(Ordering::Acquire))
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
                "daemon protocol does not match; run daemon restart",
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
        if child.try_wait()?.is_some() {
            bail!(
                "daemon exited during startup; inspect {}",
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
                },
                Duration::from_secs(2),
            )
            .unwrap();
        let err = exchange(&paths, &req).await.unwrap_err();
        let failure = err.downcast_ref::<Failure>().unwrap();
        assert_eq!(failure.kind, "daemon_transport");
        assert!(!failure.outcome_unknown);
    }
}
