//! Private runtime files and short-lived clients of the persistent daemon.
use crate::config::{LoadedConfig, digest, valid_name};
use crate::ipc::{self, Failure, Operation, Request, Response};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UnixStream;
use tokio::time::{Instant, sleep, timeout};

pub const START_WAIT: Duration = Duration::from_secs(10);
pub const STOP_WAIT: Duration = Duration::from_secs(20);

pub fn uid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

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

fn check_ancestors(path: &Path) -> Result<()> {
    for ancestor in path.ancestors().skip(1) {
        let meta = std::fs::symlink_metadata(ancestor)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            bail!("runtime ancestor is not a real directory");
        }
        if (meta.uid() != 0 && meta.uid() != uid())
            || (meta.mode() & 0o022 != 0 && meta.mode() & libc::S_ISVTX == 0)
        {
            bail!("runtime ancestor has unsafe ownership or permissions");
        }
    }
    Ok(())
}

pub fn private_dir(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("runtime directory must be absolute");
    }
    check_ancestors(path)?;
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.into()),
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != uid()
        || metadata.mode() & 0o777 != 0o700
    {
        bail!("runtime directory must be owned by this user, mode 0700, and not a symlink");
    }
    Ok(())
}

pub fn secure_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .context("cannot open private runtime file")?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.uid() != uid() || meta.nlink() != 1 || meta.mode() & 0o777 != 0o600 {
        bail!("runtime file must be a single-link regular file owned by this user with mode 0600");
    }
    Ok(file)
}

pub struct Lock(File);
impl Lock {
    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        let file = secure_file(path)?;
        // SAFETY: the descriptor is a live owned file, flags are valid.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(Self(file)));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            Ok(None)
        } else {
            Err(error.into())
        }
    }
    pub async fn acquire(path: &Path, budget: Duration) -> Result<Self> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(lock) = Self::try_acquire(path)? {
                return Ok(lock);
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for daemon startup lock");
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}
impl Drop for Lock {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains live until after this drop method.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn build_id() -> Result<String> {
    let mut file = File::open("/proc/self/exe")?;
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
        let root = if let Some(root) =
            std::env::var_os("MCPCLI_RUNTIME_DIR").filter(|v| !v.is_empty())
        {
            PathBuf::from(root)
        } else if let Some(root) = std::env::var_os("XDG_RUNTIME_DIR").filter(|v| !v.is_empty()) {
            PathBuf::from(root).join("mcpcli")
        } else {
            PathBuf::from(format!("/tmp/mcpcli-{}", uid()))
        };
        Self::with_root(config, session, &root)
    }
    fn with_root(config: PathBuf, session: &str, root: &Path) -> Result<Self> {
        if !valid_name(session) {
            bail!("session must be 1-64 ASCII letters, digits, '_' or '-'");
        }
        private_dir(root)?;
        let mut key = config.as_os_str().as_encoded_bytes().to_vec();
        key.push(0);
        key.extend_from_slice(session.as_bytes());
        let namespace = digest(&key)[..24].to_owned();
        let directory = root.join(&namespace);
        private_dir(&directory)?;
        let socket = directory.join("sock");
        let control_socket = directory.join("control.sock");
        if control_socket.as_os_str().as_encoded_bytes().len() > 107 {
            bail!(
                "runtime socket path exceeds Linux's 107 byte limit; choose a shorter MCPCLI_RUNTIME_DIR"
            );
        }
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
    pub fn request(&self, digest: &str, operation: Operation, duration: Duration) -> Request {
        Request {
            protocol: ipc::PROTOCOL,
            namespace: self.namespace.clone(),
            build: self.build.clone(),
            digest: digest.into(),
            timeout_ms: duration.as_millis().min(3_600_000) as u64,
            operation,
        }
    }
    pub fn remove_socket(&self) -> Result<()> {
        for socket in [&self.socket, &self.control_socket] {
            match std::fs::symlink_metadata(socket) {
                Ok(meta) if meta.file_type().is_socket() && meta.uid() == uid() => {
                    std::fs::remove_file(socket)?
                }
                Ok(_) => bail!("refusing to remove an unexpected file at the daemon socket"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
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
    #[serde(default)]
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
        let path = paths
            .directory
            .join(format!("state-{}.tmp", std::process::id()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        let result = (|| -> Result<()> {
            file.write_all(&serde_json::to_vec(self)?)?;
            file.sync_all()?;
            std::fs::rename(&path, paths.state_path())?;
            File::open(&paths.directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(path);
        }
        result
    }
}

pub async fn exchange(paths: &Paths, request: &Request) -> Result<Response> {
    let uncertain = request.operation.is_call();
    let future = async {
        let socket = if request.operation.management() {
            &paths.control_socket
        } else {
            &paths.socket
        };
        let meta = std::fs::symlink_metadata(socket).context("daemon socket is unavailable")?;
        if !meta.file_type().is_socket() || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
            bail!("daemon socket ownership, type or permissions are unsafe");
        }
        let mut stream = UnixStream::connect(socket)
            .await
            .context("cannot connect to daemon")?;
        if stream.peer_cred()?.uid() != uid() {
            bail!("daemon peer belongs to a different user");
        }
        ipc::write_frame(&mut stream, request, ipc::REQUEST_LIMIT).await?;
        let response: Response = ipc::read_frame(&mut stream, ipc::RESPONSE_LIMIT)
            .await?
            .context("daemon disconnected before returning a result")?;
        if response.protocol != ipc::PROTOCOL || response.namespace != paths.namespace {
            bail!("daemon IPC identity does not match");
        }
        if !request.operation.management() && response.build != paths.build {
            return Err(Failure::new(
                6,
                "daemon_mismatch",
                "daemon binary changed; run daemon restart",
            )
            .into());
        }
        if let Some(error) = response.error.as_ref() {
            return Err(error.clone().into());
        }
        if response.result.is_none() {
            bail!("daemon response has no result");
        }
        Ok(response)
    };
    match timeout(
        Duration::from_millis(request.timeout_ms) + Duration::from_secs(5),
        future,
    )
    .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) if error.downcast_ref::<Failure>().is_some() => Err(error),
        Ok(Err(_)) => Err(Failure::new(
            4,
            "daemon_transport",
            "daemon communication failed; tool calls are never replayed",
        )
        .uncertain(uncertain)
        .into()),
        Err(_) => Err(Failure::new(
            5,
            "timeout",
            "daemon deadline exceeded; cancellation is not rollback",
        )
        .uncertain(uncertain)
        .into()),
    }
}

pub async fn ensure(paths: &Paths, loaded: &LoadedConfig, explicit: bool) -> Result<()> {
    let _startup = Lock::acquire(&paths.startup_lock(), START_WAIT).await?;
    if paths.running()? {
        let response = exchange(
            paths,
            &paths.request("", Operation::Status, Duration::from_secs(2)),
        )
        .await?;
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
    let mut command = Command::new("/proc/self/exe");
    command
        .args(["--config"])
        .arg(&loaded.path)
        .args(["--session", &paths.session, "daemon", "run"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    // SAFETY: setsid and umask are async-signal-safe; no allocation occurs in this closure.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            libc::umask(0o077);
            Ok(())
        });
    }
    let mut child = command.spawn().context("cannot start daemon")?;
    let deadline = Instant::now() + START_WAIT;
    while Instant::now() < deadline {
        if paths.running()?
            && exchange(
                paths,
                &paths.request("", Operation::Status, Duration::from_millis(100)),
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
    exchange(
        paths,
        &paths.request("", Operation::Stop, Duration::from_secs(2)),
    )
    .await?;
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
    use std::os::unix::fs::{PermissionsExt, symlink};
    #[test]
    fn private_directory_refuses_links_and_weak_modes() {
        let base = tempfile::tempdir().unwrap();
        let real = base.path().join("real");
        private_dir(&real).unwrap();
        let link = base.path().join("link");
        symlink(&real, &link).unwrap();
        assert!(private_dir(&link).is_err());
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(private_dir(&real).is_err());
    }
    #[test]
    fn locks_remain_singletons_and_refuse_hardlinks() {
        let base = tempfile::tempdir().unwrap();
        let path = base.path().join("lock");
        let first = Lock::try_acquire(&path).unwrap().unwrap();
        assert!(Lock::try_acquire(&path).unwrap().is_none());
        drop(first);
        assert!(Lock::try_acquire(&path).unwrap().is_some());
        std::fs::hard_link(&path, base.path().join("alias")).unwrap();
        assert!(Lock::try_acquire(&path).is_err());
    }
    #[test]
    fn namespace_isolates_config_and_session() {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("runtime");
        let a = Paths::with_root(base.path().join("a.json"), "a", &root).unwrap();
        let b = Paths::with_root(base.path().join("a.json"), "b", &root).unwrap();
        let c = Paths::with_root(base.path().join("b.json"), "a", &root).unwrap();
        assert_ne!(a.namespace, b.namespace);
        assert_ne!(a.namespace, c.namespace);
    }
}
