//! Native runtime isolation, local IPC, and filesystem operations.
pub mod local;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) mod windows_job;
#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

use sha2::{Digest, Sha256};
use std::io;
use std::path::{Path, PathBuf};

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Use the mapped executable on Linux even if its pathname was replaced.
pub fn image_path() -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    return Ok(PathBuf::from("/proc/self/exe"));
    #[cfg(not(target_os = "linux"))]
    std::env::current_exe()
}

/// Incoming standard handles belong to this CLI, not to its detached descendants.
/// Rust duplicates explicitly configured child stdio with inheritance enabled when spawning.
pub fn initialize_standard_handles() -> io::Result<()> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{
            HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
        };
        use windows_sys::Win32::System::Console::{
            GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };
        for stream in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
            // SAFETY: these process-owned handles stay open; only their inheritance flag changes.
            let handle = unsafe { GetStdHandle(stream) };
            if !handle.is_null()
                && handle != INVALID_HANDLE_VALUE
                && unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

pub async fn interrupted() -> io::Result<i32> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(windows)]
    let mut terminate = tokio::signal::windows::ctrl_break()?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => { result?; Ok(130) },
        _ = terminate.recv() => Ok(143),
    }
}

pub fn default_runtime() -> io::Result<PathBuf> {
    #[cfg(windows)]
    let parent = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("LOCALAPPDATA is not set"))?;
    #[cfg(unix)]
    let parent = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    // The OS temporary directory can contain system aliases such as macOS /var.
    let parent = parent.canonicalize()?;
    #[cfg(windows)]
    let name = "mcpcli".to_owned();
    #[cfg(unix)]
    let name = format!("mcpcli-{}", uid());
    Ok(parent.join(name))
}

pub fn endpoint(directory: &Path, control: bool) -> io::Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = directory.join(if control { "control.sock" } else { "sock" });
        // Leave room for the terminating NUL on every supported Unix.
        let address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        if path.as_os_str().as_bytes().len() >= address.sun_path.len() {
            return Err(io::Error::other(
                "runtime socket path is too long; choose a shorter MCPCLI_RUNTIME_DIR",
            ));
        }
        Ok(path)
    }
    #[cfg(windows)]
    {
        let mut identity = local::user_sid()?.into_bytes();
        identity.push(0);
        identity.extend_from_slice(directory.as_os_str().as_encoded_bytes());
        let hash = digest(&identity);
        Ok(PathBuf::from(format!(
            r"\\.\pipe\mcpcli-{}-{}",
            &hash[..32],
            if control { "control" } else { "data" }
        )))
    }
}

pub use local::{DirectoryEntry, FileKind};

#[cfg(test)]
mod tests;

pub(crate) fn canonical_skill_root(path: &Path) -> io::Result<PathBuf> {
    canonical_skill_root_with(path, |path| path.canonicalize())
}

pub(crate) fn canonical_directory(path: &Path) -> io::Result<PathBuf> {
    canonical_directory_with(path, "configured directory", |path| path.canonicalize())
}

fn canonical_skill_root_with(
    path: &Path,
    canonicalize: impl FnOnce(&Path) -> io::Result<PathBuf>,
) -> io::Result<PathBuf> {
    canonical_directory_with(path, "skill root", canonicalize)
}

#[cfg(windows)]
fn normalize_directory_path(path: &Path) -> io::Result<PathBuf> {
    use std::path::Component;
    if !path.is_absolute()
        && (path.has_root() || matches!(path.components().next(), Some(Component::Prefix(_))))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "directory path must not be drive-relative",
        ));
    }
    let is_verbatim = matches!(
        path.components().next(),
        Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim()
    );
    if !is_verbatim {
        return Ok(path.components().collect());
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Normal(part) if part == "." => {}
            Component::Normal(part) if part == ".." => {
                let _ = normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

fn canonical_directory_with(
    path: &Path,
    label: &str,
    canonicalize: impl FnOnce(&Path) -> io::Result<PathBuf>,
) -> io::Result<PathBuf> {
    // Trailing separators or '/.' must not turn a symlink leaf into an ancestor.
    #[cfg(windows)]
    let path = normalize_directory_path(path)?;
    #[cfg(not(windows))]
    let path: PathBuf = path.components().collect();
    // Pin the configured leaf before canonicalization can follow a replacement link.
    let original = open_skill_root(&path)?;
    let resolved = canonicalize(&path)?;
    let verified = open_skill_root(&resolved)?;
    if !same_file(&original, &verified)? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{label} changed during canonicalization"),
        ));
    }
    Ok(resolved)
}
