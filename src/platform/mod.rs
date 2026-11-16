//! Native runtime isolation, local IPC, and filesystem operations.
pub mod local;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;
#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

use std::io;
use std::path::{Path, PathBuf};

/// Use the mapped executable on Linux even if its pathname was replaced.
pub fn image_path() -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    return Ok(PathBuf::from("/proc/self/exe"));
    #[cfg(not(target_os = "linux"))]
    std::env::current_exe()
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
        let identity = format!("{}\0{}", local::user_sid()?, directory.display());
        let hash = crate::config::digest(identity.as_bytes());
        Ok(PathBuf::from(format!(
            r"\\.\pipe\mcpcli-{}-{}",
            &hash[..32],
            if control { "control" } else { "data" }
        )))
    }
}

#[derive(Clone, Copy)]
pub enum FileKind {
    File,
    Directory,
    Link,
    Other,
}
impl FileKind {
    pub fn is_file(self) -> bool {
        matches!(self, Self::File)
    }
    pub fn is_dir(self) -> bool {
        matches!(self, Self::Directory)
    }
    pub fn is_symlink(self) -> bool {
        matches!(self, Self::Link)
    }
}
pub struct DirectoryEntry {
    pub name: std::ffi::OsString,
    pub kind: FileKind,
}

#[cfg(test)]
mod tests;

pub(crate) fn is_link(metadata: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        metadata.file_type().is_symlink()
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes()
            & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
            != 0
    }
}
