use anyhow::{Context, Result, bail};
use std::ffi::{CStr, CString, OsString};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(not(target_os = "macos"))]
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path};
use std::process::{Child, Command};

#[cfg(target_os = "freebsd")]
#[path = "freebsd_acl.rs"]
mod freebsd_acl;
#[cfg(target_os = "macos")]
#[path = "macos_acl.rs"]
mod macos_acl;

pub fn uid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

fn check_ancestors(path: &Path) -> Result<()> {
    for ancestor in path.ancestors().skip(1) {
        let file = open_skill_root(ancestor)?;
        let meta = file.metadata()?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            bail!("runtime ancestor is not a real directory");
        }
        if (meta.uid() != 0 && meta.uid() != uid())
            || (meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0)
        {
            bail!("runtime ancestor has unsafe ownership or permissions");
        }
        #[cfg(target_os = "macos")]
        macos_acl::check_ancestor(&file, ancestor)?;
        #[cfg(target_os = "freebsd")]
        freebsd_acl::check_ancestor(&file)?;
    }
    Ok(())
}

pub fn private_dir(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("runtime directory must be absolute");
    }
    check_ancestors(path)?;
    #[cfg(target_os = "macos")]
    let created = macos_acl::create_leaf(path);
    #[cfg(not(target_os = "macos"))]
    let created = std::fs::DirBuilder::new().mode(0o700).create(path);
    match created {
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
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    check_private_acl(&file, path)?;
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
    check_private_acl(&file, path)?;
    Ok(file)
}

fn check_private_acl(file: &File, path: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    return macos_acl::check_acl(file, path);
    #[cfg(target_os = "freebsd")]
    {
        let _ = path;
        freebsd_acl::check_private_acl(file)
    }
    #[cfg(target_os = "linux")]
    {
        let _ = (file, path);
        Ok(())
    }
}

pub fn new_private_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

pub fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub fn monotonic_millis() -> io::Result<u64> {
    let mut time: libc::timespec = unsafe { std::mem::zeroed() };
    // CLOCK_MONOTONIC is shared by every process on this boot.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((time.tv_sec as u64)
        .saturating_mul(1000)
        .saturating_add(time.tv_nsec as u64 / 1_000_000))
}

pub fn spawn_daemon(command: &mut Command) -> io::Result<Child> {
    // Only async-signal-safe operations run after fork.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            libc::umask(0o077);
            Ok(())
        });
    }
    command.spawn()
}

pub fn remove_endpoint(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() && meta.uid() == uid() => {
            std::fs::remove_file(path)?
        }
        Ok(_) => bail!("refusing to remove an unexpected file at the daemon socket"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

pub fn secure_endpoint(path: &Path) -> io::Result<()> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

pub fn validate_endpoint(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path).context("daemon socket is unavailable")?;
    if !meta.file_type().is_socket() || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
        bail!("daemon socket ownership, type or permissions are unsafe");
    }
    Ok(())
}

pub fn validate_client(stream: &super::local::Conn) -> io::Result<()> {
    if stream.peer_cred()?.uid() != uid() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "IPC peer belongs to a different user",
        ));
    }
    Ok(())
}

pub fn validate_server(stream: &super::local::Client) -> io::Result<()> {
    validate_client(stream)
}

pub fn open_input(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
}

#[cfg(target_os = "linux")]
const TRAVERSE: i32 = libc::O_PATH;
#[cfg(any(target_os = "freebsd", target_os = "macos"))]
const TRAVERSE: i32 = libc::O_SEARCH;

pub fn open_beneath(root: &Path, relative: &Path, directory: bool) -> io::Result<File> {
    if !root.is_absolute()
        || !root
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "root directory must be absolute and normalized",
        ));
    }
    if relative.is_absolute()
        || !relative
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "relative path must contain only normal components",
        ));
    }
    let raw_rel = relative.as_os_str().as_bytes();
    if !directory && (raw_rel.is_empty() || raw_rel.last() == Some(&b'/')) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file path must name a regular file",
        ));
    }
    let path = root.join(relative);
    let names: Vec<_> = path
        .components()
        .filter_map(|part| match part {
            Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect();
    let root = CString::new("/").unwrap();
    let fd = unsafe {
        libc::open(
            root.as_ptr(),
            if names.is_empty() {
                libc::O_RDONLY
            } else {
                TRAVERSE
            } | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_DIRECTORY,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut current = unsafe { File::from_raw_fd(fd) };
    for (index, name) in names.iter().enumerate() {
        let intermediate = index + 1 < names.len();
        let name = CString::new(name.as_bytes())?;
        let flags = if intermediate {
            TRAVERSE | libc::O_DIRECTORY
        } else {
            libc::O_RDONLY | libc::O_NONBLOCK | if directory { libc::O_DIRECTORY } else { 0 }
        };
        let fd = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        current = unsafe { File::from_raw_fd(fd) };
    }
    let meta = current.metadata()?;
    if !(if directory {
        meta.is_dir()
    } else {
        meta.is_file()
    }) {
        return Err(io::Error::other(
            "resource is not a regular file or directory",
        ));
    }
    Ok(current)
}

pub fn directory_entries(file: &File, limit: usize) -> io::Result<Vec<super::DirectoryEntry>> {
    // fdopendir takes ownership only on success; a duplicated descriptor keeps the caller's file live.
    let duplicate = file.try_clone()?;
    let directory = unsafe { libc::fdopendir(duplicate.as_raw_fd()) };
    if directory.is_null() {
        return Err(io::Error::last_os_error());
    }
    let _ = duplicate.into_raw_fd();
    struct Directory(*mut libc::DIR);
    impl Drop for Directory {
        fn drop(&mut self) {
            unsafe { libc::closedir(self.0) };
        }
    }
    let directory = Directory(directory);
    let mut entries = Vec::new();
    loop {
        #[cfg(target_os = "linux")]
        unsafe {
            *libc::__errno_location() = 0;
        }
        #[cfg(any(target_os = "macos", target_os = "freebsd"))]
        unsafe {
            *libc::__error() = 0;
        }
        let entry = unsafe { libc::readdir(directory.0) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(0) {
                return Err(error);
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if matches!(name.to_bytes(), b"." | b"..") {
            continue;
        }
        if entries.len() >= limit {
            return Err(io::Error::other("directory exceeds its entry limit"));
        }
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::fstatat(
                file.as_raw_fd(),
                name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let kind = match stat.st_mode & libc::S_IFMT {
            libc::S_IFDIR => super::FileKind::Directory,
            libc::S_IFREG => super::FileKind::File,
            libc::S_IFLNK => super::FileKind::Link,
            _ => super::FileKind::Other,
        };
        entries.push(super::DirectoryEntry {
            name: OsString::from_vec(name.to_bytes().to_vec()),
            kind,
        });
    }
    Ok(entries)
}

pub fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::rename(from, to)
}

pub fn replace_file_with_retry(from: &Path, to: &Path) -> io::Result<()> {
    replace_file(from, to)
}

pub(super) fn open_skill_root(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(TRAVERSE | libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)
}

pub(super) fn same_file(first: &File, second: &File) -> io::Result<bool> {
    let first = first.metadata()?;
    let second = second.metadata()?;
    Ok(first.dev() == second.dev() && first.ino() == second.ino())
}

/// Put the child in its own process group and kill it if this parent exits.
pub fn configure_child_process_group(command: &mut tokio::process::Command) {
    command.process_group(0);
    #[cfg(target_os = "linux")]
    {
        let parent = std::process::id() as libc::pid_t;
        // SAFETY: only async-signal-safe libc calls run between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    libc::_exit(127);
                }
                Ok(())
            });
        }
    }
    #[cfg(target_os = "freebsd")]
    {
        let parent = std::process::id() as libc::pid_t;
        // SAFETY: procctl/getppid/_exit do not allocate or acquire Rust locks.
        unsafe {
            command.pre_exec(move || {
                let signal: libc::c_int = libc::SIGKILL;
                if libc::procctl(
                    libc::P_PID,
                    0,
                    libc::PROC_PDEATHSIG_CTL,
                    (&signal as *const libc::c_int).cast_mut().cast(),
                ) == -1
                {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    libc::_exit(127);
                }
                Ok(())
            });
        }
    }
}

pub fn signal_process_group(group: i32, signal: i32) {
    // SAFETY: caller owns the unreaped child's process group leader PID.
    unsafe {
        libc::kill(-group, signal);
    }
}

/// Check whether the process group leader has exited without reaping it (keeps PGID pinned).
pub fn child_exited_unreaped(group: &mut Option<i32>) -> io::Result<bool> {
    let Some(pid) = *group else {
        return Ok(true);
    };
    let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
    // SAFETY: WNOWAIT inspects exit status without reaping, keeping PGID pinned.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ECHILD) {
            // Another reaper has taken ownership; this numeric PGID is no longer ours.
            *group = None;
        }
        return Err(error);
    }
    Ok(unsafe { info.si_pid() } == pid)
}

pub async fn wait_child_unreaped(
    group: &mut Option<i32>,
    wait: std::time::Duration,
) -> io::Result<bool> {
    tokio::time::timeout(wait, async {
        while !child_exited_unreaped(group)? {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(true)
    })
    .await
    .unwrap_or(Ok(false))
}
