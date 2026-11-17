use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

pub const MAX_AGENTS_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentsDocument {
    pub path: PathBuf,
    pub content: String,
}

pub(crate) fn validate_path(path: &Path) -> Result<()> {
    let raw = path.as_os_str().as_encoded_bytes();
    if raw.is_empty() {
        bail!("agents path must not be empty");
    }
    if raw.contains(&0) {
        bail!("agents path must not contain NUL");
    }
    #[cfg(windows)]
    if !path.is_absolute()
        && (path.has_root() || matches!(path.components().next(), Some(Component::Prefix(_))))
    {
        bail!("agents path must be absolute or relative, not drive-relative");
    }
    let is_sep = |b: u8| b == b'/' || (cfg!(windows) && b == b'\\');
    if raw.last().copied().is_some_and(is_sep) {
        bail!("agents path must name a regular file, not a directory");
    }
    let tail = match raw.iter().rposition(|&b| is_sep(b)) {
        Some(index) => &raw[index + 1..],
        None => raw,
    };
    if matches!(tail, b"." | b"..") {
        bail!("agents path must name a regular file, not '.' or '..'");
    }
    #[cfg(windows)]
    if tail.contains(&b':')
        || path.components().any(|part| {
            matches!(part, Component::Normal(name) if name.as_encoded_bytes().contains(&b':'))
        })
    {
        bail!("alternate data streams are not supported");
    }
    if !matches!(path.components().next_back(), Some(Component::Normal(_))) {
        bail!("agents path must name a regular file");
    }
    Ok(())
}

/// Read the explicitly configured global AGENTS.md file without project-directory discovery.
pub fn load(configured: Option<&Path>) -> Result<Option<AgentsDocument>> {
    let Some(configured) = configured else {
        return Ok(None);
    };
    validate_path(configured)?;
    let file_name = configured
        .file_name()
        .context("agents path must have a file name")?;
    let raw_parent = match configured.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let parent = crate::platform::canonical_directory(raw_parent)
        .with_context(|| format!("open configured agents parent directory {raw_parent:?}"))?;
    let relative = Path::new(file_name);
    let file = open_regular_file(&parent, relative)
        .with_context(|| format!("open configured agents file {configured:?}"))?;
    ensure!(
        file.metadata()?.len() <= MAX_AGENTS_BYTES,
        "agents file exceeds {MAX_AGENTS_BYTES} bytes"
    );
    let mut bytes = Vec::new();
    file.take(MAX_AGENTS_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_AGENTS_BYTES,
        "agents file exceeds {MAX_AGENTS_BYTES} bytes"
    );
    let content = String::from_utf8(bytes).context("agents file is not UTF-8 text")?;
    Ok(Some(AgentsDocument {
        path: parent.join(file_name),
        content,
    }))
}

fn open_regular_file(parent: &Path, file_name: &Path) -> Result<File> {
    ensure!(
        !file_name.is_absolute(),
        "agents file name must be relative"
    );
    let components: Vec<_> = file_name.components().collect();
    ensure!(
        components.len() == 1 && matches!(components[0], Component::Normal(_)),
        "agents file name must be a single path component"
    );
    ensure!(
        parent.is_absolute(),
        "agents parent directory must be absolute"
    );
    ensure!(
        parent.components().all(|part| matches!(
            part,
            Component::RootDir | Component::Prefix(_) | Component::Normal(_)
        )),
        "agents parent directory must be normalized"
    );
    crate::platform::open_beneath(parent, file_name, false)
        .with_context(|| format!("open agents file {file_name:?} beneath {parent:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::ffi::CString;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStrExt;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    #[cfg(windows)]
    fn symlink(source: impl AsRef<Path>, target: impl AsRef<Path>) -> std::io::Result<()> {
        if source.as_ref().is_dir() {
            std::os::windows::fs::symlink_dir(source, target)
        } else {
            std::os::windows::fs::symlink_file(source, target)
        }
    }
    use tempfile::TempDir;

    #[test]
    fn loads_full_content_and_reflects_updates_without_project_discovery() {
        let temp = TempDir::new().unwrap();
        let project = temp.path().join("project");
        fs::create_dir_all(&project).unwrap();
        fs::write(project.join("AGENTS.md"), "repo-scoped instructions").unwrap();
        assert_eq!(load(None).unwrap(), None);

        let global_dir = temp.path().join("global");
        fs::create_dir_all(&global_dir).unwrap();
        let agents_path = global_dir.join("AGENTS.md");
        let initial = "\u{feff}# Global Instructions\r\nAlways inspect tool schemas first.\r\n";
        fs::write(&agents_path, initial).unwrap();
        let doc = load(Some(&agents_path)).unwrap().unwrap();
        assert_eq!(doc.path, agents_path.canonicalize().unwrap());
        assert_eq!(doc.content, initial);

        let updated = "# Global Instructions\nUpdated guidance.\n";
        fs::write(&agents_path, updated).unwrap();
        assert_eq!(load(Some(&agents_path)).unwrap().unwrap().content, updated);
    }

    #[test]
    fn rejects_invalid_paths_links_directories_special_files_and_oversize_binary() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("dir");
        fs::create_dir_all(&dir).unwrap();
        let real = dir.join("AGENTS.md");
        fs::write(&real, "# Global\n").unwrap();

        for invalid in ["", ".", "..", "dir/", "dir/.", "dir/.."] {
            assert!(load(Some(Path::new(invalid))).is_err(), "{invalid:?}");
        }
        assert!(load(Some(&dir)).is_err());
        assert!(load(Some(&dir.join("missing.md"))).is_err());

        for suffix in ["/", "/."] {
            let mut with_suffix = real.as_os_str().to_os_string();
            with_suffix.push(suffix);
            assert!(load(Some(&PathBuf::from(with_suffix))).is_err());
        }
        #[cfg(windows)]
        for suffix in [r"\", r"\."] {
            let mut with_suffix = real.as_os_str().to_os_string();
            with_suffix.push(suffix);
            assert!(load(Some(&PathBuf::from(with_suffix))).is_err());
        }

        let linked_file = dir.join("linked-agents.md");
        symlink(&real, &linked_file).unwrap();
        assert!(load(Some(&linked_file)).is_err());

        let linked_dir = temp.path().join("linked-dir");
        symlink(&dir, &linked_dir).unwrap();
        assert!(load(Some(&linked_dir.join("AGENTS.md"))).is_err());

        #[cfg(windows)]
        {
            fs::write(dir.join("AGENTS.md:secret"), "hidden").unwrap();
            assert!(load(Some(&dir.join("AGENTS.md:secret"))).is_err());
            for drive_relative in ["C:AGENTS.md", r"\AGENTS.md"] {
                assert!(
                    load(Some(Path::new(drive_relative))).is_err(),
                    "{drive_relative:?}"
                );
            }
        }
        #[cfg(unix)]
        {
            let fifo_path = dir.join("fifo.md");
            let fifo = CString::new(fifo_path.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
            assert!(load(Some(&fifo_path)).is_err());
        }

        let large = dir.join("large.md");
        File::create(&large)
            .unwrap()
            .set_len(MAX_AGENTS_BYTES + 1)
            .unwrap();
        assert!(load(Some(&large)).is_err());

        let binary = dir.join("binary.md");
        fs::write(&binary, [0xff, 0xfe]).unwrap();
        assert!(load(Some(&binary)).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn traverses_execute_only_ancestor_as_unprivileged_user() {
        use std::os::unix::fs::PermissionsExt;
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let temp = TempDir::new_in("/tmp").unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o711)).unwrap();
        let guard = temp.path().join("restricted");
        fs::create_dir(&guard).unwrap();
        fs::set_permissions(&guard, fs::Permissions::from_mode(0o711)).unwrap();
        let config_dir = guard.join("etc");
        fs::create_dir(&config_dir).unwrap();
        let agents_path = config_dir.join("AGENTS.md");
        fs::write(&agents_path, "# Global Rules\n").unwrap();
        fs::set_permissions(&config_dir, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&agents_path, fs::Permissions::from_mode(0o644)).unwrap();

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            let ok = unsafe { libc::setgid(65534) == 0 && libc::setuid(65534) == 0 }
                && load(Some(&agents_path))
                    .map(|doc| doc.is_some_and(|d| d.content == "# Global Rules\n"))
                    .unwrap_or(false);
            unsafe { libc::_exit(if ok { 0 } else { 1 }) };
        }
        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    }
}
