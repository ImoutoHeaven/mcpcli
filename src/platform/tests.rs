use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn local_streams_serve_concurrent_clients_and_authenticate_peers() {
    #[cfg(unix)]
    let base = tempfile::tempdir_in("/tmp").unwrap();
    #[cfg(windows)]
    let base = tempfile::tempdir().unwrap();
    let directory = base.path().canonicalize().unwrap().join("private");
    private_dir(&directory).unwrap();
    let endpoint = endpoint(&directory, false).unwrap();
    let mut listener = local::Listener::bind(endpoint.as_os_str()).unwrap();
    secure_endpoint(&endpoint).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        let clients: Vec<_> = (0..3)
            .map(|_| {
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    let mut client = local::connect(endpoint.as_os_str()).await.unwrap();
                    validate_server(&client).unwrap();
                    client.write_all(b"ping").await.unwrap();
                    let mut reply = [0u8; 4];
                    client.read_exact(&mut reply).await.unwrap();
                    assert_eq!(&reply, b"pong");
                })
            })
            .collect();
        for _ in 0..3 {
            let mut connection = listener.accept().await.unwrap();
            validate_client(&connection).unwrap();
            let mut request = [0u8; 4];
            connection.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"ping");
            connection.write_all(b"pong").await.unwrap();
        }
        for client in clients {
            client.await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[test]
fn monotonic_deadlines_advance() {
    let before = monotonic_millis().unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let after = monotonic_millis().unwrap();
    assert!(after >= before + 10);
}

#[cfg(windows)]
#[test]
fn runtime_dacls_and_reparse_points_are_checked() {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, READ_CONTROL, WRITE_DAC,
    };
    let base = tempfile::tempdir().unwrap();
    let directory = base.path().canonicalize().unwrap().join("private");
    private_dir(&directory).unwrap();
    let path = directory.join("private-file");
    let file = secure_file(&path).unwrap();
    drop(file);
    let alias = directory.join("linked-file");
    std::os::windows::fs::symlink_file(&path, &alias).unwrap();
    assert!(secure_file(&alias).is_err());
    let link = base.path().join("linked-dir");
    std::os::windows::fs::symlink_dir(&directory, &link).unwrap();
    assert!(private_dir(&link).is_err());
    for target in [&path, &directory] {
        let file = std::fs::OpenOptions::new()
            .access_mode(READ_CONTROL | WRITE_DAC)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(target)
            .unwrap();
        assert_eq!(
            unsafe {
                SetSecurityInfo(
                    file.as_raw_handle(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        drop(file);
        if target == &path {
            assert!(secure_file(target).is_err());
        } else {
            assert!(private_dir(target).is_err());
        }
    }
}

#[cfg(windows)]
#[test]
fn skill_resources_refuse_alternate_data_streams() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().canonicalize().unwrap();
    std::fs::write(root.join("file"), "ordinary").unwrap();
    std::fs::write(root.join("file:secret"), "hidden").unwrap();
    assert!(open_beneath(&root, Path::new("file:secret"), false).is_err());
}

#[cfg(unix)]
#[test]
fn canonical_skill_root_detects_replacement_during_resolution() {
    let base = tempfile::tempdir().unwrap();
    let original = base.path().join("original");
    let outside = base.path().join("outside");
    std::fs::create_dir(&original).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let error = canonical_skill_root_with(&original, |path| {
        std::fs::rename(path, base.path().join("old")).unwrap();
        std::os::unix::fs::symlink(&outside, path).unwrap();
        path.canonicalize()
    })
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
}

#[cfg(windows)]
#[test]
fn canonical_skill_root_pins_the_leaf_against_replacement() {
    let base = tempfile::tempdir().unwrap();
    let original = base.path().join("original");
    std::fs::create_dir(&original).unwrap();
    canonical_skill_root_with(&original, |path| {
        assert!(std::fs::rename(path, base.path().join("old")).is_err());
        path.canonicalize()
    })
    .unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn macos_ancestor_acl_mutation_is_rejected() {
    use std::os::unix::fs::PermissionsExt;
    let base = tempfile::tempdir_in("/tmp").unwrap();
    let parent = base.path().canonicalize().unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let chmod = |rule: &str| {
        let output = std::process::Command::new("/bin/chmod")
            .args(["+a", rule])
            .arg(&parent)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    chmod("user:nobody allow readattr,readsecurity");
    private_dir(&parent.join("read-only-parent")).unwrap();
    chmod("group:everyone deny delete");
    private_dir(&parent.join("deny-parent")).unwrap();
    chmod("user:nobody allow delete_child");
    let mode = std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777;
    let rejected = private_dir(&parent.join("must-not-exist"));
    let cleared = std::process::Command::new("/bin/chmod")
        .arg("-N")
        .arg(&parent)
        .status()
        .unwrap();
    assert!(cleared.success());
    assert_eq!(mode, 0o755);
    let error = rejected.unwrap_err();
    assert!(format!("{error:#}").contains("ancestor ACL"), "{error:#}");
    assert!(!parent.join("must-not-exist").exists());
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_ancestor_acl_mutation_is_rejected() {
    use std::os::unix::fs::PermissionsExt;
    let base = tempfile::tempdir().unwrap();
    let parent = base.path().canonicalize().unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let acl = |rule: &str| {
        std::process::Command::new("setfacl")
            .args(["-m", rule])
            .arg(&parent)
            .output()
            .unwrap()
    };
    let read = acl("u:65534:raRc::allow");
    if !read.status.success() {
        assert_ne!(
            std::env::var("MCPCLI_REQUIRE_NFS4_ACL").as_deref(),
            Ok("1"),
            "required NFSv4 ACL fixture is unavailable: {}",
            String::from_utf8_lossy(&read.stderr)
        );
        eprintln!(
            "NFSv4 ancestor ACL regression runs in the required disposable-filesystem CI lane"
        );
        return;
    }
    private_dir(&parent.join("read-only-parent")).unwrap();
    let added = acl("u:65534:D::allow");
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    assert_eq!(
        std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
        0o755
    );
    let error = private_dir(&parent.join("must-not-exist")).unwrap_err();
    assert!(format!("{error:#}").contains("ancestor ACL"), "{error:#}");
    assert!(!parent.join("must-not-exist").exists());
}

#[cfg(windows)]
#[test]
fn daemon_spawn_respects_job_breakaway_policy() {
    use std::io::{Read, Write};
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use std::process::{Command, Stdio};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject,
    };
    if let Ok(mode) = std::env::var("MCPCLI_TEST_JOB_PROBE") {
        std::io::stdin().read_exact(&mut [0u8; 1]).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "no_such_test"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let result = spawn_daemon(&mut command);
        if mode == "allow" {
            assert!(result.unwrap().wait().unwrap().success());
        } else {
            assert!(result.unwrap_err().to_string().contains("Job"));
        }
        return;
    }
    for allow in [false, true] {
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        assert!(!raw.is_null());
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            | if allow {
                JOB_OBJECT_LIMIT_BREAKAWAY_OK
            } else {
                0
            };
        assert_ne!(
            unsafe {
                SetInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    std::mem::size_of_val(&limits) as u32,
                )
            },
            0
        );
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "platform::tests::daemon_spawn_respects_job_breakaway_policy",
                "--nocapture",
            ])
            .env(
                "MCPCLI_TEST_JOB_PROBE",
                if allow { "allow" } else { "deny" },
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        assert_ne!(
            unsafe { AssignProcessToJobObject(job.as_raw_handle(), child.as_raw_handle()) },
            0
        );
        child.stdin.take().unwrap().write_all(b"x").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("job breakaway probe did not finish");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(windows)]
#[test]
fn default_runtime_parent_accepts_private_user_files() {
    let profile =
        PathBuf::from(std::env::var_os("LOCALAPPDATA").expect("Windows user local app data"))
            .canonicalize()
            .unwrap();
    assert_eq!(default_runtime().unwrap().parent(), Some(profile.as_path()));
    let base = tempfile::tempdir_in(&profile).unwrap();
    let private = base.path().join("runtime");
    private_dir(&private).unwrap();
    secure_file(&private.join("state")).unwrap();
    eprintln!(
        "Windows native temp: {}; local app data: {}",
        std::env::temp_dir().display(),
        profile.display()
    );
}
