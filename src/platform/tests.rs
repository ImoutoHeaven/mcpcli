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
    use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, WRITE_DAC};
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
            .access_mode(WRITE_DAC)
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
