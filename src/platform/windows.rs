use super::local::{self, SecurityAttributes};
use anyhow::{Context, Result, bail};
use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::os::windows::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, LocalFree};
use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, DACL_SECURITY_INFORMATION, GetAce, OWNER_SECURITY_INFORMATION,
    TOKEN_QUERY,
};
use windows_sys::Win32::Storage::FileSystem::*;
use windows_sys::Win32::System::Threading::*;

fn wide(path: &OsStr) -> io::Result<Vec<u16>> {
    let mut result: Vec<u16> = path.encode_wide().collect();
    if result.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains NUL",
        ));
    }
    result.push(0);
    Ok(result)
}

fn refuse(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

struct SecurityData(windows_sys::Win32::Foundation::HLOCAL);
impl Drop for SecurityData {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

fn sid_text(sid: windows_sys::Win32::Security::PSID) -> io::Result<String> {
    let mut text = std::ptr::null_mut();
    if unsafe {
        windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW(sid, &mut text)
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let _allocation = SecurityData(text.cast());
    let mut length = 0;
    unsafe {
        while *text.add(length) != 0 {
            length += 1;
        }
    }
    Ok(String::from_utf16_lossy(unsafe {
        std::slice::from_raw_parts(text, length)
    }))
}

/// Validate the opened object, including grants invisible to Rust's readonly permission flag.
fn check_security(file: &File, private: bool) -> io::Result<()> {
    let mut owner = std::ptr::null_mut();
    let mut dacl = std::ptr::null_mut();
    let mut descriptor = std::ptr::null_mut();
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let _allocation = SecurityData(descriptor);
    let user = local::user_sid()?;
    let privileged = |sid: &str| {
        sid == user
            || matches!(
                sid,
                "S-1-5-18"
                    | "S-1-5-32-544"
                    | "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"
            )
    };
    if owner.is_null() || !privileged(&sid_text(owner)?) {
        return Err(refuse("runtime object belongs to another user"));
    }
    if dacl.is_null() {
        return Err(refuse("runtime object has an unrestricted DACL"));
    }
    for index in 0..unsafe { (*dacl).AceCount } {
        let mut ace = std::ptr::null_mut();
        if unsafe { GetAce(dacl, u32::from(index), &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let header = unsafe { &*ace.cast::<ACE_HEADER>() };
        // Inherit-only grants do not grant access to this ancestor. Private leaf children are checked too.
        if header.AceFlags & 0x08 != 0 && !private {
            continue;
        }
        if header.AceType == 1 {
            continue;
        }
        if header.AceType != 0 {
            return Err(refuse("runtime object has an unsupported access rule"));
        }
        let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
        let sid = sid_text(std::ptr::addr_of!(allowed.SidStart).cast_mut().cast())?;
        if privileged(&sid) || matches!(sid.as_str(), "S-1-3-0" | "S-1-3-4") {
            continue;
        }
        // Either write-data or write-attributes can turn an empty directory into a junction.
        const MUTATES_ANCESTOR: u32 = 0x10000000
            | 0x40000000
            | WRITE_DAC
            | WRITE_OWNER
            | DELETE
            | FILE_DELETE_CHILD
            | FILE_WRITE_DATA
            | FILE_WRITE_ATTRIBUTES;
        if private || allowed.Mask & MUTATES_ANCESTOR != 0 {
            return Err(refuse("runtime object grants access to another user"));
        }
    }
    Ok(())
}

fn open_directory(path: &Path, enumerate: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .access_mode(
            READ_CONTROL
                | FILE_READ_ATTRIBUTES
                | if enumerate {
                    FILE_LIST_DIRECTORY
                } else {
                    FILE_TRAVERSE
                },
        )
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_dir() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(refuse("path is not a real directory"));
    }
    Ok(file)
}

pub fn private_dir(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("runtime directory must be absolute");
    }
    // Pin each ancestor against replacement; its ACL also excludes conversion into a junction.
    let mut guards = Vec::new();
    let parents: Vec<_> = path.ancestors().skip(1).collect();
    for parent in parents.into_iter().rev() {
        let guard = open_directory(parent, false)?;
        check_security(&guard, false)
            .with_context(|| format!("unsafe runtime ancestor {parent:?}"))?;
        guards.push(guard);
    }
    let attributes = SecurityAttributes::owner_only(true)?;
    let name = wide(path.as_os_str())?;
    let created = attributes.with_raw(|raw| unsafe { CreateDirectoryW(name.as_ptr(), raw.cast()) });
    if created == 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
    }
    let file = open_directory(path, false)?;
    check_security(&file, true).with_context(|| format!("unsafe runtime directory {path:?}"))?;
    Ok(())
}

fn create_file(path: &Path, disposition: u32) -> io::Result<File> {
    let attributes = SecurityAttributes::owner_only(false)?;
    let name = wide(path.as_os_str())?;
    let handle = attributes.with_raw(|raw| unsafe {
        CreateFileW(
            name.as_ptr(),
            windows_sys::Win32::Foundation::GENERIC_READ
                | windows_sys::Win32::Foundation::GENERIC_WRITE
                | READ_CONTROL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            raw.cast(),
            disposition,
            FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    });
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let file = unsafe { File::from_raw_handle(handle) };
    let meta = file.metadata()?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if !meta.is_file()
        || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || info.nNumberOfLinks != 1
    {
        return Err(refuse(
            "runtime file must be a single-link regular file, not a reparse point",
        ));
    }
    check_security(&file, true)?;
    Ok(file)
}

pub fn secure_file(path: &Path) -> Result<File> {
    Ok(create_file(path, OPEN_ALWAYS)?)
}
pub fn new_private_file(path: &Path) -> io::Result<File> {
    create_file(path, CREATE_NEW)
}

pub fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    let from = wide(from.as_os_str())?;
    let to = wide(to.as_os_str())?;
    if unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}
pub fn monotonic_millis() -> io::Result<u64> {
    Ok(unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() })
}

pub fn spawn_daemon(command: &mut Command) -> io::Result<Child> {
    use windows_sys::Win32::System::JobObjects::IsProcessInJob;
    let mut inside = 0;
    if unsafe { IsProcessInJob(GetCurrentProcess(), std::ptr::null_mut(), &mut inside) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let flags = DETACHED_PROCESS
        | CREATE_NEW_PROCESS_GROUP
        | if inside != 0 {
            CREATE_BREAKAWAY_FROM_JOB
        } else {
            0
        };
    let mut child = command.creation_flags(flags).spawn().map_err(|error| {
        if inside != 0 { io::Error::other(format!("daemon cannot leave the starter's Job; start it from a terminal outside the host, or use daemon run: {error}")) } else { error }
    })?;
    let mut child_inside = 0;
    if unsafe {
        IsProcessInJob(
            child.as_raw_handle(),
            std::ptr::null_mut(),
            &mut child_inside,
        )
    } == 0
        || child_inside != 0
    {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::other("daemon did not escape the starter's Job"));
    }
    Ok(child)
}

pub fn remove_endpoint(_path: &Path) -> Result<()> {
    Ok(())
}
pub fn secure_endpoint(_path: &Path) -> io::Result<()> {
    Ok(())
}
pub fn validate_endpoint(_path: &Path) -> Result<()> {
    Ok(())
}
pub fn validate_client(_stream: &local::Conn) -> io::Result<()> {
    Ok(())
}

pub fn validate_server(stream: &local::Client) -> io::Result<()> {
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    let mut pid = 0;
    if unsafe { GetNamedPipeServerProcessId(stream.as_raw_handle(), &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let mut token: HANDLE = std::ptr::null_mut();
    let opened = unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) };
    unsafe { CloseHandle(process) };
    if opened == 0 {
        return Err(io::Error::last_os_error());
    }
    let sid = local::token_user_sid(token);
    unsafe { CloseHandle(token) };
    if sid? != local::user_sid()? {
        return Err(refuse("daemon peer belongs to another user"));
    }
    Ok(())
}

pub fn open_input(path: &Path) -> io::Result<File> {
    let meta = std::fs::metadata(path)?;
    if !meta.is_file() {
        return Err(io::Error::other("input must be a regular file"));
    }
    File::open(path)
}

pub fn open_beneath(root: &Path, relative: &Path, directory: bool) -> io::Result<File> {
    let combined = root.join(relative);
    let mut path = PathBuf::new();
    let mut guards = Vec::new();
    let parts: Vec<_> = combined.components().collect();
    for (index, component) in parts.iter().enumerate() {
        path.push(component.as_os_str());
        match component {
            Component::Prefix(_) => continue,
            Component::RootDir | Component::Normal(_) => {}
            _ => return Err(refuse("resource path is not normalized")),
        }
        if let Component::Normal(name) = component
            && name.encode_wide().any(|unit| unit == u16::from(b':'))
        {
            return Err(refuse("alternate data streams are not skill resources"));
        }
        let intermediate = index + 1 < parts.len();
        if intermediate || directory {
            let file = open_directory(&path, !intermediate)?;
            if !intermediate {
                return Ok(file);
            }
            guards.push(file);
        } else {
            let file = OpenOptions::new()
                .read(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&path)?;
            let meta = file.metadata()?;
            if !meta.is_file() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return Err(refuse("resource is not a regular file"));
            }
            return Ok(file);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "empty skill root",
    ))
}

pub fn directory_entries(file: &File, limit: usize) -> io::Result<Vec<super::DirectoryEntry>> {
    let mut entries = Vec::new();
    let mut buffer = vec![0u64; 8192];
    let length = (buffer.len() * std::mem::size_of::<u64>()) as u32;
    loop {
        let result = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                FileIdBothDirectoryInfo,
                buffer.as_mut_ptr().cast(),
                length,
            )
        };
        if result == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error()
                == Some(windows_sys::Win32::Foundation::ERROR_NO_MORE_FILES as i32)
            {
                break;
            }
            return Err(error);
        }
        let bytes = buffer.as_ptr().cast::<u8>();
        let mut offset = 0usize;
        loop {
            const NAME_OFFSET: usize = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
            if offset + std::mem::size_of::<FILE_ID_BOTH_DIR_INFO>() > length as usize {
                return Err(io::Error::other("invalid directory record"));
            }
            let entry = unsafe { &*bytes.add(offset).cast::<FILE_ID_BOTH_DIR_INFO>() };
            let name_length = entry.FileNameLength as usize;
            if !name_length.is_multiple_of(2)
                || offset + NAME_OFFSET + name_length > length as usize
            {
                return Err(io::Error::other("invalid directory filename"));
            }
            let units = unsafe {
                std::slice::from_raw_parts(
                    bytes.add(offset + NAME_OFFSET).cast::<u16>(),
                    name_length / 2,
                )
            };
            let name = OsString::from_wide(units);
            if name != "." && name != ".." {
                if entries.len() >= limit {
                    return Err(io::Error::other("directory exceeds its entry limit"));
                }
                let kind = if entry.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                    super::FileKind::Link
                } else if entry.FileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0 {
                    super::FileKind::Directory
                } else if entry.FileAttributes & FILE_ATTRIBUTE_DEVICE != 0 {
                    super::FileKind::Other
                } else {
                    super::FileKind::File
                };
                entries.push(super::DirectoryEntry { name, kind });
            }
            if entry.NextEntryOffset == 0 {
                break;
            }
            if entry.NextEntryOffset < NAME_OFFSET as u32 {
                return Err(io::Error::other("invalid directory offset"));
            }
            offset += entry.NextEntryOffset as usize;
        }
    }
    Ok(entries)
}

pub(super) fn open_skill_root(path: &Path) -> io::Result<File> {
    open_directory(path, false)
}

pub(super) fn same_file(first: &File, second: &File) -> io::Result<bool> {
    let info = |file: &File| -> io::Result<BY_HANDLE_FILE_INFORMATION> {
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(info)
    };
    let a = info(first)?;
    let b = info(second)?;
    Ok(a.dwVolumeSerialNumber == b.dwVolumeSerialNumber
        && a.nFileIndexHigh == b.nFileIndexHigh
        && a.nFileIndexLow == b.nFileIndexLow)
}
