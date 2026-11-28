//! The local byte streams the daemon listens on and a client connects to: a named pipe whose
//! access is the current user's alone on Windows, a Unix socket elsewhere.

use std::ffi::{OsStr, OsString};
use std::io;

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
    pub name: OsString,
    pub kind: FileKind,
}

/// The server side of one accepted connection.
#[cfg(windows)]
pub type Conn = tokio::net::windows::named_pipe::NamedPipeServer;
/// The server side of one accepted connection.
#[cfg(unix)]
pub type Conn = tokio::net::UnixStream;

/// The client side of one connection.
#[cfg(windows)]
pub type Client = tokio::net::windows::named_pipe::NamedPipeClient;
/// The client side of one connection.
#[cfg(unix)]
pub type Client = tokio::net::UnixStream;

/// How long a client waits between attempts on a busy pipe.
#[cfg(windows)]
const CONNECT_PAUSE: std::time::Duration = std::time::Duration::from_millis(50);

/// A listening endpoint: the daemon's side of every connection it will accept.
pub struct Listener {
    #[cfg(windows)]
    name: std::ffi::OsString,
    #[cfg(windows)]
    attributes: SecurityAttributes,
    #[cfg(windows)]
    current: Conn,
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
}

impl Listener {
    /// Listens on `name`: a pipe name on Windows, a socket path on Unix, whose parent directory
    /// the caller made private.
    pub fn bind(name: &OsStr) -> io::Result<Listener> {
        #[cfg(windows)]
        {
            let attributes = SecurityAttributes::owner_only(false)?;
            // Only the first instance creates the name, so a second listener on a live name
            // fails instead of joining it.
            let options = server_options(true);
            let current = attributes.with_raw(|raw| {
                // SAFETY: `raw` points at valid attributes for the length of this call.
                unsafe { options.create_with_security_attributes_raw(name, raw) }
            })?;
            Ok(Listener {
                name: name.to_os_string(),
                attributes,
                current,
            })
        }
        #[cfg(unix)]
        {
            Ok(Listener {
                inner: tokio::net::UnixListener::bind(name)?,
            })
        }
    }

    /// Waits for the next connection.
    pub async fn accept(&mut self) -> io::Result<Conn> {
        #[cfg(windows)]
        {
            self.current.connect().await?;
            // The next instance exists before the accepted one is handed out: a client that
            // connects the moment this returns finds a pipe to open rather than a busy name.
            let options = server_options(false);
            let next = self.attributes.with_raw(|raw| {
                // SAFETY: `raw` points at valid attributes for the length of this call.
                unsafe { options.create_with_security_attributes_raw(&self.name, raw) }
            })?;
            Ok(std::mem::replace(&mut self.current, next))
        }
        #[cfg(unix)]
        {
            let (stream, _address) = self.inner.accept().await?;
            Ok(stream)
        }
    }
}

/// Connects to the listener on `name`.
pub async fn connect(name: &OsStr) -> io::Result<Client> {
    #[cfg(windows)]
    {
        loop {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(name) {
                Ok(client) => return Ok(client),
                // Every instance is serving someone else: wait for one to come free.
                Err(error)
                    if error.raw_os_error()
                        == Some(windows_sys::Win32::Foundation::ERROR_PIPE_BUSY as i32) =>
                {
                    tokio::time::sleep(CONNECT_PAUSE).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
    #[cfg(unix)]
    {
        tokio::net::UnixStream::connect(name).await
    }
}

#[cfg(windows)]
fn server_options(first: bool) -> tokio::net::windows::named_pipe::ServerOptions {
    let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
    // A pipe name is machine-wide, so it refuses remote clients: only this machine reaches it,
    // and the DACL below limits it to this user.
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true);
    options
}

/// A security descriptor with a protected DACL that grants full access to the current user and to
/// nobody else. A pipe and a directory both use one, with different inheritance.
///
/// The descriptor is held as an address rather than a pointer, so a listener stays `Send`: it is
/// read only while an object is created, and this value owns it until it is freed.
#[cfg(windows)]
pub(crate) struct SecurityAttributes {
    descriptor: usize,
}

#[cfg(windows)]
impl SecurityAttributes {
    /// The owner-only descriptor. `inherit` also applies the ACE to children, which a directory
    /// needs and a pipe does not.
    pub(crate) fn owner_only(inherit: bool) -> io::Result<Self> {
        let ace = if inherit { "OICI" } else { "" };
        let sddl = format!("D:P(A;{ace};GA;;;{})", user_sid()?);
        let wide = wide(&sddl);
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated, and the call writes the descriptor it allocates.
        let converted = unsafe {
            windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        };
        if converted == 0 {
            return Err(io::Error::other(format!(
                "cannot build the security descriptor {sddl:?}: {}",
                io::Error::last_os_error()
            )));
        }
        Ok(Self {
            descriptor: descriptor as usize,
        })
    }

    /// Calls `create` with the raw attributes, which are valid for the length of the call.
    pub(crate) fn with_raw<T>(&self, create: impl FnOnce(*mut std::ffi::c_void) -> T) -> T {
        let mut raw = windows_sys::Win32::Security::SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<windows_sys::Win32::Security::SECURITY_ATTRIBUTES>()
                as u32,
            lpSecurityDescriptor: self.descriptor
                as windows_sys::Win32::Security::PSECURITY_DESCRIPTOR,
            bInheritHandle: 0,
        };
        create((&mut raw as *mut windows_sys::Win32::Security::SECURITY_ATTRIBUTES).cast())
    }
}

#[cfg(windows)]
impl Drop for SecurityAttributes {
    fn drop(&mut self) {
        // SAFETY: the descriptor came from the SDDL conversion above and is freed once.
        unsafe {
            windows_sys::Win32::Foundation::LocalFree(
                self.descriptor as windows_sys::Win32::Foundation::HLOCAL,
            );
        }
    }
}

#[cfg(windows)]
const SDDL_REVISION_1: u32 = 1;

/// The current user's SID as `S-1-5-...`, which is what the pipe's DACL names.
#[cfg(windows)]
pub fn user_sid() -> io::Result<String> {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Security::TOKEN_QUERY;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: the call opens this process's token for query, and the handle is closed below.
    let opened = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) };
    if opened == 0 {
        return Err(io::Error::last_os_error());
    }
    let sid = token_user_sid(token);
    // SAFETY: the handle came from OpenProcessToken and is closed once.
    unsafe { CloseHandle(token) };
    sid
}

#[cfg(windows)]
pub(super) fn token_user_sid(token: windows_sys::Win32::Foundation::HANDLE) -> io::Result<String> {
    use windows_sys::Win32::Foundation::{HLOCAL, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_USER, TokenUser};

    let mut needed = 0u32;
    // SAFETY: a null buffer with no length is the documented size query; its error is expected.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut needed) };
    if needed == 0 {
        return Err(io::Error::last_os_error());
    }
    // The buffer is word-sized, so the TOKEN_USER the call writes into it is aligned.
    let mut buffer = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
    let length = (buffer.len() * std::mem::size_of::<usize>()) as u32;
    // SAFETY: the buffer holds `length` bytes, and the call writes at most `needed` of them.
    let filled = unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast::<std::ffi::c_void>(),
            length,
            &mut needed,
        )
    };
    if filled == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call filled the buffer with a TOKEN_USER whose SID points inside it.
    let user: &TOKEN_USER = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut text: *mut u16 = std::ptr::null_mut();
    // SAFETY: `user.User.Sid` is a valid SID, and the string it returns is freed below.
    let converted = unsafe { ConvertSidToStringSidW(user.User.Sid, &mut text) };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `text` is a NUL-terminated string the call allocated.
    let sid = unsafe { wide_to_string(text) };
    // SAFETY: the string came from ConvertSidToStringSidW and is freed once.
    unsafe { LocalFree(text as HLOCAL) };
    Ok(sid)
}

#[cfg(windows)]
fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Reads a NUL-terminated UTF-16 string.
///
/// # Safety
///
/// `text` must point at a NUL-terminated UTF-16 string.
#[cfg(windows)]
unsafe fn wide_to_string(text: *const u16) -> String {
    let mut length = 0;
    // SAFETY: the caller promises a terminated string, so this stops at its terminator.
    while unsafe { *text.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: the string is `length` units long and the loop above reached its terminator.
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, length) })
}
