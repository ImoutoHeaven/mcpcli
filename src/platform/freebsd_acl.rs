//! Reject FreeBSD ACL grants that Unix mode bits do not describe.

use std::ffi::{c_int, c_void};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

const ACL_TYPE_ACCESS: c_int = 2;
const ACL_TYPE_DEFAULT: c_int = 3;
const ACL_TYPE_NFS4: c_int = 4;
const ACL_FIRST_ENTRY: c_int = 0;

// FreeBSD public <sys/acl.h> APIs not exposed by libc.
unsafe extern "C" {
    fn acl_get_tag_type(entry: *mut c_void, tag: *mut u32) -> c_int;
    fn acl_get_entry_type_np(entry: *mut c_void, kind: *mut u16) -> c_int;
    fn acl_get_permset(entry: *mut c_void, permissions: *mut *mut c_void) -> c_int;
    fn acl_get_perm_np(permissions: *mut c_void, permission: u32) -> c_int;
    fn acl_get_qualifier(entry: *mut c_void) -> *mut c_void;
    fn acl_get_fd_np(fd: c_int, kind: c_int) -> *mut c_void;
    fn acl_free(acl: *mut c_void) -> c_int;
    fn acl_is_trivial_np(acl: *mut c_void, trivial: *mut c_int) -> c_int;
    fn acl_get_entry(acl: *mut c_void, entry_id: c_int, entry: *mut *mut c_void) -> c_int;
}

struct Acl {
    raw: *mut c_void,
    kind: c_int,
}

impl Drop for Acl {
    fn drop(&mut self) {
        // SAFETY: every Acl owns exactly one allocation from the native ACL API.
        unsafe { acl_free(self.raw) };
    }
}

impl Acl {
    fn read(file: &File, kind: c_int) -> io::Result<Self> {
        // SAFETY: the descriptor is live; the ACL allocation is checked and owned below.
        let raw = unsafe { acl_get_fd_np(file.as_raw_fd(), kind) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { raw, kind })
    }

    fn trivial(&self) -> io::Result<bool> {
        let mut trivial = 0;
        // SAFETY: the native ACL is valid and the output points to writable storage.
        if unsafe { acl_is_trivial_np(self.raw, &mut trivial) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(trivial != 0)
    }

    fn empty(&self) -> io::Result<bool> {
        let mut entry = std::ptr::null_mut();
        // SAFETY: the allocation owns any entry the API returns; it is not freed separately.
        match unsafe { acl_get_entry(self.raw, ACL_FIRST_ENTRY, &mut entry) } {
            0 => Ok(true),
            1 => Ok(false),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

fn unsupported(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
    )
}

fn access_acl(file: &File) -> io::Result<Option<Acl>> {
    for kind in [ACL_TYPE_NFS4, ACL_TYPE_ACCESS] {
        match Acl::read(file, kind) {
            Ok(acl) => return Ok(Some(acl)),
            Err(error) if unsupported(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

/// Existing runtime directories are checked without changing their owner's policy.
pub fn check_private_acl(file: &File) -> io::Result<()> {
    if let Some(acl) = access_acl(file)? {
        if !acl.trivial()? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a private directory has a non-trivial ACL",
            ));
        }
        if file.metadata()?.is_dir()
            && acl.kind == ACL_TYPE_ACCESS
            && !Acl::read(file, ACL_TYPE_DEFAULT)?.empty()?
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a private directory has an inheritable default ACL",
            ));
        }
    }
    Ok(())
}

/// A trivial ACL is represented by the checked mode. Nontrivial NFSv4 grants need an explicit check.
pub(super) fn check_ancestor(file: &File) -> io::Result<()> {
    let Some(acl) = access_acl(file)? else {
        return Ok(());
    };
    if acl.kind == ACL_TYPE_ACCESS || acl.trivial()? {
        return Ok(());
    }
    let mut position = ACL_FIRST_ENTRY;
    loop {
        let mut entry = std::ptr::null_mut();
        match unsafe { acl_get_entry(acl.raw, position, &mut entry) } {
            0 => return Ok(()),
            1 => {}
            _ => return Err(io::Error::last_os_error()),
        }
        position = 1;
        let mut kind = 0u16;
        let mut tag = 0u32;
        let mut permissions = std::ptr::null_mut();
        if unsafe { acl_get_entry_type_np(entry, &mut kind) } != 0
            || unsafe { acl_get_tag_type(entry, &mut tag) } != 0
            || unsafe { acl_get_permset(entry, &mut permissions) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if kind == 0x0200 {
            continue;
        } // ACL_ENTRY_TYPE_DENY
        if kind != 0x0100 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "runtime ancestor has an unsupported ACL entry",
            ));
        }
        let mut mutating = false;
        for permission in [0x10, 0x20, 0x80, 0x100, 0x400, 0x800, 0x2000, 0x4000] {
            match unsafe { acl_get_perm_np(permissions, permission) } {
                0 => {}
                1 => mutating = true,
                _ => return Err(io::Error::last_os_error()),
            }
        }
        if !mutating || tag == 0x0001 {
            continue;
        } // ACL_USER_OBJ owner already validated
        if tag == 0x0002 {
            // ACL_USER
            let qualifier = unsafe { acl_get_qualifier(entry) };
            if qualifier.is_null() {
                return Err(io::Error::last_os_error());
            }
            let uid = unsafe { *qualifier.cast::<libc::uid_t>() };
            unsafe { acl_free(qualifier) };
            if uid == 0 || uid == unsafe { libc::geteuid() } {
                continue;
            }
        }
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime ancestor ACL grants mutation to another user",
        ));
    }
}
