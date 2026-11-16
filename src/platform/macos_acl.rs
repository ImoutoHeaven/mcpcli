use std::ffi::{CString, c_void};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::{io, path::Path};

// Public Darwin <sys/acl.h>/<sys/fcntl.h> APIs not currently exposed by Rust libc.
const FILESEC_MODE: libc::c_int = 4;
const FILESEC_ACL: libc::c_int = 5;
const ACL_TYPE_EXTENDED: libc::c_int = 0x100;
const ACL_FIRST_ENTRY: libc::c_int = 0;
const ACL_FLAG_NO_INHERIT: libc::c_int = 1 << 17;

unsafe extern "C" {
    fn acl_get_tag_type(entry: *mut c_void, tag: *mut libc::c_int) -> libc::c_int;
    fn acl_get_permset_mask_np(entry: *mut c_void, mask: *mut u64) -> libc::c_int;
    fn acl_get_qualifier(entry: *mut c_void) -> *mut c_void;
    fn mbr_uid_to_uuid(uid: libc::uid_t, uuid: *mut u8) -> libc::c_int;
    fn acl_init(count: libc::c_int) -> *mut c_void;
    fn acl_free(object: *mut c_void) -> libc::c_int;
    fn acl_get_flagset_np(object: *mut c_void, flags: *mut *mut c_void) -> libc::c_int;
    fn acl_add_flag_np(flags: *mut c_void, flag: libc::c_int) -> libc::c_int;
    fn acl_get_fd_np(fd: libc::c_int, kind: libc::c_int) -> *mut c_void;
    fn acl_get_entry(
        acl: *mut c_void,
        entry_id: libc::c_int,
        entry: *mut *mut c_void,
    ) -> libc::c_int;
    fn filesec_init() -> *mut c_void;
    fn filesec_free(security: *mut c_void);
    fn filesec_set_property(
        security: *mut c_void,
        property: libc::c_int,
        value: *const c_void,
    ) -> libc::c_int;
    fn mkdirx_np(path: *const libc::c_char, security: *mut c_void) -> libc::c_int;
}

struct Acl(*mut c_void);
impl Drop for Acl {
    fn drop(&mut self) {
        // SAFETY: this object was allocated by acl_init or acl_get_fd_np and is owned once.
        unsafe { acl_free(self.0) };
    }
}

struct Security(*mut c_void);
impl Drop for Security {
    fn drop(&mut self) {
        // SAFETY: this filesec_init allocation and its copied ACL are owned once.
        unsafe { filesec_free(self.0) };
    }
}

pub(super) fn create_leaf(dir: &Path) -> io::Result<()> {
    let path = CString::new(dir.as_os_str().as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    // SAFETY: zero requests an empty ACL; allocation failure is reported as null.
    let acl = unsafe { acl_init(0) };
    if acl.is_null() {
        return Err(io::Error::last_os_error());
    }
    let acl = Acl(acl);
    let mut flags = std::ptr::null_mut();
    // SAFETY: the live ACL owns the returned flagset; setting NO_INHERIT suppresses the
    // parent ACE merge inside the kernel's kauth_acl_inherit during mkdir itself.
    if unsafe { acl_get_flagset_np(acl.0, &mut flags) } != 0
        || unsafe { acl_add_flag_np(flags, ACL_FLAG_NO_INHERIT) } != 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this API has no arguments and returns a newly allocated descriptor.
    let security = unsafe { filesec_init() };
    if security.is_null() {
        return Err(io::Error::last_os_error());
    }
    let security = Security(security);
    let mode: libc::mode_t = 0o700;
    // SAFETY: MODE takes a mode_t pointer and ACL takes an acl_t pointer. filesec copies the
    // ACL, so both RAII allocations remain independently owned throughout the mkdir call.
    if unsafe { filesec_set_property(security.0, FILESEC_MODE, std::ptr::addr_of!(mode).cast()) }
        != 0
        || unsafe {
            filesec_set_property(security.0, FILESEC_ACL, std::ptr::addr_of!(acl.0).cast())
        } != 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the terminated path and complete mode/ACL descriptor live through the call.
    if unsafe { mkdirx_np(path.as_ptr(), security.0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn read_acl(file: &File, dir: &Path) -> io::Result<Option<Acl>> {
    let fd = file.as_raw_fd();
    let refuse = |reason: &str| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} is not a private directory: {reason}", dir.display()),
        )
    };
    // SAFETY: statfs is a plain C output structure and fd belongs to a live directory.
    let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatfs(fd, &mut filesystem) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if filesystem.f_flags & libc::MNT_IGNORE_OWNERSHIP as u32 != 0 {
        return Err(refuse("its volume ignores file ownership and permissions"));
    }
    // SAFETY: the descriptor is live. Reset errno to distinguish an indeterminate result
    // from a real error; neither is proof that ACL enforcement is absent.
    let extended = unsafe {
        *libc::__error() = 0;
        libc::fpathconf(fd, libc::_PC_EXTENDED_SECURITY_NP)
    };
    if extended == 0 {
        return Ok(None);
    }
    if extended < 0 {
        return Err(refuse("extended ACL support could not be determined"));
    }
    // SAFETY: fd is live and ACL_TYPE_EXTENDED is the supported Darwin ACL flavor.
    let acl = unsafe { acl_get_fd_np(fd, ACL_TYPE_EXTENDED) };
    if acl.is_null() {
        let error = io::Error::last_os_error();
        // Darwin's fstatx/filesec reports ENOENT for an absent ACL on a live descriptor.
        return if error.raw_os_error() == Some(libc::ENOENT) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    Ok(Some(Acl(acl)))
}

pub(super) fn check_acl(file: &File, dir: &Path) -> io::Result<()> {
    let Some(acl) = read_acl(file, dir)? else {
        return Ok(());
    };
    let refuse = |reason: &str| io::Error::new(io::ErrorKind::PermissionDenied, reason);
    let mut entry = std::ptr::null_mut();
    // SAFETY: acl is a live returned allocation and entry is a writable output pointer.
    let found = unsafe { acl_get_entry(acl.0, ACL_FIRST_ENTRY, &mut entry) };
    // Darwin returns 0 for an entry and -1/EINVAL for the end, unlike Linux's 1/0 convention.
    if found == 0 {
        return Err(refuse(
            "its extended ACL contains access entries despite mode 0700",
        ));
    }
    let error = io::Error::last_os_error();
    if found == -1 && error.raw_os_error() == Some(libc::EINVAL) {
        Ok(())
    } else {
        Err(error)
    }
}

/// Darwin ACL grants are independent of mode bits; deny/read-only entries are harmless here.
pub(super) fn check_ancestor(file: &File, path: &Path) -> io::Result<()> {
    let Some(acl) = read_acl(file, path)? else {
        return Ok(());
    };
    let mut position = ACL_FIRST_ENTRY;
    loop {
        let mut entry = std::ptr::null_mut();
        let found = unsafe { acl_get_entry(acl.0, position, &mut entry) };
        if found != 0 {
            let error = io::Error::last_os_error();
            return if found == -1 && error.raw_os_error() == Some(libc::EINVAL) {
                Ok(())
            } else {
                Err(error)
            };
        }
        // Darwin's ACL_NEXT_ENTRY is -1, unlike FreeBSD's value 1.
        position = -1;
        let mut tag = 0;
        let mut permissions = 0u64;
        if unsafe { acl_get_tag_type(entry, &mut tag) } != 0
            || unsafe { acl_get_permset_mask_np(entry, &mut permissions) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if tag == 2 {
            continue;
        } // ACL_EXTENDED_DENY
        if tag != 1 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "runtime ancestor has an unsupported ACL entry",
            ));
        }
        const MUTATING: u64 = (1 << 2)
            | (1 << 4)
            | (1 << 5)
            | (1 << 6)
            | (1 << 8)
            | (1 << 10)
            | (1 << 12)
            | (1 << 13);
        if permissions & MUTATING == 0 {
            continue;
        }
        let qualifier = unsafe { acl_get_qualifier(entry) };
        if qualifier.is_null() {
            return Err(io::Error::last_os_error());
        }
        let qualifier = Acl(qualifier);
        let uuid: [u8; 16] = unsafe { *qualifier.0.cast::<[u8; 16]>() };
        let mut trusted = false;
        for uid in [0, unsafe { libc::geteuid() }] {
            let mut expected = [0u8; 16];
            let result = unsafe { mbr_uid_to_uuid(uid, expected.as_mut_ptr()) };
            if result != 0 {
                return Err(io::Error::from_raw_os_error(result));
            }
            trusted |= uuid == expected;
        }
        if !trusted {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "runtime ancestor ACL grants mutation to another user",
            ));
        }
    }
}
