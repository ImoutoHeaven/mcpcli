//! A child starts suspended so it cannot create descendants before joining its job.
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use tokio::process::Child;
use windows_sys::Win32::{
    Foundation::INVALID_HANDLE_VALUE,
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
        },
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
            QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
        },
        Threading::{
            GetProcessIdOfThread, OpenThread, ResumeThread, THREAD_QUERY_LIMITED_INFORMATION,
            THREAD_SUSPEND_RESUME,
        },
    },
};

pub(crate) struct Job(OwnedHandle);

impl Job {
    fn create_kill_on_close() -> io::Result<Self> {
        // SAFETY: null arguments create a new unnamed, non-inheritable job.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this is the newly created handle, owned exactly once.
        let job = Self(unsafe { OwnedHandle::from_raw_handle(raw) });
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the handle and correctly sized information buffer are live.
        if unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    pub(crate) fn adopt(child: &Child) -> io::Result<Self> {
        let job = Self::create_kill_on_close()?;
        let process = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("MCP child has no process handle"))?;
        // SAFETY: the suspended child and job handles remain live through assignment.
        if unsafe { AssignProcessToJobObject(job.0.as_raw_handle(), process) } == 0 {
            return Err(io::Error::last_os_error());
        }
        resume(
            child
                .id()
                .ok_or_else(|| io::Error::other("MCP child has no PID"))?,
        )?;
        Ok(job)
    }

    /// Bind an already-running short-lived child without CREATE_SUSPENDED.
    /// Returns Ok(None) if the child exited before assignment (ERROR_ACCESS_DENIED).
    pub(crate) fn assign_running(child: &Child) -> io::Result<Option<Self>> {
        let Some(process) = child.raw_handle() else {
            return Ok(None);
        };
        let job = Self::create_kill_on_close()?;
        // SAFETY: the child process handle and newly created job handle are live.
        if unsafe { AssignProcessToJobObject(job.0.as_raw_handle(), process) } == 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error()
                == Some(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED as i32)
            {
                return Ok(None);
            }
            return Err(err);
        }
        Ok(Some(job))
    }

    pub(crate) fn terminate(&self) -> io::Result<()> {
        // SAFETY: this owned handle has JOB_OBJECT_TERMINATE access.
        if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> io::Result<bool> {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: the information class matches the live output buffer.
        if unsafe {
            QueryInformationJobObject(
                self.0.as_raw_handle(),
                JobObjectBasicAccountingInformation,
                (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                std::mem::size_of_val(&info) as u32,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(info.ActiveProcesses == 0)
    }
}

fn resume(pid: u32) -> io::Result<()> {
    // SAFETY: TH32CS_SNAPTHREAD ignores the process ID argument.
    let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the newly created snapshot is owned here.
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut entry = THREADENTRY32 {
        dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
        ..Default::default()
    };
    let mut found = false;
    // SAFETY: the live snapshot and output structure have the expected types.
    let mut more = unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } != 0;
    while more {
        if entry.th32OwnerProcessID == pid {
            resume_owned_thread(pid, entry.th32ThreadID)?;
            found = true;
        }
        // SAFETY: the snapshot and output buffer remain live.
        more = unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } != 0;
    }
    if !found {
        return Err(io::Error::other(
            "cannot find the suspended MCP child thread",
        ));
    }
    Ok(())
}

fn resume_owned_thread(pid: u32, tid: u32) -> io::Result<()> {
    // A snapshot is only a hint: an exited thread's ID may already have been reused.
    let raw = unsafe {
        OpenThread(
            THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
            0,
            tid,
        )
    };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenThread returned a new owned handle.
    let thread = unsafe { OwnedHandle::from_raw_handle(raw) };
    // SAFETY: the live handle has THREAD_QUERY_LIMITED_INFORMATION access.
    if unsafe { GetProcessIdOfThread(thread.as_raw_handle()) } != pid {
        return Err(io::Error::other("MCP child thread ownership changed"));
    }
    // SAFETY: the verified child thread handle has THREAD_SUSPEND_RESUME access.
    if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refuses_thread_outside_owned_child_before_resuming() {
        // SAFETY: GetCurrentThreadId has no preconditions; PID zero cannot own this thread.
        let tid = unsafe { windows_sys::Win32::System::Threading::GetCurrentThreadId() };
        assert!(resume_owned_thread(0, tid).is_err());
    }
}
