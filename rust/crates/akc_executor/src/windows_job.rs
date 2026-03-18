use std::os::windows::io::AsRawHandle as _;

use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, HANDLE};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectCpuRateControlInformation,
    JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
    JOBOBJECT_CPU_RATE_CONTROL_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE, JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
    JOB_OBJECT_LIMIT_JOB_MEMORY, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Threading::{
    SetPriorityClass, BELOW_NORMAL_PRIORITY_CLASS, IDLE_PRIORITY_CLASS, NORMAL_PRIORITY_CLASS,
};

pub(crate) struct WindowsJob {
    handle: HANDLE,
}

#[derive(Clone, Debug)]
pub(crate) struct JobSetupReport {
    pub(crate) job_assigned: bool,
    pub(crate) memory_limit_set: bool,
    pub(crate) cpu_rate_percent: Option<u32>,
    pub(crate) cpu_rate_set: Option<bool>,
    pub(crate) priority_class: &'static str,
}

pub(crate) struct JobSetupResult {
    pub(crate) job: Option<WindowsJob>,
    pub(crate) report: JobSetupReport,
}

impl WindowsJob {
    pub(crate) fn terminate(&self) {
        unsafe {
            // Best-effort; if it fails, the caller will still fall back to Child::kill.
            let _ = TerminateJobObject(self.handle, 1);
        }
    }
}

impl Drop for WindowsJob {
    fn drop(&mut self) {
        unsafe {
            if self.handle != std::ptr::null_mut() {
                // `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` ensures the whole tree is terminated.
                let _ = CloseHandle(self.handle);
            }
        }
    }
}

fn set_job_limits(job: HANDLE, memory_bytes: Option<u64>) -> bool {
    unsafe {
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

        if let Some(mem) = memory_bytes {
            // Clamp to usize for the API field.
            let limit: usize = mem.min(usize::MAX as u64) as usize;
            info.JobMemoryLimit = limit;
            info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
        }

        let ok = SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &mut info as *mut _ as *mut _,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        ok != 0
    }
}

fn choose_priority_class() -> (u32, &'static str) {
    match std::env::var("AKC_EXEC_WIN_PRIORITY_CLASS")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        Some("idle") => (IDLE_PRIORITY_CLASS, "idle"),
        Some("normal") => (NORMAL_PRIORITY_CLASS, "normal"),
        Some("below_normal") | None | Some("") => (BELOW_NORMAL_PRIORITY_CLASS, "below_normal"),
        Some(_) => (BELOW_NORMAL_PRIORITY_CLASS, "below_normal"),
    }
}

fn try_set_process_priority(child_process: HANDLE) -> &'static str {
    unsafe {
        // Optional process priority policy (best-effort).
        //
        // - `AKC_EXEC_WIN_PRIORITY_CLASS=idle|below_normal|normal`
        // - default: below_normal
        let (class, label) = choose_priority_class();
        let _ = SetPriorityClass(child_process, class);
        label
    }
}

fn parse_cpu_rate_percent() -> Result<Option<u32>, ()> {
    // Optional CPU cap policy (best-effort). This is job-wide and applies to the entire tree.
    //
    // Env var: `AKC_EXEC_WIN_CPU_RATE_PERCENT` (1..=100)
    // - unset/empty => disabled
    // - when set => uses HARD_CAP (strict cap) if supported by the host.
    let raw = match std::env::var("AKC_EXEC_WIN_CPU_RATE_PERCENT") {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let percent: u32 = match trimmed.parse::<u32>() {
        Ok(v) => v.clamp(1, 100),
        Err(_) => return Err(()),
    };
    Ok(Some(percent))
}

fn try_set_job_cpu_rate(job: HANDLE, percent: u32) -> bool {
    // CpuRate is expressed in 1/100th of a percent (valid: 1..=10000).
    let cpu_rate: u32 = percent.saturating_mul(100);

    unsafe {
        let mut info: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION = std::mem::zeroed();
        info.ControlFlags =
            JOB_OBJECT_CPU_RATE_CONTROL_ENABLE | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP;
        info.Anonymous.CpuRate = cpu_rate;

        let ok = SetInformationJobObject(
            job,
            JobObjectCpuRateControlInformation,
            &mut info as *mut _ as *mut _,
            std::mem::size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>() as u32,
        );
        ok != 0
    }
}

/// Create a per-exec Windows Job Object and assign the child process to it.
///
/// This is the Windows-native mechanism that provides:
/// - process-tree kill semantics (`TerminateJobObject` / KILL_ON_JOB_CLOSE)
/// - job-wide memory limit (when set)
///
/// Notes:
/// - Assignment can fail if the current process is already in a job that disallows
///   nested jobs. In that case we return `None` and the executor falls back to
///   best-effort `Child::kill()`.
pub(crate) fn try_create_and_assign_job(
    child: &std::process::Child,
    memory_bytes: Option<u64>,
    _wall_time_ms: Option<u64>,
) -> JobSetupResult {
    let mut report: JobSetupReport = JobSetupReport {
        job_assigned: false,
        memory_limit_set: memory_bytes.is_none(),
        cpu_rate_percent: None,
        cpu_rate_set: None,
        priority_class: "below_normal",
    };

    unsafe {
        // Unnamed job object: avoids collisions between concurrent executions.
        let job: HANDLE = CreateJobObjectW(std::ptr::null_mut(), std::ptr::null());
        if job == std::ptr::null_mut() {
            let _err = GetLastError();
            return JobSetupResult { job: None, report };
        }

        if !set_job_limits(job, memory_bytes) {
            let _ = CloseHandle(job);
            return JobSetupResult { job: None, report };
        }
        report.memory_limit_set = true;

        let proc_handle: HANDLE = child.as_raw_handle() as HANDLE;
        report.priority_class = try_set_process_priority(proc_handle);

        // Apply optional CPU cap (best-effort). If it fails, keep the job (kill semantics still matter).
        match parse_cpu_rate_percent() {
            Ok(None) => {}
            Ok(Some(p)) => {
                report.cpu_rate_percent = Some(p);
                report.cpu_rate_set = Some(try_set_job_cpu_rate(job, p));
            }
            Err(()) => {
                report.cpu_rate_percent = None;
                report.cpu_rate_set = Some(false);
            }
        }

        let ok = AssignProcessToJobObject(job, proc_handle);
        if ok == 0 {
            let _err = GetLastError();
            let _ = CloseHandle(job);
            return JobSetupResult { job: None, report };
        }

        report.job_assigned = true;
        JobSetupResult {
            job: Some(WindowsJob { handle: job }),
            report,
        }
    }
}
