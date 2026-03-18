#[cfg(unix)]
use std::io;

#[cfg(unix)]
pub(crate) fn try_apply_unix_memory_rlimits(memory_bytes: Option<u64>) -> io::Result<()> {
    let Some(mem) = memory_bytes else {
        return Ok(());
    };

    // Best-effort: address space limit. This is the most broadly supported memory-ish limit
    // across Unix platforms. Some platforms may ignore or partially enforce it; callers
    // should treat this as a "best effort" containment mechanism outside Linux cgroups.
    unsafe {
        let lim = libc::rlimit {
            rlim_cur: mem as libc::rlim_t,
            rlim_max: mem as libc::rlim_t,
        };
        if libc::setrlimit(libc::RLIMIT_AS, &lim as *const libc::rlimit) != 0 {
            let err = io::Error::last_os_error();
            // Some hosts (notably some macOS configurations) return EINVAL for RLIMIT_AS even
            // for reasonable values. Treat that as "limit unsupported" rather than failing
            // the whole execution.
            match err.raw_os_error() {
                Some(code) if code == libc::EINVAL || code == libc::ENOSYS => return Ok(()),
                _ => return Err(err),
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
use akc_protocol::{RunId, TenantId};
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
fn cgroup_v2_root() -> Option<&'static Path> {
    // Heuristic for cgroup v2: `cgroup.controllers` exists at the v2 mount root.
    let root: &'static Path = Path::new("/sys/fs/cgroup");
    if root.join("cgroup.controllers").exists() {
        Some(root)
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn should_try_cgroup_v2() -> bool {
    // Optional enforcement:
    // - AKC_EXEC_CGROUPV2=1 enables
    // - AKC_EXEC_CGROUPV2=0 disables
    // - unset => enabled automatically in CI (best effort), disabled otherwise
    match std::env::var("AKC_EXEC_CGROUPV2") {
        Ok(v) => {
            let t = v.trim().to_ascii_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => std::env::var("CI")
            .ok()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false),
    }
}

#[cfg(target_os = "linux")]
fn write_if_exists(path: &Path, contents: &str) -> io::Result<()> {
    if path.exists() {
        fs::write(path, contents)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn sanitize_id_component(raw: &str) -> String {
    // Keep cgroup paths simple and safe.
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Best-effort cgroup v2 limiter for Linux runners.
///
/// This is intentionally optional and only activates when:
/// - cgroup v2 is detected at `/sys/fs/cgroup`
/// - and the user enables it, or we're running in CI (best-effort auto-enable)
///
/// If any step fails (permissions, missing controllers, etc.), the caller should
/// continue with rlimits-only enforcement.
#[cfg(target_os = "linux")]
pub(crate) struct LinuxCgroupV2Guard {
    dir: PathBuf,
}

#[cfg(target_os = "linux")]
impl LinuxCgroupV2Guard {
    pub(crate) fn try_create(
        tenant_id: &TenantId,
        run_id: &RunId,
        memory_bytes: Option<u64>,
    ) -> Option<Self> {
        let mem = memory_bytes?;
        if !should_try_cgroup_v2() {
            return None;
        }
        let root = cgroup_v2_root()?;

        // A stable-ish per-exec directory name without requiring randomness.
        let now_ms: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let t = sanitize_id_component(&tenant_id.0);
        let r = sanitize_id_component(&run_id.0);
        let dir = root
            .join("akc-exec")
            .join(t)
            .join(r)
            .join(format!("exec-{}", now_ms));

        if fs::create_dir_all(&dir).is_err() {
            return None;
        }

        // memory.max is in bytes; "max" means unlimited.
        if write_if_exists(&dir.join("memory.max"), &format!("{}", mem)).is_err() {
            let _ = fs::remove_dir_all(&dir);
            return None;
        }

        // Best-effort: prevent swap explosion if supported.
        let _ = write_if_exists(&dir.join("memory.swap.max"), "0");

        Some(Self { dir })
    }

    pub(crate) fn add_pid(&self, pid: u32) -> io::Result<()> {
        // Add the process to the cgroup (applies to its future descendants too).
        fs::write(self.dir.join("cgroup.procs"), format!("{}", pid))
    }
}

#[cfg(target_os = "linux")]
impl Drop for LinuxCgroupV2Guard {
    fn drop(&mut self) {
        // Best-effort cleanup. If the process is still alive or permissions don't allow it,
        // leaving the directory behind is acceptable (CI scratch space).
        let _ = fs::remove_dir_all(&self.dir);
    }
}
