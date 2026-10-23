/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_char;
#[cfg(any(feature = "hotpath", test))]
use std::path::PathBuf;

/// Starts profiling the Rust code with hotpath.
///
/// A no-op unless `redisearch_rs` is built with its `hotpath` feature. Redis
/// exits through `exit()` and never drops `static` values, so the report is
/// written from an `atexit` handler on a graceful shutdown. A crash or
/// `SIGKILL` loses it; the live `hotpath console` TUI does not depend on it.
///
/// `logfile` is the redis `logfile` config value, or null. The report is
/// written next to the log file, with its `.log` extension replaced by
/// `.hotpath.txt`; otherwise to stdout. `HOTPATH_OUTPUT_PATH` overrides both.
///
/// Must be called at most once.
///
/// # Safety
///
/// `logfile` must either be null or point to a valid, null-terminated C string.
#[unsafe(no_mangle)]
#[cfg_attr(not(feature = "hotpath"), expect(clippy::missing_const_for_fn))]
pub unsafe extern "C" fn RSHotpath_Init(logfile: *const c_char) {
    #[cfg(feature = "hotpath")]
    {
        let report = if logfile.is_null() {
            None
        } else {
            // SAFETY: the caller guarantees a valid, null-terminated C string.
            let logfile = unsafe { std::ffi::CStr::from_ptr(logfile) };
            report_path(&logfile.to_string_lossy())
        };

        guard::start(report);
    }
    #[cfg(not(feature = "hotpath"))]
    let _ = logfile;
}

/// The report path for the redis log file `logfile`: its `.log` extension, if
/// any, replaced by `.hotpath.txt`.
///
/// Every redis-server of a test run logs to its own file, so each gets its own
/// report next to its log. `None` when redis logs to stdout or `/dev/null`.
#[cfg(any(feature = "hotpath", test))]
fn report_path(logfile: &str) -> Option<PathBuf> {
    if logfile.is_empty() || logfile == "/dev/null" {
        return None;
    }
    let stem = logfile.strip_suffix(".log").unwrap_or(logfile);
    Some(PathBuf::from(format!("{stem}.hotpath.txt")))
}

#[cfg(feature = "hotpath")]
mod guard {
    //! Owns the [`hotpath::HotpathGuard`] on a dedicated thread.
    //!
    //! glibc's `exit()` destroys the calling thread's thread-locals before it runs
    //! the `atexit` handlers, and dropping the guard needs them. So the guard must
    //! neither be created nor dropped on the main thread: its thread drops it when
    //! [`stop`] wakes it, and [`stop`] waits for the report to be written.

    use std::{
        path::PathBuf,
        sync::{Condvar, Mutex, PoisonError},
        thread::JoinHandle,
    };

    static STOP: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
    static THREAD: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

    pub(super) fn start(report: Option<PathBuf>) {
        let handle = std::thread::Builder::new()
            .name("hotpath-guard".to_owned())
            .spawn(move || {
                let mut builder = ::hotpath::HotpathGuardBuilder::new("redisearch");
                if let Some(report) = report {
                    builder = builder.output_path(report);
                }
                let guard = builder.build();
                let (stop, cvar) = &STOP;
                let stop = stop.lock().unwrap_or_else(PoisonError::into_inner);
                drop(cvar.wait_while(stop, |stop| !*stop));
                drop(guard);
            })
            .expect("failed to spawn the hotpath guard thread");
        *THREAD.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle);
        // SAFETY: `stop` is an `extern "C"` function taking no arguments, as `atexit` expects.
        unsafe { libc::atexit(stop) };
    }

    extern "C" fn stop() {
        let (stop, cvar) = &STOP;
        *stop.lock().unwrap_or_else(PoisonError::into_inner) = true;
        cvar.notify_one();
        if let Some(handle) = THREAD.lock().unwrap_or_else(PoisonError::into_inner).take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_path_replaces_log_extension() {
        assert_eq!(
            report_path("abc.master-1-test_x-oss.log"),
            Some(PathBuf::from("abc.master-1-test_x-oss.hotpath.txt"))
        );
    }

    #[test]
    fn report_path_appends_extension_without_log_extension() {
        assert_eq!(
            report_path("/var/log/redis"),
            Some(PathBuf::from("/var/log/redis.hotpath.txt"))
        );
    }

    #[test]
    fn report_path_keeps_non_final_log_extension() {
        assert_eq!(
            report_path("redis.log.1"),
            Some(PathBuf::from("redis.log.1.hotpath.txt"))
        );
    }

    #[test]
    fn report_path_none_without_log_file() {
        assert_eq!(report_path(""), None);
        assert_eq!(report_path("/dev/null"), None);
    }
}
