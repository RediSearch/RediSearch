//! timespec utilities when going between C and Rust

use std::time::Duration;

pub(crate) fn duration_from_redis_timespec(timeout: ffi::timespec) -> Option<Duration> {
    if timeout.tv_sec >= libc::time_t::MAX - 1 {
        return None;
    }
    if timeout.tv_sec < 0 {
        return Some(Duration::ZERO);
    }
    let secs = timeout.tv_sec as u64;
    let nanos = timeout.tv_nsec.clamp(0, 999_999_999) as u32;
    Some(Duration::new(secs, nanos))
}
