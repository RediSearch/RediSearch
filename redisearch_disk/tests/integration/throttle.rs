//! Tests for throttle callback infrastructure.
//!
//! These tests verify that the Rust API (`BasicDiskAPI.setThrottleCallbacks`)
//! correctly sets callbacks that are invoked by the C++ throttle functions.

use std::ffi::c_void;
use std::sync::atomic::{AtomicI32, Ordering};

/// ThrottleCB signature matching the C definition
type ThrottleCB = Option<extern "C" fn() -> i32>;

/// Minimal BasicDiskAPI layout - only the fields we need for testing
/// The actual struct has more fields, but we only access setThrottleCallbacks
#[repr(C)]
struct BasicDiskAPI {
    // 7 function pointers before setThrottleCallbacks
    _open: *const c_void,
    _close: *const c_void,
    _open_index_spec: *const c_void,
    _close_index_spec: *const c_void,
    _index_spec_rdb_save: *const c_void,
    _index_spec_rdb_load: *const c_void,
    _is_async_io_supported: *const c_void,
    set_throttle_callbacks: Option<unsafe extern "C" fn(ThrottleCB, ThrottleCB)>,
}

/// Minimal RedisSearchDiskAPI layout - only basic field needed
#[repr(C)]
struct RedisSearchDiskAPI {
    basic: BasicDiskAPI,
    // Other fields omitted - we only access basic
}

// Counters for mock callbacks
static ENABLE_CALL_COUNT: AtomicI32 = AtomicI32::new(0);
static DISABLE_CALL_COUNT: AtomicI32 = AtomicI32::new(0);

// Mock callbacks that match ThrottleCB signature: extern "C" fn() -> i32
extern "C" fn mock_enable_throttle() -> i32 {
    ENABLE_CALL_COUNT.fetch_add(1, Ordering::SeqCst);
    0
}

extern "C" fn mock_disable_throttle() -> i32 {
    DISABLE_CALL_COUNT.fetch_add(1, Ordering::SeqCst);
    0
}

// Import the Rust API getter and C++ invoke functions
unsafe extern "C" {
    fn SearchDisk_GetAPI() -> *mut RedisSearchDiskAPI;
    fn VecSimDisk_InvokeEnableThrottle();
    fn VecSimDisk_InvokeDisableThrottle();
}

fn reset_counters() {
    ENABLE_CALL_COUNT.store(0, Ordering::SeqCst);
    DISABLE_CALL_COUNT.store(0, Ordering::SeqCst);
}

/// Helper to call setThrottleCallbacks via the Rust API
fn set_throttle_callbacks_via_api(
    enable: Option<extern "C" fn() -> i32>,
    disable: Option<extern "C" fn() -> i32>,
) {
    // SAFETY: SearchDisk_GetAPI returns a valid static pointer
    unsafe {
        let api = SearchDisk_GetAPI();
        let set_fn = (*api)
            .basic
            .set_throttle_callbacks
            .expect("setThrottleCallbacks not set");
        set_fn(enable, disable);
    }
}

#[test]
fn test_set_and_invoke_callbacks_via_rust_api() {
    reset_counters();

    // Set callbacks via the Rust API (BasicDiskAPI.setThrottleCallbacks)
    set_throttle_callbacks_via_api(Some(mock_enable_throttle), Some(mock_disable_throttle));

    assert_eq!(ENABLE_CALL_COUNT.load(Ordering::SeqCst), 0);
    assert_eq!(DISABLE_CALL_COUNT.load(Ordering::SeqCst), 0);

    // SAFETY: Callbacks have been set above
    unsafe {
        VecSimDisk_InvokeEnableThrottle();
    }
    assert_eq!(ENABLE_CALL_COUNT.load(Ordering::SeqCst), 1);
    assert_eq!(DISABLE_CALL_COUNT.load(Ordering::SeqCst), 0);

    // Call enable again to verify it can be called multiple times
    unsafe {
        VecSimDisk_InvokeEnableThrottle();
    }
    assert_eq!(ENABLE_CALL_COUNT.load(Ordering::SeqCst), 2);
    assert_eq!(DISABLE_CALL_COUNT.load(Ordering::SeqCst), 0);

    unsafe {
        VecSimDisk_InvokeDisableThrottle();
    }
    assert_eq!(ENABLE_CALL_COUNT.load(Ordering::SeqCst), 2);
    assert_eq!(DISABLE_CALL_COUNT.load(Ordering::SeqCst), 1);

    // Call disable again to verify it can be called multiple times
    unsafe {
        VecSimDisk_InvokeDisableThrottle();
    }
    assert_eq!(ENABLE_CALL_COUNT.load(Ordering::SeqCst), 2);
    assert_eq!(DISABLE_CALL_COUNT.load(Ordering::SeqCst), 2);

    // One more enable to show interleaving works
    unsafe {
        VecSimDisk_InvokeEnableThrottle();
    }
    assert_eq!(ENABLE_CALL_COUNT.load(Ordering::SeqCst), 3);
    assert_eq!(DISABLE_CALL_COUNT.load(Ordering::SeqCst), 2);
}
