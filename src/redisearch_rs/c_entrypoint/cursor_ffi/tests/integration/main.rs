/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// This integration test executes C request/reference destructors, unavailable in Miri.
#![cfg(not(miri))]
use cursor_ffi::*;
use proptest::prelude::*;
use query_error::{QueryError, QueryErrorCode};
use redis_module::raw;
use std::{
    ffi::{CStr, c_char, c_int, c_void},
    ptr,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

static REQUEST: AtomicPtr<c_void> = AtomicPtr::new(ptr::null_mut());
static FREES: AtomicUsize = AtomicUsize::new(0);
static RELEASES: AtomicUsize = AtomicUsize::new(0);
static LOCKED: AtomicUsize = AtomicUsize::new(0);

const LISTS: [&CursorList; 2] = [&g_CursorsList, &g_CursorsListCoord];
static SERIAL: LazyLock<Mutex<()>> = LazyLock::new(|| {
    redis_mock::init_redis_module_mock();
    for (list, coord) in LISTS.into_iter().zip([false, true]) {
        // SAFETY: 1. LazyLock initializes each global once before any test can acquire SERIAL.
        unsafe { CursorList_Init(list, coord) };
    }
    Mutex::new(())
});

/// Records destruction before delegating to the mock allocator.
/// # Safety
/// 1. `p` must be an allocation returned by the mock allocator, relinquished by its owner.
unsafe extern "C" fn freed(p: *mut c_void) {
    if p == REQUEST.load(Ordering::Relaxed) {
        FREES.fetch_add(1, Ordering::Relaxed);
    }
    redis_mock::allocator::free_shim(p);
}
extern "C" fn release(_: *mut c_void) {
    RELEASES.fetch_add(1, Ordering::Relaxed);
}
const extern "C" fn end(_: *mut raw::RedisModuleInfoCtx) -> c_int {
    0
}
const extern "C" fn no_begin(_: *mut raw::RedisModuleInfoCtx, _: *const c_char) -> c_int {
    0
}
/// Re-enters the renderer while its locks are held.
/// # Safety
/// 1. The single-threaded test must have initialized and locked both globals and installed INFO mocks.
unsafe extern "C" fn number(ctx: *mut raw::RedisModuleInfoCtx, _: *const c_char, _: i64) -> c_int {
    let [user, coord] = LISTS;
    // SAFETY: 1. Both globals are initialized. 2. Held locks prevent spec access. 3. INFO is mocked.
    unsafe { Cursors_RenderStatsForInfo(user, coord, ptr::null(), ctx) };
    0
}
/// Checks the nonblocking fallback fields.
/// # Safety
/// 1. `key` and `value` must point to live nul-terminated strings.
unsafe extern "C" fn locked(
    _: *mut raw::RedisModuleInfoCtx,
    key: *const c_char,
    value: *const c_char,
) -> c_int {
    // SAFETY: the public renderer supplies terminated static strings.
    assert_eq!(unsafe { CStr::from_ptr(key) }, c"status");
    // SAFETY: the public renderer supplies terminated static strings.
    assert_eq!(unsafe { CStr::from_ptr(value) }, c"locked");
    LOCKED.fetch_add(1, Ordering::Relaxed);
    0
}

#[test]
fn busy_list_reports_locked() {
    let _serial = SERIAL.lock().unwrap();
    let [user, coord] = LISTS;
    // SAFETY: bindgen's C struct contains only integers, floats and nullable pointers.
    let spec: ffi::IndexSpec = unsafe { std::mem::zeroed() };
    // SAFETY: this test exclusively owns the INFO mock table.
    unsafe { raw::RedisModule_InfoBeginDictField = Some(no_begin) };
    // SAFETY: this test exclusively owns the INFO mock table.
    unsafe { raw::RedisModule_InfoEndDictField = Some(end) };
    // SAFETY: this test exclusively owns the INFO mock table.
    unsafe { raw::RedisModule_InfoAddFieldCString = Some(locked) };
    // SAFETY: this test exclusively owns the INFO mock table.
    unsafe { raw::RedisModule_InfoAddFieldLongLong = Some(number) };
    // SAFETY: 1. Both globals are initialized. 2. spec is live. 3. INFO mocks accept a null context.
    unsafe { Cursors_RenderStatsForInfo(user, coord, &raw const spec, ptr::null_mut()) };
    assert_eq!(LOCKED.load(Ordering::Relaxed), 5);
}

#[test]
fn destruction_releases_request_and_invalidated_weak_reference() {
    let _serial = SERIAL.lock().unwrap();
    let [user, _] = LISTS;
    RELEASES.store(0, Ordering::Relaxed);
    // SAFETY: this test owns the allocator table; freed delegates to the same allocator.
    unsafe { RedisModule_Free = freed };
    // SAFETY: no other thread accesses configuration in this binary.
    unsafe { ffi::RSGlobalConfig.indexCursorLimit = 3 };
    // SAFETY: bindgen's C struct contains only integers, floats and nullable pointers.
    let mut spec: ffi::IndexSpec = unsafe { std::mem::zeroed() };
    // SAFETY: spec lives through all references; release records without freeing stack storage.
    let strong = unsafe { ffi::StrongRef_New((&raw mut spec).cast(), Some(release)) };
    // SAFETY: 1. Mocks and user list are initialized. 2. strong is live. 3. Capacity permits reservation.
    let a = unsafe { Cursors_Reserve(user, strong, 1000, ptr::null_mut()) };
    // SAFETY: Redis mocks are initialized; empty argv is supported.
    let req = unsafe { ffi::AREQ_New(ptr::null_mut(), 0) };
    REQUEST.store(req.cast(), Ordering::Relaxed);
    // SAFETY: a is exclusively owned for execution and takes the new request.
    unsafe { a.as_mut() }.expect("executing cursor").query = req.cast();
    // SAFETY: strong still owns the reference created above.
    unsafe { ffi::StrongRef_Invalidate(strong) };
    // SAFETY: invalidation does not release the owned strong reference.
    unsafe { ffi::StrongRef_Release(strong) };
    // SAFETY: 1. a remains exclusively owned with no active cycle despite spec invalidation.
    unsafe { Cursor_Free(a) };
    assert_eq!(FREES.load(Ordering::Relaxed), 1);
    REQUEST.store(ptr::null_mut(), Ordering::Relaxed);
    assert_eq!(RELEASES.load(Ordering::Relaxed), 1);
    assert_eq!(spec.activeCursors, 1);
}

#[test]
fn idle_expiry_uses_monotonic_seconds() {
    let _serial = SERIAL.lock().unwrap();
    let [user, _] = LISTS;
    let no_spec = ffi::StrongRef {
        rm: ptr::null_mut(),
    };
    // SAFETY: 1. The global and mocks are initialized. 2, 3. Null specs bypass capacity accounting.
    let cur = unsafe { Cursors_Reserve(user, no_spec, 1000, ptr::null_mut()) };
    // SAFETY: 1. cur is exclusively owned for execution with unchanged fields and no request.
    unsafe { Cursor_Pause(cur) };
    // SAFETY: 1. The global is initialized; mock timer APIs are absent.
    assert_eq!(unsafe { Cursors_CollectIdle(user) }, 0);
    // Cross a whole monotonic second to detect incorrect seconds conversion.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    // SAFETY: 1. The global is initialized; mock timer APIs are absent.
    assert_eq!(unsafe { Cursors_CollectIdle(user) }, 1);
}

proptest! {
    #[test]
    fn capacity_counts_active_and_idle_independently(limit in 0usize..9, seed in any::<u8>(), reverse in any::<bool>()) {
        let _serial = SERIAL.lock().unwrap();
        let mut lists = LISTS;
        if reverse { lists.reverse(); }
        // SAFETY: SERIAL excludes all other configuration readers/writers in this binary.
        unsafe { ffi::RSGlobalConfig.indexCursorLimit = limit as i64 };
        // SAFETY: all fields of this C struct admit zero initialization.
        let mut spec: ffi::IndexSpec = unsafe { std::mem::zeroed() };
        // SAFETY: spec outlives the strong and weak references; release does not free stack storage.
        let strong = unsafe { ffi::StrongRef_New((&raw mut spec).cast(), Some(release)) };
        let message = format!("INDEX_CURSOR_LIMIT of {limit} has been reached for an index");
        let mut valid = true;
        let mut executing = Vec::new();
        for (index, list) in lists.into_iter().enumerate() {
            let idle = (usize::from(seed) + index) % (limit + 1);
            for n in 0..=limit {
                let mut error = QueryError::default();
                // SAFETY: 1. SERIAL initialized the global. 2. strong is live. 3. error is exclusively writable.
                let cur = unsafe { Cursors_Reserve(list, strong, u32::MAX, &raw mut error) };
                valid &= cur.is_null() == (n == limit);
                if n == limit {
                    valid &= error.code() == QueryErrorCode::Limit
                        && error.public_message().map(CStr::to_bytes) == Some(message.as_bytes());
                }
                if !cur.is_null() {
                    if n < idle {
                        // SAFETY: 1. This new cursor is exclusively owned and carries no request.
                        unsafe { Cursor_Pause(cur) };
                    } else { executing.push(cur); }
                }
            }
        }
        let counts = (spec.activeCursors, spec.activeCoordCursors);
        for cur in executing {
            // SAFETY: 1. Each retained pointer is still exclusively owned for execution.
            unsafe { Cursor_Free(cur) };
        }
        for list in lists {
            // SAFETY: 1. The initialized globals have no timers in this mocked binary.
            unsafe { CursorList_Empty(list) };
        }
        // SAFETY: this releases the sole strong reference after all cursor weak references.
        unsafe { ffi::StrongRef_Release(strong) };
        prop_assert!(valid);
        prop_assert_eq!(counts, (limit, limit));
        prop_assert_eq!((spec.activeCursors, spec.activeCoordCursors), (0, 0));
    }
}
