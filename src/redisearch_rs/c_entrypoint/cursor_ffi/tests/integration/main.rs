/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 16 } else { 256 },
        failure_persistence: if cfg!(miri) { None } else { ProptestConfig::default().failure_persistence },
        .. ProptestConfig::default()
    })]
    #[test]
    fn timer_delay_rounds_up_to_the_nearest_millisecond(
        remaining in prop_oneof![any::<u64>(), (0..=u64::MAX / 1_000_000).prop_map(|ms| ms * 1_000_000)]
    ) {
        let delay = u128::from(cursor_ffi::timer_delay_ms(remaining));
        let remaining = u128::from(remaining);
        prop_assert!(delay * 1_000_000 >= remaining);
        prop_assert!(delay == 0 || (delay - 1) * 1_000_000 < remaining);
    }
}

// These tests execute C request/reference destructors, unavailable in Miri.
#[cfg(not(miri))]
mod c_api {
    use cursor_ffi::*;
    use proptest::prelude::*;
    use query_error::{QueryError, QueryErrorCode};
    use std::{
        ffi::{CStr, c_void},
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
        // SAFETY: 1. The initialized global has no timer or remaining execution owner.
        unsafe { CursorList_Empty(user) };
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
        // SAFETY: 1. The initialized global has no timer or remaining execution owner.
        unsafe { CursorList_Empty(user) };
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
}
