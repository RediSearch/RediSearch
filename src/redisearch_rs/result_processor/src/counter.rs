/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{Header, ResultProcessor, ResultProcessorWrapper};
use search_result::SearchResult;
use std::ptr::NonNull;

// Link both Rust-provided and C-provided symbols
#[cfg(all(test, feature = "unittest"))]
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
#[cfg(all(test, feature = "unittest"))]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// A processor to track the number of entries yielded by the previous processor in the chain.
#[derive(Debug)]
pub struct Counter {
    count: usize,
}

impl ResultProcessor for Counter {
    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_COUNTER;

    fn next(
        &mut self,
        mut cx: crate::Context,
        res: &mut SearchResult<'_>,
    ) -> Result<Option<()>, crate::Error> {
        let mut upstream = cx
            .upstream()
            .expect("There is no processor upstream of this counter.");

        while upstream.next(res)?.is_some() {
            self.count += 1;

            res.clear();
        }

        // In profiling mode, RPProfile is interleaved into the result processor chain: A chain of
        // processors A -> B -> C becomes A -> RPProfile -> B -> RPProfile -> C -> RPProfile, to
        // profile each of the individual result processors.
        //
        // Because the Counter result processor returns Ok(None), this is equivalent to returning
        // ffi::RPStatus_RS_RESULT_EOF (see ResultProcessorWrapper::result_processor_next). This
        // apparently (in a way enricozb cannot figure out) prevents the very last RPProfile from
        // appropriately counting, so this patches that by manually incrementing the counter.
        if upstream.ty() == ffi::ResultProcessorType_RP_PROFILE {
            // Safety: We trust that the result processor parent structure (QueryProcessingCtx) was
            // constructed correctly, and thus has a valid pointer to the end processor.
            let end_proc = unsafe {
                *cx.parent()
                    .expect("This processor has no parent.")
                    .endProc
                    .get()
            };

            // Safety: If the previous (upstream) result processor is a profiling result processor,
            // then we are in profiling mode, and every other result processor is an RPProfile.
            // Thus, the last result processor is also an RPProfile.
            unsafe { ffi::RPProfile_IncrementCount(end_proc) };
        }

        Ok(None)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl Counter {
    pub const fn new() -> Self {
        Self { count: 0 }
    }

    /// Transfer this counter to a pinned C pipeline allocation.
    ///
    /// Its execution entry keeps no Rust reference across an upstream call, so
    /// C may release execution ownership locally while waiting. The caller must
    /// retain the allocation through all callbacks, keep it pinned, and eventually
    /// destroy it through [`ffi::ResultProcessor::Free`].
    pub fn into_raw(self) -> NonNull<ffi::ResultProcessor> {
        let mut wrapper = ResultProcessorWrapper::new(self);
        wrapper.header.next = Some(counter_next);
        // SAFETY: ownership is transferred as a pinned allocation; the documented
        // C lifetime contract governs its subsequent use and destruction.
        unsafe { ResultProcessorWrapper::into_ptr(Box::pin(wrapper)).cast() }
    }
}

/// C execution entry for [`Counter::into_raw`].
///
/// # Safety
///
/// 1. Both pointers are aligned and [valid]; `ptr` names a pinned
///    [`ResultProcessorWrapper<Counter>`] and `res` is initialized private output.
/// 2. The caller owns pipeline execution on entry. Upstream may release ownership
///    locally but must return timeout if readmission fails; successful results and
///    EOF require ownership. No other executor accesses the in-flight `res`.
/// 3. The chain and output allocations outlive this call, including a parked wait.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn counter_next(ptr: *mut Header, res: *mut SearchResult) -> libc::c_int {
    debug_assert!(!ptr.is_null() && ptr.is_aligned());
    debug_assert!(!res.is_null() && res.is_aligned());
    // SAFETY: invariants 1 and 2. Only raw handles survive an upstream call;
    // borrowing the whole wrapper here would conflict with timeout recovery.
    let upstream = unsafe { (*ptr).upstream };
    debug_assert!(!upstream.is_null());
    loop {
        // SAFETY: invariants 2 and 3. Upstream can replace its Next entry between
        // successful rows, so fetch it again while execution is still owned.
        let next = unsafe { (*upstream).next }.expect("counter upstream has no Next entry");
        // SAFETY: invariants 1 through 3; no wrapper or output reference crosses this call.
        let rc = unsafe { next(upstream, res) };
        if rc != ffi::RPStatus_RS_RESULT_OK as libc::c_int {
            // A timeout may mean execution ownership was lost. Do not inspect
            // any pipeline field on that path, including profiling metadata.
            if rc == ffi::RPStatus_RS_RESULT_EOF as libc::c_int {
                // SAFETY: EOF requires ownership by invariant 2. The common C
                // prefix is sufficient; upstream need not be a Rust Header.
                if unsafe { (*upstream).ty } == ffi::ResultProcessorType_RP_PROFILE {
                    // SAFETY: invariants 1 and 2; a profiled chain installs the
                    // parent and final profile wrapper before execution.
                    let parent = unsafe { (*ptr).parent };
                    // SAFETY: the live parent is exclusively accessible after EOF.
                    let end_slot = unsafe { (*parent).endProc.get() };
                    // SAFETY: the slot contains the installed final profile wrapper.
                    let end = unsafe { *end_slot };
                    // SAFETY: the final processor is a live profile wrapper.
                    unsafe { ffi::RPProfile_IncrementCount(end) };
                }
            }
            return rc;
        }
        // SAFETY: successful upstream output implies ownership (invariant 2).
        // These accesses end before the next upstream call can release it.
        unsafe {
            (*ptr.cast::<ResultProcessorWrapper<Counter>>())
                .result_processor
                .count += 1
        };
        // SAFETY: the initialized worker-private output is exclusively owned here.
        unsafe { (*res).clear() };
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::test_utils::{Chain, from_iter};
    use std::iter;

    // This must inspect private Counter state and exercise the real C callback
    // layout. Reentrant recovery models ownership release without OS scheduling,
    // allowing Miri to catch references incorrectly retained across upstream Next.
    #[test]
    fn c_entry_allows_recovery_inside_upstream_without_live_counter_borrows() {
        #[repr(C)]
        struct Source {
            header: ffi::ResultProcessor,
            counter: *mut ffi::ResultProcessor,
            calls: usize,
            terminal: libc::c_int,
        }
        unsafe extern "C" fn next(
            ptr: *mut ffi::ResultProcessor,
            _: *mut ffi::SearchResult,
        ) -> libc::c_int {
            // SAFETY: this test installs the callback only on its live Source.
            let source = unsafe { &mut *ptr.cast::<Source>() };
            source.calls += 1;
            if source.calls == 1 {
                return ffi::RPStatus_RS_RESULT_OK as libc::c_int;
            }
            // SAFETY: the counter's raw execution entry holds no Rust references
            // during this callback. This simulates main's exclusive recovery.
            let count = unsafe {
                (*source.counter.cast::<ResultProcessorWrapper<Counter>>())
                    .result_processor
                    .count
            };
            assert_eq!(count, 1);
            // SAFETY: recovery owns the counter header during this callback.
            unsafe { (*source.counter).rpGILTime = 42 };
            let mut recovered = SearchResult::new();
            // SAFETY: the constructor installed this callback on the live counter.
            let drain = unsafe { (*source.counter).Drain.unwrap() };
            // SAFETY: the counter is exclusively accessible and output is private.
            let rc = unsafe { drain(source.counter, (&raw mut recovered).cast()) };
            assert_eq!(rc, ffi::RPDrainStatus_RP_DRAIN_EOF);
            source.terminal
        }

        for terminal in [
            ffi::RPStatus_RS_RESULT_TIMEDOUT,
            ffi::RPStatus_RS_RESULT_ERROR,
            ffi::RPStatus_RS_RESULT_EOF,
        ] {
            let counter = Counter::new().into_raw().as_ptr();
            let mut source = Box::new(Source {
                header: ffi::ResultProcessor {
                    parent: std::ptr::null_mut(),
                    upstream: std::ptr::null_mut(),
                    type_: ffi::ResultProcessorType_RP_INDEX,
                    rpGILTime: 0,
                    Next: Some(next),
                    Free: None,
                    Drain: None,
                },
                counter,
                calls: 0,
                terminal: terminal as libc::c_int,
            });
            let mut row = SearchResult::new();
            // SAFETY: both allocations stay pinned and live through execution;
            // recovery touches only Counter, never Source or the worker's row.
            // Derive from the whole allocation because Next accesses Source's
            // trailing fields, not only its repr(C) header.
            unsafe { (*counter).upstream = (&raw mut *source).cast() };
            // SAFETY: the live counter's constructor installed this callback.
            let next = unsafe { (*counter).Next.unwrap() };
            // SAFETY: the chain is exclusive and its private output is initialized.
            let rc = unsafe { next(counter, (&raw mut row).cast()) };
            assert_eq!(rc, terminal as libc::c_int);
            assert_eq!(source.calls, 2);
            // SAFETY: execution returned, so no callback is borrowing the counter.
            assert_eq!(unsafe { (*counter).rpGILTime }, 42);
            // SAFETY: the allocation contains Counter and has no remaining borrower.
            let count = unsafe {
                (*counter.cast::<ResultProcessorWrapper<Counter>>())
                    .result_processor
                    .count
            };
            assert_eq!(count, 1);
            // SAFETY: the live counter's constructor installed this destructor.
            let free = unsafe { (*counter).Free.unwrap() };
            // SAFETY: ownership of the allocation is released exactly once.
            unsafe { free(counter) };
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "invokes the C RPProfile_New implementation")]
    fn c_entry_preserves_profile_eof_accounting() {
        unsafe extern "C" {
            fn RPProfile_New(
                upstream: *mut ffi::ResultProcessor,
                parent: *mut ffi::QueryProcessingCtx,
            ) -> *mut ffi::ResultProcessor;
            fn RPProfile_GetCount(profile: *mut ffi::ResultProcessor) -> u64;
        }
        unsafe extern "C" fn eof(
            _: *mut ffi::ResultProcessor,
            _: *mut ffi::SearchResult,
        ) -> libc::c_int {
            ffi::RPStatus_RS_RESULT_EOF as libc::c_int
        }
        let mut parent = ffi::QueryProcessingCtx::new();
        let parent_ptr = parent.as_mut().get_mut() as *mut ffi::QueryProcessingCtx;
        let counter = Counter::new().into_raw().as_ptr();
        let mut source = ffi::ResultProcessor {
            parent: parent_ptr,
            upstream: std::ptr::null_mut(),
            type_: ffi::ResultProcessorType_RP_PROFILE,
            rpGILTime: 0,
            Next: Some(eof),
            Free: None,
            Drain: None,
        };
        let mut row = SearchResult::new();
        // SAFETY: the test owns every allocation, keeps it at a stable address,
        // and invokes callbacks sequentially before freeing their state once.
        let profile = unsafe { RPProfile_New(counter, parent_ptr) };
        // SAFETY: parent is exclusively owned and the profile outlives execution.
        unsafe { *parent.endProc.get() = profile };
        // SAFETY: the counter is exclusively owned during chain construction.
        unsafe { (*counter).parent = parent_ptr };
        // SAFETY: source stays at this address through execution.
        unsafe { (*counter).upstream = &raw mut source };
        // SAFETY: the live counter's constructor installed this callback.
        let next = unsafe { (*counter).Next.unwrap() };
        // SAFETY: the constructed chain and private output are exclusively owned.
        let rc = unsafe { next(counter, (&raw mut row).cast()) };
        assert_eq!(rc, ffi::RPStatus_RS_RESULT_EOF as libc::c_int);
        // SAFETY: profile is a live C RPProfile, accessed after execution ended.
        assert_eq!(unsafe { RPProfile_GetCount(profile) }, 1);
        for processor in [profile, counter] {
            // SAFETY: each live processor has its own installed destructor.
            let free = unsafe { (*processor).Free.unwrap() };
            // SAFETY: both callbacks finished and each allocation is freed once.
            unsafe { free(processor) };
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn basically_works() {
        // Set up the result processor chain
        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::default())).take(3),
        ));
        chain.append(Counter::new());

        let (cx, rp) = chain.last_as_context_and_inner::<Counter>();

        assert!(rp.next(cx, &mut SearchResult::default()).unwrap().is_none());
        assert_eq!(rp.count, 3);
    }
}
