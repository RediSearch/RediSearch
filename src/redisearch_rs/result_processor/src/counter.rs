/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::ResultProcessor;
use search_result::SearchResult;

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
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::test_utils::{Chain, MockResultProcessor, from_iter};
    use std::{
        iter,
        sync::atomic::{AtomicUsize, Ordering},
    };

    static PROFILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Mock implementation of `RPProfile_IncrementCount` for tests
    ///
    // FIXME: replace with `Profile::increment_count` once the profile result processor is ported.
    #[unsafe(no_mangle)]
    unsafe extern "C" fn RPProfile_IncrementCount(_r: *mut ffi::ResultProcessor) {
        PROFILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    }

    #[test]
    fn basically_works() {
        // Set up the result processor chain
        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::new())).take(3),
        ));
        chain.append(Counter::new());

        let (cx, rp) = chain.last_as_context_and_inner::<Counter>();

        assert!(rp.next(cx, &mut SearchResult::new()).unwrap().is_none());
        assert_eq!(rp.count, 3);
    }

    /// Tests that RPProfile_IncrementCount is incremented one when the pipeline runs.
    #[test]
    fn test_profile_count() {
        type MockRPProfile = MockResultProcessor<{ ffi::ResultProcessorType_RP_PROFILE }>;

        let mut chain = Chain::new();
        chain.append(from_iter(
            iter::from_fn(|| Some(SearchResult::new())).take(3),
        ));
        chain.append(MockRPProfile::new());
        chain.append(Counter::new());
        chain.append(MockRPProfile::new());

        let (cx, rp) = chain.last_as_context_and_inner::<MockRPProfile>();
        rp.next(cx, &mut SearchResult::new()).unwrap();

        assert_eq!(PROFILE_COUNTER.load(Ordering::Relaxed), 1);
    }
}
