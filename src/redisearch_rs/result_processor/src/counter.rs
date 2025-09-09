/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::ResultProcessor;

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
        res: &mut ffi::SearchResult,
    ) -> Result<Option<()>, crate::Error> {
        let mut upstream = cx
            .upstream()
            .expect("There is no processor upstream of this counter.");

        while upstream.next(res)?.is_some() {
            self.count += 1;

            // Safety: This function should only be called on initialized SearchResults. Luckily,
            // a ResultProcessor returning `RPStatus_RS_RESULT_OK` means "Result is filled with valid data"
            // so it is safe to call this function inside this loop.
            unsafe {
                ffi::SearchResult_Clear(res);
            }
        }

        // This logic was present in the original C code, with the following comment:
        // > Since this never returns RM_OK, in profile mode, count should be increased to compensate for EOF.
        if upstream.ty() == ffi::ResultProcessorType_RP_PROFILE {
            // Safety: If the upstream processor is a Profile, then the original C code currently
            // expects the last processor (`base->parent->endProc`) to be a `RPProfile*`, and
            // increments its count. This is replicating that behavior.
            unsafe { ffi::RPProfile_IncrementCount(cx.last_processor()) }
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
    use crate::test_utils::{Chain, default_search_result, from_iter};
    use std::iter;

    #[test]
    fn basically_works() {
        // Set up the result processor chain
        let mut chain = Chain::new();
        chain.append(from_iter(iter::repeat_n(default_search_result(), 3)));
        chain.append(Counter::new());

        let (cx, rp) = chain.last_as_context_and_inner::<Counter>();

        assert!(rp.next(cx, &mut default_search_result()).unwrap().is_none());
        assert_eq!(rp.count, 3);
    }
}
