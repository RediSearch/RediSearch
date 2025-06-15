/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::ResultProcessor;

#[derive(Debug)]
/// A processor to track the number of entries yielded by the previous processor in the chain.
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
    use crate::{Context, ResultProcessorWrapper, test::from_iter};
    use std::{
        pin::Pin,
        ptr::{self, NonNull},
    };

    #[test]
    fn basically_works() {
        const INIT: ffi::SearchResult = ffi::SearchResult {
            docId: 0,
            score: 0.0,
            scoreExplain: ptr::null_mut(),
            dmd: ptr::null(),
            indexResult: ptr::null_mut(),
            rowdata: ffi::RLookupRow {
                sv: ptr::null(),
                dyn_: ptr::null_mut(),
                ndyn: 0,
            },
            flags: 0,
        };

        // Set up the result processors on the heap
        let upstream = Box::pin(ResultProcessorWrapper::new(from_iter([INIT, INIT, INIT])));
        let upstream =
            unsafe { NonNull::new(Box::into_raw(Pin::into_inner_unchecked(upstream))).unwrap() };

        let mut rp = Box::pin(ResultProcessorWrapper::new(Counter::new()));

        // Emulate what `QITR_PushRP` would be doing
        rp.header.upstream = upstream.as_ptr().cast();

        let mut rp = NonNull::new(unsafe { ResultProcessorWrapper::into_ptr(rp) }).unwrap();

        let cx = unsafe { Context::from_raw(rp.cast()) };

        // we don't care about the exact search result value here
        #[allow(const_item_mutation)]
        unsafe {
            assert!(
                rp.as_mut()
                    .result_processor
                    .next(cx, &mut INIT)
                    .unwrap()
                    .is_none()
            );
        };

        assert_eq!(unsafe { rp.as_mut() }.result_processor.count, 3);

        // we need to correctly free both result processors at the end
        unsafe {
            let mut rp = rp.cast::<crate::Header>();
            (rp.as_mut().free.unwrap())(rp.as_ptr());

            let mut upstream = upstream.cast::<crate::Header>();
            (upstream.as_mut().free.unwrap())(upstream.as_ptr());
        }
    }
}
