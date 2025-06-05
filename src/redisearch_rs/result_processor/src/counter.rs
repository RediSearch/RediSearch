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
            #[cfg(not(test))] // can we link Rust unit tests against the redis code?
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
