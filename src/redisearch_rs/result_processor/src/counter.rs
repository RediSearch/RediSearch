/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{Context, ResultProcessor, ResultProcessorType};
use std::mem::MaybeUninit;

unsafe extern "C" {
    unsafe fn SearchResult_Clear(r: *mut crate::ffi::SearchResult);
}

#[derive(Debug)]
pub struct Counter {
    count: usize,
}

impl ResultProcessor for Counter {
    const TYPE: ResultProcessorType = ResultProcessorType::Counter;

    fn next(
        &mut self,
        mut cx: Context,
        res: &mut MaybeUninit<crate::ffi::SearchResult>,
    ) -> Result<(), crate::Error> {
        let mut upstream = cx.upstream().unwrap();

        while upstream.next(res).is_ok() {
            self.count += 1;
            unsafe {
                SearchResult_Clear(res.as_mut_ptr());
            }
        }

        Ok(())
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
