/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{ResultProcessor, ResultProcessorType};

pub struct Counter {
    count: usize,
}

impl ResultProcessor for Counter {
    const TYPE: ResultProcessorType = ResultProcessorType::Counter;

    fn next(
        &mut self,
        mut cx: crate::Context,
    ) -> Result<Option<crate::ffi::SearchResult>, crate::Error> {
        let mut upstream = cx.upstream().unwrap();

        while upstream.next()?.is_some() {
            self.count += 1;
        }

        Ok(Some(crate::ffi::SearchResult::default()))
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
