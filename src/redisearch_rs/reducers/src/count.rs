/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Reducer;
use rlookup::RLookupRow;
use value::RSValueFFI;

/// A simple counter reducer instance.
pub struct Counter {
    count: usize,
}

impl Counter {
    /// Create a new counter reducer instance.
    pub const fn new(_r: &Reducer) -> Self {
        Self { count: 0 }
    }

    /// Process the provided `RLookupRow` with the counter reducer instance.
    pub const fn add(&mut self, _r: &Reducer, _srcrow: &RLookupRow) {
        self.count += 1;
    }

    /// Finalize the counter reducer instance result into an `RSValueFFI`.
    pub fn finalize(&self, _r: &Reducer) -> RSValueFFI {
        RSValueFFI::new_num(self.count as f64)
    }

    /// Free the provided counter reducer instance.
    pub const fn free(self, _r: &Reducer) {}
}
