/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use bumpalo::Bump;

use crate::{Reducer, ReducerOptions};
use query_error::QueryErrorCode;
use rlookup::RLookupRow;
use value::RSValueFFI;

/// A simple counter reducer.
///
/// This struct must be `#[repr(C)]` and its first attribute must be a `Reducer` because it's being
/// downcasted for use in C as a `ffi::Reducer` where C will read out fields of `ffi::Reducer`
/// directly such as the VTable pointers to the proper reducer functions.
#[repr(C)]
pub struct CounterReducer {
    reducer: Reducer,
    /// Arena allocator for [`CounterCtx`] instances, matching the `BlkAlloc` pattern used by C
    /// reducers. All instances are freed at once when the reducer is dropped.
    arena: Bump,
}

/// A simple counter reducer instance.
pub struct CounterCtx {
    count: usize,
}

impl CounterReducer {
    /// Create a new counter reducer
    pub fn new(options: &mut ReducerOptions) -> Option<Self> {
        if options.args().argc != 0 {
            options.status().set_code_and_message(
                QueryErrorCode::BadAttr,
                Some(c"Count accepts 0 values only".into()),
            );

            return None;
        }

        Some(Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
        })
    }

    /// Get a mutable reference to the base reducer.
    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a new [`CounterCtx`] instance from the arena.
    pub fn alloc_instance(&self) -> &mut CounterCtx {
        self.arena.alloc(CounterCtx::new(self))
    }
}

impl CounterCtx {
    /// Create a new counter reducer instance.
    pub const fn new(_r: &CounterReducer) -> Self {
        Self { count: 0 }
    }

    /// Process the provided `RLookupRow` with the counter reducer instance.
    pub const fn add(&mut self, _r: &CounterReducer, _srcrow: &RLookupRow) {
        self.count += 1;
    }

    /// Finalize the counter reducer instance result into an `RSValueFFI`.
    pub fn finalize(&self, _r: &CounterReducer) -> RSValueFFI {
        RSValueFFI::new_num(self.count as f64)
    }

    /// Free the provided counter reducer instance.
    pub const fn free(&mut self, _r: &CounterReducer) {}
}
