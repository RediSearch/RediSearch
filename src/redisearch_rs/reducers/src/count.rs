/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! COUNT reducer: counts the rows of a group. It reads no source key.
//!
//! C parses the reducer arguments and constructs the reducer via
//! `reducers_ffi`.

use bumpalo::Bump;
use value::SharedValue;

use crate::Reducer;

/// Group-independent state of COUNT.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer
/// can downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct CountReducer {
    /// C-visible vtable header. Pinned to offset 0 by `#[repr(C)]` so the
    /// C layer can downcast `CountReducer*` to `ffi::Reducer*`.
    reducer: Reducer,
    /// Arena for per-group contexts. [`CountCtx`] has no destructor, so
    /// instances need no explicit drop; the memory is freed when the
    /// reducer (and its [`Bump`]) is dropped.
    arena: Bump,
}

const _: () = assert!(core::mem::offset_of!(CountReducer, reducer) == 0);

/// Per-group accumulator of [`CountReducer`].
#[derive(Default)]
pub struct CountCtx {
    count: u64,
}

impl CountReducer {
    #[expect(clippy::new_without_default, reason = "constructed via FFI only")]
    pub fn new() -> Self {
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    /// Allocate a zeroed per-group accumulator in the arena.
    pub fn alloc_instance(&self) -> &mut CountCtx {
        self.arena.alloc(CountCtx::default())
    }
}

impl CountCtx {
    pub const fn add(&mut self) {
        self.count += 1;
    }

    pub fn finalize(&self) -> SharedValue {
        SharedValue::new_num(self.count as f64)
    }
}
