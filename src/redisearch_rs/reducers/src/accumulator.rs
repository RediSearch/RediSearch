/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reducers that fold each group's rows into a single per-group state.

use bumpalo::Bump;
use rlookup::RLookupRow;
use value::SharedValue;

use crate::Reducer;

/// The logic of a reducer whose per-group state is one [`Accumulator::State`].
pub trait Accumulator {
    /// The per-group state. It must not need dropping: the states live in an arena
    /// that never runs their destructors, and [`AccumulatorReducer::new`] rejects a
    /// state that has one at compile time.
    type State;

    /// The state of a group before any of its rows.
    fn init(&self) -> Self::State;

    /// Folds one of the group's rows into `state`.
    fn add(&self, state: &mut Self::State, row: &RLookupRow<'_>);

    /// The group's result.
    fn finalize(&self, state: &Self::State) -> SharedValue;
}

/// A [`Reducer`] running an [`Accumulator`], with the per-group states in an arena.
///
/// Must remain `#[repr(C)]` with [`Reducer`] at offset 0 so the C layer can
/// downcast this struct to `ffi::Reducer*` and read the vtable directly.
#[repr(C)]
pub struct AccumulatorReducer<A: Accumulator> {
    reducer: Reducer,
    /// Arena for the per-group states; see [`Accumulator::State`].
    arena: Bump,
    accumulator: A,
}

const _: () = assert!(core::mem::offset_of!(AccumulatorReducer<crate::count::Count>, reducer) == 0);

impl<A: Accumulator> AccumulatorReducer<A> {
    pub fn new(accumulator: A) -> Self {
        const {
            assert!(
                !std::mem::needs_drop::<A::State>(),
                "accumulator states must not need dropping"
            );
        }
        Self {
            reducer: Reducer::new(),
            arena: Bump::new(),
            accumulator,
        }
    }

    pub const fn reducer_mut(&mut self) -> &mut Reducer {
        &mut self.reducer
    }

    pub const fn accumulator(&self) -> &A {
        &self.accumulator
    }

    /// Allocates the state of a new group.
    pub fn new_state(&self) -> &mut A::State {
        self.arena.alloc(self.accumulator.init())
    }
}
