/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Utility types for the numeric range tree.
//!
//! This module provides [`CheckedCount`], a newtype wrapper around `usize`
//! that enforces checked arithmetic for all mutations. It is used by
//! [`TreeStats`](super::TreeStats) to make overflow/underflow bugs
//! impossible to introduce silently.

use std::fmt;
use std::ops::{AddAssign, SubAssign};

/// A `usize` wrapper that panics on overflow or underflow instead of wrapping.
///
/// All [`TreeStats`](super::TreeStats) fields use this type so that
/// incrementing or decrementing a counter is always bounds-checked,
/// even when using `+=` / `-=` operators.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CheckedCount(usize);

impl CheckedCount {
    /// Create a new `CheckedCount` with the given value.
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Return the inner `usize` value.
    pub const fn get(self) -> usize {
        self.0
    }

    /// Apply a signed delta, panicking on overflow or underflow.
    pub const fn apply_delta(self, delta: i64) -> Self {
        if delta < 0 {
            Self(self.0.checked_sub((-delta) as usize).expect("Underflow!"))
        } else {
            Self(self.0.checked_add(delta as usize).expect("Overflow!"))
        }
    }
}

/// Panics on overflow.
impl AddAssign<usize> for CheckedCount {
    fn add_assign(&mut self, rhs: usize) {
        self.0 = self.0.checked_add(rhs).expect("Overflow!");
    }
}

/// Panics on underflow.
impl SubAssign<usize> for CheckedCount {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 = self.0.checked_sub(rhs).expect("Underflow!");
    }
}

/// Delegates to the inner `usize`, used in assertion messages.
impl fmt::Display for CheckedCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
