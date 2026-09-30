/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `MIN` and `MAX` reducers.

use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::accumulator::Accumulator;

/// Which extreme a [`MinMax`] reducer keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Extreme {
    Min,
    Max,
}

/// `MIN` or `MAX` of a property, over the rows where it converts to a number (see
/// [`value::Value::to_number`]). A group with no such row reduces to positive
/// infinity for `MIN` and negative infinity for `MAX`.
pub struct MinMax<'a> {
    key: &'a RLookupKey<'a>,
    extreme: Extreme,
}

impl<'a> MinMax<'a> {
    pub const fn new(key: &'a RLookupKey<'a>, extreme: Extreme) -> Self {
        Self { key, extreme }
    }
}

impl Accumulator for MinMax<'_> {
    type State = f64;

    fn init(&self) -> f64 {
        match self.extreme {
            Extreme::Min => f64::INFINITY,
            Extreme::Max => f64::NEG_INFINITY,
        }
    }

    fn add(&self, best: &mut f64, row: &RLookupRow<'_>) {
        let Some(num) = row.get(self.key).and_then(|value| value.to_number()) else {
            return;
        };
        // The comparisons of C's `MIN`/`MAX` macros. Unlike `f64::min`/`f64::max`,
        // which skip NaN, they let a NaN replace the current extreme, and the next
        // number replace the NaN.
        let keep = match self.extreme {
            Extreme::Min => *best < num,
            Extreme::Max => *best > num,
        };
        if !keep {
            *best = num;
        }
    }

    fn finalize(&self, best: &f64) -> SharedValue {
        SharedValue::new_num(*best)
    }
}
