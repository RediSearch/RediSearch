/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `SUM` and `AVG` reducers.

use bumpalo::Bump;
use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::accumulator::Accumulator;

/// Whether a [`Sum`] reducer returns the total, the average, or how many numbers
/// it added up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SumMode {
    Sum,
    Average,
    /// What a distributed `AVG` divides by: the shards count the numbers, and the
    /// coordinator adds up the counts.
    Count,
}

/// `SUM` or `AVG` of a property, over its [values][RLookupRow::values_of] that convert to
/// a number (see [`value::Value::to_number`]). A group with no such value reduces to
/// NaN.
pub struct Sum<'a> {
    key: &'a RLookupKey<'a>,
    mode: SumMode,
}

impl<'a> Sum<'a> {
    pub const fn new(key: &'a RLookupKey<'a>, mode: SumMode) -> Self {
        Self { key, mode }
    }
}

/// The per-group state of [`Sum`].
#[derive(Default)]
pub struct SumState {
    count: usize,
    total: f64,
}

impl Accumulator for Sum<'_> {
    type State<'arena> = SumState;

    fn init(&self, _arena: &Bump) -> SumState {
        SumState::default()
    }

    fn add(&self, state: &mut SumState, row: &RLookupRow<'_>) {
        for num in row
            .values_of(self.key)
            .filter_map(|value| value.to_number())
        {
            state.total += num;
            state.count += 1;
        }
    }

    fn finalize(&self, state: &SumState) -> SharedValue {
        let result = match (state.count, self.mode) {
            (count, SumMode::Count) => count as f64,
            (0, _) => f64::NAN,
            (count, SumMode::Average) => state.total / count as f64,
            (_, SumMode::Sum) => state.total,
        };
        SharedValue::new_num(result)
    }
}
