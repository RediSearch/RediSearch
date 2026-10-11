/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `SUM` and `AVG` reducers.

use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use crate::accumulator::Accumulator;

/// Whether a [`Sum`] reducer returns the total or the average.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SumMode {
    Sum,
    Average,
}

/// `SUM` or `AVG` of a property, over the rows where it converts to a number (see
/// [`value::Value::to_number`]). A group with no such row reduces to NaN.
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
    type State = SumState;

    fn init(&self) -> SumState {
        SumState::default()
    }

    fn add(&self, state: &mut SumState, row: &RLookupRow<'_>) {
        if let Some(num) = row.get(self.key).and_then(|value| value.to_number()) {
            state.total += num;
            state.count += 1;
        }
    }

    fn finalize(&self, state: &SumState) -> SharedValue {
        let result = match (state.count, self.mode) {
            (0, _) => f64::NAN,
            (count, SumMode::Average) => state.total / count as f64,
            (_, SumMode::Sum) => state.total,
        };
        SharedValue::new_num(result)
    }
}
