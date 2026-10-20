/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `STDDEV` reducer.

use rlookup::{RLookupKey, RLookupRow};
use value::{SharedValue, Value};

use crate::accumulator::Accumulator;

/// The sample standard deviation of a property, over its values that convert to a
/// number (see [`Value::to_number`]); an array contributes each of its elements. A
/// group with fewer than two such values reduces to 0.
pub struct StdDev<'a> {
    key: &'a RLookupKey<'a>,
}

impl<'a> StdDev<'a> {
    pub const fn new(key: &'a RLookupKey<'a>) -> Self {
        Self { key }
    }
}

/// The per-group state of [`StdDev`], updated with Welford's online algorithm.
#[derive(Default)]
pub struct StdDevState {
    count: usize,
    mean: f64,
    /// The sum of squared differences from the mean.
    squares: f64,
}

impl StdDevState {
    fn add(&mut self, num: f64) {
        self.count += 1;
        if self.count == 1 {
            self.mean = num;
            self.squares = 0.0;
        } else {
            let mean = self.mean + (num - self.mean) / self.count as f64;
            self.squares += (num - self.mean) * (num - mean);
            self.mean = mean;
        }
    }
}

impl Accumulator for StdDev<'_> {
    type State = StdDevState;

    fn init(&self) -> StdDevState {
        StdDevState::default()
    }

    fn add(&self, state: &mut StdDevState, row: &RLookupRow<'_>) {
        let Some(value) = row.get(self.key) else {
            return;
        };
        // Only a direct array is flattened, not one behind a reference.
        let items = match &**value {
            Value::Array(items) => &items[..],
            _ => std::slice::from_ref(value),
        };
        for num in items.iter().filter_map(|item| item.to_number()) {
            state.add(num);
        }
    }

    fn finalize(&self, state: &StdDevState) -> SharedValue {
        let variance = if state.count > 1 {
            state.squares / (state.count - 1) as f64
        } else {
            0.0
        };
        SharedValue::new_num(variance.sqrt())
    }
}
