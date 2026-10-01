/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `COUNT` reducer.

use rlookup::RLookupRow;
use value::SharedValue;

use crate::accumulator::Accumulator;

/// `COUNT`: the number of rows in the group.
pub struct Count;

impl Accumulator for Count {
    type State = usize;

    fn init(&self) -> usize {
        0
    }

    fn add(&self, count: &mut usize, _row: &RLookupRow<'_>) {
        *count += 1;
    }

    fn finalize(&self, count: &usize) -> SharedValue {
        SharedValue::new_num(*count as f64)
    }
}
