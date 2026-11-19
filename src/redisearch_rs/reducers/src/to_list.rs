/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `TOLIST` reducer.

use std::hash::{Hash, Hasher};

use bumpalo::Bump;
use indexmap::IndexSet;
use rlookup::{RLookupKey, RLookupRow};
use value::comparison::compare_on_equality_only;
use value::hash::hash_value;
use value::{SharedValue, Value};

use crate::accumulator::Accumulator;

/// `TOLIST` of a property: its distinct values in the group, in the order first
/// seen. An array contributes each of its elements instead of itself. Rows where
/// the property is missing are skipped.
pub struct ToList<'a> {
    key: &'a RLookupKey<'a>,
}

impl<'a> ToList<'a> {
    pub const fn new(key: &'a RLookupKey<'a>) -> Self {
        Self { key }
    }
}

impl Accumulator for ToList<'_> {
    type State<'arena> = IndexSet<Distinct>;

    fn init(&self, _arena: &Bump) -> IndexSet<Distinct> {
        IndexSet::new()
    }

    fn add(&self, values: &mut IndexSet<Distinct>, row: &RLookupRow<'_>) {
        let Some(value) = row.get(self.key) else {
            return;
        };
        match &**value {
            Value::Array(elements) => values.extend(elements.iter().cloned().map(Distinct)),
            _ => {
                values.insert(Distinct(value.clone()));
            }
        }
    }

    fn finalize(&self, values: &IndexSet<Distinct>) -> SharedValue {
        SharedValue::new_array(values.iter().map(|value| value.0.clone()))
    }
}

/// A value as [`ToList`] tells values apart: hashed with [`hash_value`] and
/// compared with [`compare_on_equality_only`].
///
/// The two can disagree: values the comparison equates but that hash apart, such
/// as the number 3 and the string "3", are usually both kept.
pub struct Distinct(SharedValue);

impl PartialEq for Distinct {
    fn eq(&self, other: &Self) -> bool {
        compare_on_equality_only(&self.0, &other.0)
    }
}

impl Eq for Distinct {}

impl Hash for Distinct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(&self.0, state);
    }
}
