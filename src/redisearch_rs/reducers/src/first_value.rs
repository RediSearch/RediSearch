/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `FIRST_VALUE` reducer.

use std::cmp::Ordering;

use rlookup::{RLookupKey, RLookupRow};
use value::comparison::compare_with_query_error;
use value::{SharedValue, Value};

use crate::accumulator::Accumulator;

/// `FIRST_VALUE` of a property: its value in the group's first row, or, with a
/// sort key, in the row whose sort key comes first (see [`SortBy`]). A missing
/// property reads as null.
pub struct FirstValue<'a> {
    key: &'a RLookupKey<'a>,
    sort_by: Option<SortBy<'a>>,
}

/// The sort key of a [`FirstValue`] reducer.
///
/// A null sort key never wins over a non-null one. A group whose first sort key
/// is null keeps the first row's value, though: the first non-null sort key only
/// becomes the one later rows must beat.
pub struct SortBy<'a> {
    pub key: &'a RLookupKey<'a>,
    pub ascending: bool,
}

impl<'a> FirstValue<'a> {
    pub const fn new(key: &'a RLookupKey<'a>, sort_by: Option<SortBy<'a>>) -> Self {
        Self { key, sort_by }
    }
}

/// The per-group state of [`FirstValue`]: the value kept so far and, with a sort
/// key, the sort key it must be beaten on. Both are `None` before the first row.
#[derive(Default)]
pub struct FirstValueState {
    value: Option<SharedValue>,
    sort_value: Option<SharedValue>,
}

fn get_or_null(row: &RLookupRow<'_>, key: &RLookupKey<'_>) -> SharedValue {
    row.get(key)
        .cloned()
        .unwrap_or_else(SharedValue::null_static)
}

impl Accumulator for FirstValue<'_> {
    type State = FirstValueState;

    fn init(&self) -> FirstValueState {
        FirstValueState::default()
    }

    fn add(&self, state: &mut FirstValueState, row: &RLookupRow<'_>) {
        let Some(sort_by) = &self.sort_by else {
            if state.value.is_none() {
                state.value = Some(get_or_null(row, self.key));
            }
            return;
        };

        let Some(best) = &mut state.sort_value else {
            state.value = Some(get_or_null(row, self.key));
            state.sort_value = Some(get_or_null(row, sort_by.key));
            return;
        };
        // Borrowed: most rows do not win, so only a winning sort key is cloned.
        let Some(sort_value) = row.get(sort_by.key).filter(|value| !is_null(value)) else {
            return;
        };
        if is_null(best) {
            *best = sort_value.clone();
            return;
        }
        let wanted = if sort_by.ascending {
            Ordering::Less
        } else {
            Ordering::Greater
        };
        if compare_with_query_error(sort_value, best, None) == wanted {
            *best = sort_value.clone();
            state.value = Some(get_or_null(row, self.key));
        }
    }

    fn finalize(&self, state: &FirstValueState) -> SharedValue {
        state.value.clone().unwrap_or_else(SharedValue::null_static)
    }
}

/// Whether `value` is null, following references.
fn is_null(value: &SharedValue) -> bool {
    matches!(value.fully_dereferenced_ref(), Value::Null)
}
