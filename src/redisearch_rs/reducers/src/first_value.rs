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
/// A null sort key never wins over a non-null one. Ties keep the earlier row.
pub struct SortBy<'a> {
    pub key: &'a RLookupKey<'a>,
    pub direction: Direction,
}

/// Which end of the sort order a [`FirstValue`] reducer takes its value from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Direction {
    /// How a sort key must compare against the current best to replace it.
    const fn winning(self) -> Ordering {
        match self {
            Self::Ascending => Ordering::Less,
            Self::Descending => Ordering::Greater,
        }
    }
}

impl<'a> FirstValue<'a> {
    pub const fn new(key: &'a RLookupKey<'a>, sort_by: Option<SortBy<'a>>) -> Self {
        Self { key, sort_by }
    }
}

/// The per-group state of [`FirstValue`]: nothing before the first row.
#[derive(Default)]
pub struct FirstValueState(Option<Kept>);

/// The value kept so far, and the sort key a later row must beat to replace it.
struct Kept {
    value: SharedValue,
    /// `None` while no non-null sort key was seen, and always without a sort key.
    sort_value: Option<SharedValue>,
}

fn get_or_null(row: &RLookupRow<'_>, key: &RLookupKey<'_>) -> SharedValue {
    row.get(key)
        .cloned()
        .unwrap_or_else(SharedValue::null_static)
}

impl FirstValue<'_> {
    /// Keeps the first row's value; later rows change nothing.
    fn add_unsorted(&self, state: &mut FirstValueState, row: &RLookupRow<'_>) {
        state.0.get_or_insert_with(|| Kept {
            value: get_or_null(row, self.key),
            sort_value: None,
        });
    }

    fn add_sorted(&self, state: &mut FirstValueState, row: &RLookupRow<'_>, sort_by: &SortBy<'_>) {
        // Borrowed: most rows do not win, so only a winning sort key is cloned.
        let row_sort_value = row.get(sort_by.key).filter(|value| !is_null(value));

        match (&mut state.0, row_sort_value) {
            // The first row is kept, whatever its sort key.
            (None, sort_value) => {
                state.0 = Some(Kept {
                    value: get_or_null(row, self.key),
                    sort_value: sort_value.cloned(),
                });
            }
            // A null sort key never wins.
            (Some(_), None) => {}
            // Any non-null sort key beats a null best one.
            (Some(kept), Some(sort_value))
                if kept.sort_value.as_ref().is_none_or(|best| {
                    compare_with_query_error(sort_value, best, None) == sort_by.direction.winning()
                }) =>
            {
                kept.sort_value = Some(sort_value.clone());
                kept.value = get_or_null(row, self.key);
            }
            // The row does not beat the best.
            (Some(_), Some(_)) => {}
        }
    }
}

impl Accumulator for FirstValue<'_> {
    type State = FirstValueState;

    fn init(&self) -> FirstValueState {
        FirstValueState::default()
    }

    fn add(&self, state: &mut FirstValueState, row: &RLookupRow<'_>) {
        match &self.sort_by {
            None => self.add_unsorted(state, row),
            Some(sort_by) => self.add_sorted(state, row, sort_by),
        }
    }

    fn finalize(&self, state: &FirstValueState) -> SharedValue {
        state
            .0
            .as_ref()
            .map_or_else(SharedValue::null_static, |kept| kept.value.clone())
    }
}

/// Whether `value` is null, following references.
fn is_null(value: &SharedValue) -> bool {
    matches!(value.fully_dereferenced_ref(), Value::Null)
}
