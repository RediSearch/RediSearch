/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The `FIRST_VALUE` reducer.
//!
//! Terms: the *return key* reads the *return value* a group keeps and returns;
//! the *sort key* reads the *sort value* that rows are compared on.

use std::cmp::Ordering;

use rlookup::{RLookupKey, RLookupRow};
use value::comparison::compare_with_query_error;
use value::{SharedValue, Value};

use crate::accumulator::Accumulator;

/// `FIRST_VALUE` of a property: the return value of the group's first row, or,
/// with a sort key, of the row whose sort value comes first (see [`SortBy`]). A
/// missing return value reads as null.
pub struct FirstValue<'a> {
    ret_key: &'a RLookupKey<'a>,
    sort_by: Option<SortBy<'a>>,
}

/// The sort key and direction of a [`FirstValue`] reducer.
///
/// A null sort value never wins over a non-null one. Ties keep the earlier row.
pub struct SortBy<'a> {
    pub sort_key: &'a RLookupKey<'a>,
    pub direction: Direction,
}

/// Which end of the sort order a [`FirstValue`] reducer takes its return value from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Direction {
    /// How a row's sort value must compare against the current best to replace it.
    const fn winning(self) -> Ordering {
        match self {
            Self::Ascending => Ordering::Less,
            Self::Descending => Ordering::Greater,
        }
    }
}

impl<'a> FirstValue<'a> {
    pub const fn new(ret_key: &'a RLookupKey<'a>, sort_by: Option<SortBy<'a>>) -> Self {
        Self { ret_key, sort_by }
    }
}

/// The per-group state of [`FirstValue`]: nothing before the first row.
#[derive(Default)]
pub struct FirstValueState(Option<Kept>);

/// The return value kept so far, and the sort value a later row must beat to replace it.
struct Kept {
    ret_value: SharedValue,
    /// `None` while no non-null sort value was seen, and always without a sort key.
    sort_value: Option<SharedValue>,
}

fn get_or_null(row: &RLookupRow<'_>, key: &RLookupKey<'_>) -> SharedValue {
    row.get(key)
        .cloned()
        .unwrap_or_else(SharedValue::null_static)
}

impl FirstValue<'_> {
    /// Keeps the first row's return value; later rows change nothing.
    fn add_unsorted(&self, state: &mut FirstValueState, row: &RLookupRow<'_>) {
        state.0.get_or_insert_with(|| Kept {
            ret_value: get_or_null(row, self.ret_key),
            sort_value: None,
        });
    }

    fn add_sorted(&self, state: &mut FirstValueState, row: &RLookupRow<'_>, sort_by: &SortBy<'_>) {
        // Borrowed: most rows do not win, so only a winning sort value is cloned.
        let row_sort_value = row.get(sort_by.sort_key).filter(|value| !is_null(value));

        match (&mut state.0, row_sort_value) {
            // The first row is kept, whatever its sort value.
            (None, sort_value) => {
                state.0 = Some(Kept {
                    ret_value: get_or_null(row, self.ret_key),
                    sort_value: sort_value.cloned(),
                });
            }
            // A null sort value never wins.
            (Some(_), None) => {}
            // Any non-null sort value beats a null best one.
            (Some(kept), Some(sort_value))
                if kept.sort_value.as_ref().is_none_or(|best| {
                    compare_with_query_error(sort_value, best, None) == sort_by.direction.winning()
                }) =>
            {
                kept.sort_value = Some(sort_value.clone());
                kept.ret_value = get_or_null(row, self.ret_key);
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
            .map_or_else(SharedValue::null_static, |kept| kept.ret_value.clone())
    }
}

/// Whether `value` is null, following references.
fn is_null(value: &SharedValue) -> bool {
    matches!(value.fully_dereferenced_ref(), Value::Null)
}
