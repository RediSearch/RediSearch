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
/// becomes the one later rows must beat. Ties keep the earlier row.
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
    /// Null without a sort key, which never reads it.
    sort_value: SharedValue,
}

impl Kept {
    fn of_row(
        row: &RLookupRow<'_>,
        key: &RLookupKey<'_>,
        sort_key: Option<&RLookupKey<'_>>,
    ) -> Self {
        Self {
            value: get_or_null(row, key),
            sort_value: sort_key.map_or_else(SharedValue::null_static, |key| get_or_null(row, key)),
        }
    }
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
            if state.0.is_none() {
                state.0 = Some(Kept::of_row(row, self.key, None));
            }
            return;
        };
        // Borrowed: most rows do not win, so only a winning sort key is cloned.
        let sort_value = row.get(sort_by.key).filter(|value| !is_null(value));
        let Some(kept) = &mut state.0 else {
            state.0 = Some(Kept::of_row(row, self.key, Some(sort_by.key)));
            return;
        };
        let Some(sort_value) = sort_value else {
            return;
        };
        if is_null(&kept.sort_value) {
            kept.sort_value = sort_value.clone();
        } else if compare_with_query_error(sort_value, &kept.sort_value, None)
            == sort_by.direction.winning()
        {
            *kept = Kept::of_row(row, self.key, Some(sort_by.key));
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
