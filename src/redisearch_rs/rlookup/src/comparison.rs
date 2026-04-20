/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cmp::Ordering;

use query_error::QueryError;
use value::comparison::{compare, map_compare_error};

use crate::{RLookupKey, row::RLookupRow};

/// Lexicographically compares two rows by the given sort keys.
///
/// Bit `i` of `ascend_map` set means key `i` is ASCending; clear means DESCending.
/// A row missing a key value always compares as "less" against a row that has a value,
/// regardless of sort direction — so rows with values are always output before rows without.
///
/// When `qerr` is `None`, a number compared against a string that cannot be parsed as a
/// number is formatted as a string and compared byte-wise (number-to-string fallback).
/// When `qerr` is `Some`, the error is recorded into it instead and the key is treated as
/// equal, letting the next sort key decide. This mirrors the semantics of
/// [`compare_with_query_error_to_int`].
///
/// The caller is responsible for any docid tiebreak after this function returns.
///
/// [`compare_with_query_error_to_int`]: value::comparison::compare_with_query_error_to_int
pub fn cmp_rows_by_fields(
    row1: &RLookupRow<'_>,
    row2: &RLookupRow<'_>,
    keys: &[&RLookupKey],
    ascend_map: u64,
    mut qerr: Option<&mut QueryError>,
) -> Ordering {
    // When there is no `qerr` to record an error into, fall back to formatting
    // the number as a string for comparison instead of returning an error.
    let use_num_to_str_fallback = qerr.is_none();

    for (i, key) in keys.iter().enumerate() {
        let v1 = row1.get(key);
        let v2 = row2.get(key);
        let ascending = (ascend_map & (1u64 << i)) != 0;

        match (v1, v2) {
            (Some(a), Some(b)) => {
                let ord = match compare(a, b, use_num_to_str_fallback) {
                    Ok(ord) => ord,
                    Err(e) => map_compare_error(e, qerr.as_deref_mut()),
                };
                if ord == Ordering::Equal {
                    continue;
                }
                return if ascending { ord.reverse() } else { ord };
            }
            // A row missing a value always ranks as "worst" (last in output), regardless of
            // ASC/DESC direction. Do NOT apply the ascending reversal here.
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
        };
    }
    Ordering::Equal
}
