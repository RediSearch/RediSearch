/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cmp::Ordering;

use value::comparison::{CompareError, compare};

use crate::{RLookupKey, row::RLookupRow};

/// Lexicographically compares two rows by the given sort keys.
///
/// Bit `i` of `ascend_map` set means key `i` is ASCending; clear means DESCending.
/// A row missing a key value always compares as "less" against a row that has a value,
/// regardless of sort direction — so rows with values are always output before rows without.
///
/// Returns the decisive [`Ordering`] and the first [`CompareError`] encountered, if any.
/// Errors do not stop the loop — the errored key is treated as equal and the next key
/// is tried, matching the original C behavior. The caller is responsible for surfacing
/// any error (e.g. writing to a [`QueryError`]) and for any docid tiebreak.
///
/// [`QueryError`]: query_error::QueryError
pub fn cmp_rows_by_fields(
    row1: &RLookupRow<'_>,
    row2: &RLookupRow<'_>,
    keys: &[&RLookupKey],
    ascend_map: u64,
) -> (Ordering, Option<CompareError>) {
    let mut first_err: Option<CompareError> = None;

    for (i, key) in keys.iter().enumerate() {
        let v1 = row1.get(key);
        let v2 = row2.get(key);
        let ascending = (ascend_map & (1u64 << i)) != 0;

        match (v1, v2) {
            (Some(a), Some(b)) => {
                let ord = match compare(a, b, false) {
                    Ok(Ordering::Equal) => continue,
                    Ok(ord) => ord,
                    Err(CompareError::IncompatibleAgainstString(ord)) => ord,
                    Err(e @ CompareError::NoNumberToStringFallback) => {
                        first_err.get_or_insert(e);
                        continue;
                    }
                    Err(
                        CompareError::NaNFloat
                        | CompareError::MapComparison
                        | CompareError::IncompatibleTypes,
                    ) => continue,
                };
                return (if ascending { ord.reverse() } else { ord }, first_err);
            }
            // A row missing a value always ranks as "worst" (last in output), regardless of
            // ASC/DESC direction. Do NOT apply the ascending reversal here.
            (Some(_), None) => return (Ordering::Greater, first_err),
            (None, Some(_)) => return (Ordering::Less, first_err),
            (None, None) => continue,
        };
    }
    (Ordering::Equal, first_err)
}
