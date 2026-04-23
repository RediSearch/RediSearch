/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::Value;
use crate::util::{num_to_str, str_to_float};
use query_error::{QueryError, QueryErrorCode};
use std::cmp::Ordering;
use std::ops::Deref;

/// Errors that can occur when comparing two [`Value`]s.
#[derive(Debug, PartialEq, Eq)]
pub enum CompareError {
    /// One or both of the compared numbers were NaN, which has no defined ordering.
    NaNFloat,
    /// A number-to-string comparison was attempted without the string-fallback enabled.
    NoNumberToStringFallback,
    /// Map values do not support comparison.
    MapComparison,
    /// Incompatible type compared against string. The contained `Ordering` is provided for
    /// compatibility to the C implementation.
    IncompatibleAgainstString(Ordering),
    /// The two value variants have no defined comparison (e.g. array vs. map).
    IncompatibleTypes,
}

/// Compare two [`RsValue`]s, folding non-fatal [`CompareError`]s into
/// [`Ordering::Equal`] (matching the C implementation).
///
/// Passing `qerr` disables the num-to-string fallback: a failed conversion is
/// recorded on the [`QueryError`] and the pair is treated as equal.
#[inline]
pub fn compare_with_query_error(v1: &Value, v2: &Value, qerr: Option<&mut QueryError>) -> Ordering {
    // This is a performance optimization to check for string comparisons early
    // as that is used most often in searches and aggregates.
    if let (Value::String(s1), Value::String(s2)) = (v1, v2) {
        return s1.as_bytes().cmp(s2.as_bytes());
    }

    match compare(v1, v2, qerr.is_none()) {
        Ok(ord) => ord,
        Err(CompareError::NaNFloat)
        | Err(CompareError::MapComparison)
        | Err(CompareError::IncompatibleTypes) => Ordering::Equal,
        Err(CompareError::IncompatibleAgainstString(ord)) => ord,
        Err(CompareError::NoNumberToStringFallback) => {
            // SAFETY: `qerr` is `Some` because `num_to_str_cmp_fallback` was
            // `false` (set from `qerr.is_none()`).
            let query_error = qerr.unwrap();
            let message = c"Error converting string".to_owned();
            query_error.set_code_and_message(QueryErrorCode::NumericValueInvalid, Some(message));
            Ordering::Equal
        }
    }
}

#[inline]
pub fn compare_on_equality_only(v1: &Value, v2: &Value) -> bool {
    match compare(v1, v2, false) {
        Ok(Ordering::Less) => false,
        Ok(Ordering::Equal) => true,
        Ok(Ordering::Greater) => false,
        Err(CompareError::NaNFloat) => true,
        Err(CompareError::MapComparison) => true,
        Err(CompareError::IncompatibleAgainstString(Ordering::Equal)) => true,
        Err(CompareError::IncompatibleAgainstString(_)) => false,
        Err(CompareError::IncompatibleTypes) => true,
        Err(CompareError::NoNumberToStringFallback) => false,
    }
}

/// Lexicographically compare two sequences of sort-key values under the classic
/// sort-by-fields policy.
///
/// `pairs` yields the `i`-th sort key as `(v1, v2)` from the two sides being compared.
/// Bit `i` of `ascend_map` (LSB-first) selects the direction: set = ASC, clear = DESC.
///
/// A `None` value always ranks "worst" regardless of direction; both-`None` is treated
/// as equal and the next pair decides. Per-pair comparison — including the num-to-string
/// fallback and `qerr` recording — is delegated to [`compare_with_query_error`].
///
/// Callers are responsible for any docid tiebreak when this function returns
/// [`Ordering::Equal`].
#[inline]
pub fn cmp_fields<'a>(
    pairs: impl IntoIterator<Item = (Option<&'a RsValue>, Option<&'a RsValue>)>,
    ascend_map: u64,
    mut qerr: Option<&mut QueryError>,
) -> Ordering {
    for (i, (v1, v2)) in pairs.into_iter().enumerate() {
        let ascending = (ascend_map & (1u64 << i)) != 0;

        match (v1, v2) {
            // Delegates to `compare_with_query_error` so we inherit its
            // `(String, String)` fast path and the num-to-string fallback policy
            // (kept in sync by construction, not by duplication).
            (Some(a), Some(b)) => match compare_with_query_error(a, b, qerr.as_deref_mut()) {
                Ordering::Equal => continue,
                ord => return if ascending { ord.reverse() } else { ord },
            },
            // A row missing a value always ranks as "worst" (last in output), regardless of
            // ASC/DESC direction. Do NOT apply the ascending reversal here.
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
        }
    }
    Ordering::Equal
}

/// Lexicographically compare two sequences of sort-key values under the classic
/// sort-by-fields policy.
///
/// `pairs` yields the `i`-th sort key as `(v1, v2)` from the two sides being compared.
/// Bit `i` of `ascend_map` (LSB-first) selects the direction: set = ASC, clear = DESC.
///
/// A `None` value always ranks "worst" regardless of direction; both-`None` is treated
/// as equal and the next pair decides. Per-pair comparison — including the num-to-string
/// fallback and `qerr` recording — is delegated to [`compare_with_query_error`].
///
/// Callers are responsible for any docid tiebreak when this function returns
/// [`Ordering::Equal`].
#[inline]
pub fn cmp_fields<'a>(
    pairs: impl IntoIterator<Item = (Option<&'a RsValue>, Option<&'a RsValue>)>,
    ascend_map: u64,
    mut qerr: Option<&mut QueryError>,
) -> Ordering {
    for (i, (v1, v2)) in pairs.into_iter().enumerate() {
        let ascending = (ascend_map & (1u64 << i)) != 0;

        match (v1, v2) {
            // Delegates to `compare_with_query_error` so we inherit its
            // `(String, String)` fast path and the num-to-string fallback policy
            // (kept in sync by construction, not by duplication).
            (Some(a), Some(b)) => match compare_with_query_error(a, b, qerr.as_deref_mut()) {
                Ordering::Equal => continue,
                ord => return if ascending { ord.reverse() } else { ord },
            },
            // A row missing a value always ranks as "worst" (last in output), regardless of
            // ASC/DESC direction. Do NOT apply the ascending reversal here.
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
        }
    }
    Ordering::Equal
}

/// Compare two [`Value`]s, returning their [`Ordering`].
///
/// When a number is compared to a string, the string is first parsed as a
/// number. If parsing fails, behaviour depends on `num_to_str_cmp_fallback`:
/// - `true` - the number is formatted as a string and a byte-wise
///   comparison is performed.
/// - `false` - returns [`CompareError::NoNumberToStringFallback`].
///
/// [`Value::Trio`] values are compared by their left element.
/// [`Value::Array`] values are compared lexicographically.
/// [`Value::Map`] values cannot be compared and yield [`CompareError::MapComparison`].
pub fn compare(
    v1: &Value,
    v2: &Value,
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    match (v1, v2) {
        (Value::Ref(r1), Value::Ref(r2)) => compare(r1, r2, num_to_str_cmp_fallback),
        (Value::Ref(r1), _) => compare(r1, v2, num_to_str_cmp_fallback),
        (_, Value::Ref(r2)) => compare(v1, r2, num_to_str_cmp_fallback),
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Less),
        (_, Value::Null) => Ok(Ordering::Greater),
        (Value::Number(n1), Value::Number(n2)) => n1.partial_cmp(n2).ok_or(CompareError::NaNFloat),
        (Value::String(s1), Value::String(s2)) => Ok(s1.as_bytes().cmp(s2.as_bytes())),
        (Value::RedisString(rs1), Value::RedisString(rs2)) => {
            Ok(rs1.as_bytes().cmp(rs2.as_bytes()))
        }
        (Value::Trio(t1), Value::Trio(t2)) => {
            compare(t1.left(), t2.left(), num_to_str_cmp_fallback)
        }
        (Value::Array(a1), Value::Array(a2)) => {
            for (i1, i2) in a1.iter().zip(a2.deref()) {
                let cmp = compare(i1, i2, num_to_str_cmp_fallback)?;
                if cmp != Ordering::Equal {
                    return Ok(cmp);
                }
            }
            Ok(a1.len().cmp(&a2.len()))
        }
        (Value::Map(_), Value::Map(_)) => Err(CompareError::MapComparison),
        (Value::Number(n1), Value::String(s2)) => {
            compare_number_to_string(*n1, s2.as_bytes(), num_to_str_cmp_fallback)
        }
        (Value::Number(n1), Value::RedisString(s2)) => {
            compare_number_to_string(*n1, s2.as_bytes(), num_to_str_cmp_fallback)
        }
        (Value::String(s1), Value::Number(n2)) => {
            compare_number_to_string(*n2, s1.as_bytes(), num_to_str_cmp_fallback)
                .map(Ordering::reverse)
        }
        (Value::RedisString(s1), Value::Number(n2)) => {
            compare_number_to_string(*n2, s1.as_bytes(), num_to_str_cmp_fallback)
                .map(Ordering::reverse)
        }
        (Value::String(s1), Value::RedisString(rs2)) => Ok(s1.as_bytes().cmp(rs2.as_bytes())),
        (Value::RedisString(rs1), Value::String(s2)) => Ok(rs1.as_bytes().cmp(s2.as_bytes())),
        (Value::String(s1), _) => Err(CompareError::IncompatibleAgainstString(
            s1.as_bytes().cmp(b""),
        )),
        (_, Value::String(s2)) => Err(CompareError::IncompatibleAgainstString(
            b""[..].cmp(s2.as_bytes()),
        )),
        (Value::RedisString(rs1), _) => Err(CompareError::IncompatibleAgainstString(
            rs1.as_bytes().cmp(b""),
        )),
        (_, Value::RedisString(rs2)) => Err(CompareError::IncompatibleAgainstString(
            b""[..].cmp(rs2.as_bytes()),
        )),
        _ => Err(CompareError::IncompatibleTypes),
    }
}

/// Compare a number to a byte-string.
///
/// Tries to parse `slice` as a float first. If that fails and
/// `num_to_str_cmp_fallback` is enabled, the number is formatted into a
/// stack buffer via [`num_to_str`] and compared byte-wise.
fn compare_number_to_string(
    number: f64,
    slice: &[u8],
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    if let Some(other_number) = str_to_float(slice) {
        number
            .partial_cmp(&other_number)
            .ok_or(CompareError::NaNFloat)
    } else if num_to_str_cmp_fallback {
        let mut buf = [0; 32];
        let n = num_to_str(number, &mut buf);
        Ok(buf[0..n].cmp(slice))
    } else {
        Err(CompareError::NoNumberToStringFallback)
    }
}
