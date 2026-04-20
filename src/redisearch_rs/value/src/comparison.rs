/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RsValue;
use crate::util::{num_to_str, str_to_float};
use query_error::{QueryError, QueryErrorCode};
use std::cmp::Ordering;
use std::ops::Deref;

/// Errors that can occur when comparing two [`RsValue`]s.
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

#[inline]
pub fn compare_with_query_error_to_int(
    v1: &RsValue,
    v2: &RsValue,
    qerr: Option<&mut QueryError>,
) -> i32 {
    // This is a performance optimization to check for string comparisons early
    // as that is used most often in searches and aggregates.
    if let (RsValue::String(s1), RsValue::String(s2)) = (v1, v2) {
        return match s1.as_bytes().cmp(s2.as_bytes()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        };
    }

    match compare(v1, v2, qerr.is_none()) {
        Ok(Ordering::Less) => -1,
        Ok(Ordering::Equal) => 0,
        Ok(Ordering::Greater) => 1,
        Err(CompareError::NaNFloat) => 0,
        Err(CompareError::MapComparison) => 0,
        Err(CompareError::IncompatibleAgainstString(Ordering::Less)) => -1,
        Err(CompareError::IncompatibleAgainstString(Ordering::Equal)) => 0,
        Err(CompareError::IncompatibleAgainstString(Ordering::Greater)) => 1,
        Err(CompareError::IncompatibleTypes) => 0,
        Err(CompareError::NoNumberToStringFallback) => {
            // SAFETY: `qerr` is Some because `num_to_str_cmp_fallback` was
            // `false` (set from `qerr.is_none()`).
            let query_error = qerr.unwrap();
            let message = c"Error converting string".to_owned();
            query_error.set_code_and_message(QueryErrorCode::NumericValueInvalid, Some(message));
            0
        }
    }
}

#[inline]
pub fn compare_on_equality_only(v1: &RsValue, v2: &RsValue) -> bool {
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

/// Lexicographically compare two rows of sort-key values under the classic sort-by-fields policy.
///
/// Each yielded pair `(v1, v2)` represents the `i`-th sort key's value in the two rows being
/// compared. Bit `i` of `ascend_map` (LSB-first) controls the direction for pair `i`: set means
/// ASCending, clear means DESCending.
///
/// # Missing values
///
/// A `None` value always compares as "worst" — a row with `Some` ranks before a row with `None`
/// regardless of ASC/DESC direction. When both sides are `None`, the pair is treated as equal and
/// the next pair decides.
///
/// # Errors and the num-to-string fallback
///
/// When `qerr` is `None`, comparing a number against a string that cannot be parsed as a number
/// falls back to formatting the number as a string and comparing byte-wise. When `qerr` is
/// `Some`, that case records a [`QueryErrorCode::NumericValueInvalid`] into the error and treats
/// the pair as equal, letting the next pair decide. Other [`CompareError`] variants use their
/// defined fallback ordering.
///
/// Callers are responsible for any docid tiebreak when this function returns `Ordering::Equal`.
///
/// [`QueryErrorCode::NumericValueInvalid`]: query_error::QueryErrorCode::NumericValueInvalid
#[inline]
pub fn cmp_fields<'a>(
    pairs: impl IntoIterator<Item = (Option<&'a RsValue>, Option<&'a RsValue>)>,
    ascend_map: u64,
    mut qerr: Option<&mut QueryError>,
) -> Ordering {
    for (i, (v1, v2)) in pairs.into_iter().enumerate() {
        let ascending = (ascend_map & (1u64 << i)) != 0;

        match (v1, v2) {
            (Some(a), Some(b)) => {
                // Delegates to `compare_with_query_error_to_int` so we inherit its
                // `(String, String)` fast path and the num-to-string fallback policy
                // (kept in sync by construction, not by duplication).
                let rc = compare_with_query_error_to_int(a, b, qerr.as_deref_mut());
                if rc == 0 {
                    continue;
                }
                let ord = if rc < 0 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                return if ascending { ord.reverse() } else { ord };
            }
            // A row missing a value always ranks as "worst" (last in output), regardless of
            // ASC/DESC direction. Do NOT apply the ascending reversal here.
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
        }
    }
    Ordering::Equal
}

/// Compare two [`RsValue`]s, returning their [`Ordering`].
///
/// When a number is compared to a string, the string is first parsed as a
/// number. If parsing fails, behaviour depends on `num_to_str_cmp_fallback`:
/// - `true` - the number is formatted as a string and a byte-wise
///   comparison is performed.
/// - `false` - returns [`CompareError::NoNumberToStringFallback`].
///
/// [`RsValue::Trio`] values are compared by their left element.
/// [`RsValue::Array`] values are compared lexicographically.
/// [`RsValue::Map`] values cannot be compared and yield [`CompareError::MapComparison`].
pub fn compare(
    v1: &RsValue,
    v2: &RsValue,
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    match (v1, v2) {
        (RsValue::Ref(r1), RsValue::Ref(r2)) => compare(r1, r2, num_to_str_cmp_fallback),
        (RsValue::Ref(r1), _) => compare(r1, v2, num_to_str_cmp_fallback),
        (_, RsValue::Ref(r2)) => compare(v1, r2, num_to_str_cmp_fallback),
        (RsValue::Null, RsValue::Null) => Ok(Ordering::Equal),
        (RsValue::Null, _) => Ok(Ordering::Less),
        (_, RsValue::Null) => Ok(Ordering::Greater),
        (RsValue::Number(n1), RsValue::Number(n2)) => {
            n1.partial_cmp(n2).ok_or(CompareError::NaNFloat)
        }
        (RsValue::String(s1), RsValue::String(s2)) => Ok(s1.as_bytes().cmp(s2.as_bytes())),
        (RsValue::RedisString(rs1), RsValue::RedisString(rs2)) => {
            Ok(rs1.as_bytes().cmp(rs2.as_bytes()))
        }
        (RsValue::Trio(t1), RsValue::Trio(t2)) => {
            compare(t1.left(), t2.left(), num_to_str_cmp_fallback)
        }
        (RsValue::Array(a1), RsValue::Array(a2)) => {
            for (i1, i2) in a1.iter().zip(a2.deref()) {
                let cmp = compare(i1, i2, num_to_str_cmp_fallback)?;
                if cmp != Ordering::Equal {
                    return Ok(cmp);
                }
            }
            Ok(a1.len().cmp(&a2.len()))
        }
        (RsValue::Map(_), RsValue::Map(_)) => Err(CompareError::MapComparison),
        (RsValue::Number(n1), RsValue::String(s2)) => {
            compare_number_to_string(*n1, s2.as_bytes(), num_to_str_cmp_fallback)
        }
        (RsValue::Number(n1), RsValue::RedisString(s2)) => {
            compare_number_to_string(*n1, s2.as_bytes(), num_to_str_cmp_fallback)
        }
        (RsValue::String(s1), RsValue::Number(n2)) => {
            compare_number_to_string(*n2, s1.as_bytes(), num_to_str_cmp_fallback)
                .map(Ordering::reverse)
        }
        (RsValue::RedisString(s1), RsValue::Number(n2)) => {
            compare_number_to_string(*n2, s1.as_bytes(), num_to_str_cmp_fallback)
                .map(Ordering::reverse)
        }
        (RsValue::String(s1), RsValue::RedisString(rs2)) => Ok(s1.as_bytes().cmp(rs2.as_bytes())),
        (RsValue::RedisString(rs1), RsValue::String(s2)) => Ok(rs1.as_bytes().cmp(s2.as_bytes())),
        (RsValue::String(s1), _) => Err(CompareError::IncompatibleAgainstString(
            s1.as_bytes().cmp(b""),
        )),
        (_, RsValue::String(s2)) => Err(CompareError::IncompatibleAgainstString(
            b""[..].cmp(s2.as_bytes()),
        )),
        (RsValue::RedisString(rs1), _) => Err(CompareError::IncompatibleAgainstString(
            rs1.as_bytes().cmp(b""),
        )),
        (_, RsValue::RedisString(rs2)) => Err(CompareError::IncompatibleAgainstString(
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
