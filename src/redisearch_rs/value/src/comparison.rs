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
        (RsValue::Ref(r1), RsValue::Ref(r2)) => {
            compare(r1.value(), r2.value(), num_to_str_cmp_fallback)
        }
        (RsValue::Ref(r1), _) => compare(r1.value(), v2, num_to_str_cmp_fallback),
        (_, RsValue::Ref(r2)) => compare(v1, r2.value(), num_to_str_cmp_fallback),
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
        (RsValue::Trio(t1), RsValue::Trio(t2)) => compare(
            t1.left().value(),
            t2.left().value(),
            num_to_str_cmp_fallback,
        ),
        (RsValue::Array(a1), RsValue::Array(a2)) => {
            for (i1, i2) in a1.iter().zip(a2.deref()) {
                let cmp = compare(i1.value(), i2.value(), num_to_str_cmp_fallback)?;
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
