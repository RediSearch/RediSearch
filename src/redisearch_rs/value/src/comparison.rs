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

#[derive(Debug)]
pub enum CompareError {
    NaNNumber,
    NoNumberToStringFallback,
    MapComparison,
    IncompatibleTypes,
}

/// Compare two values.
/// If `num_to_str_cmp_fallback` is true, falls back to string comparison when number conversion fails.
/// If `num_to_str_cmp_fallback` is false, returns CompareError::NoNumberToStringFallback when number conversion fails.
pub fn compare(
    v1: &RsValue,
    v2: &RsValue,
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    match (v1, v2) {
        (RsValue::Null, RsValue::Null) => Ok(Ordering::Equal),
        (RsValue::Null, _) => Ok(Ordering::Less),
        (_, RsValue::Null) => Ok(Ordering::Greater),
        (RsValue::Number(n1), RsValue::Number(n2)) => {
            n1.partial_cmp(n2).ok_or(CompareError::NaNNumber)
        }
        (RsValue::String(s1), RsValue::String(s2)) => Ok(s1.as_bytes().cmp(s2.as_bytes())),
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
        (RsValue::RedisString(rs1), RsValue::RedisString(rs2)) => {
            Ok(rs1.as_bytes().cmp(rs2.as_bytes()))
        }
        (RsValue::String(s1), RsValue::RedisString(rs2)) => Ok(s1.as_bytes().cmp(rs2.as_bytes())),
        (RsValue::RedisString(rs1), RsValue::String(s2)) => Ok(rs1.as_bytes().cmp(s2.as_bytes())),
        _ => Err(CompareError::IncompatibleTypes),
    }
}

fn compare_number_to_string(
    number: f64,
    slice: &[u8],
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    // first try to convert the slice to a number for comparison
    if let Some(other_number) = str_to_float(slice) {
        number
            .partial_cmp(&other_number)
            .ok_or(CompareError::NaNNumber)
    // else only if num_to_str_cmp_fallback is enabled, convert the number to a slice for comparison
    } else if num_to_str_cmp_fallback {
        let mut buf = [0; 32];
        let n = num_to_str(number, &mut buf);
        Ok(buf[0..n].cmp(slice))
    } else {
        Err(CompareError::NoNumberToStringFallback)
    }
}
