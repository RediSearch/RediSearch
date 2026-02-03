use crate::util::expect_value;
use query_error::{QueryError, QueryErrorCode};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::{cmp::Ordering, ffi::c_int};
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Cmp(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    let v1 = unsafe { expect_value(v1) };
    let v2 = unsafe { expect_value(v2) };

    if let (RsValue::String(s1), RsValue::String(s2)) = (v1, v2) {
        match s1.as_bytes().cmp(s2.as_bytes()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    } else {
        match compare(v1, v2, status.is_null()) {
            Ok(Ordering::Less) => -1,
            Ok(Ordering::Equal) => 0,
            Ok(Ordering::Greater) => 1,
            Err(CompareError::NaNNumber) => 0,
            Err(CompareError::MapComparison) => 0,
            Err(CompareError::IncompatibleTypes) => 0,
            Err(CompareError::NoNumberToStringFallback) => {
                // SAFETY: Number conversion failed and status was provided.
                let query_error = unsafe { status.as_mut().unwrap() };
                let message = c"Error converting string".to_owned();
                query_error.set_code_and_message(QueryErrorCode::NotNumeric, Some(message));
                // even though we're returning 'equal', the query error code is likely checked for a possible error.
                0
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Equal(
    v1: *const RsValue,
    v2: *const RsValue,
    status: *mut QueryError,
) -> c_int {
    let v1 = unsafe { expect_value(v1) };
    let v2 = unsafe { expect_value(v2) };

    // For equality, don't fall back to string comparison - conversion failure means not equal
    match compare(v1, v2, false) {
        Ok(Ordering::Less) => 0,
        Ok(Ordering::Equal) => 1,
        Ok(Ordering::Greater) => 0,
        Err(CompareError::NaNNumber) => 1,
        Err(CompareError::MapComparison) => 1,
        Err(CompareError::IncompatibleTypes) => 1,
        Err(CompareError::NoNumberToStringFallback) => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_BoolTest(value: *const RsValue) -> c_int {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.fully_dereferenced_value();

    let result = match value {
        RsValue::Number(num) => *num != 0.0,
        RsValue::Array(arr) => arr.len() != 0,
        _ if crate::util::rsvalue_any_str(value) => {
            crate::util::rsvalue_as_byte_slice2(value).unwrap().len() != 0
        }
        _ => false,
    };

    result as c_int
}

#[derive(Debug)]
enum CompareError {
    NaNNumber,
    NoNumberToStringFallback,
    MapComparison,
    IncompatibleTypes,
}

/// Compare two values.
/// If `num_to_str_cmp_fallback` is true, falls back to string comparison when number conversion fails.
/// If `num_to_str_cmp_fallback` is false, returns CompareError::NoNumberToStringFallback when number conversion fails.
fn compare(
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
        (RsValue::Number(n1), right) if crate::util::rsvalue_any_str(right) => {
            compare_number_to_string(*n1, right, num_to_str_cmp_fallback)
        }
        (left, RsValue::Number(n2)) if crate::util::rsvalue_any_str(left) => {
            compare_number_to_string(*n2, left, num_to_str_cmp_fallback).map(Ordering::reverse)
        }
        (left, right)
            if crate::util::rsvalue_any_str(left) && crate::util::rsvalue_any_str(right) =>
        {
            let slice1 = crate::util::rsvalue_as_byte_slice2(left).unwrap();
            let slice2 = crate::util::rsvalue_as_byte_slice2(right).unwrap();
            Ok(slice1.cmp(slice2))
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
        (RsValue::Map(m1), RsValue::Map(m2)) => Err(CompareError::MapComparison),
        _ => Err(CompareError::IncompatibleTypes),
    }
}

fn compare_number_to_string(
    number: f64,
    string: &RsValue,
    num_to_str_cmp_fallback: bool,
) -> Result<Ordering, CompareError> {
    let slice = crate::util::rsvalue_as_byte_slice2(string).unwrap();
    // first try to convert the string to a number for comparison
    if let Some(other_number) = crate::util::rsvalue_str_to_float(slice) {
        number
            .partial_cmp(&other_number)
            .ok_or(CompareError::NaNNumber)
    // else only if num_to_str_cmp_fallback is enabled, convert the number to a string for comparison
    } else if num_to_str_cmp_fallback {
        Ok(crate::util::rsvalue_num_to_str(number)
            .as_bytes()
            .cmp(slice))
    } else {
        Err(CompareError::NoNumberToStringFallback)
    }
}
