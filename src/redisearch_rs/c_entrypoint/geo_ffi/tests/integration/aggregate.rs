/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo_ffi::{GeoDistance_Evaluate, encodeGeo};
use query_error::{QueryError, QueryErrorCode};
use value::{SharedValue, Value};
use value_ffi::RSValue;

fn evaluate(values: &[&Value], error: &mut QueryError) -> f64 {
    let pointers: Vec<*mut RSValue> = values
        .iter()
        .map(|value| std::ptr::from_ref(*value).cast_mut().cast())
        .collect();
    // SAFETY: each pointer borrows a live Value for this call; arity is
    // constrained by the test cases, and error is initialized and writable.
    unsafe { GeoDistance_Evaluate(pointers.as_ptr(), pointers.len(), error.as_opaque_mut_ptr()) }
}

#[test]
fn numeric_and_nonconvertible_value_views() {
    let score = Value::Number(encodeGeo(0.0, 0.0));
    let zero = Value::Number(0.0);
    let mut error = QueryError::default();
    assert_eq!(evaluate(&[&score, &score], &mut error), 0.0);
    assert_eq!(evaluate(&[&zero, &zero, &zero, &zero], &mut error), 0.0);
    assert!(evaluate(&[&Value::Null, &score], &mut error).is_nan());
    assert!(evaluate(&[&Value::Undefined, &zero, &zero, &zero], &mut error).is_nan());
    assert!(error.is_ok());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "SharedValue strings and references use extern Redis allocator statics"
)]
fn string_reference_and_trio_views_preserve_error_mapping() {
    let point = SharedValue::new_string(b"0,0".to_vec());
    let reference = Value::Ref(point.clone());
    let zero = Value::Number(0.0);
    let trio = SharedValue::new_trio(
        SharedValue::new_num(0.0),
        SharedValue::new_num(1.0),
        SharedValue::new_num(2.0),
    );
    let mut error = QueryError::default();
    assert_eq!(evaluate(&[&reference, &point], &mut error), 0.0);
    assert_eq!(evaluate(&[&zero, &zero, &reference], &mut error), 0.0);
    assert_eq!(evaluate(&[&reference, &zero, &zero], &mut error), 0.0);
    assert_eq!(evaluate(&[&trio, &zero, &reference], &mut error), 0.0);
    assert!(evaluate(&[&trio, &point], &mut error).is_nan());
    assert!(error.is_ok());

    for bytes in [b"bad".as_slice(), b"\xff"] {
        let invalid = SharedValue::new_string(bytes.to_vec());
        assert!(evaluate(&[&invalid, &point], &mut error).is_nan());
        assert_eq!(error.code(), QueryErrorCode::ParseArgs);
        assert!(error.private_message().is_some());
        error.clear();
    }
}
