/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::helpers::{extract_num_field, make_key, num_row, run_collect};

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
fn array_no_sortby_no_limit_preserves_insertion_order() {
    let v = make_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        None,
        vec![num_row(3.0), num_row(1.0), num_row(2.0)],
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![3.0, 1.0, 2.0]);
}

#[test]
fn array_no_sortby_with_limit_takes_first_k() {
    let v = make_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((0, 3)),
        (0..5).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn array_limit_offset_exceeds_len_yields_empty() {
    let v = make_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((10, 5)),
        (0..3).map(|i| num_row(i as f64)).collect(),
    );
    assert!(extract_num_field(&out, b"v").is_empty());
}

#[test]
fn array_limit_count_exceeds_remainder_no_padding() {
    let v = make_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((0, 10)),
        (0..3).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn array_limit_with_offset_skips_correctly() {
    let v = make_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((2, 10)),
        (0..5).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![2.0, 3.0, 4.0]);
}

#[test]
fn array_overflow_skips_projection_beyond_cap() {
    // End-to-end check that the cap holds; the closure-call count itself
    // is asserted by the storage-layer unit tests.
    let v = make_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((0, 3)),
        (0..7).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}
