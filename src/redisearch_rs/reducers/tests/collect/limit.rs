/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use reducers::collect::{RemoteCollectCtx, RemoteCollectReducer};
use rlookup::RLookupKey;
use value::SharedValue;

use super::helpers::{extract_num_field, make_key, make_row, num_row, run_collect};
use crate::common::SORT_ASC;

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

#[test]
fn remote_internal_mode_does_not_apply_limit_offset_locally() {
    // Regression canary for the contract documented on
    // `RemoteCollectReducer::is_internal`: if a future change rewrites
    // the shard wire's LIMIT to `(0, offset+count)` without flipping that
    // gate, the offset gets dropped twice and this test fails.
    let v = make_key(c"v", 0);
    let s = make_key(c"s", 1);
    let field_keys: Box<[&RLookupKey]> = vec![&v].into_boxed_slice();
    let sort_keys: Box<[&RLookupKey]> = vec![&s].into_boxed_slice();
    let run_with_is_internal = |is_internal: bool| -> Vec<f64> {
        let r = RemoteCollectReducer::new(
            field_keys.clone(),
            None,
            sort_keys.clone(),
            SORT_ASC,
            Some((5, 3)),
            is_internal,
            None,
        );
        let mut ctx = RemoteCollectCtx::new(&r);
        for i in 0..10 {
            let row = make_row(
                &field_keys,
                &sort_keys,
                &[SharedValue::new_num(i as f64)],
                &[SharedValue::new_num(i as f64)],
            );
            ctx.add(&r, &row);
        }
        let out = ctx.finalize(&r);
        extract_num_field(&out, b"v")
    };

    let standalone = run_with_is_internal(false);
    let internal = run_with_is_internal(true);

    assert_eq!(
        standalone,
        vec![5.0, 6.0, 7.0],
        "standalone shard must apply skip(5).take(3) locally"
    );
    assert_eq!(
        internal,
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        "internal shard must NOT apply skip(offset) locally"
    );
}
