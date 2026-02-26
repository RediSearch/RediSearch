/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RSAggregateResult, RSResultData, RSResultKind, RSTermRecord};
use pretty_assertions::assert_eq;

#[test]
fn synced_discriminants() {
    let tests = [
        (
            RSResultData::Union(RSAggregateResult::borrowed_with_capacity(0)),
            RSResultKind::Union,
        ),
        (
            RSResultData::Intersection(RSAggregateResult::borrowed_with_capacity(0)),
            RSResultKind::Intersection,
        ),
        (
            RSResultData::Term(RSTermRecord::default()),
            RSResultKind::Term,
        ),
        (RSResultData::Virtual, RSResultKind::Virtual),
        (RSResultData::Numeric(0f64), RSResultKind::Numeric),
        (RSResultData::Metric(0f64), RSResultKind::Metric),
        (
            RSResultData::HybridMetric(RSAggregateResult::borrowed_with_capacity(0)),
            RSResultKind::HybridMetric,
        ),
    ];

    for (data, kind) in tests {
        assert_eq!(data.kind(), kind);

        // SAFETY: since `RSResultData` is a `repr(u8)` it will have a C `union` layout with a
        // `repr(C)` struct for each variant. Each of these structs has the discriminant as the
        // first field, which we can read here without offsetting the pointer.
        //
        // For more see https://doc.rust-lang.org/std/mem/fn.discriminant.html#accessing-the-numeric-value-of-the-discriminant
        let data_discriminant = unsafe { *<*const _>::from(&data).cast::<u8>() };
        let kind_discriminant = kind as u8;

        assert_eq!(data_discriminant, kind_discriminant, "for {kind:?}");
    }
}
