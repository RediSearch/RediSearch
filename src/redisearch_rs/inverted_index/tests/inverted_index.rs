/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RS_FIELDMASK_ALL;
use inverted_index::{RSAggregateResult, RSIndexResult, RSResultType, RSResultTypeMask};

mod c_mocks;

#[test]
fn pushing_to_aggregate_result() {
    let mut agg = RSAggregateResult::with_capacity(2);

    assert_eq!(agg.type_mask(), RSResultTypeMask::empty());

    let num_first = RSIndexResult::numeric(10.0).doc_id(2);
    agg.push(&num_first);

    assert_eq!(
        agg.type_mask(),
        RSResultType::Numeric,
        "type mask should be ORed"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    let num_second = RSIndexResult::numeric(100.0).doc_id(3);
    agg.push(&num_second);

    assert_eq!(agg.type_mask(), RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(100.0).doc_id(3)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    let virt_first = RSIndexResult::virt().doc_id(4);
    agg.push(&virt_first);

    assert_eq!(
        agg.type_mask(),
        RSResultType::Numeric | RSResultType::Virtual,
        "types should be combined"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(100.0).doc_id(3)));
    assert_eq!(agg.get(2), Some(&RSIndexResult::virt().doc_id(4)));
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}

#[test]
fn pushing_to_index_result() {
    let mut ir = RSIndexResult::union(1).doc_id(1).weight(1.0);

    assert_eq!(ir.doc_id, 1);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 1);
    assert_eq!(ir.field_mask, 0);

    let result_virt = RSIndexResult::virt().doc_id(2).frequency(3).field_mask(4);
    ir.push(&result_virt);
    assert_eq!(ir.doc_id, 2, "should inherit doc id of the child");
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 4, "frequency should accumulate");
    assert_eq!(ir.field_mask, 4, "field mask should be ORed");
    assert_eq!(
        ir.get(0),
        Some(&RSIndexResult::virt().doc_id(2).frequency(3).field_mask(4))
    );

    let result_with_frequency = RSIndexResult::numeric(5.0).doc_id(2).frequency(7);

    ir.push(&result_with_frequency);
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 11, "frequency should accumulate");
    assert_eq!(ir.field_mask, RS_FIELDMASK_ALL);
}
