/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::{RSAggregateResult, RSIndexResult, RSResultType, RSResultTypeMask};

#[unsafe(no_mangle)]
pub extern "C" fn IndexResult_ConcatMetrics(
    _parent: *mut RSIndexResult,
    _child: *mut RSIndexResult,
) {
    // Do nothing since the code will call this
}

#[test]
fn pushing_to_aggregate_result() {
    let mut agg = RSAggregateResult::new(2);

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 0);
    assert_eq!(agg.type_mask, RSResultTypeMask::empty());

    agg.push(&mut RSIndexResult::numeric(2, 10.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 1);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push(&mut RSIndexResult::numeric(3, 100.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 2);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push(&mut RSIndexResult::virt(4, 2, 2.0));

    assert_eq!(agg.children_cap, 4);
    assert_eq!(agg.num_children, 3);
    assert_eq!(
        agg.type_mask,
        RSResultType::Numeric | RSResultType::Virtual,
        "types should be combined"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), Some(&RSIndexResult::virt(4, 2, 2.0)));
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}

#[test]
fn pushing_to_index_result() {
    let mut ir = RSIndexResult::union(1, 1, 1.0);

    assert_eq!(ir.doc_id, 1);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 0);
    assert_eq!(ir.field_mask, 0);

    ir.push(&mut RSIndexResult::virt(2, 4, 2.0));
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 0);
    assert_eq!(ir.field_mask, 4);
    assert_eq!(ir.get(0), Some(&RSIndexResult::virt(2, 4, 2.0)));

    let mut result_with_frequency = RSIndexResult::numeric(2, 5.0);
    result_with_frequency.freq = 7;

    ir.push(&mut result_with_frequency);
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 7);
    assert_eq!(ir.field_mask, 4);
}
