/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr;

use crate::{FilterNumericReader, IndexReader, NumericFilter, RSIndexResult};
use pretty_assertions::assert_eq;

#[test]
fn reading_filter_based_on_numeric_filter() {
    // Make an iterator with three records having different numeric values. The second record will be
    // filtered out based on the numeric filter.
    let iter = vec![
        RSIndexResult::numeric(5.0).doc_id(10),
        RSIndexResult::numeric(25.0).doc_id(11),
        RSIndexResult::numeric(15.0).doc_id(12),
    ];

    let filter = NumericFilter {
        min: 0.0,
        max: 15.0,
        min_inclusive: true,
        max_inclusive: true,
        field_spec: ptr::null(),
        geo_filter: ptr::null(),
        ascending: true,
        limit: 10,
        offset: 0,
    };

    let mut reader = FilterNumericReader::new(&filter, iter.into_iter());
    let mut result = RSIndexResult::numeric(0.0);

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result, RSIndexResult::numeric(5.0).doc_id(10));

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result, RSIndexResult::numeric(15.0).doc_id(12));
}
