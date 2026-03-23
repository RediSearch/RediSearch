/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{FilterMaskReader, IndexReader, RSIndexResult};
use pretty_assertions::assert_eq;

#[test]
fn reading_filter_based_on_field_mask() {
    // Make an iterator with three records having different field masks. The second record will be
    // filtered out based on the field mask.
    let iter = vec![
        RSIndexResult::build_virt()
            .doc_id(10)
            .field_mask(0b0001)
            .build(),
        RSIndexResult::build_virt()
            .doc_id(11)
            .field_mask(0b0010)
            .build(),
        RSIndexResult::build_virt()
            .doc_id(12)
            .field_mask(0b0100)
            .build(),
    ];

    let mut reader = FilterMaskReader::new(0b0101 as _, iter.into_iter());
    let mut result = RSIndexResult::build_virt().build();

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(
        result,
        RSIndexResult::build_virt()
            .doc_id(10)
            .field_mask(0b0001)
            .build()
    );

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(
        result,
        RSIndexResult::build_virt()
            .doc_id(12)
            .field_mask(0b0100)
            .build()
    );
}
