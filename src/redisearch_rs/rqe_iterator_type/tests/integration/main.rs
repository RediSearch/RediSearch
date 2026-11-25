/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterator_type::IteratorType;

#[test]
fn every_discriminant_round_trips_and_nothing_past_max_converts() {
    let max = IteratorType::Max as u32;
    for value in 0..=max {
        let type_ = IteratorType::try_from(value)
            .unwrap_or_else(|v| panic!("discriminant {v} is below `Max` but does not convert"));
        assert_eq!(type_ as u32, value);
    }
    for value in [max + 1, u32::MAX] {
        assert_eq!(IteratorType::try_from(value), Err(value));
    }
}

#[test]
fn index_load_is_a_decorator_named_index_load() {
    assert!(!IteratorType::IndexLoad.is_leaf());
    assert_eq!(IteratorType::IndexLoad.as_str(), "INDEX_LOAD");
    assert_eq!(IteratorType::IndexLoad.to_string(), "INDEX_LOAD");
}
