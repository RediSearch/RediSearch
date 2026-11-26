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
fn only_types_nothing_casts_on_may_be_impersonated() {
    let impersonable: Vec<_> = (0..IteratorType::Max as u32)
        .map(|value| IteratorType::try_from(value).unwrap())
        .filter(|type_| type_.may_be_impersonated())
        .collect();
    assert_eq!(
        impersonable,
        [
            IteratorType::InvIdxNumeric,
            IteratorType::InvIdxTerm,
            IteratorType::InvIdxMissing,
            IteratorType::InvIdxTag,
            IteratorType::Union,
            IteratorType::Empty,
            IteratorType::Mock,
        ]
    );
}
