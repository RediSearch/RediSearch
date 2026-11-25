/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{CompoundIndex, MAX_LEVELS, SchemaError, TagSet};

use crate::util::{Index, tag_level};

#[test]
fn an_index_needs_between_one_and_max_levels() {
    assert_eq!(Index::new(vec![]).err(), Some(SchemaError::NoLevels));
    let too_many = (0..=MAX_LEVELS).map(|_| tag_level()).collect();
    assert_eq!(
        CompoundIndex::<Vec<u32>>::new(too_many).err(),
        Some(SchemaError::TooManyLevels {
            max: MAX_LEVELS,
            got: MAX_LEVELS + 1
        })
    );
    let most = (0..MAX_LEVELS).map(|_| tag_level()).collect();
    assert!(Index::new(most).is_ok());
}

#[test]
fn a_tag_set_is_sorted_and_counts_a_repeated_value_once() {
    let set = TagSet::new([&b"red"[..], b"blue", b"red", b""]);
    assert_eq!(set.values(), &[&b""[..], b"blue", b"red"]);
    assert!(set.contains(b"blue"));
    assert!(!set.contains(b"green"));
}
