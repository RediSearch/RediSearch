/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use deleted_docs::{DeletedDocs, RoaringTreemap};

#[test]
fn a_new_set_is_empty_and_records_ids() {
    let mut docs = DeletedDocs::new();
    assert!(docs.is_empty());
    docs.record(5);
    docs.record(5);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs.snapshot(), [5].into_iter().collect());
}

/// Pruning takes exactly the collected IDs: ones a cycle could not collect and
/// ones recorded after its snapshot both stay.
#[test]
fn pruning_keeps_uncollected_and_newer_ids() {
    let mut docs = DeletedDocs::from_ids([1, 2, 3, 4]);
    let mut collected = docs.snapshot();
    docs.record(9);
    collected.remove(3);
    docs.prune(&collected);
    assert_eq!(docs.snapshot(), [3, 9].into_iter().collect());
}

#[test]
fn pruning_ids_the_set_never_held_is_harmless() {
    let mut docs = DeletedDocs::from_ids([1]);
    docs.prune(&[1, 2, 3].into_iter().collect::<RoaringTreemap>());
    assert!(docs.is_empty());
}
