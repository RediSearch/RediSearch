/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use deleted_docs::{Coverage, DeletedDocs, RoaringTreemap};

#[test]
fn a_new_set_has_no_snapshot_until_a_cycle_collected() {
    let mut docs = DeletedDocs::new();
    docs.record(5);
    assert_eq!(docs.coverage(), Coverage::Partial);
    assert_eq!(docs.snapshot(), None);

    docs.finish_cycle(&RoaringTreemap::new(), &RoaringTreemap::new());
    assert_eq!(docs.coverage(), Coverage::Complete);
    assert_eq!(docs.snapshot(), Some([5].into_iter().collect()));
}

#[test]
fn collected_ids_leave_and_retained_or_newer_ones_stay() {
    let mut docs = DeletedDocs::complete_from([1, 2, 3, 4]);
    let snapshot = docs.snapshot().unwrap();
    docs.record(9);
    let mut collected = snapshot.clone();
    collected.remove(3); // a cycle that could not collect doc 3's entries
    docs.finish_cycle(&snapshot, &collected);
    assert_eq!(docs.snapshot(), Some([3, 9].into_iter().collect()));
}

/// A cycle that found its dead IDs by sweeping the doc table, not in the set,
/// still owes the ones it could not collect to the next cycle: they are added.
#[test]
fn ids_a_table_sweep_found_but_the_cycle_left_behind_are_added() {
    let mut docs = DeletedDocs::new();
    let dead: RoaringTreemap = [2, 5, 7].into_iter().collect();
    let collected: RoaringTreemap = [2, 7].into_iter().collect();
    docs.finish_cycle(&dead, &collected);
    assert_eq!(docs.coverage(), Coverage::Complete);
    assert_eq!(docs.snapshot(), Some([5].into_iter().collect()));
}
