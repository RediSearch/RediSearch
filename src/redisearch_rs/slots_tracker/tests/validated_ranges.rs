/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use slots_tracker::{CLUSTER_SLOTS, SlotRange, ValidatedSlotRange};

#[test]
fn rejects_invalid_bounds_and_accepts_cluster_edges() {
    assert!(ValidatedSlotRange::new(0, 0).is_ok());
    assert!(ValidatedSlotRange::new(CLUSTER_SLOTS - 1, CLUSTER_SLOTS - 1).is_ok());
    for (start, end) in [(2, 1), (0, CLUSTER_SLOTS), (u16::MAX, u16::MAX)] {
        let error = ValidatedSlotRange::new(start, end).unwrap_err();
        assert_eq!((error.start, error.end), (start, end));
    }
}

#[test]
fn raw_ranges_are_validated_before_becoming_checked_ranges() {
    let raw = SlotRange { start: 2, end: 5 };
    let range = ValidatedSlotRange::try_from(raw).unwrap();
    assert_eq!(range.as_raw(), &raw);
    assert_eq!((range.start(), range.end()), (2, 5));
    assert_eq!(range.slots().collect::<Vec<_>>(), [2, 3, 4, 5]);
    assert!(ValidatedSlotRange::try_from(SlotRange { start: 5, end: 2 }).is_err());
}

#[test]
fn normalization_merges_unsorted_overlapping_adjacent_and_duplicate_ranges() {
    let input: Vec<_> = [(20, 21), (4, 7), (0, 2), (2, 4), (4, 7), (10, 12)]
        .into_iter()
        .map(|(start, end)| ValidatedSlotRange::new(start, end).unwrap())
        .collect();
    let normalized = ValidatedSlotRange::normalize(&input);
    let bounds: Vec<_> = normalized
        .iter()
        .map(|range| (range.start(), range.end()))
        .collect();
    assert_eq!(bounds, [(0, 7), (10, 12), (20, 21)]);
    assert_eq!(ValidatedSlotRange::normalize(&normalized), normalized);
}

#[test]
fn normalization_preserves_the_last_slot_and_empty_input() {
    let ranges = [
        ValidatedSlotRange::new(CLUSTER_SLOTS - 1, CLUSTER_SLOTS - 1).unwrap(),
        ValidatedSlotRange::new(CLUSTER_SLOTS - 2, CLUSTER_SLOTS - 2).unwrap(),
    ];
    let normalized = ValidatedSlotRange::normalize(&ranges);
    assert_eq!(normalized.len(), 1);
    assert_eq!(
        normalized[0].slots().collect::<Vec<_>>(),
        [CLUSTER_SLOTS - 2, CLUSTER_SLOTS - 1]
    );
    assert!(ValidatedSlotRange::normalize(&[]).is_empty());
}
