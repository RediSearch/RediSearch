/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRange.

use numeric_range_tree::NumericRange;

#[test]
fn test_new_range_bounds() {
    let range = NumericRange::new(false);
    assert_eq!(range.min_val(), f64::INFINITY);
    assert_eq!(range.max_val(), f64::NEG_INFINITY);
    assert_eq!(range.num_entries(), 0);
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_updates_bounds() {
    let mut range = NumericRange::new(false);

    range.add(1, 5.0);
    assert_eq!(range.min_val(), 5.0);
    assert_eq!(range.max_val(), 5.0);

    range.add(2, 10.0);
    assert_eq!(range.min_val(), 5.0);
    assert_eq!(range.max_val(), 10.0);

    range.add(3, 2.0);
    assert_eq!(range.min_val(), 2.0);
    assert_eq!(range.max_val(), 10.0);
}

#[test]
fn test_add_updates_cardinality() {
    let mut range = NumericRange::new(false);

    range.add(1, 1.0);
    range.add(2, 2.0);
    range.add(3, 3.0);

    // HLL gives approximate count, but for small counts it should be reasonably accurate
    let card = range.cardinality();
    assert!(
        (2..=4).contains(&card),
        "Cardinality {card} not in expected range"
    );
}

#[test]
fn test_contained_in() {
    let mut range = NumericRange::new(false);
    range.add(1, 5.0);
    range.add(2, 10.0);

    // Range [5, 10] is contained in [0, 20]
    assert!(range.contained_in(0.0, 20.0));
    // Range [5, 10] is contained in [5, 10]
    assert!(range.contained_in(5.0, 10.0));
    // Range [5, 10] is NOT contained in [6, 20]
    assert!(!range.contained_in(6.0, 20.0));
    // Range [5, 10] is NOT contained in [0, 9]
    assert!(!range.contained_in(0.0, 9.0));
}

#[test]
fn test_overlaps() {
    let mut range = NumericRange::new(false);
    range.add(1, 5.0);
    range.add(2, 10.0);

    // Overlapping cases
    assert!(range.overlaps(0.0, 6.0)); // left overlap
    assert!(range.overlaps(8.0, 20.0)); // right overlap
    assert!(range.overlaps(6.0, 8.0)); // contained
    assert!(range.overlaps(0.0, 20.0)); // contains

    // Non-overlapping cases
    assert!(!range.overlaps(11.0, 20.0)); // completely right
    assert!(!range.overlaps(0.0, 4.0)); // completely left
}

#[test]
fn test_default_impl() {
    let range: NumericRange = Default::default();
    assert_eq!(range.min_val(), f64::INFINITY);
    assert_eq!(range.max_val(), f64::NEG_INFINITY);
    assert_eq!(range.num_entries(), 0);
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_without_cardinality() {
    let mut range = NumericRange::new(false);

    // Add entries without updating cardinality
    range.add_without_cardinality(1, 5.0);
    range.add_without_cardinality(2, 10.0);
    range.add_without_cardinality(3, 2.0);

    // Bounds should be updated
    assert_eq!(range.min_val(), 2.0);
    assert_eq!(range.max_val(), 10.0);
    assert_eq!(range.num_entries(), 3);

    // Cardinality should be 0 (HLL not updated)
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_without_cardinality_vs_add() {
    let mut range_with_card = NumericRange::new(false);
    let mut range_without_card = NumericRange::new(false);

    // Add same values to both
    range_with_card.add(1, 5.0);
    range_with_card.add(2, 10.0);

    range_without_card.add_without_cardinality(1, 5.0);
    range_without_card.add_without_cardinality(2, 10.0);

    // Bounds should be the same
    assert_eq!(range_with_card.min_val(), range_without_card.min_val());
    assert_eq!(range_with_card.max_val(), range_without_card.max_val());
    assert_eq!(
        range_with_card.num_entries(),
        range_without_card.num_entries()
    );

    // Cardinality differs
    assert!(range_with_card.cardinality() > 0);
    assert_eq!(range_without_card.cardinality(), 0);
}

#[test]
fn test_num_docs() {
    let mut range = NumericRange::new(false);
    assert_eq!(range.num_docs(), 0);

    range.add(1, 5.0);
    assert_eq!(range.num_docs(), 1);

    range.add(2, 10.0);
    assert_eq!(range.num_docs(), 2);

    range.add(3, 15.0);
    assert_eq!(range.num_docs(), 3);
}

#[test]
fn test_entries_accessor() {
    use numeric_range_tree::NumericIndex;

    let mut range = NumericRange::new(false);
    range.add(1, 5.0);
    range.add(2, 10.0);

    let entries = range.entries();
    match entries {
        NumericIndex::Uncompressed(idx) => {
            assert_eq!(idx.number_of_entries(), 2);
            assert!(idx.memory_usage() > 0);
        }
        NumericIndex::Compressed(idx) => {
            assert_eq!(idx.number_of_entries(), 2);
            assert!(idx.memory_usage() > 0);
        }
    }
}

#[test]
fn test_hll_accessor() {
    let mut range = NumericRange::new(false);
    range.add(1, 5.0);
    range.add(2, 10.0);
    range.add(3, 15.0);

    let hll = range.hll();
    // HLL count should approximate the number of distinct values
    let count = hll.count();
    assert!(
        (2..=4).contains(&count),
        "HLL count {count} not in expected range"
    );
}

#[test]
fn test_inverted_index_size() {
    let mut range = NumericRange::new(false);
    let initial_size = range.memory_usage();

    range.add(1, 5.0);
    let size_after_one = range.memory_usage();
    assert!(size_after_one >= initial_size);

    range.add(2, 10.0);
    let size_after_two = range.memory_usage();
    assert!(size_after_two >= size_after_one);
}
