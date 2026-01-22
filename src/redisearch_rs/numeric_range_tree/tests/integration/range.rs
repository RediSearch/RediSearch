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
    let range = NumericRange::new();
    assert_eq!(range.min_val(), f64::INFINITY);
    assert_eq!(range.max_val(), f64::NEG_INFINITY);
    assert_eq!(range.num_entries(), 0);
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_updates_bounds() {
    let mut range = NumericRange::new();

    range.add(1, 5.0).unwrap();
    assert_eq!(range.min_val(), 5.0);
    assert_eq!(range.max_val(), 5.0);

    range.add(2, 10.0).unwrap();
    assert_eq!(range.min_val(), 5.0);
    assert_eq!(range.max_val(), 10.0);

    range.add(3, 2.0).unwrap();
    assert_eq!(range.min_val(), 2.0);
    assert_eq!(range.max_val(), 10.0);
}

#[test]
fn test_add_updates_cardinality() {
    let mut range = NumericRange::new();

    range.add(1, 1.0).unwrap();
    range.add(2, 2.0).unwrap();
    range.add(3, 3.0).unwrap();

    // HLL gives approximate count, but for small counts it should be reasonably accurate
    let card = range.cardinality();
    assert!(card >= 2 && card <= 4, "Cardinality {card} not in expected range");
}

#[test]
fn test_contained_in() {
    let mut range = NumericRange::new();
    range.add(1, 5.0).unwrap();
    range.add(2, 10.0).unwrap();

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
    let mut range = NumericRange::new();
    range.add(1, 5.0).unwrap();
    range.add(2, 10.0).unwrap();

    // Overlapping cases
    assert!(range.overlaps(0.0, 6.0));   // left overlap
    assert!(range.overlaps(8.0, 20.0));  // right overlap
    assert!(range.overlaps(6.0, 8.0));   // contained
    assert!(range.overlaps(0.0, 20.0));  // contains

    // Non-overlapping cases
    assert!(!range.overlaps(11.0, 20.0)); // completely right
    assert!(!range.overlaps(0.0, 4.0));   // completely left
}
