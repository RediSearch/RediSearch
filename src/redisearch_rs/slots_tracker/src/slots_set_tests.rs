/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the SlotsSet data structure.

use crate::{SlotRange, slots_set::{SlotsSet, CoverageRelation}};

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a SlotRange
    fn range(start: u16, end: u16) -> SlotRange {
        SlotRange { start, end }
    }

    // ========================================================================
    // Basic construction and equality tests
    // ========================================================================

    #[test]
    fn test_new_is_empty() {
        let set = SlotsSet::new();
        assert!(set.is_empty());
    }

    #[test]
    fn test_default_is_empty() {
        let set = SlotsSet::default();
        assert!(set.is_empty());
    }

    #[test]
    fn test_equality_with_self() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        assert_eq!(set, set.clone());
    }

    #[test]
    fn test_equality_with_slice() {
        let mut set = SlotsSet::new();
        let ranges = [range(0, 10), range(20, 30)];
        set.set_from_ranges(&ranges);
        assert_eq!(set, &ranges[..]);
        assert_eq!(set, ranges.as_slice());
    }

    // ========================================================================
    // set_from_ranges tests
    // ========================================================================

    #[test]
    fn test_set_from_ranges_empty() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_set_from_ranges_single() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(5, 10)]);
        assert_eq!(set, &[range(5, 10)][..]);
    }

    #[test]
    fn test_set_from_ranges_multiple() {
        let mut set = SlotsSet::new();
        let ranges = [range(0, 5), range(10, 15), range(20, 25)];
        set.set_from_ranges(&ranges);
        assert_eq!(set, &ranges[..]);
    }

    #[test]
    fn test_set_from_ranges_replaces_existing() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 100)]);
        set.set_from_ranges(&[range(200, 300)]);
        assert_eq!(set, &[range(200, 300)][..]);
    }

    // ========================================================================
    // add_ranges tests (union operation)
    // ========================================================================

    #[test]
    fn test_add_ranges_to_empty() {
        let mut set = SlotsSet::new();
        set.add_ranges(&[range(10, 20)]);
        assert_eq!(set, &[range(10, 20)][..]);
    }

    #[test]
    fn test_add_ranges_non_overlapping() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        set.add_ranges(&[range(20, 30)]);
        assert_eq!(set, &[range(0, 10), range(20, 30)][..]);
    }

    #[test]
    fn test_add_ranges_overlapping() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        set.add_ranges(&[range(5, 15)]);
        assert_eq!(set, &[range(0, 15)][..]);
    }

    #[test]
    fn test_add_ranges_adjacent_merges() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        set.add_ranges(&[range(11, 20)]);
        assert_eq!(set, &[range(0, 20)][..]);
    }

    #[test]
    fn test_add_ranges_subset() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 100)]);
        set.add_ranges(&[range(10, 20)]);
        assert_eq!(set, &[range(0, 100)][..]);
    }

    #[test]
    fn test_add_ranges_superset() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        set.add_ranges(&[range(0, 100)]);
        assert_eq!(set, &[range(0, 100)][..]);
    }

    #[test]
    fn test_add_ranges_multiple_merges() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 5), range(10, 15), range(20, 25)]);
        set.add_ranges(&[range(6, 19)]);
        assert_eq!(set, &[range(0, 25)][..]);
    }

    #[test]
    fn test_add_ranges_empty() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        set.add_ranges(&[]);
        assert_eq!(set, &[range(0, 10)][..]);
    }

    // ========================================================================
    // remove_ranges tests
    // ========================================================================

    #[test]
    fn test_remove_ranges_from_empty() {
        let mut set = SlotsSet::new();
        set.remove_ranges(&[range(10, 20)]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_ranges_no_overlap() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        set.remove_ranges(&[range(20, 30)]);
        assert_eq!(set, &[range(0, 10)][..]);
    }

    #[test]
    fn test_remove_ranges_exact_match() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(10, 20)]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_ranges_complete_overlap() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(0, 30)]);
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_ranges_trim_left() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(5, 15)]);
        assert_eq!(set, &[range(16, 20)][..]);
    }

    #[test]
    fn test_remove_ranges_trim_right() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(15, 25)]);
        assert_eq!(set, &[range(10, 14)][..]);
    }

    #[test]
    fn test_remove_ranges_split_middle() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 30)]);
        set.remove_ranges(&[range(15, 20)]);
        assert_eq!(set, &[range(10, 14), range(21, 30)][..]);
    }

    #[test]
    fn test_remove_ranges_multiple_overlaps() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        set.remove_ranges(&[range(5, 25)]);
        assert_eq!(set, &[range(0, 4), range(26, 30), range(40, 50)][..]);
    }

    #[test]
    fn test_remove_ranges_multiple_splits() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 100)]);
        set.remove_ranges(&[range(10, 20), range(30, 40), range(50, 60)]);
        assert_eq!(set, &[range(0, 9), range(21, 29), range(41, 49), range(61, 100)][..]);
    }

    #[test]
    fn test_remove_ranges_consecutive_removes_same_range() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        // Remove range that covers multiple of our ranges
        set.remove_ranges(&[range(5, 45)]);
        assert_eq!(set, &[range(0, 4), range(46, 50)][..]);
    }

    #[test]
    fn test_remove_ranges_empty() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        set.remove_ranges(&[]);
        assert_eq!(set, &[range(0, 10)][..]);
    }

    // ========================================================================
    // has_overlap tests
    // ========================================================================

    #[test]
    fn test_has_overlap_empty_sets() {
        let set = SlotsSet::new();
        assert!(!set.has_overlap(&[]));
    }

    #[test]
    fn test_has_overlap_empty_self() {
        let set = SlotsSet::new();
        assert!(!set.has_overlap(&[range(0, 10)]));
    }

    #[test]
    fn test_has_overlap_empty_input() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        assert!(!set.has_overlap(&[]));
    }

    #[test]
    fn test_has_overlap_no_overlap() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        assert!(!set.has_overlap(&[range(20, 30)]));
    }

    #[test]
    fn test_has_overlap_exact_match() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        assert!(set.has_overlap(&[range(10, 20)]));
    }

    #[test]
    fn test_has_overlap_partial_left() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        assert!(set.has_overlap(&[range(5, 15)]));
    }

    #[test]
    fn test_has_overlap_partial_right() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        assert!(set.has_overlap(&[range(15, 25)]));
    }

    #[test]
    fn test_has_overlap_contained() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 30)]);
        assert!(set.has_overlap(&[range(15, 20)]));
    }

    #[test]
    fn test_has_overlap_contains() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(15, 20)]);
        assert!(set.has_overlap(&[range(10, 30)]));
    }

    #[test]
    fn test_has_overlap_adjacent_no_overlap() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10)]);
        assert!(!set.has_overlap(&[range(11, 20)]));
    }

    #[test]
    fn test_has_overlap_multiple_ranges_with_overlap() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        assert!(set.has_overlap(&[range(25, 35)]));
    }

    #[test]
    fn test_has_overlap_multiple_ranges_no_overlap() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 10), range(20, 30), range(40, 50)]);
        assert!(!set.has_overlap(&[range(11, 19), range(31, 39)]));
    }

    // ========================================================================
    // union_relation tests
    // ========================================================================

    #[test]
    fn test_union_relation_equals_empty() {
        let set1 = SlotsSet::new();
        let set2 = SlotsSet::new();
        assert_eq!(set1.union_relation(&set2, &[]), CoverageRelation::Equals);
    }

    #[test]
    fn test_union_relation_equals() {
        let mut set1 = SlotsSet::new();
        set1.set_from_ranges(&[range(0, 10)]);
        let mut set2 = SlotsSet::new();
        set2.set_from_ranges(&[range(20, 30)]);
        assert_eq!(
            set1.union_relation(&set2, &[range(0, 10), range(20, 30)]),
            CoverageRelation::Equals
        );
    }

    #[test]
    fn test_union_relation_covers() {
        let mut set1 = SlotsSet::new();
        set1.set_from_ranges(&[range(0, 100)]);
        let set2 = SlotsSet::new();
        assert_eq!(
            set1.union_relation(&set2, &[range(10, 20)]),
            CoverageRelation::Covers
        );
    }

    #[test]
    fn test_union_relation_no_match() {
        let mut set1 = SlotsSet::new();
        set1.set_from_ranges(&[range(0, 10)]);
        let set2 = SlotsSet::new();
        assert_eq!(
            set1.union_relation(&set2, &[range(20, 30)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_union_relation_partial_coverage() {
        let mut set1 = SlotsSet::new();
        set1.set_from_ranges(&[range(0, 10)]);
        let mut set2 = SlotsSet::new();
        set2.set_from_ranges(&[range(20, 30)]);
        assert_eq!(
            set1.union_relation(&set2, &[range(0, 30)]),
            CoverageRelation::NoMatch
        );
    }

    #[test]
    fn test_union_relation_with_gaps() {
        let mut set1 = SlotsSet::new();
        set1.set_from_ranges(&[range(0, 10), range(30, 40)]);
        let mut set2 = SlotsSet::new();
        set2.set_from_ranges(&[range(11, 29)]);
        assert_eq!(
            set1.union_relation(&set2, &[range(0, 40)]),
            CoverageRelation::Equals
        );
    }

    // ========================================================================
    // Edge cases and boundary tests
    // ========================================================================

    #[test]
    fn test_max_slot_value() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(16380, 16383)]);
        assert_eq!(set, &[range(16380, 16383)][..]);
    }

    #[test]
    fn test_single_slot_range() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(100, 100)]);
        assert_eq!(set, &[range(100, 100)][..]);
    }

    #[test]
    fn test_remove_single_slot() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(10, 20)]);
        set.remove_ranges(&[range(15, 15)]);
        assert_eq!(set, &[range(10, 14), range(16, 20)][..]);
    }

    #[test]
    fn test_full_range() {
        let mut set = SlotsSet::new();
        set.set_from_ranges(&[range(0, 16383)]);
        assert_eq!(set, &[range(0, 16383)][..]);
    }

    #[test]
    fn test_complex_operations_sequence() {
        let mut set = SlotsSet::new();
        
        // Start with some ranges
        set.set_from_ranges(&[range(0, 100), range(200, 300)]);
        
        // Add overlapping ranges
        set.add_ranges(&[range(50, 150), range(250, 350)]);
        assert_eq!(set, &[range(0, 150), range(200, 350)][..]);
        
        // Remove from middle
        set.remove_ranges(&[range(75, 275)]);
        assert_eq!(set, &[range(0, 74), range(276, 350)][..]);
        
        // Check overlap
        assert!(set.has_overlap(&[range(70, 80)]));
        assert!(!set.has_overlap(&[range(100, 200)]));
    }
}
