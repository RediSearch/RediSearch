/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Property-based tests for the numeric range tree using `proptest`.

#[cfg(not(miri))]
mod proptests {
    use numeric_range_tree::NumericRangeTree;

    proptest::proptest! {
        #[test]
        fn prop_add_never_loses_entries(
            // Generate 1..200 doc/value pairs
            values in proptest::collection::vec((1u64..10000, -1000.0f64..1000.0), 1..200)
        ) {
            let mut tree = NumericRangeTree::new(false);
            let mut unique_ids = std::collections::HashSet::new();
            let mut last_id = 0u64;

            for (raw_id, value) in &values {
                // Ensure strictly increasing doc IDs.
                let doc_id = last_id + (*raw_id).max(1);
                last_id = doc_id;
                tree.add(doc_id, *value, false, 0);
                unique_ids.insert(doc_id);
            }

            assert_eq!(tree.num_entries(), unique_ids.len());
        }

        #[test]
        fn prop_tree_invariants_after_operations(
            values in proptest::collection::vec(-5000.0f64..5000.0, 1..300)
        ) {
            let mut tree = NumericRangeTree::new(false);
            // The depth imbalance invariant in `check_tree_invariants` (which
            // runs after every `add` under the `unittest` feature) validates
            // balance at every node automatically.
            for (i, value) in values.iter().enumerate() {
                tree.add((i + 1) as u64, *value, false, 0);
            }
        }

        #[test]
        fn prop_memory_usage_monotonic_with_adds(
            values in proptest::collection::vec(-1000.0f64..1000.0, 1..100)
        ) {
            let mut tree = NumericRangeTree::new(false);
            let mut last_mem = tree.mem_usage();

            for (i, value) in values.iter().enumerate() {
                tree.add((i + 1) as u64, *value, false, 0);
                let current_mem = tree.mem_usage();
                assert!(
                    current_mem >= last_mem,
                    "mem_usage decreased from {last_mem} to {current_mem} after add"
                );
                last_mem = current_mem;
            }
        }

    }
}
