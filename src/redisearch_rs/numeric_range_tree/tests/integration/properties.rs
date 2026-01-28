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
    use inverted_index::NumericFilter;
    use numeric_range_tree::NumericRangeTree;

    /// Walk the tree and verify structural invariants:
    /// (a) all internal nodes have both children
    /// (b) all leaves have ranges
    /// (c) num_leaves matches actual leaf count
    /// (d) num_ranges matches actual range count
    fn verify_tree_invariants(tree: &NumericRangeTree) {
        let mut actual_leaves = 0usize;
        let mut actual_ranges = 0usize;

        fn walk(
            node: &numeric_range_tree::NumericRangeNode,
            leaves: &mut usize,
            ranges: &mut usize,
            depth: i32,
        ) {
            if node.range().is_some() {
                *ranges += 1;
            }
            if node.is_leaf() {
                *leaves += 1;
                assert!(node.range().is_some(), "leaf must have a range");
            } else {
                assert!(
                    node.left().is_some() && node.right().is_some(),
                    "internal node must have both children"
                );
                if let Some(left) = node.left() {
                    walk(left, leaves, ranges, depth + 1);
                }
                if let Some(right) = node.right() {
                    walk(right, leaves, ranges, depth + 1);
                }
            }
        }

        walk(tree.root(), &mut actual_leaves, &mut actual_ranges, 0);
        assert_eq!(actual_leaves, tree.num_leaves(), "leaf count mismatch");
        assert_eq!(actual_ranges, tree.num_ranges(), "range count mismatch");
    }

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
        fn prop_find_returns_overlapping_ranges(
            // Build tree from random entries, then query with random filter
            values in proptest::collection::vec(-1000.0f64..1000.0, 1..100),
            filter_min in -1500.0f64..1500.0,
            filter_width in 0.0f64..3000.0,
        ) {
            let mut tree = NumericRangeTree::new(false);
            for (i, value) in values.iter().enumerate() {
                tree.add((i + 1) as u64, *value, false, 0);
            }

            let filter_max = filter_min + filter_width;
            let filter = NumericFilter {
                min: filter_min,
                max: filter_max,
                ..Default::default()
            };
            let ranges = tree.find(&filter);

            for range in &ranges {
                assert!(
                    range.overlaps(filter_min, filter_max),
                    "range [{}, {}] does not overlap filter [{filter_min}, {filter_max}]",
                    range.min_val(),
                    range.max_val(),
                );
            }
        }

        #[test]
        fn prop_split_preserves_entries(
            values in proptest::collection::vec(-1000.0f64..1000.0, 20..200)
        ) {
            let mut tree = NumericRangeTree::new(false);
            for (i, value) in values.iter().enumerate() {
                tree.add((i + 1) as u64, *value, false, 0);
            }

            // Sum entries from all leaf ranges.
            let mut total_leaf_entries = 0usize;
            for node in &tree {
                if node.is_leaf() {
                    if let Some(range) = node.range() {
                        total_leaf_entries += range.num_entries();
                    }
                }
            }

            // Each entry goes to exactly one leaf, so the sum should equal num_entries.
            assert_eq!(total_leaf_entries, tree.num_entries());
        }

        #[test]
        fn prop_tree_invariants_after_operations(
            values in proptest::collection::vec(-5000.0f64..5000.0, 1..300)
        ) {
            let mut tree = NumericRangeTree::new(false);
            for (i, value) in values.iter().enumerate() {
                tree.add((i + 1) as u64, *value, false, 0);
            }

            verify_tree_invariants(&tree);

            // Verify depth imbalance at every node is bounded.
            fn check_balance(node: &numeric_range_tree::NumericRangeNode) {
                if !node.is_leaf() {
                    let left_depth = node.left().map(|n| n.max_depth()).unwrap_or(0);
                    let right_depth = node.right().map(|n| n.max_depth()).unwrap_or(0);
                    let imbalance = (left_depth - right_depth).abs();
                    assert!(
                        imbalance <= 3,
                        "depth imbalance ({imbalance}) exceeds threshold at node"
                    );
                    if let Some(left) = node.left() {
                        check_balance(left);
                    }
                    if let Some(right) = node.right() {
                        check_balance(right);
                    }
                }
            }
            check_balance(tree.root());
        }

        #[test]
        fn prop_find_ascending_descending_same_ranges(
            values in proptest::collection::vec(-1000.0f64..1000.0, 1..100),
            filter_min in -1500.0f64..1500.0,
            filter_width in 0.0f64..3000.0,
        ) {
            let mut tree = NumericRangeTree::new(false);
            for (i, value) in values.iter().enumerate() {
                tree.add((i + 1) as u64, *value, false, 0);
            }

            let filter_max = filter_min + filter_width;
            let filter_asc = NumericFilter {
                min: filter_min,
                max: filter_max,
                ascending: true,
                ..Default::default()
            };
            let filter_desc = NumericFilter {
                min: filter_min,
                max: filter_max,
                ascending: false,
                ..Default::default()
            };

            let ranges_asc = tree.find(&filter_asc);
            let ranges_desc = tree.find(&filter_desc);

            assert_eq!(
                ranges_asc.len(),
                ranges_desc.len(),
                "asc and desc should return the same number of ranges"
            );

            // Compare as sets by sorting on bit representation of bounds.
            let mut asc_ids: Vec<(u64, u64)> = ranges_asc
                .iter()
                .map(|r| (r.min_val().to_bits(), r.max_val().to_bits()))
                .collect();
            let mut desc_ids: Vec<(u64, u64)> = ranges_desc
                .iter()
                .map(|r| (r.min_val().to_bits(), r.max_val().to_bits()))
                .collect();
            asc_ids.sort();
            desc_ids.sort();
            assert_eq!(asc_ids, desc_ids);
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

        #[test]
        fn prop_gc_then_trim_preserves_surviving_docs(
            num_entries in 10u64..200,
            delete_ratio in 0.1f64..0.9,
        ) {
            let mut tree = NumericRangeTree::new(false);
            for i in 1..=num_entries {
                // Use varied values to trigger splits.
                tree.add(i, (i % 50) as f64, false, 0);
            }

            let delete_threshold = (num_entries as f64 * delete_ratio) as u64;

            // GC all leaves, deleting docs with id <= delete_threshold.
            // We need to replicate the gc_all_leaves helper inline since
            // it's defined in gc.rs and not accessible here.
            let surviving_count = num_entries - delete_threshold;

            // Apply GC to each leaf.
            gc_all_leaves_inline(&mut tree, delete_threshold);

            // Trim empty leaves.
            tree.trim_empty_leaves();

            // num_entries should equal surviving count.
            assert_eq!(
                tree.num_entries(),
                surviving_count as usize,
                "after GC + trim, num_entries should be {surviving_count}"
            );
        }
    }

    /// Inline GC helper — applies GC to every leaf in the tree.
    fn gc_all_leaves_inline(tree: &mut NumericRangeTree, delete_threshold: u64) {
        use numeric_range_tree::NumericRangeNode;

        #[derive(Clone, Copy)]
        enum Dir {
            Left,
            Right,
        }

        struct Work {
            path: Vec<Dir>,
            delta: inverted_index::GcScanDelta,
        }

        fn collect(
            node: &NumericRangeNode,
            doc_exist: &dyn Fn(u64) -> bool,
            work: &mut Vec<Work>,
            path: &mut Vec<Dir>,
        ) {
            if node.is_leaf() {
                if let Some(range) = node.range()
                    && let Some(delta) = range
                        .entries()
                        .scan_gc(doc_exist)
                        .expect("scan should not fail")
                {
                    work.push(Work {
                        path: path.clone(),
                        delta,
                    });
                }
            } else {
                if let Some(left) = node.left() {
                    path.push(Dir::Left);
                    collect(left, doc_exist, work, path);
                    path.pop();
                }
                if let Some(right) = node.right() {
                    path.push(Dir::Right);
                    collect(right, doc_exist, work, path);
                    path.pop();
                }
            }
        }

        fn find_node<'a>(
            mut node: &'a mut NumericRangeNode,
            path: &[Dir],
        ) -> &'a mut NumericRangeNode {
            for dir in path {
                node = match dir {
                    Dir::Left => node.left_mut().expect("left child"),
                    Dir::Right => node.right_mut().expect("right child"),
                };
            }
            node
        }

        let doc_exist = move |doc_id: u64| doc_id > delete_threshold;
        let mut work = Vec::new();
        collect(tree.root(), &doc_exist, &mut work, &mut Vec::new());

        for item in work {
            let node_ptr = find_node(tree.root_mut(), &item.path) as *mut NumericRangeNode;
            // SAFETY: Same disjoint-mutation pattern as in gc.rs tests.
            let node = unsafe { &mut *node_ptr };
            let hll_with = [0u8; 64];
            let hll_without = [0u8; 64];
            tree.apply_node_gc(node, item.delta, &hll_with, &hll_without);
        }
    }
}
