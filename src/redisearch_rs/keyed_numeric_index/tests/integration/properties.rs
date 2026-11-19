//! Property tests: every operation sequence agrees with a brute-force model.

use crate::helpers::{MAX_DEPTH_RANGE, matches};
use keyed_numeric_index::KeyedNumericIndex;
use numeric_range_tree::NumericFilter;
use numeric_range_tree::test_utils::gc_all_ranges;
use proptest::prelude::*;
use std::collections::BTreeMap;

const KEYS: [&[u8]; 4] = [b"rev:fy0", b"rev:fym1", b"headcount:m2026_06", b""];

/// Few distinct values, so equal values, equal range bounds and the infinities are common.
const GRID: [f64; 9] = [
    f64::NEG_INFINITY,
    -2.0,
    -1.0,
    0.0,
    1.0,
    2.0,
    3.0,
    4.0,
    f64::INFINITY,
];

#[derive(Debug, Clone)]
enum Op {
    /// A new document, with a value for each key it carries.
    Doc {
        step: u64,
        values: BTreeMap<usize, usize>,
    },
    /// GC deletes the documents of one key whose id is `rem` modulo `modulus`.
    Gc {
        key_idx: usize,
        modulus: u64,
        rem: u64,
    },
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    let doc = (
        1u64..4,
        proptest::collection::btree_map(0..KEYS.len(), 0..GRID.len(), 0..=KEYS.len()),
    )
        .prop_map(|(step, values)| Op::Doc { step, values });
    let gc = (0..KEYS.len(), 2u64..5, 0u64..5).prop_map(|(key_idx, modulus, rem)| Op::Gc {
        key_idx,
        modulus,
        rem: rem % modulus,
    });
    proptest::collection::vec(prop_oneof![4 => doc, 1 => gc], 1..400)
}

fn filters() -> impl Strategy<Value = Vec<NumericFilter>> {
    proptest::collection::vec(
        (0..GRID.len(), 0..GRID.len(), any::<bool>(), any::<bool>()).prop_map(
            |(lo, hi, min_inclusive, max_inclusive)| NumericFilter {
                min: GRID[lo],
                max: GRID[hi],
                min_inclusive,
                max_inclusive,
                ..Default::default()
            },
        ),
        1..12,
    )
}

fn passes(filter: &NumericFilter, v: f64) -> bool {
    (v > filter.min || (filter.min_inclusive && v == filter.min))
        && (v < filter.max || (filter.max_inclusive && v == filter.max))
}

proptest! {
    /// For every key and filter, the range query returns exactly the live pairs that pass it,
    /// and the memory accounting stays exact after every operation.
    ///
    /// Floats are stored uncompressed: compression is lossy and would break the exact
    /// comparison with the model.
    #[test]
    #[cfg_attr(miri, ignore = "Too slow to run under miri")]
    fn range_results_match_brute_force(ops in ops(), filters in filters()) {
        let mut index = KeyedNumericIndex::new(false);
        // (doc_id, key index, value) still alive.
        let mut model: Vec<(u64, usize, f64)> = Vec::new();
        let mut doc_id = 0;
        for op in ops {
            match op {
                Op::Doc { step, values } => {
                    // Doc ids ascend; a doc carries each key at most once, as a JSON object does.
                    doc_id += step;
                    for (key_idx, grid_idx) in values {
                        let value = GRID[grid_idx];
                        index.add(doc_id, KEYS[key_idx], value, false, MAX_DEPTH_RANGE);
                        model.push((doc_id, key_idx, value));
                    }
                }
                Op::Gc { key_idx, modulus, rem } => {
                    let key = KEYS[key_idx];
                    if let Some(id) = index.get(key).map(|tree| tree.unique_id()) {
                        index.update_tree(key, id, |tree| {
                            gc_all_ranges(tree, &|doc| doc % modulus != rem);
                        });
                        model.retain(|&(d, k, _)| !(k == key_idx && d % modulus == rem));
                        index.remove_key_if_empty(key, id);
                    }
                }
            }
            prop_assert_eq!(index.memory_usage(), index.computed_memory_usage());
        }

        for (key_idx, key) in KEYS.iter().enumerate() {
            let live = model.iter().filter(|&&(_, k, _)| k == key_idx).count();
            prop_assert_eq!(index.get(key).is_some(), live > 0, "key presence");
            for filter in &filters {
                let mut expected: Vec<(u64, f64)> = model
                    .iter()
                    .filter(|&&(_, k, v)| k == key_idx && passes(filter, v))
                    .map(|&(d, _, v)| (d, v))
                    .collect();
                expected.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let actual = index.get(key).map_or_else(Vec::new, |tree| matches(tree, filter));
                prop_assert_eq!(actual, expected);
            }
        }
    }
}
