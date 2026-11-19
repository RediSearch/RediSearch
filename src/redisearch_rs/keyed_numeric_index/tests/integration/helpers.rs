//! Helpers shared by the test modules.

use index_result::RSIndexResult;
use inverted_index::IndexReader as _;
use keyed_numeric_index::KeyedNumericIndex;
use numeric_range_tree::{NumericFilter, NumericRangeTree};

pub const MAX_DEPTH_RANGE: usize = 0;

/// A filter over `[min, max]`, with both bounds inclusive.
pub fn inclusive(min: f64, max: f64) -> NumericFilter {
    NumericFilter {
        min,
        max,
        ..Default::default()
    }
}

/// Every `(doc_id, value)` of `tree` that passes `filter`, sorted.
///
/// A tree returns whole ranges, which overshoot the filter, so the values are
/// filtered again here.
pub fn matches(tree: &NumericRangeTree, filter: &NumericFilter) -> Vec<(u64, f64)> {
    let mut found = Vec::new();
    for range in tree.find(filter) {
        let mut reader = range.reader();
        let mut result = RSIndexResult::build_numeric(0.0).build();
        while reader.next_record(&mut result).unwrap_or(false) {
            // SAFETY: a numeric index only ever holds numeric records
            let value = unsafe { result.as_numeric_unchecked() };
            if filter.value_in_range(value) {
                found.push((result.doc_id, value));
            }
        }
    }
    found.sort_by(|a, b| a.partial_cmp(b).unwrap());
    found
}

/// Every pair under `key`, in any range.
pub fn all(index: &KeyedNumericIndex, key: &[u8]) -> Vec<(u64, f64)> {
    index.get(key).map_or_else(Vec::new, |tree| {
        matches(tree, &inclusive(f64::NEG_INFINITY, f64::INFINITY))
    })
}

pub fn add(index: &mut KeyedNumericIndex, doc_id: u64, key: &[u8], value: f64) {
    index.add(doc_id, key, value, false, MAX_DEPTH_RANGE);
}
