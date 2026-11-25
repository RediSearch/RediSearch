/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The routing layer with a [`NumericRangeTree`] as its terminal, used directly rather than
//! through [`nested_index::NestedIndex`]: tag levels route each element to the tree of its path,
//! and every distinct path owns a tree of its own.
//!
//! Indexing is [route, then post](compound_index#route-then-post): find or create the path's
//! tree, then add the element's price to it. A query walks to the trees whose path matches the
//! tag constraints and runs the price range in each, so a doc matches only when one element
//! satisfies every condition.

use compound_index::{CompoundIndex, Constraint, TagLevel, TagSet};
use index_result::RSIndexResult;
use inverted_index::{IndexReader as _, NumericFilter};
use numeric_range_tree::NumericRangeTree;
use timeout::NoTimeoutChecker;

use crate::util::tag;

type PriceIndex = CompoundIndex<NumericRangeTree>;

fn tag_in<'a>(values: &[&'a str]) -> Constraint<'a> {
    Constraint::TagIn(TagSet::new(values.iter().map(|value| value.as_bytes())))
}

fn tag_levels(n: usize) -> PriceIndex {
    PriceIndex::new((0..n).map(|_| TagLevel::new()).collect()).unwrap()
}

/// Indexes one element of `doc`: routes `path` to its tree, creating an empty one for a new
/// path, then posts `price` there.
fn post(index: &mut PriceIndex, path: &[&[u8]], doc: u64, price: f64) {
    let tree = index
        .get_or_insert_with(path, || NumericRangeTree::new(false))
        .unwrap();
    // Several elements of one document may share a path, which makes it multi-valued there.
    tree.add(doc, price, false, true, 0);
}

/// The docs with an element on a path matching `constraints` and a price in `[min, max]`.
fn query(index: &PriceIndex, constraints: &[Constraint<'_>], min: f64, max: f64) -> Vec<u64> {
    let trees = index
        .collect_terminals(constraints, usize::MAX, &mut NoTimeoutChecker)
        .unwrap();
    let filter = NumericFilter {
        min,
        max,
        ..Default::default()
    };
    let mut docs = Vec::new();
    for tree in trees {
        // A range comes back whole, so it may hold prices outside the filter.
        for range in tree.find(&filter) {
            let mut reader = range.reader();
            let mut record = RSIndexResult::build_numeric(0.0).build();
            while reader.next_record(&mut record).unwrap() {
                let price = record
                    .as_numeric()
                    .expect("a numeric range holds numeric records");
                if (min..=max).contains(&price) {
                    docs.push(record.doc_id);
                }
            }
        }
    }
    docs.sort_unstable();
    docs.dedup();
    docs
}

/// One tag level, `color`, over the price trees.
///
/// | doc | items                  |
/// |-----|------------------------|
/// | 1   | (red, 20), (blue, 90)  |
/// | 2   | (red, 95)              |
/// | 3   | (blue, 15)             |
/// | 4   | (red, 30), (red, 40)   |
#[test]
fn one_tag_level_gives_each_value_its_own_tree() {
    let mut index = tag_levels(1);
    for (doc, color, price) in [
        (1, "red", 20.0),
        (1, "blue", 90.0),
        (2, "red", 95.0),
        (3, "blue", 15.0),
        (4, "red", 30.0),
        (4, "red", 40.0),
    ] {
        post(&mut index, &[tag(color)], doc, price);
    }

    // One tree per color, each holding only that color's prices.
    assert_eq!(index.num_terminals(), 2);
    let red = index.get(&[tag("red")]).unwrap();
    assert_eq!(red.num_entries(), 4);
    assert!(red.has_multivalued_docs(), "doc 4 has two red items");
    assert_eq!(index.get(&[tag("blue")]).unwrap().num_entries(), 2);

    assert_eq!(query(&index, &[tag_in(&["red"])], 0.0, 50.0), [1, 4]);
    assert_eq!(
        query(&index, &[tag_in(&["red", "blue"])], 0.0, 50.0),
        [1, 3, 4]
    );
    // Doc 1's blue item costs 90: its red item's 20 lives in another tree.
    assert_eq!(query(&index, &[tag_in(&["blue"])], 80.0, 100.0), [1]);
    assert_eq!(query(&index, &[Constraint::Any], 80.0, 100.0), [1, 2]);
}

/// Two tag levels, `color` then `size`, over the price trees.
///
/// | doc | items                         |
/// |-----|-------------------------------|
/// | 1   | (red, M, 20), (blue, L, 90)   |
/// | 2   | (red, L, 95)                  |
/// | 3   | (blue, M, 15)                 |
#[test]
fn two_tag_levels_keep_an_elements_values_together() {
    let mut index = tag_levels(2);
    for (doc, color, size, price) in [
        (1, "red", "M", 20.0),
        (1, "blue", "L", 90.0),
        (2, "red", "L", 95.0),
        (3, "blue", "M", 15.0),
    ] {
        post(&mut index, &[tag(color), tag(size)], doc, price);
    }

    // Every (color, size) pair owns a tree; red's M and L items are in different ones.
    assert_eq!(index.num_terminals(), 4);
    let red_m = index.get(&[tag("red"), tag("M")]).unwrap();
    let red_l = index.get(&[tag("red"), tag("L")]).unwrap();
    assert_eq!((red_m.num_entries(), red_l.num_entries()), (1, 1));

    assert_eq!(
        query(&index, &[tag_in(&["red"]), tag_in(&["M"])], 0.0, 50.0),
        [1]
    );
    // The size level left open: every red tree is searched.
    assert_eq!(
        query(&index, &[tag_in(&["red"]), Constraint::Any], 0.0, 100.0),
        [1, 2]
    );
    // The color level left open: every M tree is searched.
    assert_eq!(
        query(&index, &[Constraint::Any, tag_in(&["M"])], 0.0, 50.0),
        [1, 3]
    );
    // Doc 1 has a blue item and an item priced 20, but no blue item priced 20.
    assert_eq!(
        query(&index, &[tag_in(&["blue"]), Constraint::Any], 0.0, 50.0),
        [3]
    );
}
