/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use nested_index::{
    Element, FloatCompression, IndexDelta, IndexError, IndexParams, Level, NestedIndex, PathError,
    TagLevel, Terminal, TerminalKind, TerminalValue,
};
use numeric_range_tree::NumericRangeTree;

use crate::util::{PARAMS, elements, item, materials, priced, tag, tag_value};

fn numeric_terminal<'a>(index: &'a NestedIndex, color: &str, size: &str) -> &'a NumericRangeTree {
    match index.routing().get(&[tag(color), tag(size)]).unwrap() {
        Some(Terminal::Numeric(tree)) => tree,
        other => panic!("expected a numeric terminal at ({color}, {size}), got {other:?}"),
    }
}

fn tag_terminal<'a>(
    index: &'a NestedIndex,
    color: &str,
) -> &'a tag_index::TagIndex<tag_index::InMemoryMode> {
    match index.routing().get(&[tag(color)]).unwrap() {
        Some(Terminal::Tag(tags)) => tags,
        other => panic!("expected a tag terminal at ({color}), got {other:?}"),
    }
}

/// The deltas of a run of documents, summed.
fn sum(deltas: impl IntoIterator<Item = IndexDelta>) -> IndexDelta {
    deltas
        .into_iter()
        .fold(IndexDelta::default(), |total, delta| IndexDelta {
            new_terminals: total.new_terminals + delta.new_terminals,
            records: total.records + delta.records,
            size_delta: total.size_delta + delta.size_delta,
            block_delta: total.block_delta + delta.block_delta,
        })
}

#[test]
fn each_element_lands_in_the_terminal_of_its_path() {
    let mut index = priced(None);
    let doc = [item("red", "M", 20.0), item("blue", "L", 90.0)];
    let delta = index.index_document(1, &elements(&doc), PARAMS).unwrap();

    assert_eq!(delta.new_terminals, 2);
    assert_eq!(delta.records, 2);
    assert_eq!(numeric_terminal(&index, "red", "M").num_entries(), 1);
    assert_eq!(numeric_terminal(&index, "blue", "L").num_entries(), 1);
}

#[test]
fn elements_of_one_document_sharing_a_path_all_land_in_its_terminal() {
    let mut index = priced(None);
    let doc = [item("red", "M", 35.0), item("red", "M", 15.0)];
    let delta = index.index_document(3, &elements(&doc), PARAMS).unwrap();

    assert_eq!(delta.new_terminals, 1);
    let tree = numeric_terminal(&index, "red", "M");
    assert_eq!(tree.num_entries(), 2);
    assert!(tree.has_multivalued_docs());
}

#[test]
fn later_documents_reuse_existing_terminals() {
    let mut index = priced(None);
    index
        .index_document(1, &elements(&[item("red", "M", 20.0)]), PARAMS)
        .unwrap();
    let delta = index
        .index_document(2, &elements(&[item("red", "M", 25.0)]), PARAMS)
        .unwrap();

    assert_eq!(delta.new_terminals, 0);
    assert_eq!(numeric_terminal(&index, "red", "M").num_entries(), 2);
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn numeric_deltas_add_up_to_the_trees_postings() {
    // Enough distinct values to split leaves, with inner ranges kept two levels deep, so the
    // tree re-adds entries on splits and drops some on rebalances. The `unittest` feature makes
    // the tree check each add's record delta against its own records.
    let params = IndexParams { max_depth_range: 2 };
    let mut index = priced(None);
    let total = sum((1..=2_000u64).map(|doc| {
        index
            .index_document(doc, &elements(&[item("red", "M", doc as f64)]), params)
            .unwrap()
    }));
    let tree = numeric_terminal(&index, "red", "M");

    assert!(
        tree.num_ranges() > 1,
        "the values should have split the tree"
    );
    // The deltas include the new tree's first range, so they sum to its whole postings size.
    assert_eq!(total.size_delta, tree.inverted_indexes_size() as i64);
    let blocks: usize = tree
        .iter()
        .filter_map(|node| node.range())
        .map(|range| range.entries().num_blocks())
        .sum();
    assert_eq!(total.block_delta, blocks as i64);
    // Inner ranges hold copies of their children's entries, so the tree's records are those of
    // every range, not its documents.
    let records: usize = tree
        .iter()
        .filter_map(|node| node.range())
        .map(|range| range.num_entries())
        .sum();
    assert_eq!(total.records, records as i64);
    assert_eq!(index.num_records() as i64, total.records);
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn tag_deltas_add_up_to_the_posting_lists() {
    let mut index = materials();
    let path = [tag("red")];
    let total = sum((1..=300u64).map(|doc| {
        // Every document has steel; every third also has wood.
        let mut doc_elements = vec![Element {
            path: &path,
            value: tag_value("steel"),
        }];
        if doc % 3 == 0 {
            doc_elements.push(Element {
                path: &path,
                value: tag_value("wood"),
            });
        }
        index.index_document(doc, &doc_elements, PARAMS).unwrap()
    }));
    let tags = tag_terminal(&index, "red");
    let steel = tags.find_value(b"steel").unwrap();
    let wood = tags.find_value(b"wood").unwrap();

    assert_eq!(total.records, 300 + 100);
    assert_eq!(index.num_records(), 400);
    assert_eq!(steel.unique_docs() + wood.unique_docs(), 400);
    assert_eq!(
        total.size_delta,
        (steel.memory_usage() + wood.memory_usage()) as i64
    );
    assert_eq!(
        total.block_delta,
        (steel.number_of_blocks() + wood.number_of_blocks()) as i64
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn float_compression_stores_values_in_less_memory() {
    let size_with = |compression| {
        let mut index = NestedIndex::new(
            vec![Level::Tag(TagLevel::new())],
            TerminalKind::Numeric(compression),
        )
        .unwrap();
        let path = [tag("red")];
        let total = sum((1..=500u64).map(|doc| {
            // Not exact as an f32, but within the compression threshold of one: stored as an
            // f64 without compression and as an f32 with it.
            let value = TerminalValue::Numeric(doc as f64 + 0.1);
            index
                .index_document(doc, &[Element { path: &path, value }], PARAMS)
                .unwrap()
        }));
        total.size_delta
    };
    assert!(size_with(FloatCompression::On) < size_with(FloatCompression::Off));
}

#[test]
fn infinite_values_are_ordinary_numbers() {
    let mut index = priced(None);
    let doc = [
        item("red", "M", f64::INFINITY),
        item("red", "M", f64::NEG_INFINITY),
    ];
    index.index_document(1, &elements(&doc), PARAMS).unwrap();
    assert_eq!(numeric_terminal(&index, "red", "M").num_entries(), 2);
}

#[test]
fn a_document_without_elements_changes_nothing() {
    let mut index = priced(None);
    assert_eq!(
        index.index_document(1, &[], PARAMS).unwrap(),
        IndexDelta::default()
    );
    assert!(index.routing().is_empty());
}

#[test]
fn a_tag_terminal_counts_each_value_once_per_document() {
    let mut index = materials();
    let path = [tag("red")];
    let doc = [
        Element {
            path: &path,
            value: tag_value("steel"),
        },
        Element {
            path: &path,
            value: tag_value("steel"),
        },
        Element {
            path: &path,
            value: tag_value("wood"),
        },
    ];
    let delta = index.index_document(1, &doc, PARAMS).unwrap();

    assert_eq!(delta.records, 2);
    let tags = tag_terminal(&index, "red");
    assert_eq!(tags.n_tags(), 2);
    assert_eq!(tags.find_value(b"steel").unwrap().unique_docs(), 1);
}

#[test]
fn rejected_documents_leave_the_index_unchanged() {
    let red_m = [tag("red"), tag("M")];
    let red = [tag("red")];
    let nan = TerminalValue::Numeric(f64::NAN);
    let one = TerminalValue::Numeric(1.0);
    let cases: Vec<(&str, NestedIndex, Vec<Element<'_>>, IndexError)> = vec![
        (
            "a tag value for a numeric terminal",
            priced(None),
            vec![
                Element {
                    path: &red_m,
                    value: one,
                },
                Element {
                    path: &red_m,
                    value: tag_value("x"),
                },
            ],
            IndexError::ValueMismatch { element: 1 },
        ),
        (
            "a numeric value for a tag terminal",
            materials(),
            vec![
                Element {
                    path: &red,
                    value: tag_value("steel"),
                },
                Element {
                    path: &red,
                    value: one,
                },
            ],
            IndexError::ValueMismatch { element: 1 },
        ),
        (
            "a NaN alone",
            priced(None),
            vec![Element {
                path: &red_m,
                value: nan,
            }],
            IndexError::NotANumber { element: 0 },
        ),
        (
            "a NaN before a valid value",
            priced(None),
            vec![
                Element {
                    path: &red_m,
                    value: nan,
                },
                Element {
                    path: &red_m,
                    value: one,
                },
            ],
            IndexError::NotANumber { element: 0 },
        ),
        (
            "a NaN after a valid value",
            priced(None),
            vec![
                Element {
                    path: &red_m,
                    value: one,
                },
                Element {
                    path: &red_m,
                    value: nan,
                },
            ],
            IndexError::NotANumber { element: 1 },
        ),
        (
            "a path that does not route, after one that does",
            priced(None),
            vec![
                Element {
                    path: &red_m,
                    value: one,
                },
                Element {
                    path: &red_m[..1],
                    value: one,
                },
            ],
            IndexError::Path {
                element: 1,
                error: PathError::WrongLength {
                    expected: 2,
                    got: 1,
                },
            },
        ),
    ];
    for (name, mut index, doc, expected) in cases {
        assert_eq!(
            index.index_document(1, &doc, PARAMS).err(),
            Some(expected),
            "{name}"
        );
        assert!(index.routing().is_empty(), "{name}");
        assert_eq!(index.num_records(), 0, "{name}");
    }
}

#[test]
fn a_terminals_overhead_does_not_grow_with_its_postings() {
    // Postings are counted through the deltas, so overhead that grew with them would count
    // their bytes twice. A tag terminal's overhead grows with its distinct values only.
    let mut prices = priced(None);
    let mut materials = materials();
    let red_m = [tag("red"), tag("M")];
    let red = [tag("red")];
    let numeric_doc = |price| {
        [Element {
            path: &red_m,
            value: TerminalValue::Numeric(price),
        }]
    };
    let tag_doc = [Element {
        path: &red,
        value: tag_value("steel"),
    }];
    prices.index_document(1, &numeric_doc(1.0), PARAMS).unwrap();
    materials.index_document(1, &tag_doc, PARAMS).unwrap();
    let numeric_overhead = prices.routing().get(&red_m).unwrap().unwrap().mem_usage();
    let tag_overhead = materials.routing().get(&red).unwrap().unwrap().mem_usage();

    for doc in 2..=200u64 {
        prices
            .index_document(doc, &numeric_doc(doc as f64), PARAMS)
            .unwrap();
        materials.index_document(doc, &tag_doc, PARAMS).unwrap();
    }
    assert_eq!(
        prices.routing().get(&red_m).unwrap().unwrap().mem_usage(),
        numeric_overhead
    );
    assert_eq!(
        materials.routing().get(&red).unwrap().unwrap().mem_usage(),
        tag_overhead
    );
}

#[test]
fn a_rejected_documents_id_stays_free() {
    let mut index = priced(None);
    let rejected = [item("red", "M", f64::NAN)];
    assert!(
        index
            .index_document(1, &elements(&rejected), PARAMS)
            .is_err()
    );
    assert!(
        index
            .index_document(1, &elements(&[item("red", "M", 20.0)]), PARAMS)
            .is_ok()
    );
}

#[test]
fn a_document_breaking_a_cardinality_limit_is_not_indexed_at_all() {
    // One size per color; the document brings two for red.
    let mut index = priced(Some(1));
    let doc = [item("red", "M", 20.0), item("red", "L", 30.0)];
    assert_eq!(
        index.index_document(1, &elements(&doc), PARAMS).err(),
        Some(IndexError::CardinalityExceeded { level: 1, max: 1 })
    );
    assert!(index.routing().is_empty());
}

#[test]
fn a_cardinality_limit_counts_values_from_earlier_documents() {
    let mut index = priced(Some(1));
    index
        .index_document(1, &elements(&[item("red", "M", 20.0)]), PARAMS)
        .unwrap();
    assert_eq!(
        index
            .index_document(2, &elements(&[item("red", "L", 30.0)]), PARAMS)
            .err(),
        Some(IndexError::CardinalityExceeded { level: 1, max: 1 })
    );
    assert_eq!(index.routing().num_terminals(), 1);
}
