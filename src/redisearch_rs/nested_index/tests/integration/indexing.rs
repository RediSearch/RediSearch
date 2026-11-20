/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{Constraint, InsertError, PathError, WalkLimits};
use nested_index::{
    Element, FloatCompression, IndexDelta, IndexError, IndexParams, NestedIndex, Terminal,
    TerminalKind, TerminalValue,
};
use numeric_range_tree::NumericRangeTree;

use crate::util::{PARAMS, elements, item, materials, priced, tag, tag_value};

fn numeric_terminal<'a>(index: &'a NestedIndex, color: &str, size: &str) -> &'a NumericRangeTree {
    match index.routing().get(&[tag(color), tag(size)]).unwrap() {
        Some(Terminal::Numeric(tree)) => tree,
        other => panic!("expected a numeric terminal at ({color}, {size}), got {other:?}"),
    }
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
    assert_eq!(index.num_records(), 2);
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
fn numeric_deltas_add_up_to_the_trees_own_figures() {
    // Enough distinct values to split leaves, with inner ranges kept two levels deep, so the
    // tree re-adds entries on splits and drops some on rebalances.
    let params = IndexParams { max_depth_range: 2 };
    let mut index = priced(None);
    let mut total = IndexDelta::default();
    for doc in 1..=2_000u64 {
        let delta = index
            .index_document(doc, &elements(&[item("red", "M", doc as f64)]), params)
            .unwrap();
        total.records += delta.records;
        total.size_delta += delta.size_delta;
        total.block_delta += delta.block_delta;
    }
    let tree = numeric_terminal(&index, "red", "M");

    assert!(
        tree.num_ranges() > 1,
        "the values should have split the tree"
    );
    // The `unittest` feature makes the tree check each add's delta against its own records.
    assert_eq!(index.num_records() as i64, total.records);
    // The deltas include the new tree's first range, so they sum to its whole postings size.
    assert_eq!(total.size_delta, tree.inverted_indexes_size() as i64);
    assert!(total.block_delta > 0);
}

#[test]
fn compressed_numeric_terminals_index_values() {
    let mut index = NestedIndex::new(
        vec![compound_index::Level::Tag(compound_index::TagLevel::new())],
        TerminalKind::Numeric(FloatCompression::On),
    )
    .unwrap();
    let path = [tag("red")];
    let doc = [Element {
        path: &path,
        value: TerminalValue::Numeric(1.5),
    }];
    index.index_document(1, &doc, PARAMS).unwrap();

    let Some(Terminal::Numeric(tree)) = index.routing().get(&path).unwrap() else {
        panic!("expected a numeric terminal");
    };
    assert_eq!(tree.num_entries(), 1);
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
    assert!(delta.size_delta > 0);
    let Some(Terminal::Tag(tags)) = index.routing().get(&path).unwrap() else {
        panic!("expected a tag terminal");
    };
    assert_eq!(tags.n_tags(), 2);
    assert_eq!(tags.find_value(b"steel").unwrap().unique_docs(), 1);
}

#[test]
fn terminals_report_their_memory() {
    let mut index = priced(None);
    index
        .index_document(1, &elements(&[item("red", "M", 20.0)]), PARAMS)
        .unwrap();
    let terminal = index
        .routing()
        .get(&[tag("red"), tag("M")])
        .unwrap()
        .unwrap();
    assert!(terminal.mem_usage() > 0);
}

#[test]
fn walks_find_the_terminals_of_the_indexed_elements() {
    let mut index = priced(None);
    index
        .index_document(
            1,
            &elements(&[item("red", "M", 20.0), item("blue", "L", 90.0)]),
            PARAMS,
        )
        .unwrap();
    let red = index
        .routing()
        .collect_terminals(
            &[Constraint::TagIn(vec![b"red"]), Constraint::Any],
            WalkLimits::UNLIMITED,
        )
        .unwrap();
    assert_eq!(red.len(), 1);
}

#[test]
fn rejected_documents_leave_the_index_unchanged() {
    let red_m = [tag("red"), tag("M")];
    let red = [tag("red")];
    let cases: Vec<(&str, NestedIndex, Vec<Element<'_>>, IndexError)> = vec![
        (
            "a tag value for a numeric terminal",
            priced(None),
            vec![
                Element {
                    path: &red_m,
                    value: TerminalValue::Numeric(1.0),
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
                    value: TerminalValue::Numeric(1.0),
                },
            ],
            IndexError::ValueMismatch { element: 1 },
        ),
        (
            "a NaN after a valid value",
            priced(None),
            vec![
                Element {
                    path: &red_m,
                    value: TerminalValue::Numeric(1.0),
                },
                Element {
                    path: &red_m,
                    value: TerminalValue::Numeric(f64::NAN),
                },
            ],
            IndexError::NotANumber { element: 1 },
        ),
        (
            "a path that does not route",
            priced(None),
            vec![Element {
                path: &red_m[..1],
                value: TerminalValue::Numeric(1.0),
            }],
            IndexError::Path(InsertError::Path(PathError::WrongLength {
                expected: 2,
                got: 1,
            })),
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
fn a_document_breaking_a_cardinality_limit_is_not_indexed_at_all() {
    // One size per color; the document brings two for red.
    let mut index = priced(Some(1));
    let doc = [item("red", "M", 20.0), item("red", "L", 30.0)];
    assert_eq!(
        index.index_document(1, &elements(&doc), PARAMS).err(),
        Some(IndexError::Path(InsertError::CardinalityExceeded {
            level: 1,
            max: 1
        }))
    );
    assert!(index.routing().is_empty());
    assert_eq!(index.num_records(), 0);
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
        Some(IndexError::Path(InsertError::CardinalityExceeded {
            level: 1,
            max: 1
        }))
    );
    assert_eq!(index.routing().num_terminals(), 1);
    assert_eq!(index.num_records(), 1);
}

#[test]
fn doc_ids_must_increase_across_documents() {
    let mut index = priced(None);
    index
        .index_document(5, &elements(&[item("red", "M", 20.0)]), PARAMS)
        .unwrap();
    for doc_id in [5, 4] {
        assert_eq!(
            index
                .index_document(doc_id, &elements(&[item("blue", "L", 1.0)]), PARAMS)
                .err(),
            Some(IndexError::DocIdNotIncreasing { doc_id, last: 5 })
        );
    }
    assert_eq!(index.routing().num_terminals(), 1);
}
