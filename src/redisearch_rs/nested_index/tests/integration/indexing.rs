/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{Constraint, InsertError, WalkLimits};
use nested_index::{Element, IndexError, Terminal, TerminalValue};

use crate::util::{PARAMS, elements, item, materials, priced, tag, tag_value};

fn numeric_terminal<'a>(
    index: &'a nested_index::NestedIndex,
    color: &str,
    size: &str,
) -> &'a numeric_range_tree::NumericRangeTree {
    match index.routing().terminal(&[tag(color), tag(size)]).unwrap() {
        Some(Terminal::Numeric(tree)) => tree,
        other => panic!("expected a numeric terminal, got {}", other.is_some()),
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
    let Some(Terminal::Tag(tags)) = index.routing().terminal(&path).unwrap() else {
        panic!("expected a tag terminal");
    };
    assert_eq!(tags.n_tags(), 2);
    assert_eq!(tags.find_value(b"steel").unwrap().unique_docs(), 1);
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
    let cases: &[(&str, Vec<Element<'_>>, IndexError)] = &[
        (
            "a tag value for a numeric terminal",
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
            "a NaN value",
            vec![Element {
                path: &red_m,
                value: TerminalValue::Numeric(f64::NAN),
            }],
            IndexError::NotANumber { element: 0 },
        ),
        (
            "a path that does not route",
            vec![Element {
                path: &red_m[..1],
                value: TerminalValue::Numeric(1.0),
            }],
            IndexError::Path(InsertError::Path(compound_index::PathError::WrongLength {
                expected: 2,
                got: 1,
            })),
        ),
    ];
    for (name, doc, expected) in cases {
        let mut index = priced(None);
        assert_eq!(
            index.index_document(1, doc, PARAMS).err(),
            Some(expected.clone()),
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
