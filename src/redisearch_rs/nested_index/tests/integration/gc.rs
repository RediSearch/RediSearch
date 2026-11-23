/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! What a garbage collector does to an index: remove postings through
//! [`NestedIndex::collect_terminal`], which counts the records removed, then
//! drop the emptied terminals with [`NestedIndex::finish_collection`], which revokes each dropped
//! terminal's token.

use nested_index::{Element, NestedIndex, PathStep, PostingsMut, PrunedTerminals, Terminal};
use rqe_core::DocId;

use crate::util::{PARAMS, elements, item, materials, priced, tag, tag_value};

/// Remove every posting of a document `live` says is gone, through
/// [`NestedIndex::collect_terminal`] as a collector does, and return how many were removed.
fn collect(index: &mut NestedIndex, live: impl Fn(DocId) -> bool + Copy) -> usize {
    let mut paths: Vec<Vec<PathStep>> = Vec::new();
    index
        .routing()
        .for_each_terminal(|path, _| paths.push(path.to_vec()));
    let mut removed = 0;
    for path in paths {
        removed += index
            .collect_terminal(&path, |terminal| {
                let removed = collect_postings(terminal, live);
                (removed, removed)
            })
            .expect("the path was just listed");
    }
    removed
}

fn collect_postings(terminal: &mut Terminal, live: impl Fn(DocId) -> bool + Copy) -> usize {
    let mut removed = 0;
    match terminal.postings_mut() {
        PostingsMut::Numeric(tree) => {
            let deltas: Vec<_> = tree
                .indexed_post_order_iter()
                .filter_map(|(node, n)| n.scan_gc(live).map(|delta| (node, delta)))
                .collect();
            for (node, delta) in deltas {
                let applied = tree
                    .apply_gc_to_node_with(node, delta, None)
                    .expect("nothing changed the tree since the scan");
                removed += applied.index_gc_info.entries_removed;
            }
        }
        PostingsMut::Tag(tags) => {
            for value in tags.scan_gc(live).deltas {
                let outcome = tags
                    .apply_gc(&value.tag, value.list_unique_id, value.delta, None)
                    .expect("nothing changed the index since the scan");
                removed += outcome.info.entries_removed;
            }
        }
    }
    removed
}

/// Drop what [`collect`] emptied.
fn finish(index: &mut NestedIndex) -> PrunedTerminals {
    index.finish_collection(usize::MAX, |_| {})
}

#[test]
fn emptied_numeric_terminals_are_dropped_with_their_routing_nodes() {
    let mut index = priced(None);
    let docs = [
        [item("red", "m", 1.0)],
        [item("red", "l", 2.0)],
        [item("blue", "m", 3.0)],
    ];
    for (doc_id, items) in (1..).zip(&docs) {
        index
            .index_document(doc_id, &elements(items), PARAMS)
            .unwrap();
    }
    assert_eq!(index.routing().num_terminals(), 3);
    // Document 2 holds red L alone, document 3 everything under blue.
    assert_eq!(collect(&mut index, |id| id == 1), 2);
    let pruned = finish(&mut index);
    assert_eq!(pruned.terminals, 2);
    assert!(pruned.bytes > 0, "an emptied tree still holds its range");
    assert_eq!(index.routing().num_terminals(), 1);
    assert!(
        index
            .routing()
            .get(&[tag("red"), tag("m")])
            .unwrap()
            .is_some()
    );
    assert!(
        index
            .routing()
            .get(&[tag("red"), tag("l")])
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .routing()
            .get(&[tag("blue"), tag("m")])
            .unwrap()
            .is_none()
    );
    assert_eq!(index.num_records(), 1);
}

#[test]
fn a_terminal_with_postings_left_is_kept() {
    let mut index = priced(None);
    index
        .index_document(1, &elements(&[item("red", "m", 1.0)]), PARAMS)
        .unwrap();
    assert_eq!(finish(&mut index), PrunedTerminals::default());

    // A terminal that lost some postings but not all is kept.
    index
        .index_document(2, &elements(&[item("red", "m", 2.0)]), PARAMS)
        .unwrap();
    assert_eq!(collect(&mut index, |id| id == 1), 1);
    assert_eq!(finish(&mut index), PrunedTerminals::default());
}

#[test]
fn emptied_tag_terminals_are_dropped() {
    let mut index = materials();
    for (doc_id, color) in [(1, "red"), (2, "blue")] {
        let path = [tag(color)];
        let element = Element {
            path: &path,
            value: tag_value("steel"),
        };
        index.index_document(doc_id, &[element], PARAMS).unwrap();
    }
    assert_eq!(collect(&mut index, |id| id == 1), 1);
    let pruned = finish(&mut index);
    assert_eq!(pruned.terminals, 1);
    // The emptied value went with the collection; nothing is left to count.
    assert_eq!((pruned.bytes, pruned.blocks), (0, 0));
    assert!(index.routing().get(&[tag("blue")]).unwrap().is_none());
    assert_eq!(index.num_records(), 1);
}

#[test]
fn dropping_a_terminal_revokes_its_token_only() {
    let mut index = priced(None);
    for (doc_id, color) in [(1, "red"), (2, "blue")] {
        index
            .index_document(doc_id, &elements(&[item(color, "m", 1.0)]), PARAMS)
            .unwrap();
    }
    let token = |index: &NestedIndex, color: &str| {
        let terminal = index.routing().get(&[tag(color), tag("m")]).unwrap();
        terminal.expect("the terminal exists").alive().clone()
    };
    let (red, blue) = (token(&index, "red"), token(&index, "blue"));
    collect(&mut index, |id| id == 1);
    assert_eq!(finish(&mut index).terminals, 1);
    assert!(red.is_alive(), "red keeps its postings and its token");
    assert!(!blue.is_alive(), "blue's iterators must stop reading it");
}

#[test]
fn a_dropped_terminal_gives_back_exactly_what_it_held() {
    let mut index = priced(None);
    for doc_id in 1..=3 {
        index
            .index_document(
                doc_id,
                &elements(&[item("red", "m", doc_id as f64)]),
                PARAMS,
            )
            .unwrap();
    }
    let path = [PathStep::Tag(b"red".to_vec()), PathStep::Tag(b"m".to_vec())];
    collect(&mut index, |_| false);
    let terminal = index
        .routing()
        .terminal_at(&path)
        .expect("kept until finished");
    let (bytes, blocks) = (terminal.postings_size(), terminal.num_blocks());
    let pruned = finish(&mut index);
    assert_eq!(
        (pruned.terminals, pruned.bytes, pruned.blocks),
        (1, bytes, blocks)
    );
    assert_eq!(index.num_records(), 0);
}

#[test]
fn a_collection_left_unfinished_is_finished_by_the_next() {
    let mut index = priced(None);
    index
        .index_document(1, &elements(&[item("red", "m", 1.0)]), PARAMS)
        .unwrap();
    collect(&mut index, |_| false);
    // The collector stopped before finishing: the emptied terminal waits.
    assert!(index.has_collected());
    assert_eq!(index.routing().num_terminals(), 1);
    // The next collection reaches nothing, yet its finish drops it.
    assert_eq!(finish(&mut index).terminals, 1);
    assert!(!index.has_collected());
    assert!(index.routing().is_empty());
}

#[test]
fn finishing_sees_every_terminal_reached_and_only_those() {
    let mut index = priced(None);
    for (doc_id, color) in [(1, "red"), (2, "blue")] {
        index
            .index_document(doc_id, &elements(&[item(color, "m", 1.0)]), PARAMS)
            .unwrap();
    }
    let red = [PathStep::Tag(b"red".to_vec()), PathStep::Tag(b"m".to_vec())];
    index.collect_terminal(&red, |_| ((), 0)).unwrap();
    let mut seen = 0;
    let pruned = index.finish_collection(usize::MAX, |_| seen += 1);
    assert_eq!(seen, 1, "blue was not reached");
    assert_eq!(pruned.terminals, 0, "red still holds its record");
}

#[test]
fn a_terminal_whose_inner_ranges_hold_entries_is_kept_until_they_go() {
    // With ranges kept on inner nodes, a split leaves copies of the leaves' entries above them.
    let params = nested_index::IndexParams { max_depth_range: 2 };
    let mut index = priced(None);
    let path = [PathStep::Tag(b"red".to_vec()), PathStep::Tag(b"m".to_vec())];
    let has_inner_range = |index: &NestedIndex| {
        let tree = index
            .routing()
            .terminal_at(&path)
            .unwrap()
            .numeric()
            .unwrap();
        tree.iter()
            .any(|node| !node.is_leaf() && node.range().is_some())
    };
    let mut doc_id = 0;
    while doc_id == 0 || !has_inner_range(&index) {
        doc_id += 1;
        index
            .index_document(
                doc_id,
                &elements(&[item("red", "m", doc_id as f64)]),
                params,
            )
            .unwrap();
    }

    // Every leaf emptied, the inner ranges left as they were.
    let gone = |_| false;
    index
        .collect_terminal(&path, |terminal| {
            let PostingsMut::Numeric(tree) = terminal.postings_mut() else {
                panic!("a numeric terminal");
            };
            let leaves: Vec<_> = tree
                .indexed_post_order_iter()
                .filter(|(_, node)| node.is_leaf())
                .filter_map(|(idx, node)| node.scan_gc(gone).map(|delta| (idx, delta)))
                .collect();
            let removed: usize = leaves
                .into_iter()
                .map(|(idx, delta)| {
                    tree.apply_gc_to_node_with(idx, delta, None)
                        .unwrap()
                        .index_gc_info
                        .entries_removed
                })
                .sum();
            ((), removed)
        })
        .unwrap();
    assert_eq!(
        finish(&mut index).terminals,
        0,
        "the inner ranges still hold records"
    );
    assert!(index.num_records() > 0);

    // Once the inner ranges go too, so does the terminal, with every record.
    collect(&mut index, gone);
    assert_eq!(finish(&mut index).terminals, 1);
    assert_eq!(index.num_records(), 0);
}
