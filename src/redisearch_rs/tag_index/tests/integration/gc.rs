/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Garbage collection of a whole tag index: [`TagIndex::scan_gc`] and [`TagIndex::apply_gc`].

use inverted_index::DocId;
use tag_index::{InMemoryMode, TagGcOutcome, TagIndex};

use crate::util::{commit_mem, index_mem, value_iter_keys};

/// `red` on documents 1 to 4, `blue` on document 2 only.
fn red_and_blue(with_suffix: bool) -> TagIndex<InMemoryMode> {
    let mut index = TagIndex::<InMemoryMode>::new(with_suffix);
    for doc_id in 1..=4 {
        let tags: &[&[u8]] = if doc_id == 2 {
            &[b"red", b"blue"]
        } else {
            &[b"red"]
        };
        index_mem(&mut index, tags, doc_id);
        commit_mem(&mut index, tags);
    }
    index
}

const fn gone(ids: &'static [DocId]) -> impl Fn(DocId) -> bool + Copy {
    move |id| !ids.contains(&id)
}

/// Scan and apply every value, as a collector holding the index throughout does.
fn collect(
    index: &mut TagIndex<InMemoryMode>,
    live: impl Fn(DocId) -> bool + Copy,
) -> Vec<TagGcOutcome> {
    let scan = index.scan_gc(live);
    assert!(scan.failed.is_empty());
    scan.deltas
        .into_iter()
        .filter_map(|value| {
            index.apply_gc(&value.tag, value.list_unique_id, value.delta, Some(&live))
        })
        .collect()
}

#[test]
fn a_scan_finds_only_the_values_holding_a_gone_document() {
    let index = red_and_blue(false);
    let scan = index.scan_gc(gone(&[3]));
    let tags: Vec<_> = scan.deltas.iter().map(|d| d.tag.as_slice()).collect();
    assert_eq!(tags, [b"red".as_slice()]);
    assert_eq!(scan.values_visited, 2);
    assert!(index.scan_gc(gone(&[])).deltas.is_empty());
}

#[test]
fn an_emptied_value_is_removed() {
    let mut index = red_and_blue(false);
    let outcomes = collect(&mut index, gone(&[2, 3]));
    assert_eq!(outcomes.len(), 2);
    let blue = outcomes
        .iter()
        .find(|o| o.removed.is_some())
        .expect("blue lost its only document");
    assert_eq!(blue.info.entries_removed, 1);
    assert_eq!(value_iter_keys(index.value_iter()), [b"red".to_vec()]);
    assert_eq!(
        index.find_value(b"red").map(|list| list.unique_docs()),
        Some(2)
    );
}

#[test]
fn an_emptied_value_leaves_the_suffix_trie_too() {
    let mut index = red_and_blue(true);
    assert!(index.suffix_contains(b"lue"));
    let outcomes = collect(&mut index, gone(&[2]));
    assert!(outcomes.iter().any(|o| o.removed.is_some()));
    assert_eq!(value_iter_keys(index.value_iter()), [b"red".to_vec()]);
    assert!(
        !index.suffix_contains(b"lue"),
        "blue's suffixes went with it"
    );
    assert!(!index.suffix_contains(b"blue"));
    assert!(index.suffix_contains(b"ed"), "red's suffixes stay");
}

#[test]
fn a_delta_is_applied_only_to_the_list_it_was_scanned_from() {
    let mut index = red_and_blue(false);
    let mut deltas = index.scan_gc(gone(&[1])).deltas;
    let red = deltas.pop().expect("red holds document 1");
    assert!(
        index
            .apply_gc(b"green", red.list_unique_id, red.delta, None)
            .is_none()
    );

    let red = index
        .scan_gc(gone(&[1]))
        .deltas
        .pop()
        .expect("nothing was applied");
    let wrong_id = red.list_unique_id.wrapping_add(1);
    assert!(
        index
            .apply_gc(&red.tag, wrong_id, red.delta, None)
            .is_none()
    );

    let red = index
        .scan_gc(gone(&[1]))
        .deltas
        .pop()
        .expect("nothing was applied");
    let outcome = index
        .apply_gc(&red.tag, red.list_unique_id, red.delta, None)
        .expect("the list is the one scanned");
    assert_eq!(outcome.info.entries_removed, 1);
}

#[test]
fn a_posting_list_that_fails_to_decode_is_reported_with_its_span() {
    let mut index = red_and_blue(false);
    let red = index.find_value_mut(b"red").expect("red is indexed");
    // A varint whose continuation bit promises a byte that is not there.
    inverted_index::test_utils::overwrite_block_buffer(red, 0, &[0x80]);

    let scan = index.scan_gc(gone(&[3]));
    assert_eq!(scan.failed, [1..=4]);
    assert!(scan.deltas.is_empty());

    let snapshot = index
        .gc_snapshot_value(b"red", &gone(&[3]))
        .expect("red holds the gone document");
    let Err((_, span)) = snapshot.scan(gone(&[3])) else {
        panic!("the copy of a corrupt list fails to decode too");
    };
    assert_eq!(span, Some(1..=4));
}

#[test]
fn a_snapshot_walk_resumes_after_the_last_value_it_visited() {
    let index = red_and_blue(false);
    let live = gone(&[2]);
    let mut found = Vec::new();
    let mut stop_after_one = || true;
    let walk = index.gc_snapshots_after(None, &live, &mut stop_after_one, &mut found);
    assert!(walk.stopped);
    assert_eq!(
        (walk.visited, walk.resume_after.as_deref()),
        (1, Some(b"blue".as_slice()))
    );
    // Stopped on the last value: nothing is left to resume.
    let walk = index.gc_snapshots_after(
        walk.resume_after.as_deref(),
        &live,
        &mut stop_after_one,
        &mut found,
    );
    assert!(walk.stopped);
    assert_eq!((walk.visited, walk.resume_after), (1, None));
    let tags: Vec<_> = found
        .iter()
        .map(|snapshot| snapshot.tag.as_slice())
        .collect();
    assert_eq!(tags, [b"blue".as_slice(), b"red".as_slice()]);
}
