/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot tests for the lock-free [`State`](crate::index::state::State) field on
//! `InvertedIndex`. During Story 1.1 of Epic 1 the state is a parallel mirror of the
//! `blocks` `ThinVec`; these tests check that it stays consistent across every mutation
//! path (initial construction, `add_record` in all its sub-cases, and `apply_gc`).

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;

use crate::{Encoder, IndexBlock, InvertedIndex};

use super::Dummy;

/// Assert that the `state` snapshot matches `blocks`: empty `sealed`, `pending` mirrors
/// every block except the last, and `in_progress` is the last block (or `None` if empty).
fn assert_state_mirrors_blocks<E: Encoder>(ii: &InvertedIndex<E>) {
    let state = ii.state.load_full();

    assert!(
        state.sealed.is_empty(),
        "sealed must be empty during the dual-write phase (Stories 1.1-1.3)"
    );

    let expected_pending_len = ii.blocks.len().saturating_sub(1);
    assert_eq!(
        state.pending.len(),
        expected_pending_len,
        "pending must mirror blocks[..len-1]"
    );
    for (i, arc) in state.pending.iter().enumerate() {
        assert_eq!(
            &**arc, &ii.blocks[i],
            "pending[{i}] must equal blocks[{i}]"
        );
    }

    match (ii.blocks.last(), state.in_progress.as_ref()) {
        (None, None) => {}
        (Some(last), Some(ip)) => assert_eq!(
            &**ip, last,
            "in_progress must equal blocks.last()"
        ),
        (None, Some(_)) => panic!("in_progress is Some but blocks is empty"),
        (Some(_), None) => panic!("in_progress is None but blocks is non-empty"),
    }
}

#[test]
fn empty_index_has_empty_state() {
    let ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let state = ii.state.load_full();

    assert!(state.sealed.is_empty());
    assert!(state.pending.is_empty());
    assert!(state.in_progress.is_none());
    assert_eq!(state.len(), 0);
}

#[test]
fn first_record_promotes_in_progress() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    ii.add_record(&record).unwrap();

    let state = ii.state.load_full();
    assert!(state.sealed.is_empty());
    assert!(state.pending.is_empty());
    let ip = state.in_progress.as_ref().expect("must have in_progress");
    assert_eq!(ip.first_doc_id, 10);
    assert_eq!(ip.last_doc_id, 10);
    assert_eq!(ip.num_entries, 1);
    assert_state_mirrors_blocks(&ii);
}

#[test]
fn appending_to_same_block_only_updates_in_progress() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    for id in 10..15 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    let state = ii.state.load_full();
    assert!(state.pending.is_empty(), "no block rolled over");
    let ip = state.in_progress.as_ref().unwrap();
    assert_eq!(ip.first_doc_id, 10);
    assert_eq!(ip.last_doc_id, 14);
    assert_eq!(ip.num_entries, 5);
    assert_state_mirrors_blocks(&ii);
}

#[test]
fn block_fill_moves_previous_in_progress_into_pending() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // Dummy::RECOMMENDED_BLOCK_ENTRIES is the default 100. Write enough records to
    // force a roll-over to a second block.
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    for id in 1..=(entries_per_block + 1) {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert_eq!(ii.blocks.len(), 2, "should have rolled to a second block");

    let state = ii.state.load_full();
    assert_eq!(state.pending.len(), 1, "first block is now pending");
    assert_eq!(state.pending[0].first_doc_id, 1);
    assert_eq!(state.pending[0].num_entries as u64, entries_per_block);

    let ip = state.in_progress.as_ref().unwrap();
    assert_eq!(ip.num_entries, 1);
    assert_eq!(ip.first_doc_id, entries_per_block + 1);
    assert_state_mirrors_blocks(&ii);
}

#[test]
fn pending_grows_monotonically_across_multiple_rollovers() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;

    // Roll over three blocks: two full + one partial.
    let total = entries_per_block * 2 + 5;
    for id in 1..=total {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert_eq!(ii.blocks.len(), 3);

    let state = ii.state.load_full();
    assert_eq!(state.pending.len(), 2);
    assert_eq!(state.pending[0].first_doc_id, 1);
    assert_eq!(state.pending[1].first_doc_id, entries_per_block + 1);
    let ip = state.in_progress.as_ref().unwrap();
    assert_eq!(ip.first_doc_id, 2 * entries_per_block + 1);
    assert_eq!(ip.num_entries, 5);
    assert_state_mirrors_blocks(&ii);
}

#[test]
fn snapshot_taken_before_write_is_not_mutated_by_later_writes() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();

    // Take a snapshot before the next write.
    let before = ii.state.load_full();
    let before_ip = before
        .in_progress
        .as_ref()
        .expect("first add already happened")
        .clone();
    assert_eq!(before_ip.num_entries, 1);

    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();

    // The snapshot's Arc'd `IndexBlock` must remain unchanged — readers holding it
    // shouldn't see a torn write.
    assert_eq!(
        before_ip.num_entries, 1,
        "snapshot taken before the second write is immutable"
    );
    assert_eq!(before_ip.last_doc_id, 1);

    // The live state reflects the new write.
    let after = ii.state.load_full();
    let after_ip = after.in_progress.as_ref().unwrap();
    assert_eq!(after_ip.num_entries, 2);
    assert_eq!(after_ip.last_doc_id, 2);
}

#[test]
fn sealed_remains_empty_after_writes() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    for id in 1..50 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    let state = ii.state.load_full();
    assert!(
        state.sealed.is_empty(),
        "only GC writes to sealed; Story 1.4 introduces that"
    );
}

#[test]
fn from_blocks_initializes_state_to_match() {
    use thin_vec::ThinVec;

    let mut blocks: ThinVec<IndexBlock, crate::BlockCapacity> = ThinVec::new();
    // Two pre-populated blocks with disjoint, sorted ranges.
    let mut b0 = IndexBlock::new(1);
    b0.last_doc_id = 10;
    b0.num_entries = 5;
    blocks.push(b0);
    let mut b1 = IndexBlock::new(11);
    b1.last_doc_id = 20;
    b1.num_entries = 3;
    blocks.push(b1);

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    let state = ii.state.load_full();
    assert_eq!(state.pending.len(), 1);
    assert_eq!(state.pending[0].first_doc_id, 1);
    assert_eq!(state.pending[0].num_entries, 5);
    let ip = state.in_progress.as_ref().unwrap();
    assert_eq!(ip.first_doc_id, 11);
    assert_eq!(ip.num_entries, 3);
    assert_state_mirrors_blocks(&ii);
}

#[test]
fn reader_uses_snapshot_block_layout() {
    // Story 1.2: the reader snapshots the index at construction. Verify the snapshot's
    // block count is frozen even when writes happen "concurrently" (here, via raw pointer
    // because the borrow checker won't otherwise let us mutate `ii` while a reader
    // borrow is live).
    use crate::IndexReader as _;

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    for id in 1..=(entries_per_block + 5) {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    assert_eq!(ii.blocks.len(), 2);

    let ii_ptr = &mut ii as *mut InvertedIndex<Dummy>;
    let mut reader = ii.reader();
    assert_eq!(reader.snapshot_block_count(), 2);

    // Add more records that roll a third block. The reader's snapshot still shows 2.
    // SAFETY: we don't touch the reader while mutating `ii`, and the InvertedIndex
    // remains valid throughout — same pattern as the existing
    // `reader_snapshot_is_frozen_against_concurrent_writes` test.
    unsafe {
        for id in (entries_per_block + 6)..=(2 * entries_per_block + 5) {
            (*ii_ptr)
                .add_record(&RSIndexResult::build_virt().doc_id(id).build())
                .unwrap();
        }
    }
    assert_eq!(reader.snapshot_block_count(), 2, "reader's snapshot is frozen");

    // After `reset()`, the reader picks up the new snapshot.
    reader.reset();
    assert_eq!(reader.snapshot_block_count(), 3);
}

#[test]
fn find_block_for_doc_id_spans_regions() {
    // Stage a State by hand: 2 pending blocks + 1 in_progress, sealed empty.
    use crate::index::state::State;
    use std::sync::Arc;
    use thin_vec::ThinVec;

    let mut p0 = IndexBlock::new(1);
    p0.last_doc_id = 10;
    p0.num_entries = 5;
    let mut p1 = IndexBlock::new(11);
    p1.last_doc_id = 20;
    p1.num_entries = 5;
    let mut ip = IndexBlock::new(21);
    ip.last_doc_id = 25;
    ip.num_entries = 3;

    let state = State {
        sealed: Arc::new(ThinVec::new()),
        pending: Arc::new(vec![Arc::new(p0), Arc::new(p1)]),
        in_progress: Some(Arc::new(ip)),
    };

    assert_eq!(state.block_count(), 3);
    assert_eq!(state.find_block_for_doc_id(0, 5), 0);
    assert_eq!(state.find_block_for_doc_id(0, 10), 0);
    assert_eq!(state.find_block_for_doc_id(0, 11), 1);
    assert_eq!(state.find_block_for_doc_id(0, 20), 1);
    assert_eq!(state.find_block_for_doc_id(0, 21), 2);
    assert_eq!(state.find_block_for_doc_id(0, 25), 2);
    assert_eq!(state.find_block_for_doc_id(0, 26), 3, "past the end");

    // Skip the first block: search starts from index 1.
    assert_eq!(state.find_block_for_doc_id(1, 5), 1);
    assert_eq!(state.find_block_for_doc_id(2, 5), 2);
    assert_eq!(state.find_block_for_doc_id(3, 5), 3, "past the end");
}

#[test]
fn apply_gc_keeps_state_in_sync() {
    use crate::GcScanDelta;
    use crate::gc::{BlockGcScanResult, RepairType};

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    let total = entries_per_block * 2 + 5;
    for id in 1..=total {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    assert_eq!(ii.blocks.len(), 3);

    // Synthetic delta: delete the first block. This exercises the GC path that calls
    // `mem::swap` and rebuilds `blocks`, which is the case the state-sync hook covers.
    let delta = GcScanDelta {
        last_block_idx: ii.blocks.len() - 1,
        last_block_num_entries: ii.blocks.last().unwrap().num_entries,
        deltas: vec![BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: entries_per_block as u32,
            },
        }],
    };

    ii.apply_gc(delta);

    assert_eq!(ii.blocks.len(), 2, "first block deleted by GC");
    assert_state_mirrors_blocks(&ii);
}
