/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Invariant tests for the directly-owned `sealed` and `pending` fields on
//! [`InvertedIndex`]. After GC, `sealed` is populated with compacted survivors;
//! otherwise writes only touch `pending` (push-on-rollover or in-place mutation of the
//! tail `Arc<IndexBlock>` via `Arc::make_mut`). These tests check the layout stays
//! internally consistent across every mutation path, and that snapshots taken before a
//! write remain frozen even when the writer mutates the live tail.

use std::sync::Arc;

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;

use crate::{Encoder, IndexBlock, InvertedIndex};

use super::Dummy;

/// Assert that the two regions are internally consistent:
/// - All blocks in `sealed` and `pending` have non-overlapping ranges and are strictly
///   ordered by doc id across both regions (sealed → pending).
/// - `number_of_blocks()` equals `sealed.len() + pending.len()`.
fn assert_state_invariants<E: Encoder>(ii: &InvertedIndex<E>) {
    let expected_total = ii.sealed.len() + ii.pending.len();
    assert_eq!(
        ii.number_of_blocks(),
        expected_total,
        "block count must match sealed + pending"
    );

    // Within each region, blocks are strictly ordered by doc id.
    for window in ii.sealed.windows(2) {
        assert!(
            window[0].last_doc_id < window[1].first_doc_id,
            "sealed blocks must be strictly ordered by doc id"
        );
    }
    for window in ii.pending.windows(2) {
        assert!(
            window[0].last_doc_id < window[1].first_doc_id,
            "pending blocks must be strictly ordered by doc id"
        );
    }

    // Across regions, the trailing block of sealed precedes the leading block of pending.
    if let (Some(last_sealed), Some(first_pending)) = (ii.sealed.last(), ii.pending.first()) {
        assert!(
            last_sealed.last_doc_id < first_pending.first_doc_id,
            "pending must come after sealed"
        );
    }
}

#[test]
fn empty_index_has_empty_state() {
    let ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    assert!(ii.sealed.is_empty());
    assert!(ii.pending.is_empty());
    assert_eq!(ii.number_of_blocks(), 0);
}

#[test]
fn first_record_creates_pending_tail() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    ii.add_record(&record).unwrap();

    assert!(ii.sealed.is_empty(), "writes don't populate sealed");
    assert_eq!(ii.pending.len(), 1);
    let tail = &ii.pending[0];
    assert_eq!(tail.first_doc_id, 10);
    assert_eq!(tail.last_doc_id, 10);
    assert_eq!(tail.num_entries, 1);
    assert_state_invariants(&ii);
}

#[test]
fn appending_to_same_block_only_updates_pending_tail() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    for id in 10..15 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert_eq!(ii.pending.len(), 1, "no block rolled over");
    let tail = &ii.pending[0];
    assert_eq!(tail.first_doc_id, 10);
    assert_eq!(tail.last_doc_id, 14);
    assert_eq!(tail.num_entries, 5);
    assert_state_invariants(&ii);
}

#[test]
fn block_fill_pushes_new_pending_tail() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    // `Dummy::RECOMMENDED_BLOCK_ENTRIES` is the default 100. Write enough records to
    // force a roll-over to a second pending block.
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    for id in 1..=(entries_per_block + 1) {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert_eq!(
        ii.number_of_blocks(),
        2,
        "should have rolled to a second block"
    );

    assert_eq!(ii.pending.len(), 2);
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[0].num_entries as u64, entries_per_block);

    let tail = &ii.pending[1];
    assert_eq!(tail.num_entries, 1);
    assert_eq!(tail.first_doc_id, entries_per_block + 1);
    assert_state_invariants(&ii);
}

#[test]
fn pending_grows_monotonically_across_multiple_rollovers() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;

    // Roll over three blocks: two full + one partial (the tail).
    let total = entries_per_block * 2 + 5;
    for id in 1..=total {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert_eq!(ii.number_of_blocks(), 3);

    assert_eq!(ii.pending.len(), 3);
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[1].first_doc_id, entries_per_block + 1);
    let tail = &ii.pending[2];
    assert_eq!(tail.first_doc_id, 2 * entries_per_block + 1);
    assert_eq!(tail.num_entries, 5);
    assert_state_invariants(&ii);
}

#[test]
fn snapshot_taken_before_write_sees_only_pre_snapshot_entries() {
    // This is the key Step B acceptance test: a snapshot must see exactly the
    // entries present at snapshot time, even when the writer's next append finds
    // the tail Arc at refcount = 1 *after* the snapshot was constructed but before
    // it could be observed elsewhere.
    //
    // In Step B today, `snapshot()` increments the tail Arc's refcount via
    // `Vec::clone`, so a subsequent `Arc::make_mut` in the writer sees refcount = 2
    // and deep-clones. That deep clone is what protects this snapshot — verify it
    // empirically by holding the snapshot across two writes and checking the
    // snapshot's tail entry count is frozen.
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();
    let snap = ii.snapshot();
    assert_eq!(snap.block_count(), 1);
    assert_eq!(snap.tail_num_entries(), 2);
    let snap_tail_arc = Arc::clone(&ii.pending[0]);

    // Writer appends a record. Because the snapshot bumped the tail Arc's
    // refcount via `Vec::clone`, `Arc::make_mut` inside `add_record` must
    // deep-clone — the live tail Arc is replaced; the one the snapshot holds is
    // untouched.
    ii.add_record(&RSIndexResult::build_virt().doc_id(3).build())
        .unwrap();

    // The snapshot's frozen view: still 2 entries, last id 2.
    let snap_block = snap.last_block().unwrap();
    assert_eq!(
        snap_block.num_entries, 2,
        "snapshot's tail entry count must not be mutated by a later write"
    );
    assert_eq!(snap_block.last_doc_id, 2);
    assert_eq!(snap_block.buffer.len(), 8, "no extra encoded delta visible");

    // The Arc the snapshot captured is no longer the live tail.
    assert!(
        !Arc::ptr_eq(&snap_tail_arc, &ii.pending[0]),
        "writer's `Arc::make_mut` must have replaced the live tail"
    );

    // The live index reflects the new write.
    assert_eq!(ii.pending[0].num_entries, 3);
    assert_eq!(ii.pending[0].last_doc_id, 3);
    assert_state_invariants(&ii);
}

#[test]
fn snapshot_block_ref_returns_same_arc_data_as_live_pending_when_no_intervening_write() {
    // Without an intervening write, the snapshot's tail block is the same Arc the
    // live index holds — readers and writers share the underlying block data until
    // the writer triggers an `Arc::make_mut` clone.
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();

    let snap = ii.snapshot();
    let snap_block_ptr = snap.last_block().unwrap() as *const IndexBlock;
    let live_block_ptr = ii.pending[0].as_ref() as *const IndexBlock;
    assert_eq!(
        snap_block_ptr, live_block_ptr,
        "snapshot's tail Arc must point at the same heap allocation as the live tail"
    );
}

#[test]
fn sealed_remains_empty_after_writes() {
    // Writes never touch `sealed`; only `apply_gc` does.
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    for id in 1..50 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert!(ii.sealed.is_empty(), "writes don't populate sealed");
}

#[test]
fn from_blocks_seeds_pending() {
    use thin_vec::ThinVec;

    let mut blocks: ThinVec<IndexBlock, crate::BlockCapacity> = ThinVec::new();
    let mut b0 = IndexBlock::new(1);
    b0.last_doc_id = 10;
    b0.num_entries = 5;
    blocks.push(b0);
    let mut b1 = IndexBlock::new(11);
    b1.last_doc_id = 20;
    b1.num_entries = 3;
    blocks.push(b1);

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);

    // All seeded blocks go to pending (Step B has no `in_progress` region; the actively-
    // mutated tail is the last entry of `pending`).
    assert!(ii.sealed.is_empty(), "sealed empty after from_blocks");
    assert_eq!(ii.pending.len(), 2);
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[0].num_entries, 5);
    assert_eq!(ii.pending[1].first_doc_id, 11);
    assert_eq!(ii.pending[1].num_entries, 3);
    assert_state_invariants(&ii);
}

#[test]
fn reader_snapshot_is_frozen_against_pending_writes() {
    // The reader takes a snapshot at construction; subsequent writes don't shift
    // its view of `block_count`.
    use crate::IndexReader as _;

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    for id in 1..=(entries_per_block + 5) {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    assert_eq!(ii.number_of_blocks(), 2);

    let ii_ptr = &mut ii as *mut InvertedIndex<Dummy>;
    let mut reader = ii.reader();
    assert_eq!(reader.snapshot_block_count(), 2);

    // Add more records that roll a third block. The reader's snapshot still shows 2.
    // SAFETY: we don't touch the reader while mutating `ii`, and the InvertedIndex
    // remains valid throughout — same pattern used elsewhere in the test suite to
    // exercise concurrent-write scenarios that the borrow checker would otherwise
    // forbid.
    unsafe {
        for id in (entries_per_block + 6)..=(2 * entries_per_block + 5) {
            (*ii_ptr)
                .add_record(&RSIndexResult::build_virt().doc_id(id).build())
                .unwrap();
        }
    }
    assert_eq!(
        reader.snapshot_block_count(),
        2,
        "reader's snapshot is frozen"
    );

    // After `reset()`, the reader picks up the new snapshot.
    reader.reset();
    assert_eq!(reader.snapshot_block_count(), 3);
}

#[test]
fn find_block_for_doc_id_spans_regions() {
    // Build a 3-block index by hand and exercise `find_block_for_doc_id` on its snapshot.
    use thin_vec::ThinVec;

    let mut p0 = IndexBlock::new(1);
    p0.last_doc_id = 10;
    p0.num_entries = 5;
    let mut p1 = IndexBlock::new(11);
    p1.last_doc_id = 20;
    p1.num_entries = 5;
    let mut tail = IndexBlock::new(21);
    tail.last_doc_id = 25;
    tail.num_entries = 3;

    let mut blocks: ThinVec<IndexBlock, crate::BlockCapacity> = ThinVec::new();
    blocks.push(p0);
    blocks.push(p1);
    blocks.push(tail);

    let ii = InvertedIndex::<Dummy>::from_blocks(IndexFlags_Index_DocIdsOnly, blocks);
    let snap = ii.snapshot();

    assert_eq!(snap.block_count(), 3);
    assert_eq!(snap.find_block_for_doc_id(0, 5), 0);
    assert_eq!(snap.find_block_for_doc_id(0, 10), 0);
    assert_eq!(snap.find_block_for_doc_id(0, 11), 1);
    assert_eq!(snap.find_block_for_doc_id(0, 20), 1);
    assert_eq!(snap.find_block_for_doc_id(0, 21), 2);
    assert_eq!(snap.find_block_for_doc_id(0, 25), 2);
    assert_eq!(snap.find_block_for_doc_id(0, 26), 3, "past the end");

    // Skip the first block: search starts from index 1.
    assert_eq!(snap.find_block_for_doc_id(1, 5), 1);
    assert_eq!(snap.find_block_for_doc_id(2, 5), 2);
    assert_eq!(snap.find_block_for_doc_id(3, 5), 3, "past the end");
}

#[test]
fn apply_gc_compacts_survivors_into_sealed() {
    // After `apply_gc`, surviving completed blocks are moved into the contiguous `sealed`
    // ThinVec. `pending` is rebuilt with the trailing survivor as the lone entry — the
    // next write mutates it in place via `Arc::make_mut`.
    use crate::GcScanDelta;
    use crate::gc::{BlockGcScanResult, RepairType};

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    // Roll three blocks (all in pending).
    let total = entries_per_block * 2 + 5;
    for id in 1..=total {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    assert!(ii.sealed.is_empty(), "sealed empty pre-GC");
    assert_eq!(ii.pending.len(), 3);

    // Synthetic delta: delete block 0. This forces apply to rebuild the layout and verifies
    // the post-GC sealed/pending split.
    let last_block_num_entries = ii.snapshot().last_block().unwrap().num_entries;
    let delta = GcScanDelta {
        last_block_idx: ii.number_of_blocks() - 1,
        last_block_num_entries,
        deltas: vec![BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: entries_per_block as u32,
            },
        }],
    };

    ii.apply_gc(delta);

    // Block 0 deleted → 2 survivors. Trailing one (was pending[2]) becomes the new
    // pending[0]; the other (was pending[1]) goes into sealed.
    assert_eq!(
        ii.sealed.len(),
        1,
        "the surviving completed block landed in sealed"
    );
    assert_eq!(
        ii.pending.len(),
        1,
        "the trailing survivor stays in pending"
    );

    // The sealed block is the survivor from old pending[1] (after pending[0] was deleted).
    assert_eq!(ii.sealed[0].first_doc_id, entries_per_block + 1);
    assert_eq!(ii.pending[0].first_doc_id, 2 * entries_per_block + 1);

    assert_state_invariants(&ii);
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
    assert_eq!(ii.number_of_blocks(), 3);

    // Synthetic delta: delete the first block. Exercises the GC rebuild path.
    let last_block_num_entries = ii.snapshot().last_block().unwrap().num_entries;
    let delta = GcScanDelta {
        last_block_idx: ii.number_of_blocks() - 1,
        last_block_num_entries,
        deltas: vec![BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: entries_per_block as u32,
            },
        }],
    };

    ii.apply_gc(delta);

    assert_eq!(ii.number_of_blocks(), 2, "first block deleted by GC");
    assert_state_invariants(&ii);

    // Subsequent writes flow back through the pending tail (the existing pending[0]).
    let next_id = total + 1;
    ii.add_record(&RSIndexResult::build_virt().doc_id(next_id).build())
        .unwrap();
    assert_eq!(ii.pending[0].last_doc_id, next_id);
    assert_state_invariants(&ii);
}
