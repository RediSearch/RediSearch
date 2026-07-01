/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Invariant tests for the directly-owned `sealed`, `pending`, `in_progress` fields on
//! `InvertedIndex`. After GC, `sealed` is populated with compacted survivors; otherwise
//! writes only touch `pending` (on roll-over) and `in_progress` (in-place). These tests
//! check the layout stays internally consistent across every mutation path.

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;

use crate::{Encoder, IndexBlock, InvertedIndex};

use super::Dummy;

/// Assert that the three regions are internally consistent:
/// - All blocks in `sealed`, `pending`, and `in_progress` have non-overlapping ranges and
///   are strictly ordered by doc id across the three regions (sealed → pending →
///   in_progress).
/// - `number_of_blocks()` equals `sealed.len() + pending.len() + in_progress.is_some() as
///   usize`.
fn assert_state_invariants<E: Encoder>(ii: &InvertedIndex<E>) {
    let expected_total = ii.sealed.len() + ii.pending.len() + usize::from(ii.in_progress.is_some());
    assert_eq!(
        ii.number_of_blocks(),
        expected_total,
        "block count must match sealed+pending+in_progress"
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

    // Across regions, the trailing block of each region precedes the leading block of the
    // next.
    if let (Some(last_sealed), Some(first_pending)) = (ii.sealed.last(), ii.pending.first()) {
        assert!(
            last_sealed.last_doc_id < first_pending.first_doc_id,
            "pending must come after sealed"
        );
    }
    if let (Some(last_pending), Some(ip)) = (ii.pending.last(), ii.in_progress.as_ref()) {
        assert!(
            last_pending.last_doc_id < ip.first_doc_id,
            "in_progress must come after all pending blocks"
        );
    }
    if ii.pending.is_empty()
        && let (Some(last_sealed), Some(ip)) = (ii.sealed.last(), ii.in_progress.as_ref())
    {
        assert!(
            last_sealed.last_doc_id < ip.first_doc_id,
            "in_progress must come after sealed when pending is empty"
        );
    }
}

#[test]
fn empty_index_has_empty_state() {
    let ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    assert!(ii.sealed.is_empty());
    assert!(ii.pending.is_empty());
    assert!(ii.in_progress.is_none());
    assert_eq!(ii.number_of_blocks(), 0);
}

#[test]
fn first_record_promotes_in_progress() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    ii.add_record(&record).unwrap();

    assert!(ii.sealed.is_empty());
    assert!(ii.pending.is_empty());
    let ip = ii.in_progress.as_ref().expect("must have in_progress");
    assert_eq!(ip.first_doc_id, 10);
    assert_eq!(ip.last_doc_id, 10);
    assert_eq!(ip.num_entries, 1);
    assert_state_invariants(&ii);
}

#[test]
fn appending_to_same_block_only_updates_in_progress() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    for id in 10..15 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }

    assert!(ii.pending.is_empty(), "no block rolled over");
    let ip = ii.in_progress.as_ref().unwrap();
    assert_eq!(ip.first_doc_id, 10);
    assert_eq!(ip.last_doc_id, 14);
    assert_eq!(ip.num_entries, 5);
    assert_state_invariants(&ii);
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

    assert_eq!(
        ii.number_of_blocks(),
        2,
        "should have rolled to a second block"
    );

    assert_eq!(ii.pending.len(), 1, "first block is now pending");
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[0].num_entries as u64, entries_per_block);

    let ip = ii.in_progress.as_ref().unwrap();
    assert_eq!(ip.num_entries, 1);
    assert_eq!(ip.first_doc_id, entries_per_block + 1);
    assert_state_invariants(&ii);
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

    assert_eq!(ii.number_of_blocks(), 3);

    assert_eq!(ii.pending.len(), 2);
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[1].first_doc_id, entries_per_block + 1);
    let ip = ii.in_progress.as_ref().unwrap();
    assert_eq!(ip.first_doc_id, 2 * entries_per_block + 1);
    assert_eq!(ip.num_entries, 5);
    assert_state_invariants(&ii);
}

#[test]
fn snapshot_taken_before_write_is_not_mutated_by_later_writes() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();

    // Take a snapshot before the next write.
    let before = ii.snapshot();
    let before_ip = before
        .in_progress()
        .expect("first add already happened")
        .clone();
    assert_eq!(before_ip.num_entries, 1);

    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();

    // The snapshot's owned `IndexBlock` must remain unchanged — readers holding it
    // shouldn't see a torn write.
    assert_eq!(
        before_ip.num_entries, 1,
        "snapshot taken before the second write is immutable"
    );
    assert_eq!(before_ip.last_doc_id, 1);

    // The live index reflects the new write.
    let after_ip = ii.in_progress.as_ref().unwrap();
    assert_eq!(after_ip.num_entries, 2);
    assert_eq!(after_ip.last_doc_id, 2);
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

    assert_eq!(ii.pending.len(), 1);
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[0].num_entries, 5);
    let ip = ii.in_progress.as_ref().unwrap();
    assert_eq!(ip.first_doc_id, 11);
    assert_eq!(ip.num_entries, 3);
    assert_state_invariants(&ii);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "violates Stacked Borrows by aliasing &InvertedIndex from `ii.reader()` with \
              a raw-pointer write — the borrow-checker-enforced fix is tracked in MOD-16139 \
              (carry NonNull<InvertedIndex> + atomics on live metadata). The test still \
              exercises the intended frozen-snapshot semantics under normal cargo test."
)]
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
    assert_eq!(ii.number_of_blocks(), 2);

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
    let mut ip = IndexBlock::new(21);
    ip.last_doc_id = 25;
    ip.num_entries = 3;

    let mut blocks: ThinVec<IndexBlock, crate::BlockCapacity> = ThinVec::new();
    blocks.push(p0);
    blocks.push(p1);
    blocks.push(ip);

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
    // ThinVec. `pending` resets to empty; the trailing survivor becomes the new
    // `in_progress`. The next write will roll back through the pending → sealed flow.
    use crate::GcScanDelta;
    use crate::gc::{BlockGcScanResult, RepairType};

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    // Roll three blocks: two full (→ pending) + one partial (→ in_progress).
    let total = entries_per_block * 2 + 5;
    for id in 1..=total {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    assert!(ii.sealed.is_empty(), "sealed empty pre-GC");
    assert_eq!(ii.pending.len(), 2);
    assert!(ii.in_progress.is_some());

    // Synthetic delta: delete block 0. This forces apply to rebuild layout and verifies
    // it flips from "pending+in_progress" to "sealed+in_progress".
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

    // Block 0 deleted → 2 survivors. Trailing one (was in_progress) becomes new
    // in_progress; the other (was pending) goes to sealed.
    assert_eq!(
        ii.sealed.len(),
        1,
        "the surviving completed block landed in sealed"
    );
    assert!(ii.pending.is_empty(), "pending reset to empty after GC");
    assert!(ii.in_progress.is_some(), "in_progress preserved");

    // The sealed block is the survivor from old pending (originally pending[1] after
    // pending[0] was deleted).
    assert_eq!(ii.sealed[0].first_doc_id, entries_per_block + 1);
    assert_eq!(
        ii.in_progress.as_ref().unwrap().first_doc_id,
        2 * entries_per_block + 1
    );

    assert_state_invariants(&ii);
}

#[test]
fn apply_gc_cow_clones_only_pinned_blocks() {
    // The COW counters must stay flat when GC runs with no live snapshot (survivor
    // blocks are moved), and rise by exactly the number of pinned survivor blocks when
    // a snapshot is held across the GC cycle (those blocks are deep-cloned). This is the
    // in-process signal for cost component C2. Relies on nextest's process-per-test
    // isolation for the global counters.
    use crate::GcScanDelta;
    use crate::gc::{BlockGcScanResult, COW_CLONED_BLOCKS, COW_CLONED_BYTES, RepairType};
    use std::sync::atomic::Ordering;

    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    let total = entries_per_block * 2 + 5; // two full (→ pending) + one partial (→ in_progress)

    // Build a fresh 3-block index and a delta that deletes block 0.
    let build = || {
        let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
        for id in 1..=total {
            ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
                .unwrap();
        }
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
        (ii, delta)
    };

    // Case A: no snapshot held → survivor blocks are moved, nothing cloned.
    let (mut ii, delta) = build();
    let blocks_before = COW_CLONED_BLOCKS.load(Ordering::Relaxed);
    ii.apply_gc(delta);
    assert_eq!(
        COW_CLONED_BLOCKS.load(Ordering::Relaxed) - blocks_before,
        0,
        "no snapshot held → GC must not deep-clone any block"
    );

    // Case B: a snapshot pins the blocks → the surviving pending block is deep-cloned.
    let (mut ii, delta) = build();
    let snap = ii.snapshot();
    let blocks_before = COW_CLONED_BLOCKS.load(Ordering::Relaxed);
    let bytes_before = COW_CLONED_BYTES.load(Ordering::Relaxed);
    ii.apply_gc(delta);
    // Survivors are old pending[1] (a pinned Arc → cloned) and the old in_progress (re-Arc'd
    // fresh inside apply_gc, unique → moved). So exactly one block is COW-cloned.
    assert_eq!(
        COW_CLONED_BLOCKS.load(Ordering::Relaxed) - blocks_before,
        1,
        "a held snapshot must force exactly the pinned survivor block to be cloned"
    );
    assert!(
        COW_CLONED_BYTES.load(Ordering::Relaxed) - bytes_before > 0,
        "cloned-bytes counter must advance alongside the block counter"
    );
    drop(snap);
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
}
