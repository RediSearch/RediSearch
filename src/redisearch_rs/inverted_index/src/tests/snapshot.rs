/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Invariant tests for the directly-owned `sealed` and `pending` fields on
//! `InvertedIndex`. After GC, `sealed` is populated with compacted survivors; writes only
//! ever touch `pending` — appending into its last slot (the writable tail), copy-on-write
//! when a snapshot shares it, and pushing a fresh tail on roll-over. These tests check the
//! layout stays internally consistent across every mutation path.

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;

use crate::{Encoder, IndexBlock, InvertedIndex};

use super::Dummy;

/// Assert that the two regions are internally consistent:
/// - All blocks in `sealed` and `pending` have non-overlapping ranges and are strictly
///   ordered by doc id across the two regions (sealed → pending, tail = last `pending`).
/// - `number_of_blocks()` equals `sealed.len() + pending.len()`.
fn assert_state_invariants<E: Encoder>(ii: &InvertedIndex<E>) {
    let expected_total = ii.sealed.len() + ii.pending.len();
    assert_eq!(
        ii.number_of_blocks(),
        expected_total,
        "block count must match sealed+pending"
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

    // Across regions, the trailing sealed block precedes the leading pending block.
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
fn first_record_creates_tail_in_pending() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);

    let record = RSIndexResult::build_virt().doc_id(10).build();
    ii.add_record(&record).unwrap();

    assert!(ii.sealed.is_empty());
    assert_eq!(ii.pending.len(), 1, "first record creates the tail block");
    let tail = &ii.pending[0];
    assert_eq!(tail.first_doc_id, 10);
    assert_eq!(tail.last_doc_id, 10);
    assert_eq!(tail.num_entries, 1);
    assert_state_invariants(&ii);
}

#[test]
fn appending_to_same_block_only_updates_tail() {
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
fn block_fill_pushes_new_tail() {
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

    // Both blocks live in `pending`; the last is the fresh tail.
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

    // Roll over three blocks: two full + one partial.
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
fn snapshot_taken_before_write_is_not_mutated_by_later_writes() {
    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();

    // Take a snapshot before the next write. It shares the tail block via `Arc`.
    let before = ii.snapshot();
    let before_tail = before.tail().expect("first add already happened").clone();
    assert_eq!(before_tail.num_entries, 1);

    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();

    // The snapshot's block must remain unchanged — the second write copies-on-write
    // (the tail `Arc` was shared), so readers holding it don't see a torn write.
    assert_eq!(
        before_tail.num_entries, 1,
        "snapshot taken before the second write is immutable"
    );
    assert_eq!(before_tail.last_doc_id, 1);

    // The live index reflects the new write.
    let after_tail = ii.pending.last().unwrap();
    assert_eq!(after_tail.num_entries, 2);
    assert_eq!(after_tail.last_doc_id, 2);
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

    // Every block goes into `pending`; the last is the writable tail.
    assert_eq!(ii.pending.len(), 2);
    assert_eq!(ii.pending[0].first_doc_id, 1);
    assert_eq!(ii.pending[0].num_entries, 5);
    let tail = &ii.pending[1];
    assert_eq!(tail.first_doc_id, 11);
    assert_eq!(tail.num_entries, 3);
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
    // All three blocks live in `pending` (the last is the tail).
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
    // ThinVec, and the trailing survivor becomes the sole `pending` entry (the writable
    // tail again). The next writes append to that tail copy-on-write.
    use crate::GcScanDelta;
    use crate::gc::{BlockGcScanResult, RepairType};

    let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;
    // Roll three blocks: two full + one partial. All live in `pending`.
    let total = entries_per_block * 2 + 5;
    for id in 1..=total {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    assert!(ii.sealed.is_empty(), "sealed empty pre-GC");
    assert_eq!(ii.pending.len(), 3);

    // Synthetic delta: delete block 0. This forces apply to rebuild layout and verifies
    // it flips from "all-pending" to "sealed + a single pending tail".
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

    // Block 0 deleted → 2 survivors. Trailing one becomes the new pending tail; the other
    // goes to sealed.
    assert_eq!(
        ii.sealed.len(),
        1,
        "the surviving completed block landed in sealed"
    );
    assert_eq!(ii.pending.len(), 1, "the compacted tail is the sole pending block");

    // The sealed block is the survivor from old pending[1] (after pending[0] was deleted).
    assert_eq!(ii.sealed[0].first_doc_id, entries_per_block + 1);
    assert_eq!(
        ii.pending.last().unwrap().first_doc_id,
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
    let total = entries_per_block * 2 + 5; // two full + one partial, all in pending

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

    // Case B: a snapshot pins the blocks → both surviving pending blocks are deep-cloned.
    // With the folded layout the tail is an `Arc` shared with the snapshot too (not a
    // separate deep copy), so both survivors — old pending[1] and the tail — are COW'd.
    let (mut ii, delta) = build();
    let snap = ii.snapshot();
    let blocks_before = COW_CLONED_BLOCKS.load(Ordering::Relaxed);
    let bytes_before = COW_CLONED_BYTES.load(Ordering::Relaxed);
    ii.apply_gc(delta);
    assert_eq!(
        COW_CLONED_BLOCKS.load(Ordering::Relaxed) - blocks_before,
        2,
        "a held snapshot must force both pinned survivor blocks (incl. the tail) to be cloned"
    );
    assert!(
        COW_CLONED_BYTES.load(Ordering::Relaxed) - bytes_before > 0,
        "cloned-bytes counter must advance alongside the block counter"
    );
    drop(snap);
}

#[test]
fn apply_gc_moves_unpinned_sealed_and_cows_pinned_sealed() {
    // After a first GC, survivors live in `sealed` (plus a single pending tail). A second
    // GC must MOVE the sealed blocks (no copy) when no snapshot pins them, and only
    // deep-copy them (bumping the COW counters) when a snapshot is held across the GC.
    // Guards the try_unwrap-when-unique optimization in `apply_gc`. Relies on nextest's
    // process-per-test isolation for the global counters.
    use crate::GcScanDelta;
    use crate::gc::{BlockGcScanResult, COW_CLONED_BLOCKS, RepairType};
    use std::sync::atomic::Ordering;

    let entries_per_block = Dummy::RECOMMENDED_BLOCK_ENTRIES as u64;

    // Build a fresh index and run one GC (delete block 0) to populate `sealed`.
    let build_with_populated_sealed = || {
        let mut ii = InvertedIndex::<Dummy>::new(IndexFlags_Index_DocIdsOnly);
        let total = entries_per_block * 4 + 5; // 4 full + 1 partial, all in pending
        for id in 1..=total {
            ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
                .unwrap();
        }
        let last_num = ii.snapshot().last_block().unwrap().num_entries;
        ii.apply_gc(GcScanDelta {
            last_block_idx: ii.number_of_blocks() - 1,
            last_block_num_entries: last_num,
            deltas: vec![BlockGcScanResult {
                index: 0,
                repair: RepairType::Delete {
                    n_unique_docs_removed: entries_per_block as u32,
                },
            }],
        });
        assert!(!ii.sealed.is_empty(), "GC #1 should populate sealed");
        ii
    };

    // Delta for GC #2: delete the first (now-sealed) block.
    let gc2_delta = |ii: &InvertedIndex<Dummy>| GcScanDelta {
        last_block_idx: ii.number_of_blocks() - 1,
        last_block_num_entries: ii.snapshot().last_block().unwrap().num_entries,
        deltas: vec![BlockGcScanResult {
            index: 0,
            repair: RepairType::Delete {
                n_unique_docs_removed: entries_per_block as u32,
            },
        }],
    };

    // Case A: no snapshot held → sealed blocks are moved, nothing cloned.
    let mut ii = build_with_populated_sealed();
    let delta = gc2_delta(&ii);
    let before = COW_CLONED_BLOCKS.load(Ordering::Relaxed);
    ii.apply_gc(delta);
    assert_eq!(
        COW_CLONED_BLOCKS.load(Ordering::Relaxed) - before,
        0,
        "unpinned sealed must be moved, not cloned"
    );

    // Case B: a snapshot pins sealed *and* the pending tail → every sealed block is
    // COW-cloned (the whole `sealed` Arc is shared) plus the pinned tail.
    let mut ii = build_with_populated_sealed();
    let n_sealed = ii.sealed.len() as u64;
    let delta = gc2_delta(&ii);
    let snap = ii.snapshot();
    let before = COW_CLONED_BLOCKS.load(Ordering::Relaxed);
    ii.apply_gc(delta);
    assert_eq!(
        COW_CLONED_BLOCKS.load(Ordering::Relaxed) - before,
        n_sealed + 1,
        "a held snapshot must force every sealed block plus the pinned tail to be cloned"
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
