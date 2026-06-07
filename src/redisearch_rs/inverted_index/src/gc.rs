/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc};

use crate::{BlockCapacity, DecodedBy, Decoder, Encoder, IndexBlock, InvertedIndex};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use rqe_core::DocId;
use smallvec::SmallVec;
use thin_vec::ThinVec;

/// Context handed to the GC repair callback for each surviving record.
///
/// Carries the block the record was decoded from plus the block's logical index
/// within the inverted index. Packaged as a struct so future fields (e.g. a
/// last-block flag, a GC marker) can ride along without changing the callback
/// signature.
#[non_exhaustive]
pub struct RepairContext<'a> {
    /// The block the surviving record was decoded from.
    pub block: &'a IndexBlock,
    /// The block's logical index within the inverted index. Use this instead of
    /// pointer-equality on `block` — pointer identity isn't reliable when blocks
    /// are read through a snapshot.
    pub block_idx: usize,
}

/// The type of repair needed for a block after a garbage collection scan.
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) enum RepairType {
    /// This block can be deleted completely.
    Delete {
        /// Number of unique records this will remove
        n_unique_docs_removed: u32,
    },

    /// The block contains GCed entries, and should be replaced with the following blocks.
    Replace {
        /// The new blocks to replace this block with
        blocks: SmallVec<[IndexBlock; 3]>,

        /// How many unique documents were removed from the block being replaced.
        n_unique_docs_removed: u32,
    },
}

/// Result of scanning the index for garbage collection
#[cheadergen::config(rename = "InvertedIndexGcDelta")]
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct GcScanDelta {
    /// The index of the last block in the index at the time of the scan. This is used to ensure
    /// that the index has not changed since the scan was performed.
    pub(crate) last_block_idx: usize,

    /// The number of entries in the last block at the time of the scan. This is used to ensure
    /// that the index has not changed since the scan was performed.
    pub(crate) last_block_num_entries: u16,

    /// The results of the scan for each block that needs to be repaired or deleted.
    ///
    /// There is at most one entry per block, and entries are sorted in ascending order
    /// by block index.
    pub(crate) deltas: Vec<BlockGcScanResult>,
}

impl GcScanDelta {
    /// Returns the index of the last block in the index at the time of the scan.
    pub const fn last_block_idx(&self) -> usize {
        self.last_block_idx
    }
}

/// Result of scanning a block for garbage collection
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct BlockGcScanResult {
    /// The index of the block in the inverted index
    pub(crate) index: usize,

    /// The type of repair needed for this block
    pub(crate) repair: RepairType,
}

/// Information about the result of applying a garbage collection scan to the index
#[cheadergen::config(rename = "II_GCScanStats")]
#[derive(Debug, Eq, PartialEq, Copy, Clone, Default)]
#[repr(C)]
pub struct GcApplyInfo {
    /// The number of bytes that were freed
    pub bytes_freed: usize,

    /// The number of bytes that were allocated
    pub bytes_allocated: usize,

    /// The number of entries that were removed from the index including duplicates
    pub entries_removed: usize,

    /// Net change in the index's block count for this apply. Positive when blocks were added
    /// (e.g. a `Replace` repair adding more blocks than it removed), negative when removed.
    /// Callers maintaining per-spec totals should add this signed value to their counter.
    pub block_count_delta: i64,

    /// Whether or not we ignored the last block in the index, since it changed
    /// compared to the time we performed the scan
    pub ignored_last_block: bool,
}

impl IndexBlock {
    /// Repair a block by removing records which no longer exists according to `doc_exists`. If a
    /// record does exist, then `repair` is called with it.
    ///
    /// The `repair` callback receives the surviving record and a [`RepairContext`]
    /// carrying the block and its logical index within the inverted index. Comparing
    /// `ctx.block_idx` against `index.number_of_blocks() - 1` answers "is this the last
    /// block?" without relying on pointer identity — pointer equality won't be stable
    /// once blocks are read through a snapshot in a later epic.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    pub(crate) fn repair<'block, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'block self,
        block_idx: usize,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'block>, &RepairContext<'block>)>,
        _encoder: PhantomData<E>,
    ) -> std::io::Result<Option<RepairType>> {
        let mut cursor: std::io::Cursor<&'block [u8]> = std::io::Cursor::new(&self.buffer);
        let mut last_read_doc_id = None;
        let mut result = D::base_result();
        let mut unique_read = 0;
        let mut unique_write = 0;
        let mut block_changed = false;

        let mut tmp_inverted_index = InvertedIndex::<E>::new(IndexFlags_Index_DocIdsOnly);

        while self.buffer.len() as u64 > cursor.position() {
            let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;

            if doc_exist(result.doc_id) {
                if let Some(repair) = repair.as_mut() {
                    let ctx = RepairContext {
                        block: self,
                        block_idx,
                    };
                    repair(&result, &ctx);
                }

                tmp_inverted_index.add_record(&result)?;

                if last_read_doc_id.is_none_or(|id| id != result.doc_id) {
                    unique_write += 1;
                }
            } else {
                block_changed = true;
            }

            if last_read_doc_id.is_none_or(|id| id != result.doc_id) {
                unique_read += 1;
            }

            last_read_doc_id = Some(result.doc_id);
        }

        let repaired_blocks = tmp_inverted_index.into_blocks_owned();
        if repaired_blocks.is_empty() {
            Ok(Some(RepairType::Delete {
                n_unique_docs_removed: unique_read,
            }))
        } else if block_changed {
            Ok(Some(RepairType::Replace {
                blocks: SmallVec::from_iter(repaired_blocks),
                n_unique_docs_removed: unique_read - unique_write,
            }))
        } else {
            Ok(None)
        }
    }
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Scan the index for blocks that can be garbage collected. A block can be garbage collected
    /// if any of its records point to documents that no longer exist. The `doc_exist`
    /// callback is used to check if a document exists. It should return `true` if the document
    /// exists and `false` otherwise.
    ///
    /// If a doc does exist, then `repair` is called with it to run any repair calculations needed.
    ///
    /// This function returns a delta if GC is needed, or `None` if no GC is needed.
    pub fn scan_gc(
        &self,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl for<'snap> FnMut(&RSIndexResult<'snap>, &RepairContext<'snap>)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        let snapshot = self.snapshot();
        let mut results = Vec::new();

        let total = snapshot.block_count();
        for i in 0..total {
            let Some(block) = snapshot.block_ref(i) else {
                continue;
            };
            let repair = block.repair(i, &doc_exist, repair.as_mut(), PhantomData::<E>)?;

            if let Some(repair) = repair {
                results.push(BlockGcScanResult { index: i, repair });
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            let last_block_idx = total.saturating_sub(1);
            let last_block_num_entries = snapshot.last_block().map(|b| b.num_entries).unwrap_or(0);
            Ok(Some(GcScanDelta {
                last_block_idx,
                last_block_num_entries,
                deltas: results,
            }))
        }
    }

    /// Apply the deltas of a garbage collection scan to the index. Mutates the direct
    /// `sealed` / `pending` fields in place: survivors are compacted into a freshly-rebuilt
    /// `sealed`, and the trailing survivor (the actively-mutated tail) is kept as the sole
    /// entry of a freshly-rebuilt `pending`.
    ///
    /// Runs under `&mut self` plus the spec write lock (C side), so no concurrent writer
    /// or reader can race. Outstanding snapshots are unaffected — they hold their own
    /// [`Arc`] / [`Vec`] clones from the pre-GC state.
    ///
    /// `bytes_freed` / `bytes_allocated` in the returned [`GcApplyInfo`] are the
    /// net change in [`Self::memory_usage`], not just the block-level mem-usage sum:
    /// they fold in the compaction savings (per-block `Arc<IndexBlock>` headers and
    /// pending Vec slots dropped as blocks move into the contiguous `sealed` ThinVec)
    /// so callers that maintain a total like [`numeric_range_tree`]'s
    /// `inverted_indexes_size` invariant stay accurate without extra bookkeeping.
    ///
    /// [`numeric_range_tree`]: ../../numeric_range_tree/index.html
    pub fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        let GcScanDelta {
            last_block_idx,
            last_block_num_entries,
            mut deltas,
        } = delta;

        let mut info = GcApplyInfo {
            bytes_freed: 0,
            bytes_allocated: 0,
            entries_removed: 0,
            block_count_delta: 0,
            ignored_last_block: false,
        };

        let mem_before = self.memory_usage();
        let n_sealed = self.sealed.len();
        let n_pending = self.pending.len();
        let blocks_before = n_sealed + n_pending;

        // Check if the actively-mutated tail (last entry of `pending`, when present) has
        // changed since the scan was performed. Sealed blocks and non-tail pending blocks
        // are immutable, so they cannot have changed.
        let tail_idx = blocks_before.saturating_sub(1);
        let last_block_changed = if last_block_idx == tail_idx && n_pending > 0 {
            self.pending
                .last()
                .is_some_and(|arc| arc.num_entries != last_block_num_entries)
        } else {
            // Either the scan recorded a block that's not the tail (immutable), or
            // pending is empty so the tail lives in `sealed` (also immutable).
            false
        };

        if last_block_changed {
            let remove_stale_delta = deltas
                .last()
                .map(|d| d.index == last_block_idx)
                .unwrap_or(false);
            if remove_stale_delta {
                deltas.pop();
            }
            info.ignored_last_block = true;
        }

        if deltas.is_empty() {
            return info;
        }

        // Build a flat working list of all current blocks in `sealed` → `pending` order.
        // Take pending by value so we don't double-account its `Arc`s. Sealed entries are
        // cloned (rebuild path) — if no reader holds an outstanding snapshot, the source
        // `Arc<ThinVec>` is uniquely owned and the clones-into-owned-IndexBlocks below
        // happen in place via `Arc::try_unwrap` at the end.
        let mut working: Vec<Arc<IndexBlock>> = Vec::with_capacity(blocks_before);
        for b in self.sealed.iter() {
            working.push(Arc::new(b.clone()));
        }
        let old_pending = std::mem::take(&mut self.pending);
        working.extend(old_pending);

        let mut deltas_iter = deltas.into_iter().peekable();
        let mut new_all: Vec<Arc<IndexBlock>> = Vec::with_capacity(working.len());

        for (block_index, arc_block) in working.into_iter().enumerate() {
            match deltas_iter.peek() {
                Some(d) if d.index == block_index => {
                    let d = deltas_iter
                        .next()
                        .expect("peek() returned Some on this iteration");
                    match d.repair {
                        RepairType::Delete {
                            n_unique_docs_removed,
                        } => {
                            info.entries_removed += arc_block.num_entries as usize;
                            info.bytes_freed += arc_block.mem_usage();
                            self.n_unique_docs -= n_unique_docs_removed;
                        }
                        RepairType::Replace {
                            blocks,
                            n_unique_docs_removed,
                        } => {
                            info.entries_removed += arc_block.num_entries as usize;
                            info.bytes_freed += arc_block.mem_usage();
                            self.n_unique_docs -= n_unique_docs_removed;
                            for b in blocks {
                                // Replace can only shrink — new blocks are always a
                                // subset of the old. `saturating_sub` guards against
                                // a malformed delta (e.g. corrupted RDB) producing a
                                // larger replacement.
                                info.entries_removed =
                                    info.entries_removed.saturating_sub(b.num_entries as usize);
                                info.bytes_allocated += b.mem_usage();
                                new_all.push(Arc::new(b));
                            }
                        }
                    }
                }
                _ => new_all.push(arc_block),
            }
        }

        // The trailing survivor becomes the new `pending` tail (kept so future writes can
        // mutate it via `Arc::make_mut`); everything before it gets compacted into the
        // contiguous `sealed` ThinVec.
        let new_pending_tail = new_all.pop();

        let mut new_sealed: ThinVec<IndexBlock, BlockCapacity> =
            ThinVec::with_capacity(new_all.len());
        for arc_b in new_all {
            // Each Arc here has refcount = 1 (we just built `working` and split it),
            // so `try_unwrap` succeeds without cloning. The clone fallback covers the
            // theoretical case where a snapshot tail Arc landed in `working` via the
            // `pending` extension — that can't happen because `std::mem::take` left
            // `self.pending` empty and the snapshot's pending Vec is its own clone.
            let b = Arc::try_unwrap(arc_b).unwrap_or_else(|arc| (*arc).clone());
            new_sealed.push(b);
        }
        new_sealed.shrink_to_fit();

        let new_pending: Vec<Arc<IndexBlock>> = new_pending_tail.into_iter().collect();
        let blocks_after = new_sealed.len() + new_pending.len();

        self.sealed = Arc::new(new_sealed);
        self.pending = new_pending;

        info.block_count_delta = blocks_after as i64 - blocks_before as i64;

        // Fold the compaction overhead (Arc headers / pending Vec slots freed,
        // sealed ThinVec header allocated) into bytes_freed / bytes_allocated so
        // the net matches the actual `memory_usage` delta. The block-level
        // accumulators above already cover the dropped/added IndexBlock contents;
        // this adjustment closes the gap for callers that maintain a running
        // `inverted_indexes_size` invariant.
        let mem_after = self.memory_usage();
        let block_net = info.bytes_freed as isize - info.bytes_allocated as isize;
        let actual_net = mem_before as isize - mem_after as isize;
        let adjust = actual_net - block_net;
        if adjust >= 0 {
            info.bytes_freed += adjust as usize;
        } else {
            info.bytes_allocated += (-adjust) as usize;
        }

        self.gc_marker_inc();

        info
    }
}
