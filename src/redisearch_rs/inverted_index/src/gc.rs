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
    /// The `repair` callback receives the surviving record, this block (read-only), and
    /// the block's logical index within the index — the latter lets callers compare
    /// against `index.number_of_blocks() - 1` to ask "is this the last block?" without
    /// resorting to pointer equality, which isn't stable in the lock-free state model.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    pub(crate) fn repair<'index, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'index self,
        block_idx: usize,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock, usize)>,
        _encoder: PhantomData<E>,
    ) -> std::io::Result<Option<RepairType>> {
        let mut cursor: std::io::Cursor<&'index [u8]> = std::io::Cursor::new(&self.buffer);
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
                    repair(&result, self, block_idx);
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
    pub fn scan_gc<'index>(
        &'index self,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock, usize)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        // Scan against an `InvertedIndexSnapshot` — gives us a stable enumeration even if
        // writes happen concurrently with the scan. The `last_block_idx` /
        // `last_block_num_entries` fields below let `apply_gc` detect drift and ignore
        // any stale delta. The snapshot lives for the duration of this function and owns
        // the cloned in_progress, so block references are valid for the local scope.
        let snapshot = self.snapshot();
        let mut results = Vec::new();

        let total = snapshot.block_count();
        for i in 0..total {
            let Some(block) = snapshot.block_ref(i) else {
                continue;
            };
            // SAFETY: lifetime extension — the snapshot is owned by this function and
            // its block buffers are immutable for the function's duration.
            let block_ref: &'index IndexBlock = unsafe { std::mem::transmute(block) };

            let repair = block_ref.repair(i, &doc_exist, repair.as_mut(), PhantomData::<E>)?;

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
    /// `sealed` / `pending` / `in_progress` fields in place: survivors are compacted
    /// into a freshly-rebuilt `sealed`, `pending` is drained to empty, and the trailing
    /// survivor becomes the new `in_progress`.
    ///
    /// Runs under `&mut self` plus the spec write lock (C side), so no concurrent
    /// writer or reader can race. Outstanding snapshots are unaffected — they hold
    /// their own [`Arc`] / [`Vec`] clones from the pre-GC state.
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

        let n_sealed = self.sealed.len();
        let n_pending = self.pending.len();
        let n_in_progress = usize::from(self.in_progress.is_some());
        let blocks_before = n_sealed + n_pending + n_in_progress;

        // Check if the in-progress block (if it's the one the scan recorded) has changed
        // since the scan. last_block_idx points into the flat sealed+pending+in_progress
        // ordering; if it points at the in_progress slot, check via self.in_progress.
        let in_progress_idx = n_sealed + n_pending;
        let last_block_changed = if last_block_idx == in_progress_idx {
            self.in_progress
                .as_ref()
                .is_some_and(|b| b.num_entries != last_block_num_entries)
        } else if last_block_idx < n_sealed {
            self.sealed
                .get(last_block_idx)
                .is_some_and(|b| b.num_entries != last_block_num_entries)
        } else {
            // last_block_idx is in pending — but pending blocks are immutable so they
            // can't have changed.
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

        // Build a flat working list of all current blocks as `Arc<IndexBlock>`s, in the
        // sealed → pending → in_progress order. The in_progress (owned directly on
        // `self`) is `take()`n out so we can move it into the working list; we'll set
        // self.in_progress to whatever survives at the trailing slot.
        let mut working: Vec<Arc<IndexBlock>> = Vec::with_capacity(blocks_before);
        for b in self.sealed.iter() {
            working.push(Arc::new(b.clone()));
        }
        let old_pending = std::mem::take(&mut self.pending);
        working.extend(old_pending);
        if let Some(ip) = self.in_progress.take() {
            working.push(Arc::new(ip));
        }

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
                                info.entries_removed -= b.num_entries as usize;
                                info.bytes_allocated += b.mem_usage();
                                new_all.push(Arc::new(b));
                            }
                        }
                    }
                }
                _ => new_all.push(arc_block),
            }
        }

        // The trailing block becomes the new `in_progress` (set on self directly).
        // Everything before it gets compacted into the contiguous `sealed` ThinVec.
        // pending resets to empty.
        let new_in_progress_arc = new_all.pop();
        let new_in_progress: Option<IndexBlock> = new_in_progress_arc.map(|arc| {
            // The trailing block was either the in_progress we take()'d above (Arc
            // unique → try_unwrap succeeds) or a Replace-emitted block. Either way,
            // try_unwrap should succeed; fall back to clone if not.
            Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())
        });

        let mut new_sealed: ThinVec<IndexBlock, BlockCapacity> =
            ThinVec::with_capacity(new_all.len());
        for arc_b in new_all {
            let b = Arc::try_unwrap(arc_b).unwrap_or_else(|a| (*a).clone());
            new_sealed.push(b);
        }

        let blocks_after = new_sealed.len() + usize::from(new_in_progress.is_some());

        self.sealed = Arc::new(new_sealed);
        // pending was drained via std::mem::take above; leave it empty.
        self.in_progress = new_in_progress;

        info.block_count_delta = blocks_after as i64 - blocks_before as i64;
        self.gc_marker_inc();

        info
    }
}
