/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::{BlockCapacity, DecodedBy, Decoder, Encoder, IndexBlock, InvertedIndex};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use rqe_core::DocId;
use smallvec::SmallVec;
use thin_vec::{Header, ThinVec};

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
    /// pointer-equality on `block` — pointer identity isn't reliable if blocks are
    /// ever decoded into temporary buffers rather than read in place.
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

#[cfg(feature = "test_utils")]
impl GcScanDelta {
    /// Returns a no-op delta with no block repairs, for use in tests that need
    /// to encode/decode the wire protocol without exercising GC logic.
    pub const fn empty_for_testing() -> Self {
        Self {
            last_block_idx: 0,
            last_block_num_entries: 0,
            deltas: vec![],
        }
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
// `export` is required: `src/fork_gc/{terms,tags}.c` declare this by value, so the
// C header needs the complete type. Nothing else reaches it by value any more.
#[cheadergen::config(export, rename = "II_GCScanStats")]
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
    /// if blocks are ever decoded into temporary buffers rather than read in place.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    ///
    /// Re-encoding is deferred until the first dead entry is seen. A block whose
    /// entries all survive is the common case in a healthy index, and it would
    /// otherwise pay to rebuild a block that is then discarded — so it costs a
    /// decode pass and no allocation. The price is that a block which *does*
    /// change re-decodes the surviving prefix once, in
    /// [`reencode_prefix`](Self::reencode_prefix).
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
        // Ordinal of the entry being read, used to carry its field-expiration bit
        // (kept in this block's side bitset, not the encoded buffer) onto the
        // re-encoded survivor so `add_record` re-propagates it into the new block.
        let mut ordinal: u16 = 0;

        // `Some` once a dead entry has proved the block must be rebuilt. While it
        // is `None`, every entry read so far survives and the block still stands
        // as it is — which is also what distinguishes "nothing to repair" from
        // "everything died" at the end, since both leave no blocks behind.
        let mut tmp_inverted_index: Option<InvertedIndex<E>> = None;

        while self.buffer.len() as u64 > cursor.position() {
            let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;
            result.has_field_expiration = self.expiration_bit(ordinal);

            let starts_new_doc = last_read_doc_id.is_none_or(|id| id != result.doc_id);

            if doc_exist(result.doc_id) {
                if let Some(repair) = repair.as_mut() {
                    let ctx = RepairContext {
                        block: self,
                        block_idx,
                    };
                    repair(&result, &ctx);
                }

                if let Some(tmp) = tmp_inverted_index.as_mut() {
                    tmp.add_record(&result)?;

                    if starts_new_doc {
                        unique_write += 1;
                    }
                }
            } else if tmp_inverted_index.is_none() {
                // First dead entry. Everything before it survived but was decoded
                // and dropped, so replay that prefix into a fresh block. The
                // `repair` callback has already seen those records and must not
                // observe them twice, which is why the replay does not take it.
                let (prefix, prefix_unique) = self.reencode_prefix::<E, D>(ordinal)?;
                tmp_inverted_index = Some(prefix);
                unique_write = prefix_unique;
            }

            if starts_new_doc {
                unique_read += 1;
            }

            last_read_doc_id = Some(result.doc_id);
            ordinal += 1;
        }

        match tmp_inverted_index {
            // Every entry survived: the block stands as it is.
            None => Ok(None),
            // Something died and nothing was re-encoded, so the whole block goes.
            Some(tmp) if tmp.blocks.is_empty() => Ok(Some(RepairType::Delete {
                n_unique_docs_removed: unique_read,
            })),
            Some(tmp) => Ok(Some(RepairType::Replace {
                blocks: SmallVec::from_iter(tmp.blocks),
                n_unique_docs_removed: unique_read - unique_write,
            })),
        }
    }

    /// Re-encode the first `count` entries of this block, all of which are known
    /// to belong to live documents, into a fresh index.
    ///
    /// Returns the index and the number of distinct documents written, which
    /// seeds `unique_write` in [`repair`](Self::repair) — entries within a block
    /// are ordered by document ID, so a document is new when it differs from the
    /// previous one.
    fn reencode_prefix<'block, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'block self,
        count: u16,
    ) -> std::io::Result<(InvertedIndex<E>, u32)> {
        let mut tmp_inverted_index = InvertedIndex::<E>::new(IndexFlags_Index_DocIdsOnly);
        let mut cursor: std::io::Cursor<&'block [u8]> = std::io::Cursor::new(&self.buffer);
        let mut last_read_doc_id = None;
        let mut result = D::base_result();
        let mut unique_write = 0;

        for ordinal in 0..count {
            let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;
            result.has_field_expiration = self.expiration_bit(ordinal);

            tmp_inverted_index.add_record(&result)?;

            if last_read_doc_id.is_none_or(|id| id != result.doc_id) {
                unique_write += 1;
            }

            last_read_doc_id = Some(result.doc_id);
        }

        Ok((tmp_inverted_index, unique_write))
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
    /// The higher-ranked bound (`for<'call> FnMut(&RSIndexResult<'call>, ..)`) scopes the
    /// record and context borrows to a single callback invocation: `repair` must accept any
    /// lifetime, so it cannot stash a borrow and use it after the call returns. This keeps the
    /// callback sound regardless of whether records are read in place or decoded into a
    /// short-lived buffer for the duration of the call.
    ///
    /// This function returns a delta if GC is needed, or `None` if no GC is needed.
    pub fn scan_gc(
        &self,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl for<'call> FnMut(&RSIndexResult<'call>, &RepairContext<'call>)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        let mut results = Vec::new();

        for (i, block) in self.blocks.iter().enumerate() {
            let repair = block.repair(i, &doc_exist, repair.as_mut(), PhantomData::<E>)?;

            if let Some(repair) = repair {
                results.push(BlockGcScanResult { index: i, repair });
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(GcScanDelta {
                last_block_idx: self.blocks.len() - 1,
                last_block_num_entries: self.blocks.last().map(|b| b.num_entries).unwrap_or(0),
                deltas: results,
            }))
        }
    }

    /// Appends between two probes of a tail block that is not yet full.
    ///
    /// Bounds the added decode rate to one block decode per this many writes. Probing only
    /// when the block fills would be cheaper still, but see
    /// [`should_probe_tail_block`](Self::should_probe_tail_block) for why that misses the
    /// blocks that need it most.
    const PROBE_STRIDE: u16 = 8;

    /// Entry count below which the tail block is probed on every append.
    ///
    /// Deliberately equal to [`Self::PROBE_STRIDE`], so a block is probed on every write
    /// until the first stride boundary and every stride thereafter — one rule, no gap at
    /// the start where a list shorter than a stride would never be probed at all.
    const PROBE_EVERY_WRITE_BELOW: u16 = Self::PROBE_STRIDE;

    /// Whether this write should probe the tail block for reclaimable entries.
    ///
    /// True when the block has filled — the last moment it can be repaired inline, since
    /// the next new document rotates it away — and periodically before then.
    ///
    /// The periodic case is what reaches short posting lists. A list that never fills a
    /// block is *entirely* tail, so the fork GC never repairs it either: it discards any
    /// delta touching the last block. Waiting for a full block would leave every such term
    /// reclaimed by neither path, and in a natural-language index those terms are most of
    /// the vocabulary.
    ///
    /// The cadence is self-limiting in the same way the full-block check was. A repair that
    /// reclaims `k` entries moves the block `k` entries back down, so the next probe is `k`
    /// writes further away; a clean block simply pays the decode.
    fn should_probe_tail_block(&self) -> bool {
        self.blocks.last().is_some_and(|b| {
            b.num_entries >= E::RECOMMENDED_BLOCK_ENTRIES
                || (b.num_entries > 0
                    && (b.num_entries < Self::PROBE_EVERY_WRITE_BELOW
                        || b.num_entries % Self::PROBE_STRIDE == 0))
        })
    }

    /// [`repair_tail_block`](Self::repair_tail_block), gated on
    /// [`should_probe_tail_block`](Self::should_probe_tail_block). This is the form the
    /// write path should call.
    ///
    /// # Errors
    ///
    /// See [`repair_tail_block`](Self::repair_tail_block).
    pub fn maybe_repair_tail_block(
        &mut self,
        min_reclaim_pct: u8,
        doc_exist: impl Fn(DocId) -> bool,
    ) -> std::io::Result<Option<GcApplyInfo>> {
        if !self.should_probe_tail_block() {
            return Ok(None);
        }

        self.repair_tail_block(min_reclaim_pct, doc_exist)
    }

    /// Reclaim entries for deleted documents from this index's **last block only**,
    /// in place, without a garbage-collection scan.
    ///
    /// This is the write-path counterpart to [`apply_gc`](Self::apply_gc). A writer only
    /// ever appends to the last block, so that is the only block it can repair without
    /// widening the cost of a write past one block — and it is the block a fork
    /// garbage collection deliberately never repairs, because a concurrent append would
    /// invalidate the scan. Every other block stays the fork GC's responsibility.
    ///
    /// Returns `None` when the block was left untouched, which happens when the index has
    /// no blocks, when no entry in the last block belongs to a deleted document, or when
    /// the reclaim would fall below `min_reclaim_pct` of the block's entries. In all three
    /// cases the GC marker is left alone, so readers positioned in the block are not made
    /// to revalidate for nothing.
    ///
    /// `min_reclaim_pct` of `0` accepts any reclaim at all.
    ///
    /// # Errors
    ///
    /// Propagates a decode failure from the block being read. The index is unmodified in
    /// that case — the decode happens before any mutation.
    pub fn repair_tail_block(
        &mut self,
        min_reclaim_pct: u8,
        doc_exist: impl Fn(DocId) -> bool,
    ) -> std::io::Result<Option<GcApplyInfo>> {
        let Some(last_idx) = self.blocks.len().checked_sub(1) else {
            return Ok(None);
        };
        let entries_before = self.blocks[last_idx].num_entries;

        let repair = self.blocks[last_idx].repair(
            last_idx,
            doc_exist,
            None::<fn(&RSIndexResult, &RepairContext<'_>)>,
            PhantomData::<E>,
        )?;

        let Some(repair) = repair else {
            return Ok(None);
        };

        if !Self::reclaim_is_worthwhile(&repair, entries_before, min_reclaim_pct) {
            return Ok(None);
        }

        let blocks_before = self.blocks.len();
        let mut info = GcApplyInfo::default();

        let block = self
            .blocks
            .pop()
            .expect("`last_idx` was derived from a non-zero length and nothing popped since");
        self.absorb_block_repair(block, repair, &mut info);
        self.finish_block_repair(blocks_before, &mut info);

        Ok(Some(info))
    }

    /// Whether a repair of the tail block earns the rewrite it costs.
    ///
    /// A `Replace` that yields more than one block is rejected outright: removing an entry
    /// can widen a document-ID delta past what the encoder can represent and force a split,
    /// which would grow the index rather than shrink it. Those entries are left for the
    /// fork GC, which can afford the split because it is not on the write path.
    fn reclaim_is_worthwhile(
        repair: &RepairType,
        entries_before: u16,
        min_reclaim_pct: u8,
    ) -> bool {
        match repair {
            // The whole block goes away; nothing is rewritten.
            RepairType::Delete { .. } => true,
            RepairType::Replace { blocks, .. } => {
                if blocks.len() > 1 {
                    return false;
                }
                let kept: u32 = blocks.iter().map(|b| u32::from(b.num_entries)).sum();
                let removed = u32::from(entries_before).saturating_sub(kept);
                removed * 100 >= u32::from(entries_before) * u32::from(min_reclaim_pct)
            }
        }
    }

    /// Retire `block`, push whatever replaces it onto [`Self::blocks`], and fold the
    /// change into `info` and [`Self::n_unique_docs`].
    ///
    /// Shared by [`apply_gc`](Self::apply_gc) and
    /// [`repair_tail_block`](Self::repair_tail_block): the two paths must agree on the
    /// index's unique-document count, and a second copy of this arithmetic is how that
    /// count goes wrong without any test noticing.
    ///
    /// The caller is responsible for having removed `block` from [`Self::blocks`] first.
    fn absorb_block_repair(
        &mut self,
        block: IndexBlock,
        repair: RepairType,
        info: &mut GcApplyInfo,
    ) {
        info.entries_removed += block.num_entries as usize;
        info.bytes_freed += block.mem_usage();

        let (replacements, n_unique_docs_removed) = match repair {
            RepairType::Delete {
                n_unique_docs_removed,
            } => (SmallVec::new(), n_unique_docs_removed),
            RepairType::Replace {
                blocks,
                n_unique_docs_removed,
            } => (blocks, n_unique_docs_removed),
        };

        self.n_unique_docs -= n_unique_docs_removed;

        for replacement in replacements {
            info.entries_removed -= replacement.num_entries as usize;
            info.bytes_allocated += replacement.mem_usage();
            self.blocks.push(replacement);
        }
    }

    /// Settle the block vector after one or more repairs and mark the index as
    /// garbage-collected.
    ///
    /// The [`gc_marker_inc`](Self::gc_marker_inc) here is what tells a reader holding a
    /// pointer into a block that the blocks moved, so every path that mutates
    /// [`Self::blocks`] must end here.
    fn finish_block_repair(&mut self, blocks_before: usize, info: &mut GcApplyInfo) {
        // Remove excess capacity from the blocks vector.
        let had_allocated = self.blocks.has_allocated();
        self.blocks.shrink_to_fit();
        // If we got rid of the heap block buffer entirely, we have also freed the memory occupied
        // by the thin vec header. That hasn't been accounted for yet, so we add it to the bytes freed now.
        if !self.blocks.has_allocated() && had_allocated {
            info.bytes_freed += Header::<BlockCapacity>::size_with_padding::<IndexBlock>();
        }

        info.block_count_delta = self.blocks.len() as i64 - blocks_before as i64;
        self.gc_marker_inc();
    }

    /// Apply the deltas of a garbage collection scan to the index. This will modify the index
    /// by deleting or repairing blocks as needed.
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

        let blocks_before = self.blocks.len();

        // Check if the last block has changed since the scan was performed
        let last_block_changed = self
            .blocks
            .get(last_block_idx)
            .is_some_and(|b| b.num_entries != last_block_num_entries);

        // If the last block has changed, then we need to ignore any deltas that refer to it
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

        // There is no point in moving everything to a new vector if there are no deltas
        if deltas.is_empty() {
            return info;
        }

        let mut tmp_blocks = ThinVec::with_capacity(self.blocks.len());
        std::mem::swap(&mut self.blocks, &mut tmp_blocks);

        let mut deltas = deltas.into_iter().peekable();

        for (block_index, block) in tmp_blocks.into_iter().enumerate() {
            match deltas.peek() {
                Some(delta) if delta.index == block_index => {
                    // This block needs to be repaired
                    let Some(delta) = deltas.next() else {
                        unreachable!(
                            "we are in the `Some` case and therefore know the next value exists"
                        )
                    };

                    self.absorb_block_repair(block, delta.repair, &mut info);
                }
                _ => {
                    // This block does not need to be repaired, so just put it back
                    self.blocks.push(block);
                }
            }
        }

        self.finish_block_repair(blocks_before, &mut info);

        info
    }
}
