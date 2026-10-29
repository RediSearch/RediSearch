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

    /// How many consecutive blocks, starting at `index`, `repair` replaces. More than one
    /// when a run of under-filled blocks was compacted into fewer.
    pub(crate) span: usize,

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

    /// Whether the last block's delta was dropped because the block changed since the
    /// scan and could not be rescanned (no liveness source given, or it would not decode).
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
    /// `check_first` looks for a dead entry before re-encoding anything, for a block the
    /// caller expects clean; it is ignored with a `repair` callback, which must see every
    /// surviving entry.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    pub(crate) fn repair<'block, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'block self,
        block_idx: usize,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'block>, &RepairContext<'block>)>,
        check_first: bool,
        _encoder: PhantomData<E>,
    ) -> std::io::Result<Option<RepairType>> {
        // A snapshot keeps a block it expects clean with no data; see `gc_snapshot`. There is
        // nothing to repair in it, and it must not pass for a block whose every entry is gone.
        if self.buffer.is_empty() && self.num_entries > 0 {
            debug_assert!(
                repair.is_none(),
                "a scan with a repair callback must snapshot every block"
            );
            return Ok(None);
        }

        // A block that turns out clean costs a decode either way; re-encoding it as we go
        // is the waste. When the caller expects it clean, look before encoding. Not with a
        // repair callback: that must see every surviving entry, clean block or not.
        if check_first && repair.is_none() {
            let mut cursor = std::io::Cursor::new(&self.buffer[..]);
            let mut last_read_doc_id = None;
            let mut result = D::base_result();
            let mut any_dead = false;
            while self.buffer.len() as u64 > cursor.position() {
                let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
                D::decode(&mut cursor, base, &mut result)?;
                if !doc_exist(result.doc_id) {
                    any_dead = true;
                    break;
                }
                last_read_doc_id = Some(result.doc_id);
            }
            if !any_dead {
                return Ok(None);
            }
        }

        let mut cursor: std::io::Cursor<&'block [u8]> = std::io::Cursor::new(&self.buffer);
        let mut last_read_doc_id = None;
        let mut result = D::base_result();
        let mut unique_read = 0;
        let mut unique_write = 0;
        let mut block_changed = false;
        // Ordinal of the entry being read, used to carry its field-expiration bit
        // (kept in this block's side bitset, not the encoded buffer) onto the
        // re-encoded survivor so `add_record` re-propagates it into the new block.
        let mut ordinal: u16 = 0;

        let mut tmp_inverted_index = InvertedIndex::<E>::new(IndexFlags_Index_DocIdsOnly);

        while self.buffer.len() as u64 > cursor.position() {
            let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;
            result.has_field_expiration = self.expiration_bit(ordinal);
            ordinal += 1;

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

        if tmp_inverted_index.blocks.is_empty() {
            Ok(Some(RepairType::Delete {
                n_unique_docs_removed: unique_read,
            }))
        } else if block_changed {
            // The replacement blocks grew by doubling; they are long-lived, so give the
            // slack back (the fork parent gets them tight through serde anyway).
            let mut blocks = tmp_inverted_index.blocks;
            for block in blocks.iter_mut() {
                block.buffer.shrink_to_fit();
            }
            Ok(Some(RepairType::Replace {
                blocks: SmallVec::from_iter(blocks),
                n_unique_docs_removed: unique_read - unique_write,
            }))
        } else {
            Ok(None)
        }
    }
}

/// Which blocks [`InvertedIndex::gc_snapshot`](crate::InvertedIndex::gc_snapshot) copies
/// with their data.
/// Re-encode the surviving entries of `blocks`, a run of consecutive under-filled
/// blocks, into fewer blocks: the GC's block compaction. Every block of the run
/// carries its data (see [`InvertedIndex::gc_snapshot`]). `repair` sees each
/// surviving entry with the block it came from, as in [`IndexBlock::repair`].
/// `Ok(None)` when the merge would not reduce the block count.
pub(crate) fn compact_run<'blocks, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
    blocks: &'blocks [IndexBlock],
    first_block_idx: usize,
    doc_exist: impl Fn(DocId) -> bool,
    mut repair: Option<impl FnMut(&RSIndexResult<'blocks>, &RepairContext<'blocks>)>,
    _encoder: PhantomData<E>,
) -> std::io::Result<Option<RepairType>> {
    debug_assert!(blocks.len() > 1, "a run to compact has at least two blocks");
    let mut tmp_inverted_index = InvertedIndex::<E>::new(IndexFlags_Index_DocIdsOnly);
    let mut unique_read = 0;
    let mut unique_write = 0;
    let mut last_read_doc_id: Option<DocId> = None;

    for (offset, block) in blocks.iter().enumerate() {
        debug_assert!(
            !block.buffer.is_empty() || block.num_entries == 0,
            "a compacted run holds only copied blocks"
        );
        let mut cursor = std::io::Cursor::new(&block.buffer[..]);
        let mut last_in_block = None;
        let mut result = D::base_result();
        let mut ordinal: u16 = 0;
        while block.buffer.len() as u64 > cursor.position() {
            let base = D::base_id(block, last_in_block.unwrap_or(block.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;
            result.has_field_expiration = block.expiration_bit(ordinal);
            ordinal += 1;

            let new_doc = last_read_doc_id != Some(result.doc_id);
            if doc_exist(result.doc_id) {
                if let Some(repair) = repair.as_mut() {
                    let ctx = RepairContext {
                        block,
                        block_idx: first_block_idx + offset,
                    };
                    repair(&result, &ctx);
                }
                tmp_inverted_index.add_record(&result)?;
                if new_doc {
                    unique_write += 1;
                }
            }
            if new_doc {
                unique_read += 1;
            }
            last_in_block = Some(result.doc_id);
            last_read_doc_id = Some(result.doc_id);
        }
    }

    if tmp_inverted_index.blocks.is_empty() {
        return Ok(Some(RepairType::Delete {
            n_unique_docs_removed: unique_read,
        }));
    }
    // A codec with a narrow delta may split the merged entries back into as many
    // blocks as before (or more); then the run is left to per-block repair.
    if tmp_inverted_index.blocks.len() >= blocks.len() {
        return Ok(None);
    }
    let mut new_blocks = tmp_inverted_index.blocks;
    for block in new_blocks.iter_mut() {
        block.buffer.shrink_to_fit();
    }
    Ok(Some(RepairType::Replace {
        blocks: SmallVec::from_iter(new_blocks),
        n_unique_docs_removed: unique_read - unique_write,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcSnapshotScope {
    /// Only the blocks whose doc-ID range holds a gone document: enough for a scan
    /// without a repair callback, which skips the other blocks anyway.
    DirtyBlocks,
    /// Every block: for a scan whose repair callback must see every surviving entry,
    /// such as the numeric tree's cardinality repair.
    AllBlocks,
}

/// Tells a GC scan which documents are gone.
pub trait DocLiveness {
    /// Whether document `id` still exists.
    fn exists(&self, id: DocId) -> bool;

    /// Whether any document in `first..=last` may be gone.
    ///
    /// `false` lets a scan skip the range without decoding it, and lets
    /// [`InvertedIndex::gc_snapshot`](crate::InvertedIndex::gc_snapshot) copy the
    /// range's blocks without their data, so it must be exact: a `false` for a range
    /// that does hold a gone document hides that document's entries from the GC until
    /// a later cycle asks again. `true` is always safe.
    fn any_gone(&self, _first: DocId, _last: DocId) -> bool {
        true
    }

    /// How many documents in `first..=last` are gone, when the source can count them
    /// cheaply. A count lets a scan look for a dead entry before re-encoding a block
    /// that most likely has none; `None`, the default, keeps the one-pass scan.
    fn gone_in_range(&self, _first: DocId, _last: DocId) -> Option<usize> {
        None
    }
}

impl<F: Fn(DocId) -> bool> DocLiveness for F {
    fn exists(&self, id: DocId) -> bool {
        self(id)
    }
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Scan the index for blocks that can be garbage collected. A block can be garbage collected
    /// if any of its records point to documents that no longer exist, according to `doc_exist`;
    /// see [`DocLiveness`]. Blocks it rules clean are skipped without being decoded, unless
    /// `repair` is given.
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
        doc_exist: impl DocLiveness,
        mut repair: Option<impl for<'call> FnMut(&RSIndexResult<'call>, &RepairContext<'call>)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        let (Some(first), Some(last)) = (self.blocks.first(), self.blocks.last()) else {
            return Ok(None);
        };
        if !doc_exist.any_gone(first.first_doc_id, last.last_doc_id) && !self.has_compactable_run()
        {
            return Ok(None);
        }
        // `repair` must see every surviving entry, so blocks are only skipped without it.
        let skip_clean_blocks = repair.is_none();
        let last_idx = self.blocks.len() - 1;

        // First pass: repair each block on its own, as before compaction existed.
        let mut repairs: Vec<Option<Option<RepairType>>> = Vec::with_capacity(self.blocks.len());
        for (i, block) in self.blocks.iter().enumerate() {
            if skip_clean_blocks && !doc_exist.any_gone(block.first_doc_id, block.last_doc_id) {
                repairs.push(None);
                continue;
            }
            // Fewer deleted IDs in the block's range than entries: most likely none of them
            // is in this block (a rare term's block spans many IDs), so check before encoding.
            let check_first = doc_exist
                .gone_in_range(block.first_doc_id, block.last_doc_id)
                .is_some_and(|gone| gone < block.num_entries as usize);
            repairs.push(Some(block.repair(
                i,
                |id| doc_exist.exists(id),
                repair.as_mut(),
                check_first,
                PhantomData::<E>,
            )?));
        }

        // Second pass: runs of consecutive copied blocks before the last whose surviving
        // entries fit one block are re-encoded together. The last block never joins: it
        // is the one writers append to, and the apply's staleness check names it by position.
        let surviving: Vec<u32> = repairs
            .iter()
            .zip(self.blocks.iter())
            .map(|(repair, block)| match repair {
                Some(Some(RepairType::Delete { .. })) => 0,
                Some(Some(RepairType::Replace { blocks, .. })) => {
                    blocks.iter().map(|b| u32::from(b.num_entries)).sum()
                }
                _ => u32::from(block.num_entries),
            })
            .collect();
        let limit = u32::from(E::RECOMMENDED_BLOCK_ENTRIES);
        let mut results = Vec::new();
        let mut i = 0;
        while i < self.blocks.len() {
            let mut run = 0;
            let mut total = 0;
            while i + run < last_idx && self.blocks[i + run].has_data() {
                let next = total + surviving[i + run];
                if next > limit {
                    break;
                }
                total = next;
                run += 1;
            }
            let compacted = if run > 1 {
                compact_run(
                    &self.blocks[i..i + run],
                    i,
                    |id| doc_exist.exists(id),
                    repair.as_mut(),
                    PhantomData::<E>,
                )?
            } else {
                None
            };
            if let Some(repair) = compacted {
                results.push(BlockGcScanResult {
                    index: i,
                    span: run,
                    repair,
                });
                i += run;
                continue;
            }
            if let Some(Some(repair)) = repairs[i].take() {
                results.push(BlockGcScanResult {
                    index: i,
                    span: 1,
                    repair,
                });
            }
            i += 1;
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(GcScanDelta {
                last_block_idx: last_idx,
                last_block_num_entries: last.num_entries,
                deltas: results,
            }))
        }
    }

    pub fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        self.apply_gc_with(delta, None)
    }

    /// [`Self::apply_gc`], but when the scanned last block grew since the scan and `live`
    /// is given, the block is rescanned here, under the caller's exclusive access, so a
    /// busy index's tail is collected instead of denied until a quieter cycle. That block
    /// is then judged by `live` as it is now while the other deltas reflect the scan; the
    /// mix is safe because a gone document never comes back.
    pub fn apply_gc_with(
        &mut self,
        delta: GcScanDelta,
        live: Option<&dyn DocLiveness>,
    ) -> GcApplyInfo {
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

        // If the last block has changed, its delta is stale: rescan the block as it is now,
        // or drop the delta.
        if last_block_changed {
            let remove_stale_delta = deltas
                .last()
                .map(|d| d.index == last_block_idx)
                .unwrap_or(false);
            if remove_stale_delta {
                deltas.pop();
            }
            let rescanned = live.map(|live| {
                self.blocks[last_block_idx].repair(
                    last_block_idx,
                    |id| live.exists(id),
                    None::<fn(&RSIndexResult<'_>, &RepairContext<'_>)>,
                    false,
                    PhantomData::<E>,
                )
            });
            match rescanned {
                Some(Ok(Some(repair))) => deltas.push(BlockGcScanResult {
                    index: last_block_idx,
                    span: 1,
                    repair,
                }),
                // Rescanned and clean.
                Some(Ok(None)) => {}
                // Nothing to rescan with, or the block would not decode: the next cycle's.
                None | Some(Err(_)) => info.ignored_last_block = true,
            }
        }

        // There is no point in moving everything to a new vector if there are no deltas
        if deltas.is_empty() {
            return info;
        }

        let mut tmp_blocks = ThinVec::with_capacity(self.blocks.len());
        std::mem::swap(&mut self.blocks, &mut tmp_blocks);

        let mut deltas = deltas.into_iter().peekable();
        let mut originals = tmp_blocks.into_iter().enumerate();

        while let Some((block_index, block)) = originals.next() {
            match deltas.peek() {
                Some(delta) if delta.index == block_index => {
                    let Some(delta) = deltas.next() else {
                        unreachable!(
                            "we are in the `Some` case and therefore know the next value exists"
                        )
                    };
                    // The delta replaces `span` originals; a compacted run spans several.
                    info.entries_removed += block.num_entries as usize;
                    info.bytes_freed += block.mem_usage();
                    for _ in 1..delta.span {
                        let (_, dropped) = originals
                            .next()
                            .expect("a delta never spans past the last block");
                        info.entries_removed += dropped.num_entries as usize;
                        info.bytes_freed += dropped.mem_usage();
                    }

                    match delta.repair {
                        RepairType::Delete {
                            n_unique_docs_removed,
                        } => {
                            self.n_unique_docs -= n_unique_docs_removed;
                        }
                        RepairType::Replace {
                            blocks,
                            n_unique_docs_removed,
                        } => {
                            self.n_unique_docs -= n_unique_docs_removed;
                            for block in blocks {
                                info.entries_removed -= block.num_entries as usize;
                                info.bytes_allocated += block.mem_usage();
                                self.blocks.push(block);
                            }
                        }
                    }
                }
                _ => {
                    self.blocks.push(block);
                }
            }
        }

        {
            let had_allocated = self.blocks.has_allocated();
            self.blocks.shrink_to_fit();
            // If we got rid of the heap block buffer entirely, we have also freed the memory occupied
            // by the thin vec header. That hasn't been accounted for yet, so we add it to the bytes freed now.
            if !self.blocks.has_allocated() && had_allocated {
                info.bytes_freed += Header::<BlockCapacity>::size_with_padding::<IndexBlock>();
            }
        }

        info.block_count_delta = self.blocks.len() as i64 - blocks_before as i64;
        self.gc_marker_inc();

        info
    }
}
