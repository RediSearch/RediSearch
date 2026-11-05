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

use crate::{
    BlockCapacity, DecodedBy, Decoder, Encoder, IndexBlock, InvertedIndex, index::ExpirationBits,
};
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
        /// The new blocks to replace this block with, each with its field-expiration bitset (see
        /// [`InvertedIndex::expiration`])
        blocks: SmallVec<[(IndexBlock, ExpirationBits); 3]>,

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
// `export` is required: `src/gc/*.c` declare this by value, so the
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

    /// Whether the last block changed since the scan and was rescanned in place with the
    /// liveness source given to [`InvertedIndex::apply_gc_with`]. The block's survivors are
    /// then whatever the rescan kept, not what the scan's delta described, so a caller
    /// keeping derived statistics about the last block (as the numeric tree does) must
    /// recompute them from the block as it is now.
    pub rescanned_last_block: bool,
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
    /// Entries before the first dead one are never re-encoded. An entry's encoding depends
    /// only on itself and the ID before it, so encoding them again would reproduce the
    /// bytes they already occupy: those bytes are copied as they are once a dead entry
    /// turns up, and a block with no dead entry costs one decode and nothing else.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    pub(crate) fn repair<'block, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'block self,
        block_idx: usize,
        expiration: Option<&ExpirationBits>,
        doc_exist: impl Fn(DocId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'block>, &RepairContext<'block>)>,
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

        let mut cursor: std::io::Cursor<&'block [u8]> = std::io::Cursor::new(&self.buffer);
        let mut last_read_doc_id = None;
        let mut result = D::base_result();
        let mut unique_read = 0;
        let mut unique_write = 0;
        // Ordinal of the entry being read, used to carry its field-expiration bit
        // (kept in `expiration`, not the encoded buffer) onto the re-encoded
        // survivor so `add_record` re-propagates it into the new block.
        let mut ordinal: u16 = 0;
        // Created at the first dead entry, seeded with the bytes of the entries before it.
        let mut survivors: Option<InvertedIndex<E>> = None;

        while self.buffer.len() as u64 > cursor.position() {
            let entry_start = cursor.position() as usize;
            let base = D::base_id(self, last_read_doc_id.unwrap_or(self.first_doc_id));
            D::decode(&mut cursor, base, &mut result)?;
            result.has_field_expiration = expiration.is_some_and(|bits| bits.contains(ordinal));
            let new_doc = last_read_doc_id.is_none_or(|id| id != result.doc_id);

            if doc_exist(result.doc_id) {
                if let Some(repair) = repair.as_mut() {
                    let ctx = RepairContext {
                        block: self,
                        block_idx,
                    };
                    repair(&result, &ctx);
                }
                if let Some(survivors) = survivors.as_mut() {
                    survivors.add_record(&result)?;
                }
                if new_doc {
                    unique_write += 1;
                }
            } else if survivors.is_none() {
                survivors = Some(self.survivors_up_to::<E>(
                    expiration,
                    entry_start,
                    ordinal,
                    last_read_doc_id,
                ));
            }

            if new_doc {
                unique_read += 1;
            }
            ordinal += 1;
            last_read_doc_id = Some(result.doc_id);
        }

        let Some(survivors) = survivors else {
            return Ok(None);
        };
        if survivors.blocks.is_empty() {
            Ok(Some(RepairType::Delete {
                n_unique_docs_removed: unique_read,
            }))
        } else {
            // The replacement blocks grew by doubling; they are long-lived, so give the
            // slack back (the fork parent gets them tight through serde anyway).
            let mut survivors = survivors;
            let blocks = survivors
                .take_blocks()
                .map(|(mut block, bits)| {
                    block.buffer.shrink_to_fit();
                    (block, bits)
                })
                .collect();
            Ok(Some(RepairType::Replace {
                blocks,
                n_unique_docs_removed: unique_read - unique_write,
            }))
        }
    }

    /// An index holding this block's first `entries` entries, which occupy its first `bytes`
    /// bytes and end with the ID `last`, copied rather than re-encoded with their bits of
    /// `expiration`; see [`Self::repair`]. Empty when `entries` is 0.
    fn survivors_up_to<E: Encoder>(
        &self,
        expiration: Option<&ExpirationBits>,
        bytes: usize,
        entries: u16,
        last: Option<DocId>,
    ) -> InvertedIndex<E> {
        let mut survivors = InvertedIndex::<E>::new(IndexFlags_Index_DocIdsOnly);
        let (Some(last), true) = (last, entries > 0) else {
            return survivors;
        };
        let mut prefix = IndexBlock::new(self.first_doc_id);
        prefix.buffer = self.buffer[..bytes].to_vec();
        prefix.num_entries = entries;
        // The base the next survivor's delta is encoded against.
        prefix.last_doc_id = last;
        let bits = expiration.map_or_else(ExpirationBits::new, |bits| bits.prefix(entries));
        survivors.push_block(prefix, bits);
        survivors
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
    expiration: &[ExpirationBits],
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
    let mut removed_entries = 0usize;

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
            result.has_field_expiration = expiration
                .get(first_block_idx + offset)
                .is_some_and(|bits| bits.contains(ordinal));
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
            } else {
                removed_entries += 1;
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
    // A codec with a narrow delta may split the merged entries back into as many blocks as
    // before (or more). A run that lost nothing is then left as it is, or every cycle would
    // rewrite it; one that lost entries is still replaced, as nothing else repairs its blocks.
    if tmp_inverted_index.blocks.len() >= blocks.len() && removed_entries == 0 {
        return Ok(None);
    }
    let new_blocks = tmp_inverted_index
        .take_blocks()
        .map(|(mut block, bits)| {
            block.buffer.shrink_to_fit();
            (block, bits)
        })
        .collect();
    Ok(Some(RepairType::Replace {
        blocks: new_blocks,
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
    /// When the scan returns a delta, `repair` has been called with every surviving entry,
    /// to run any repair calculations needed. A scan that returns `None` may call it with
    /// none: with nothing to apply, there is nothing for it to repair.
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
        let Some(span) = self.doc_id_range() else {
            return Ok(None);
        };
        let compactable = self.has_compactable_run();
        if !doc_exist.any_gone(*span.start(), *span.end()) && !compactable {
            return Ok(None);
        }
        // `repair` must see every surviving entry, so blocks are only skipped without it.
        let skip_clean_blocks = repair.is_none();
        let last_idx = self.blocks.len() - 1;
        let limit = u32::from(E::RECOMMENDED_BLOCK_ENTRIES);
        let mut results = Vec::new();
        let mut i = 0;
        while i < self.blocks.len() {
            // A run is the consecutive copied blocks from here whose entries fit one block,
            // as `has_compactable_run` finds them; the last block never joins one, as writers
            // append to it and the apply's staleness check names it by position. A run is
            // packed as it is read, so each of its entries is decoded once and each survivor
            // encoded once.
            let mut run = 0;
            if compactable {
                let mut total = 0;
                while i + run < last_idx && self.blocks[i + run].has_data() {
                    let next = total + u32::from(self.blocks[i + run].num_entries);
                    if next > limit {
                        break;
                    }
                    total = next;
                    run += 1;
                }
            }
            if run > 1 {
                if let Some(packed) = compact_run(
                    &self.blocks[i..i + run],
                    &self.expiration,
                    i,
                    |id| doc_exist.exists(id),
                    repair.as_mut(),
                    PhantomData::<E>,
                )? {
                    results.push(BlockGcScanResult {
                        index: i,
                        span: run,
                        repair: packed,
                    });
                }
                i += run;
                continue;
            }
            let block = &self.blocks[i];
            if (!skip_clean_blocks || doc_exist.any_gone(block.first_doc_id, block.last_doc_id))
                && let Some(fixed) = block.repair(
                    i,
                    self.expiration.get(i),
                    |id| doc_exist.exists(id),
                    repair.as_mut(),
                    PhantomData::<E>,
                )?
            {
                results.push(BlockGcScanResult {
                    index: i,
                    span: 1,
                    repair: fixed,
                });
            }
            i += 1;
        }
        Ok((!results.is_empty()).then(|| GcScanDelta {
            last_block_idx: last_idx,
            last_block_num_entries: self.blocks[last_idx].num_entries,
            deltas: results,
        }))
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
            rescanned_last_block: false,
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
                    self.expiration.get(last_block_idx),
                    |id| live.exists(id),
                    None::<fn(&RSIndexResult<'_>, &RepairContext<'_>)>,
                    PhantomData::<E>,
                )
            });
            match rescanned {
                Some(Ok(Some(repair))) => {
                    info.rescanned_last_block = true;
                    deltas.push(BlockGcScanResult {
                        index: last_block_idx,
                        span: 1,
                        repair,
                    })
                }
                // Rescanned and clean.
                Some(Ok(None)) => info.rescanned_last_block = true,
                // Nothing to rescan with, or the block would not decode: the next cycle's.
                None | Some(Err(_)) => info.ignored_last_block = true,
            }
        }

        // There is no point in moving everything to a new vector if there are no deltas
        if deltas.is_empty() {
            return info;
        }

        let expiration_heap_before = self.expiration_mem_usage();
        let n_blocks = self.blocks.len();
        let had_expiration = !self.expiration.is_empty();
        let old_blocks = self.take_blocks();
        self.blocks = ThinVec::with_capacity(n_blocks);
        if had_expiration {
            self.expiration = ThinVec::with_capacity(n_blocks);
        }

        let mut deltas = deltas.into_iter().peekable();
        let mut originals = old_blocks.enumerate();

        while let Some((block_index, (block, bits))) = originals.next() {
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
                        let (_, (dropped, _)) = originals
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
                            for (block, bits) in blocks {
                                info.entries_removed -= block.num_entries as usize;
                                info.bytes_allocated += block.mem_usage();
                                self.push_block(block, bits);
                            }
                        }
                    }
                }
                _ => {
                    self.push_block(block, bits);
                }
            }
        }

        // Drop the bitsets entirely once no surviving entry has a field-level expiration.
        if self.expiration.iter().all(ExpirationBits::is_empty) {
            self.expiration = ThinVec::new();
        } else {
            self.expiration.shrink_to_fit();
        }
        let expiration_heap_after = self.expiration_mem_usage();
        if expiration_heap_after > expiration_heap_before {
            info.bytes_allocated += expiration_heap_after - expiration_heap_before;
        } else {
            info.bytes_freed += expiration_heap_before - expiration_heap_after;
        }

        // Remove excess capacity from the blocks vector.
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
