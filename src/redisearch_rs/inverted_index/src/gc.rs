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

use crate::{BlockCapacity, DecodedBy, Decoder, Encoder, IndexBlock, InvertedIndex, RSIndexResult};
use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use smallvec::SmallVec;
use thin_vec::{Header, ThinVec};

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
#[derive(Debug, Eq, PartialEq)]
#[repr(C)]
pub struct GcApplyInfo {
    /// The number of bytes that were freed
    pub bytes_freed: usize,

    /// The number of bytes that were allocated
    pub bytes_allocated: usize,

    /// The number of entries that were removed from the index including duplicates
    pub entries_removed: usize,

    /// Whether or not we ignored the last block in the index, since it changed
    /// compared to the time we performed the scan
    pub ignored_last_block: bool,
}

impl IndexBlock {
    /// Repair a block by removing records which no longer exists according to `doc_exists`. If a
    /// record does exist, then `repair` is called with it.
    ///
    /// `None` is returned when there is nothing to repair in this block.
    pub(crate) fn repair<'index, E: Encoder + DecodedBy<Decoder = D>, D: Decoder>(
        &'index self,
        doc_exist: impl Fn(t_docId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
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
                    repair(&result, self);
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
            Ok(Some(RepairType::Replace {
                blocks: SmallVec::from_iter(tmp_inverted_index.blocks),
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
        doc_exist: impl Fn(t_docId) -> bool,
        mut repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        let mut results = Vec::new();

        for (i, block) in self.blocks.iter().enumerate() {
            let repair = block.repair(&doc_exist, repair.as_mut(), PhantomData::<E>)?;

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
            ignored_last_block: false,
        };

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
                info.ignored_last_block = true;
            }
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

                    match delta.repair {
                        RepairType::Delete {
                            n_unique_docs_removed,
                        } => {
                            info.entries_removed += block.num_entries as usize;
                            info.bytes_freed += block.mem_usage();
                            self.n_unique_docs -= n_unique_docs_removed;
                        }
                        RepairType::Replace {
                            blocks,
                            n_unique_docs_removed,
                        } => {
                            info.entries_removed += block.num_entries as usize;
                            info.bytes_freed += block.mem_usage();
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
                    // This block does not need to be repaired, so just put it back
                    self.blocks.push(block);
                }
            }
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

        self.gc_marker_inc();

        info
    }
}
