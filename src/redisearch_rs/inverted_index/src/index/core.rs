/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    sync::atomic::{self, AtomicU32},
};
use thin_vec::ThinVec;

use super::unique_id::IndexUniqueId;
use crate::{
    BlockCapacity, Encoder, IdDelta,
    controlled_cursor::ControlledCursor,
    debug::{BlockSummary, Summary},
};
use ffi::{IndexFlags, IndexFlags_Index_HasMultiValue};
use index_result::RSIndexResult;
use rqe_core::DocId;

/// An inverted index is a data structure that maps terms to their occurrences in documents. It is
/// used to efficiently search for documents that contain specific terms.
#[derive(Debug)]
pub struct InvertedIndex<E> {
    /// The blocks of the index. Each block contains a set of entries for a specific range of
    /// document IDs. The entries and blocks themselves are ordered by document ID, so the first
    /// block contains entries for the lowest document IDs, and the last block contains entries for
    /// the highest document IDs.
    pub(crate) blocks: ThinVec<IndexBlock, BlockCapacity>,

    /// Number of unique documents in the index. This is not the total number of entries, but rather the
    /// number of unique documents that have been indexed.
    pub(crate) n_unique_docs: u32,

    /// The flags of this index. This is used to determine the type of index and how it should be
    /// handled.
    pub(crate) flags: IndexFlags,

    /// A marker used by the garbage collector to determine if the index has been modified since
    /// the last GC pass. This is used to reset a reader if the index has been modified.
    pub(crate) gc_marker: AtomicU32,

    /// A unique identifier for this index instance, assigned at construction time from a global
    /// monotonic counter. Used together with pointer comparison to detect the ABA problem: when
    /// an index is freed and a new one is allocated at the same address, the unique ID will
    /// differ, allowing cursors to detect the replacement.
    unique_id: IndexUniqueId,

    /// The encoder to use when adding new entries to the index
    pub(crate) _encoder: PhantomData<E>,
}

/// Outcome of [`InvertedIndex::add_record`]: how the index grew during the write.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct AddRecordOutcome {
    /// Number of bytes the inverted index's memory usage grew by.
    pub mem_growth: u32,
    /// Number of new index blocks this write created.
    pub blocks_added: u32,
}

/// Each `IndexBlock` contains a set of entries for a specific range of document IDs. The entries
/// are ordered by document ID, so the first entry in the block has the lowest document ID, and the
/// last entry has the highest document ID. The block also contains a buffer that is used to
/// store the encoded entries. The buffer is dynamically resized as needed when new entries are
/// added to the block.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexBlock {
    /// The first document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    pub(crate) first_doc_id: DocId,

    /// The last document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    pub(crate) last_doc_id: DocId,

    /// The total number of non-unique entries in this block
    pub(crate) num_entries: u16,

    /// The encoded entries in this block
    pub(crate) buffer: Vec<u8>,

    /// Bitset indexed by entry ordinal within this block: bit `i` is set when the
    /// `i`-th entry belongs to a field that has a field-level expiration (HFE) for
    /// its document. (For tag/numeric indexes that is the single owning field; for
    /// term indexes it is any field in the posting's field mask.)
    ///
    /// This is *not* part of the encoded `buffer`; it is kept alongside it so the
    /// document-id codec stays untouched. It is grown lazily and only as far as
    /// the highest expiring ordinal, so a block whose documents have no field
    /// expirations (the common case) keeps it empty and pays only the empty-`Vec`
    /// header. Readers consult it by entry ordinal to set
    /// [`RSIndexResult::has_field_expiration`](index_result::RSIndexResult::has_field_expiration),
    /// letting expiration-aware iterators skip the TTL-table lookup for documents
    /// that have no field TTL.
    ///
    /// Represented as `Option<Box<[u8]>>` rather than `Vec<u8>` so that a block
    /// whose documents have no field expirations (the common case) costs only one
    /// pointer (`None`) instead of a 3-word `Vec` header — keeping the per-block
    /// overhead at 8 bytes. It is serialized along with the block (the fork GC
    /// round-trips blocks through `rmp_serde`), so the bits survive GC.
    pub(crate) expiration_bits: Option<Box<[u8]>>,
}

impl IndexBlock {
    pub(crate) const STACK_SIZE: usize = std::mem::size_of::<Self>();

    /// Make a new index block with primed with the initial doc ID. The next entry written into
    /// the block should be for this doc ID else the block will contain incoherent data.
    pub(crate) const fn new(doc_id: DocId) -> Self {
        Self {
            first_doc_id: doc_id,
            last_doc_id: doc_id,
            num_entries: 0,
            buffer: Vec::new(),
            expiration_bits: None,
        }
    }

    /// The number of bytes occupied by the field-expiration bitset, if any.
    fn expiration_bits_len(&self) -> usize {
        self.expiration_bits.as_deref().map_or(0, <[u8]>::len)
    }

    /// Get the memory usage of this block, including the stack size, the capacity of the bytes
    /// buffer, and the field-expiration bitset.
    pub fn mem_usage(&self) -> usize {
        Self::STACK_SIZE + self.buffer.capacity() + self.expiration_bits_len()
    }

    /// Record that the entry at `ordinal` (its 0-based position within this block)
    /// belongs to a document with at least one field-level expiration. The bitset
    /// grows on demand; ordinals below the highest set bit that were never set stay
    /// `0` (no expiration), which is exactly what we want for non-expiring entries.
    pub(crate) fn set_expiration_bit(&mut self, ordinal: u16) {
        let byte = ordinal as usize / 8;
        let needed = byte + 1;
        let bits = match self.expiration_bits.take() {
            Some(bits) if bits.len() >= needed => bits,
            Some(bits) => {
                let mut grown = vec![0u8; needed];
                grown[..bits.len()].copy_from_slice(&bits);
                grown.into_boxed_slice()
            }
            None => vec![0u8; needed].into_boxed_slice(),
        };
        self.expiration_bits = Some(bits);
        // SAFETY-free: we just ensured `Some` with len >= needed.
        self.expiration_bits.as_mut().unwrap()[byte] |= 1 << (ordinal % 8);
    }

    /// Whether the entry at `ordinal` belongs to a document with a field-level
    /// expiration. Ordinals beyond the (lazily grown) bitset read as `false`.
    pub(crate) fn expiration_bit(&self, ordinal: u16) -> bool {
        let byte = ordinal as usize / 8;
        self.expiration_bits
            .as_deref()
            .is_some_and(|bits| byte < bits.len() && (bits[byte] >> (ordinal % 8)) & 1 != 0)
    }

    /// Get the first document ID in this block. This is only needed for some C tests.
    pub const fn first_block_id(&self) -> DocId {
        self.first_doc_id
    }

    /// Get the last document ID in the block. This is only needed for some C tests.
    pub const fn last_block_id(&self) -> DocId {
        self.last_doc_id
    }

    /// Get the number of entries in this block. This is only needed for some C tests.
    pub const fn num_entries(&self) -> u16 {
        self.num_entries
    }

    /// Get a reference to the encoded data in this block. This is only needed for some C tests.
    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    pub(crate) const fn writer(&mut self) -> ControlledCursor<'_> {
        ControlledCursor::new(&mut self.buffer)
    }
}

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            blocks: Default::default(),
            n_unique_docs: 0,
            flags,
            gc_marker: AtomicU32::new(0),
            unique_id: IndexUniqueId::next(),
            _encoder: Default::default(),
        }
    }

    /// Create a new inverted index from the given blocks and encoder. The blocks are expected to not
    /// contain duplicate entries and be ordered by document ID.
    #[cfg(test)]
    pub(crate) fn from_blocks(
        flags: IndexFlags,
        blocks: ThinVec<IndexBlock, BlockCapacity>,
    ) -> Self {
        debug_assert!(!blocks.is_empty());
        debug_assert!(
            blocks.is_sorted_by(|a, b| a.last_doc_id < b.first_doc_id),
            "blocks must be sorted and not overlap"
        );
        debug_assert!(
            blocks.iter().all(|b| b.first_doc_id <= b.last_doc_id),
            "blocks must have valid ranges"
        );

        let n_unique_docs = blocks.iter().map(|b| b.num_entries as u32).sum();

        Self {
            blocks,
            n_unique_docs,
            flags,
            gc_marker: AtomicU32::new(0),
            unique_id: IndexUniqueId::next(),
            _encoder: Default::default(),
        }
    }

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        let blocks_heap = self.blocks.mem_usage();
        let blocks_buffers: usize = self
            .blocks
            .iter()
            .map(|b| b.buffer.capacity() + b.expiration_bits_len())
            .sum();
        let stack = std::mem::size_of::<Self>();

        blocks_heap + blocks_buffers + stack
    }

    /// Add a new record to the index. Returns an [`AddRecordOutcome`] reporting how many bytes
    /// the index's memory usage grew by and how many new index blocks the write created (0 in
    /// the common case, up to 2 when a new block was needed for the encoded delta and/or the
    /// previous block was full). Callers that maintain a per-spec block counter should add
    /// `outcome.blocks_added` to it.
    ///
    /// It is expected that the document ID of the record is greater than or equal to the last
    /// document ID in the index.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<AddRecordOutcome> {
        let doc_id = record.doc_id;

        let same_doc = match (
            E::ALLOW_DUPLICATES,
            self.last_doc_id().map(|d| d == doc_id).unwrap_or_default(),
        ) {
            (true, true) => true,
            (false, true) => {
                // Even though we might allow duplicate document IDs, this encoder does not allow
                // it since it will contain redundant information. Therefore, we are skipping this
                // record.
                return Ok(AddRecordOutcome::default());
            }
            (_, false) => false,
        };

        let blocks_before = self.blocks.len();

        // We take ownership of the block since we are going to keep using self. So we can't have a
        // mutable reference to the block we are working with at the same time.
        let mut block = self.take_block(doc_id, same_doc);
        let mut mem_growth = 0;

        let delta_base = E::delta_base(&block);
        debug_assert!(
            doc_id >= delta_base,
            "documents should be encoded in the order of their IDs"
        );
        let delta = doc_id.wrapping_sub(delta_base);

        let delta = match E::Delta::from_u64(delta) {
            Some(delta) => delta,
            None => {
                // The delta is too large for this encoder. We need to create a new block.
                // Since the new block is empty, we'll start with `delta` equal to 0.
                let new_block = IndexBlock::new(doc_id);

                // We won't use the block so make sure to put it back
                mem_growth += self.add_block(block);
                block = new_block;

                E::Delta::zero()
            }
        };

        let buf_cap = block.buffer.capacity();
        let writer = block.writer();
        let _bytes_written = E::encode(writer, delta, record)?;

        // We don't use `_bytes_written` returned by the encoder to determine by how much memory
        // grew because the buffer might have had enough capacity for the bytes in the encoding.
        // Instead we took the capacity of the buffer before the write and now check by how much it
        // has increased (if any).
        let buf_growth = block.buffer.capacity() - buf_cap;

        // Record the document-level field-expiration bit for this entry at its
        // ordinal within the block — which is the current `num_entries`, before
        // the bump below. The codec (the `buffer` above) is untouched; the bit
        // lives in the block's side bitset.
        let bits_len_before = block.expiration_bits_len();
        if record.has_field_expiration {
            block.set_expiration_bit(block.num_entries);
        }
        let buf_growth = buf_growth + (block.expiration_bits_len() - bits_len_before);

        debug_assert!(block.num_entries.saturating_add(1) < u16::MAX);
        block.num_entries += 1;
        block.last_doc_id = doc_id;

        // We took ownership of the block so put it back
        mem_growth += self.add_block(block);

        if !same_doc {
            self.n_unique_docs += 1;
        } else {
            self.flags |= IndexFlags_Index_HasMultiValue;
        }

        let total_mem_growth = buf_growth + mem_growth;
        // A single add_record can grow memory by at most one block-buffer doubling plus the
        // overhead of inserting one new block into the `blocks` ThinVec — comfortably below 4 GiB.
        debug_assert!(
            total_mem_growth <= u32::MAX as usize,
            "AddRecordOutcome::mem_growth overflowed u32 ({total_mem_growth} bytes in one add)"
        );
        Ok(AddRecordOutcome {
            mem_growth: total_mem_growth as u32,
            blocks_added: (self.blocks.len() - blocks_before) as u32,
        })
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<DocId> {
        self.blocks.last().map(|b| b.last_doc_id)
    }

    /// Take a block that can be written to.
    fn take_block(&mut self, doc_id: DocId, same_doc: bool) -> IndexBlock {
        if self.blocks.is_empty()
            || (
                // If the block is full
                !same_doc
                    && self
                        .blocks
                        .last()
                        .expect("we just confirmed there are blocks")
                        .num_entries
                        >= E::RECOMMENDED_BLOCK_ENTRIES
            )
        {
            IndexBlock::new(doc_id)
        } else {
            self.blocks
                .pop()
                .expect("to get the last block since we know there is one")
        }
    }

    /// Add a block back to the index. This allows us to control the growth strategy used by the
    /// `blocks` vector.
    ///
    /// It returns how many bytes have been added to the size of the heap allocation backing the blocks vector.
    fn add_block(&mut self, block: IndexBlock) -> usize {
        let had_allocated = self.blocks.has_allocated();
        let mem_growth = if self.blocks.len() == self.blocks.capacity() {
            self.blocks.reserve_exact(1);

            if had_allocated {
                IndexBlock::STACK_SIZE
            } else {
                // Nothing is allocated until the first block is added.
                // When that happens, the heap allocation has to grow by the size of the block
                // as well as the size of the thin vector head (i.e. length and capacity).
                self.blocks.mem_usage()
            }
        } else {
            0
        };

        self.blocks.push(block);
        mem_growth
    }

    /// Returns the number of unique documents in the index.
    pub const fn unique_docs(&self) -> u32 {
        self.n_unique_docs
    }

    /// Returns the flags of this index.
    pub const fn flags(&self) -> IndexFlags {
        self.flags
    }

    /// Return the debug summary for this inverted index.
    pub fn summary(&self) -> Summary {
        Summary {
            number_of_docs: self.n_unique_docs,
            number_of_entries: self.n_unique_docs as usize,
            last_doc_id: self.last_doc_id().unwrap_or(0),
            flags: self.flags as _,
            number_of_blocks: self.blocks.len(),
            block_efficiency: 0.0,
            has_efficiency: false,
        }
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        self.blocks
            .iter()
            .map(|b| BlockSummary {
                first_doc_id: b.first_doc_id,
                last_doc_id: b.last_doc_id,
                number_of_entries: b.num_entries,
            })
            .collect()
    }

    /// Returns the number of blocks in this index.
    pub fn number_of_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get a reference to the block at the given index, if it exists. This is only used by some C tests.
    pub fn block_ref(&self, index: usize) -> Option<&IndexBlock> {
        self.blocks.get(index)
    }

    /// Get the current GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker(&self) -> u32 {
        self.gc_marker.load(atomic::Ordering::Relaxed)
    }

    /// Increment the GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker_inc(&self) {
        self.gc_marker.fetch_add(1, atomic::Ordering::Relaxed);
    }

    /// Returns the unique identifier for this index instance. This ID is assigned once at
    /// construction time and never changes. Used to detect the ABA problem in cursor
    /// revalidation.
    pub const fn unique_id(&self) -> IndexUniqueId {
        self.unique_id
    }
}
