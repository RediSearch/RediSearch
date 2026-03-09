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
    sync::atomic::{self, AtomicU32, AtomicUsize},
};
use thin_vec::ThinVec;

use crate::{
    BlockCapacity, Encoder, IdDelta, RSIndexResult,
    controlled_cursor::ControlledCursor,
    debug::{BlockSummary, Summary},
};
use ffi::{IndexFlags, IndexFlags_Index_HasMultiValue, t_docId};

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

    /// The encoder to use when adding new entries to the index
    pub(crate) _encoder: PhantomData<E>,
}

/// Each `IndexBlock` contains a set of entries for a specific range of document IDs. The entries
/// are ordered by document ID, so the first entry in the block has the lowest document ID, and the
/// last entry has the highest document ID. The block also contains a buffer that is used to
/// store the encoded entries. The buffer is dynamically resized as needed when new entries are
/// added to the block.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct IndexBlock {
    /// The first document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    pub(crate) first_doc_id: t_docId,

    /// The last document ID in this block. This is used to determine the range of document IDs
    /// that this block covers.
    pub(crate) last_doc_id: t_docId,

    /// The total number of non-unique entries in this block
    pub(crate) num_entries: u16,

    /// The encoded entries in this block
    pub(crate) buffer: Vec<u8>,
}

static TOTAL_BLOCKS: AtomicUsize = AtomicUsize::new(0);

/// Custom deserialization for `IndexBlock` to track the total number of blocks correctly.
impl<'de> Deserialize<'de> for IndexBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct IB {
            first_doc_id: t_docId,
            last_doc_id: t_docId,
            num_entries: u16,
            buffer: Vec<u8>,
        }

        let ib = IB::deserialize(deserializer)?;

        // We are about to create a new `IndexBlock` object, so be sure to increment the global
        // counter correctly. Without this the `Drop` implementation will eventually cause an
        // underflow of the counter. This correctly counter balances the decrement in the `Drop`.
        TOTAL_BLOCKS.fetch_add(1, atomic::Ordering::Relaxed);

        Ok(IndexBlock {
            first_doc_id: ib.first_doc_id,
            last_doc_id: ib.last_doc_id,
            num_entries: ib.num_entries,
            buffer: ib.buffer,
        })
    }
}

impl IndexBlock {
    pub(crate) const STACK_SIZE: usize = std::mem::size_of::<Self>();

    /// Make a new index block with primed with the initial doc ID. The next entry written into
    /// the block should be for this doc ID else the block will contain incoherent data.
    pub(crate) fn new(doc_id: t_docId) -> Self {
        let this = Self {
            first_doc_id: doc_id,
            last_doc_id: doc_id,
            num_entries: 0,
            buffer: Vec::new(),
        };
        TOTAL_BLOCKS.fetch_add(1, atomic::Ordering::Relaxed);

        this
    }

    /// Get the memory usage of this block, including the stack size and the capacity of the bytes buffer.
    pub const fn mem_usage(&self) -> usize {
        Self::STACK_SIZE + self.buffer.capacity()
    }

    /// Get the first document ID in this block. This is only needed for some C tests.
    pub const fn first_block_id(&self) -> t_docId {
        self.first_doc_id
    }

    /// Get the last document ID in the block. This is only needed for some C tests.
    pub const fn last_block_id(&self) -> t_docId {
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

    /// Returns the total number of index blocks in existence.
    pub fn total_blocks() -> usize {
        TOTAL_BLOCKS.load(atomic::Ordering::Relaxed)
    }
}

impl Drop for IndexBlock {
    fn drop(&mut self) {
        TOTAL_BLOCKS.fetch_sub(1, atomic::Ordering::Relaxed);
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
            _encoder: Default::default(),
        }
    }

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        let blocks_heap = self.blocks.mem_usage();
        let blocks_buffers: usize = self.blocks.iter().map(|b| b.buffer.capacity()).sum();
        let stack = std::mem::size_of::<Self>();

        blocks_heap + blocks_buffers + stack
    }

    /// Add a new record to the index and return by how much memory grew. It is expected that
    /// the document ID of the record is greater than or equal the last document ID in the index.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
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
                return Ok(0);
            }
            (_, false) => false,
        };

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

        Ok(buf_growth + mem_growth)
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<t_docId> {
        self.blocks.last().map(|b| b.last_doc_id)
    }

    /// Take a block that can be written to.
    fn take_block(&mut self, doc_id: t_docId, same_doc: bool) -> IndexBlock {
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
}
