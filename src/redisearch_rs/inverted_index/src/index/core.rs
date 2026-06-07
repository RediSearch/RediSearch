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
    sync::{
        Arc,
        atomic::{self, AtomicU32, AtomicUsize},
    },
};
use thin_vec::ThinVec;

use super::snapshot::InvertedIndexSnapshot;
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
///
/// The block list is owned by an [`Arc`] so readers can take a cheap refcount clone
/// via [`Self::snapshot`] and walk a stable view independent of subsequent writes.
/// Writers go through `Arc::make_mut`, which triggers a copy-on-write of the
/// `ThinVec` only when readers still hold an outstanding snapshot. Follow-up PRs
/// will split this into `sealed`/`pending`/`in_progress` regions to avoid that COW
/// cost on the hot path.
#[derive(Debug)]
pub struct InvertedIndex<E> {
    /// The blocks of the index, owned via [`Arc`] for cheap snapshotting. Each block
    /// contains a set of entries for a specific range of document IDs; blocks are
    /// ordered by document ID. Writers use `Arc::make_mut` to obtain `&mut ThinVec`
    /// (copy-on-write when a snapshot is alive).
    pub(crate) blocks: Arc<ThinVec<IndexBlock, BlockCapacity>>,

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
#[derive(Debug, Eq, PartialEq, Serialize)]
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
            first_doc_id: DocId,
            last_doc_id: DocId,
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
    pub(crate) fn new(doc_id: DocId) -> Self {
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

// `Clone` must increment `TOTAL_BLOCKS` to stay balanced with `Drop`. The hot users
// of this impl are `Arc::make_mut` on the blocks `ThinVec` (the writer's copy-on-write
// path when a snapshot is alive) and the snapshot's deep clone of in-place blocks
// once the follow-up storage refactor lands.
impl Clone for IndexBlock {
    fn clone(&self) -> Self {
        TOTAL_BLOCKS.fetch_add(1, atomic::Ordering::Relaxed);
        Self {
            first_doc_id: self.first_doc_id,
            last_doc_id: self.last_doc_id,
            num_entries: self.num_entries,
            buffer: self.buffer.clone(),
        }
    }
}

/// Two pointer-sized atomics — the strong/weak refcount header that prefixes the
/// `T` inside every `Arc<T>` heap allocation. Used by [`InvertedIndex::memory_usage`]
/// to account for the `Arc<ThinVec<IndexBlock>>` wrapper.
pub(crate) const ARC_HEADER_BYTES: usize = std::mem::size_of::<usize>() * 2;

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            blocks: Arc::new(ThinVec::new()),
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
            blocks: Arc::new(blocks),
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
        let blocks_buffers: usize = self.blocks.iter().map(|b| b.buffer.capacity()).sum();
        // The Arc's heap allocation: refcount header + the inlined ThinVec stack
        // representation. The `ThinVec` itself moves from `InvertedIndex`'s stack
        // (in PR4) to the Arc's heap allocation here.
        let arc_heap =
            ARC_HEADER_BYTES + std::mem::size_of::<ThinVec<IndexBlock, BlockCapacity>>();
        let stack = std::mem::size_of::<Self>();

        blocks_heap + blocks_buffers + arc_heap + stack
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

    /// Returns the number of entries in the tail block, if any. Used by the reader's
    /// revalidation path to detect in-place tail appends that the `gc_marker` doesn't
    /// signal.
    pub fn tail_num_entries(&self) -> Option<u16> {
        self.blocks.last().map(|b| b.num_entries())
    }

    /// Take a block that can be written to.
    ///
    /// `Arc::make_mut` triggers a `ThinVec` clone if a reader holds an outstanding
    /// snapshot. Follow-up PRs add `pending`/`in_progress` regions so this path
    /// stops touching `sealed`/snapshotted data on every write.
    fn take_block(&mut self, doc_id: DocId, same_doc: bool) -> IndexBlock {
        let blocks = Arc::make_mut(&mut self.blocks);
        if blocks.is_empty()
            || (
                // If the block is full
                !same_doc
                    && blocks
                        .last()
                        .expect("we just confirmed there are blocks")
                        .num_entries
                        >= E::RECOMMENDED_BLOCK_ENTRIES
            )
        {
            IndexBlock::new(doc_id)
        } else {
            blocks
                .pop()
                .expect("to get the last block since we know there is one")
        }
    }

    /// Add a block back to the index. This allows us to control the growth strategy used by the
    /// `blocks` vector.
    ///
    /// It returns how many bytes have been added to the size of the heap allocation backing the blocks vector.
    fn add_block(&mut self, block: IndexBlock) -> usize {
        let blocks = Arc::make_mut(&mut self.blocks);
        let had_allocated = blocks.has_allocated();
        let mem_growth = if blocks.len() == blocks.capacity() {
            blocks.reserve_exact(1);

            if had_allocated {
                IndexBlock::STACK_SIZE
            } else {
                // Nothing is allocated until the first block is added.
                // When that happens, the heap allocation has to grow by the size of the block
                // as well as the size of the thin vector head (i.e. length and capacity).
                blocks.mem_usage()
            }
        } else {
            0
        };

        blocks.push(block);
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

    /// Take an owned [`InvertedIndexSnapshot`] of this index's block storage.
    /// Refcount-clones the underlying `Arc<ThinVec<IndexBlock>>` — O(1), no copy
    /// of the block contents.
    pub fn snapshot(&self) -> InvertedIndexSnapshot {
        InvertedIndexSnapshot::new(Arc::clone(&self.blocks))
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
