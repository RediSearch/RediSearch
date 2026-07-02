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
        atomic::{self, AtomicU32},
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
/// Block storage is split across two regions:
/// - [`Self::sealed`] — compacted blocks owned via [`Arc`]; only GC writes here. Readers'
///   snapshots share the same allocation via refcount clone.
/// - [`Self::pending`] — a small staging area for actively-written and recently-rolled-over
///   blocks. Each block is wrapped in its own [`Arc`] so the snapshot's shallow [`Vec`] clone
///   shares the underlying block data via refcount. The tail of `pending` is the
///   currently-being-written block; the writer mutates it via [`Arc::make_mut`] — cheap
///   when no reader is pinning the snapshot (refcount = 1) and amortizes to one block
///   clone per outstanding snapshot.
///
/// Both regions are protected by the spec write lock (C side) plus `&mut self` (Rust side)
/// for mutations. Readers take a snapshot under the spec read lock — see [`Self::snapshot`].
/// A follow-up PR introduces an `in_progress: Option<IndexBlock>` region to eliminate the
/// `Arc::make_mut` allocation entirely on the hot write path.
#[derive(Debug)]
pub struct InvertedIndex<E> {
    /// Compacted, immutable blocks. GC's `apply_gc` replaces this [`Arc`] under
    /// `&mut self`; readers' snapshots share the same allocation via refcount clone.
    pub(crate) sealed: Arc<ThinVec<IndexBlock, BlockCapacity>>,

    /// Recently-added blocks, in insertion order. The last entry is the
    /// currently-being-written block; writes mutate it via [`Arc::make_mut`]. Earlier
    /// entries are full and waiting for GC to compact them into [`Self::sealed`].
    /// Snapshots `.clone()` this Vec (a shallow copy of the pointer slots — the blocks
    /// behind each `Arc` are shared via refcount).
    pub(crate) pending: Vec<Arc<IndexBlock>>,

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
        }
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
}

// Manual `Clone` (rather than derive) because the hot users are `Arc::make_mut` on the
// blocks `ThinVec` — the writer's copy-on-write path when a snapshot is alive — and the
// snapshot's deep clone of in-place blocks once the follow-up storage refactor lands.
// (The former per-clone `TOTAL_BLOCKS` counter was dropped to match master, which
// removed process-wide block-instance counting.)
impl Clone for IndexBlock {
    fn clone(&self) -> Self {
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
/// and [`InvertedIndex::add_record`] (mem_growth accounting) to account for each
/// `Arc<...>` wrapper.
pub(crate) const ARC_HEADER_BYTES: usize = std::mem::size_of::<usize>() * 2;

/// Memory cost of pushing one new block onto `pending`, beyond the buffer growth from
/// later encodes: the fresh `Arc<IndexBlock>` heap allocation (refcount header + the
/// inline `IndexBlock` stack) plus one new pointer slot in the `pending` Vec. Used by
/// [`InvertedIndex::add_record`] for `mem_growth` reporting.
pub(crate) const PER_NEW_BLOCK_BYTES: usize =
    IndexBlock::STACK_SIZE + ARC_HEADER_BYTES + std::mem::size_of::<Arc<IndexBlock>>();

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            sealed: Arc::new(ThinVec::new()),
            pending: Vec::new(),
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

        // All seeded blocks land in `pending`. Tests that exercise the writer path
        // (which mutates the tail of `pending`) get a fresh in-place tail this way;
        // GC tests that need a populated `sealed` set it up separately.
        // `Vec::with_capacity` pins the heap allocation at exactly `blocks.len()` —
        // `collect()` would route through `Vec::extend`'s growth strategy and over-
        // allocate (minimum 4 slots), inflating tests that pin `memory_usage`.
        let mut pending: Vec<Arc<IndexBlock>> = Vec::with_capacity(blocks.len());
        for b in blocks {
            pending.push(Arc::new(b));
        }

        Self {
            sealed: Arc::new(ThinVec::new()),
            pending,
            n_unique_docs,
            flags,
            gc_marker: AtomicU32::new(0),
            unique_id: IndexUniqueId::next(),
            _encoder: Default::default(),
        }
    }

    /// The memory size of the index in bytes.
    ///
    /// Counts every heap allocation reachable from `self`: the [`Arc<ThinVec>`] for
    /// `sealed` (refcount header + ThinVec stack + heap if any + each block's buffer),
    /// the heap of `pending: Vec<Arc<IndexBlock>>` plus each `Arc<IndexBlock>`'s
    /// allocation (refcount header + the inline `IndexBlock` + its buffer).
    pub fn memory_usage(&self) -> usize {
        // `size_of::<Self>` covers the direct fields: the `Arc<ThinVec>` pointer, the
        // `Vec<Arc<IndexBlock>>` triple (ptr/len/cap), the atomics and PhantomData.
        // Heap-allocated portions are added below.
        let stack = std::mem::size_of::<Self>();

        // `sealed: Arc<ThinVec<IndexBlock, BlockCapacity>>` — the Arc's heap allocation
        // is the refcount header plus the inlined ThinVec stack representation.
        let sealed_arc =
            ARC_HEADER_BYTES + std::mem::size_of::<ThinVec<IndexBlock, BlockCapacity>>();
        let sealed_thinvec_heap = self.sealed.mem_usage();
        let sealed_buffers: usize = self.sealed.iter().map(|b| b.buffer.capacity()).sum();

        // `pending: Vec<Arc<IndexBlock>>` — only the Vec heap and each `Arc<IndexBlock>`
        // allocation are heap; the Vec stack triple is in `stack` above.
        let pending_vec_heap = self.pending.capacity() * std::mem::size_of::<Arc<IndexBlock>>();
        let pending_blocks: usize = self
            .pending
            .iter()
            .map(|arc_b| ARC_HEADER_BYTES + IndexBlock::STACK_SIZE + arc_b.buffer.capacity())
            .sum();

        stack
            + sealed_arc
            + sealed_thinvec_heap
            + sealed_buffers
            + pending_vec_heap
            + pending_blocks
    }

    /// Add a new record to the index. Returns an [`AddRecordOutcome`] reporting how many bytes
    /// the index's memory usage grew by and how many new index blocks the write created (0 in
    /// the common case, up to 2 when a new block was needed for the encoded delta and/or the
    /// previous block was full). Callers that maintain a per-spec block counter should add
    /// `outcome.blocks_added` to it.
    ///
    /// It is expected that the document ID of the record is greater than or equal to the last
    /// document ID in the index.
    ///
    /// The write mutates the tail of `pending` in place via [`Arc::make_mut`]: when no reader
    /// holds an outstanding snapshot (refcount = 1) this is just a `&mut` deref — no buffer
    /// clone, no `Arc` allocation. When a snapshot is alive (refcount > 1) the tail block is
    /// deep-cloned first so the reader's view stays intact, then we mutate the clone. Roll-over
    /// (block-full or delta-too-big) pushes a fresh `Arc<IndexBlock>` onto `pending`.
    ///
    /// # Safety
    ///
    /// `&mut self` guarantees no concurrent writer in Rust; the spec write lock at the FFI
    /// boundary guarantees no concurrent reader/GC. If a future story lifts the spec lock,
    /// the writer's interplay with snapshot construction must be revisited: snapshots eagerly
    /// `Arc::clone` the tail under the spec read lock, which bumps refcount before any
    /// `Arc::make_mut` here can see refcount = 1.
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

        let mut mem_growth: usize = 0;
        let mut blocks_added: u32 = 0;

        // Ensure `pending` has a tail block we can write to. We need a fresh block if:
        // - `pending` is empty (first write, or post-GC empty state), or
        // - the tail is full (and we're not appending another entry for the same doc).
        let need_fresh = match self.pending.last() {
            None => true,
            Some(arc) => !same_doc && arc.num_entries >= E::RECOMMENDED_BLOCK_ENTRIES,
        };
        if need_fresh {
            mem_growth += self.push_fresh_block(doc_id);
            blocks_added += 1;
        }

        // Capture the live tail's buffer capacity BEFORE `Arc::make_mut`. If a reader
        // snapshot pins the current tail (refcount > 1), `make_mut` deep-clones it and
        // the clone starts with `capacity == len` — measuring the post-`make_mut`
        // capacity would diff against the clone's smaller starting capacity and
        // overcount growth on the COW path. See [`Vec::clone`] for the capacity
        // contract on the cloned buffer.
        let buf_cap_before = self
            .pending
            .last()
            .expect("just ensured non-empty")
            .buffer
            .capacity();

        // First encode attempt: mutate the current tail in place via `Arc::make_mut`. If the
        // delta is too large for the encoder, we leave the tail untouched and push another
        // fresh block below.
        let delta_too_big = {
            let working_block =
                Arc::make_mut(self.pending.last_mut().expect("just ensured non-empty"));
            let delta_base = E::delta_base(working_block);
            debug_assert!(
                doc_id >= delta_base,
                "documents should be encoded in the order of their IDs"
            );
            let delta = doc_id.wrapping_sub(delta_base);
            match E::Delta::from_u64(delta) {
                Some(d) => {
                    E::encode(working_block.writer(), d, record)?;
                    // We don't use the bytes-written reported by the encoder for memory
                    // growth: the buffer may have had spare capacity. Diff against the
                    // live tail's capacity captured above. `saturating_sub` handles the
                    // rare COW case where the clone's post-write capacity ends up below
                    // the original (live index shrunk; we'd want a signed delta to
                    // reflect that, but `mem_growth` is unsigned — clamp at 0 instead).
                    mem_growth += working_block
                        .buffer
                        .capacity()
                        .saturating_sub(buf_cap_before);

                    debug_assert!(working_block.num_entries.saturating_add(1) < u16::MAX);
                    working_block.num_entries += 1;
                    working_block.last_doc_id = doc_id;
                    false
                }
                None => true,
            }
        };

        if delta_too_big {
            // The delta from this block's `delta_base` to `doc_id` doesn't fit the encoder.
            // Roll over to a fresh block (primed with `doc_id`) and encode `delta = 0` there.
            mem_growth += self.push_fresh_block(doc_id);
            blocks_added += 1;
            let working_block =
                Arc::make_mut(self.pending.last_mut().expect("just pushed a block"));
            let buf_cap = working_block.buffer.capacity();
            E::encode(working_block.writer(), E::Delta::zero(), record)?;
            mem_growth += working_block.buffer.capacity() - buf_cap;
            working_block.num_entries += 1;
            working_block.last_doc_id = doc_id;
        }

        if !same_doc {
            self.n_unique_docs += 1;
        } else {
            self.flags |= IndexFlags_Index_HasMultiValue;
        }

        // A single add_record can grow memory by at most two block-buffer doublings plus the
        // overhead of inserting one or two new blocks — comfortably below 4 GiB.
        debug_assert!(
            mem_growth <= u32::MAX as usize,
            "AddRecordOutcome::mem_growth overflowed u32 ({mem_growth} bytes in one add)"
        );
        Ok(AddRecordOutcome {
            mem_growth: mem_growth as u32,
            blocks_added,
        })
    }

    /// Push a freshly-allocated `Arc<IndexBlock>` primed at `doc_id` onto `pending`.
    /// Returns [`PER_NEW_BLOCK_BYTES`]: the Arc allocation (refcount header + the
    /// inline `IndexBlock`) plus one new pointer slot in the pending Vec.
    ///
    /// The Vec is grown by exactly one slot via [`Vec::reserve_exact`] when full,
    /// matching the previous ThinVec strategy: each new block costs a predictable
    /// `PER_NEW_BLOCK_BYTES` bytes. The standard `Vec::push` doubling would charge a
    /// variable amount per push and inflate idle memory for indexes with few blocks
    /// (common for rare-term text indexes).
    fn push_fresh_block(&mut self, doc_id: DocId) -> usize {
        if self.pending.len() == self.pending.capacity() {
            self.pending.reserve_exact(1);
        }
        self.pending.push(Arc::new(IndexBlock::new(doc_id)));
        PER_NEW_BLOCK_BYTES
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<DocId> {
        // The actively-written tail is the last entry of `pending`. If `pending` is empty
        // (fresh index or freshly-compacted-empty), fall back to the tail of `sealed`.
        self.pending
            .last()
            .map(|arc| arc.last_doc_id)
            .or_else(|| self.sealed.last().map(|b| b.last_doc_id))
    }

    /// Returns the number of entries in the tail block, if any. Used by the reader's
    /// revalidation path to detect in-place tail appends that the `gc_marker` doesn't
    /// signal.
    pub fn tail_num_entries(&self) -> Option<u16> {
        self.pending
            .last()
            .map(|arc| arc.num_entries)
            .or_else(|| self.sealed.last().map(|b| b.num_entries))
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
            number_of_blocks: self.number_of_blocks(),
            block_efficiency: 0.0,
            has_efficiency: false,
        }
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        let snap = self.snapshot();
        let total = snap.block_count();
        (0..total)
            .filter_map(|i| snap.block_ref(i))
            .map(|b| BlockSummary {
                first_doc_id: b.first_doc_id,
                last_doc_id: b.last_doc_id,
                number_of_entries: b.num_entries,
            })
            .collect()
    }

    /// Returns the number of blocks in this index.
    pub fn number_of_blocks(&self) -> usize {
        self.sealed.len() + self.pending.len()
    }

    /// Take an owned [`InvertedIndexSnapshot`] of the current block storage. Combines
    /// a refcount clone of `sealed` and a shallow Vec clone of `pending` (the Arcs share
    /// underlying block data). Captures the tail block's `num_entries` before cloning so
    /// iterators on the tail can bound themselves against the writer racing ahead — see
    /// [`InvertedIndexSnapshot`] for the full ordering contract.
    pub fn snapshot(&self) -> InvertedIndexSnapshot {
        let tail_num_entries = self.tail_num_entries().unwrap_or(0);
        InvertedIndexSnapshot::new(
            Arc::clone(&self.sealed),
            self.pending.clone(),
            tail_num_entries,
        )
    }

    /// Get a reference to the block at the given logical index, if it exists. The index
    /// is flat across `sealed` then `pending`. Only used by some C tests; production
    /// code should go through [`Self::snapshot`].
    pub fn block_ref(&self, index: usize) -> Option<&IndexBlock> {
        if let Some(b) = self.sealed.get(index) {
            return Some(b);
        }
        let idx = index.checked_sub(self.sealed.len())?;
        self.pending.get(idx).map(|arc| &**arc)
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

    /// Return a snapshot of all blocks as an owned `Vec<IndexBlock>`. Used by tests that
    /// want to inspect or compare the full block list across both regions; production
    /// code should call [`Self::snapshot`] and walk the returned [`InvertedIndexSnapshot`]
    /// instead.
    #[cfg(test)]
    pub(crate) fn blocks_snapshot(&self) -> Vec<IndexBlock> {
        let snap = self.snapshot();
        let total = snap.block_count();
        (0..total)
            .filter_map(|i| snap.block_ref(i).cloned())
            .collect()
    }

    /// Consume the index and return all its blocks as a flat `Vec<IndexBlock>`. Useful when
    /// the caller built a small throwaway index (e.g. [`IndexBlock::repair`]) and needs to
    /// extract its blocks into an owned form. Tries to avoid cloning by [`Arc::try_unwrap`]
    /// — since the consumer owns the index, the Arcs are usually unique.
    pub(crate) fn into_blocks_owned(self) -> Vec<IndexBlock> {
        let sealed = match Arc::try_unwrap(self.sealed) {
            Ok(tv) => tv,
            Err(arc) => (*arc).clone(),
        };
        let mut out: Vec<IndexBlock> = Vec::with_capacity(sealed.len() + self.pending.len());
        out.extend(sealed);
        for arc_block in self.pending {
            out.push(Arc::try_unwrap(arc_block).unwrap_or_else(|arc| (*arc).clone()));
        }
        out
    }
}
