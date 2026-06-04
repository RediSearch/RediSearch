/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{self, AtomicU32, AtomicUsize},
    },
};
use super::state::{InvertedIndexSnapshot, State};
use super::unique_id::IndexUniqueId;
use crate::{
    Encoder, IdDelta,
    controlled_cursor::ControlledCursor,
    debug::{BlockSummary, Summary},
};
use crate::BlockCapacity;
use thin_vec::ThinVec;
use ffi::{IndexFlags, IndexFlags_Index_HasMultiValue};
use index_result::RSIndexResult;
use rqe_core::DocId;

/// An inverted index is a data structure that maps terms to their occurrences in documents. It is
/// used to efficiently search for documents that contain specific terms.
///
/// The block storage lives behind a single [`ArcSwap<State>`] (see [`Self::state`]). Writers
/// publish a new [`State`] per [`Self::add_record`] via [`arc_swap::ArcSwap::store`]; readers take a
/// consistent snapshot via [`arc_swap::ArcSwap::load_full`] and walk it without locking.
#[derive(Debug)]
pub struct InvertedIndex<E> {
    /// The lock-free block storage. Every block of the index lives inside this [`State`]
    /// snapshot, split across three regions (sealed, pending, in_progress). Writers
    /// publish a new state per [`Self::add_record`] / [`Self::apply_gc`].
    pub(crate) state: ArcSwap<State>,

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

/// The strong/weak refcount header that prefixes the `T` inside every `Arc<T>` heap
/// allocation. Two pointer-sized atomics — 16 bytes on a 64-bit target. Used by
/// [`InvertedIndex::memory_usage`] and [`InvertedIndex::add_record`] (mem_growth) to
/// keep their accounting consistent.
pub(crate) const ARC_HEADER_BYTES: usize = std::mem::size_of::<usize>() * 2;

/// Memory cost of adding one new block to the index, beyond the block's buffer growth:
/// the fresh `Arc<IndexBlock>` heap allocation (`ARC_HEADER_BYTES` + the inline
/// `IndexBlock`) plus one new pointer slot in the rebuilt `pending`
/// `Vec<Arc<IndexBlock>>`. Used by [`InvertedIndex::add_record`] for `mem_growth`
/// reporting and referenced by tests that pin the per-block delta.
pub(crate) const PER_NEW_BLOCK_BYTES: usize = IndexBlock::STACK_SIZE
    + ARC_HEADER_BYTES
    + std::mem::size_of::<Arc<IndexBlock>>();

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

// `Clone` must increment `TOTAL_BLOCKS` to stay balanced with `Drop`. Constructing an
// `IndexBlock` via struct literal (as a copy) would otherwise let the counter underflow
// when the clone is dropped. `add_record` clones the current `in_progress` block on each
// write (to mutate the copy and publish a new state), so this path is hot — but each call
// still represents one logical block in existence, so the counter remains accurate.
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

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            state: ArcSwap::from_pointee(State::empty()),
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

        // All input blocks except the last go to `pending`; the last becomes `in_progress`.
        // Sealed is empty — Story 1.4 populates it.
        let mut iter = blocks.into_iter();
        let last = iter.next_back();
        let pending: Vec<Arc<IndexBlock>> = iter.map(Arc::new).collect();
        let in_progress = last.map(Arc::new);
        let state = State {
            sealed: Arc::new(ThinVec::new()),
            pending: Arc::new(pending),
            in_progress,
        };

        Self {
            state: ArcSwap::from_pointee(state),
            n_unique_docs,
            flags,
            gc_marker: AtomicU32::new(0),
            unique_id: IndexUniqueId::next(),
            _encoder: Default::default(),
        }
    }

    /// The memory size of the index in bytes.
    ///
    /// Counts every heap allocation reachable from `self`: the outer `Arc<State>`, the
    /// inner `Arc<ThinVec>` for sealed (plus its heap if it has one), the inner
    /// `Arc<Vec<Arc<IndexBlock>>>` for pending (plus its heap), each per-block
    /// `Arc<IndexBlock>` in `pending` and `in_progress`, and every block's `buffer`
    /// capacity. The pre-state model counted only `blocks.mem_usage() + buffer
    /// capacities + stack`; this model has more layers of indirection (each carrying an
    /// `Arc` header), so the same logical index reports a larger number — that's the
    /// real memory the new layout occupies.
    pub fn memory_usage(&self) -> usize {
        // `Arc<T>` heap layout: `(strong: AtomicUsize, weak: AtomicUsize, T)`. The strong
        // and weak counters together total `ARC_HEADER_BYTES`; the `T` itself is added
        // separately at each site below.
        const ARC_HEADER: usize = ARC_HEADER_BYTES;

        let state = self.state.load_full();
        let stack = std::mem::size_of::<Self>();

        // Outer `Arc<State>`: header + the three pointer-sized fields of `State`.
        let outer_state = ARC_HEADER + std::mem::size_of::<State>();

        // `sealed: Arc<ThinVec<IndexBlock, BlockCapacity>>`. The Arc allocation holds the
        // ThinVec stack representation (one pointer); the ThinVec's heap allocation, if
        // any, carries the header + slot storage via `mem_usage()`.
        let sealed_arc = ARC_HEADER + std::mem::size_of::<ThinVec<IndexBlock, BlockCapacity>>();
        let sealed_thinvec_heap = state.sealed.mem_usage();
        let sealed_buffers: usize = state.sealed.iter().map(|b| b.buffer.capacity()).sum();

        // `pending: Arc<Vec<Arc<IndexBlock>>>`. The Arc allocation holds the Vec
        // (ptr+len+cap = 24 bytes); the Vec's heap carries `capacity * sizeof(Arc<IndexBlock>)`
        // pointer slots.
        let pending_arc = ARC_HEADER + std::mem::size_of::<Vec<Arc<IndexBlock>>>();
        let pending_vec_heap = state.pending.capacity() * std::mem::size_of::<Arc<IndexBlock>>();
        let pending_blocks: usize = state
            .pending
            .iter()
            .map(|arc_b| ARC_HEADER + IndexBlock::STACK_SIZE + arc_b.buffer.capacity())
            .sum();

        // `in_progress: Option<Arc<IndexBlock>>`.
        let in_progress = match state.in_progress.as_ref() {
            Some(arc_b) => ARC_HEADER + IndexBlock::STACK_SIZE + arc_b.buffer.capacity(),
            None => 0,
        };

        stack
            + outer_state
            + sealed_arc
            + sealed_thinvec_heap
            + sealed_buffers
            + pending_arc
            + pending_vec_heap
            + pending_blocks
            + in_progress
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
    /// The write publishes a new [`State`] via [`arc_swap::ArcSwap::store`]. Readers holding a previous
    /// snapshot are unaffected — they continue to see the pre-write view until they
    /// re-snapshot.
    ///
    /// # Note on `store` vs `rcu`
    ///
    /// The design doc (Epic 1, FT.HYBRID Workers Pool Consolidation) calls for the writer
    /// to use [`arc_swap::ArcSwap::rcu`]. We use [`arc_swap::ArcSwap::store`] instead, which is functionally
    /// equivalent under the current locking model: writers take `&mut self` (Rust) and the
    /// spec write lock (C side), so no concurrent writer can race between our `load_full`
    /// and `store`. If a future story lifts the spec lock from this path to allow
    /// concurrent indexers, this must be switched to `rcu` — otherwise two racing writers
    /// would each `load`, each compute a new state, and one would silently overwrite the
    /// other.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<AddRecordOutcome> {
        let doc_id = record.doc_id;
        let prev = self.state.load_full();

        let same_doc = match (
            E::ALLOW_DUPLICATES,
            prev.in_progress
                .as_ref()
                .map(|b| b.last_doc_id == doc_id)
                .unwrap_or(false),
        ) {
            (true, true) => true,
            (false, true) => {
                // The current encoder does not allow duplicates — skip this record.
                return Ok(AddRecordOutcome::default());
            }
            (_, false) => false,
        };

        // Decide the working block: clone the existing in_progress (case A: appending to the
        // current block), or start a fresh one (case B: empty index or current block is full).
        // The previous in_progress, when not reused, gets promoted to `pending` later.
        let start_new_block = match prev.in_progress.as_ref() {
            None => true,
            Some(ip) => !same_doc && ip.num_entries >= E::RECOMMENDED_BLOCK_ENTRIES,
        };

        let mut working_block = if start_new_block {
            IndexBlock::new(doc_id)
        } else {
            (**prev.in_progress.as_ref().expect("non-empty by branch")).clone()
        };
        let mut mem_growth: usize = 0;
        let mut blocks_added: u32 = if start_new_block { 1 } else { 0 };

        // Holds an extra block to push to pending in the "delta too big" case. None most of
        // the time.
        let mut extra_pending: Option<Arc<IndexBlock>> = None;

        let delta_base = E::delta_base(&working_block);
        debug_assert!(
            doc_id >= delta_base,
            "documents should be encoded in the order of their IDs"
        );
        let delta = doc_id.wrapping_sub(delta_base);

        let delta = match E::Delta::from_u64(delta) {
            Some(delta) => delta,
            None => {
                // Delta too big for this encoder. Park the current working_block (it was
                // either the cloned in_progress or a freshly-created empty block) as pending
                // and start a brand-new block for this record.
                extra_pending = Some(Arc::new(std::mem::replace(
                    &mut working_block,
                    IndexBlock::new(doc_id),
                )));
                blocks_added += 1;
                E::Delta::zero()
            }
        };

        let buf_cap = working_block.buffer.capacity();
        E::encode(working_block.writer(), delta, record)?;
        // Buffer growth: cap before vs after the encode. We use capacity rather than the
        // encoder's `bytes_written` because the buffer may have had spare capacity.
        let buf_growth = working_block.buffer.capacity() - buf_cap;
        mem_growth += buf_growth;
        // Each new block contributes [`PER_NEW_BLOCK_BYTES`] beyond the buffer growth
        // (already counted in `buf_growth` above).
        mem_growth += (blocks_added as usize) * PER_NEW_BLOCK_BYTES;

        debug_assert!(working_block.num_entries.saturating_add(1) < u16::MAX);
        working_block.num_entries += 1;
        working_block.last_doc_id = doc_id;

        // Build the new pending. When the previous in_progress wasn't reused (start_new_block
        // or extra_pending), promote it. Append any extra_pending after that.
        let new_pending = if blocks_added == 0 {
            Arc::clone(&prev.pending)
        } else {
            let mut new = Vec::with_capacity(prev.pending.len() + blocks_added as usize);
            new.extend(prev.pending.iter().cloned());
            if start_new_block
                && let Some(old_ip) = prev.in_progress.as_ref()
            {
                new.push(Arc::clone(old_ip));
            }
            if let Some(extra) = extra_pending {
                new.push(extra);
            }
            Arc::new(new)
        };

        self.state.store(Arc::new(State {
            sealed: Arc::clone(&prev.sealed),
            pending: new_pending,
            in_progress: Some(Arc::new(working_block)),
        }));

        if !same_doc {
            self.n_unique_docs += 1;
        } else {
            self.flags |= IndexFlags_Index_HasMultiValue;
        }

        // A single add_record can grow memory by at most one block-buffer doubling plus the
        // overhead of one or two new block allocations — comfortably below 4 GiB.
        debug_assert!(
            mem_growth <= u32::MAX as usize,
            "AddRecordOutcome::mem_growth overflowed u32 ({mem_growth} bytes in one add)"
        );
        Ok(AddRecordOutcome {
            mem_growth: mem_growth as u32,
            blocks_added,
        })
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<DocId> {
        self.state.load_full().last_block().map(|b| b.last_doc_id)
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
        let state = self.state.load_full();
        Summary {
            number_of_docs: self.n_unique_docs,
            number_of_entries: self.n_unique_docs as usize,
            last_doc_id: state.last_block().map(|b| b.last_doc_id).unwrap_or(0),
            flags: self.flags as _,
            number_of_blocks: state.block_count(),
            block_efficiency: 0.0,
            has_efficiency: false,
        }
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        let state = self.state.load_full();
        let total = state.block_count();
        (0..total)
            .filter_map(|i| state.get_block(i))
            .map(|b| BlockSummary {
                first_doc_id: b.first_doc_id,
                last_doc_id: b.last_doc_id,
                number_of_entries: b.num_entries,
            })
            .collect()
    }

    /// Returns the number of blocks in this index.
    pub fn number_of_blocks(&self) -> usize {
        self.state.load_full().block_count()
    }

    /// Take an owned [`InvertedIndexSnapshot`] of the current block storage. The
    /// snapshot holds an internal `Arc` and keeps the contained blocks alive for its
    /// own lifetime — use this when handing block references to a context (FFI,
    /// another thread, a long-lived callback) where the borrow checker can't keep
    /// `&self` alive for you.
    pub fn snapshot(&self) -> InvertedIndexSnapshot {
        InvertedIndexSnapshot::from_arc(self.state.load_full())
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
    /// want to inspect or compare the full block list; production code should walk
    /// `state.load_full()` directly.
    #[cfg(test)]
    pub(crate) fn blocks_snapshot(&self) -> Vec<IndexBlock> {
        let state = self.state.load_full();
        let total = state.block_count();
        (0..total)
            .filter_map(|i| state.get_block(i).cloned())
            .collect()
    }

    /// Consume the index and return all its blocks as a flat `Vec<IndexBlock>`. Useful when
    /// the caller built a small throwaway index (e.g. [`IndexBlock::repair`]) and needs to
    /// extract its blocks into an owned form. Tries to avoid cloning by `Arc::try_unwrap` —
    /// since the consumer owns the index, the snapshot's Arcs are usually unique.
    pub(crate) fn into_blocks_owned(self) -> Vec<IndexBlock> {
        let state_arc = self.state.into_inner();
        let state = match Arc::try_unwrap(state_arc) {
            Ok(s) => s,
            Err(arc) => State {
                sealed: Arc::clone(&arc.sealed),
                pending: Arc::clone(&arc.pending),
                in_progress: arc.in_progress.clone(),
            },
        };
        let pending = match Arc::try_unwrap(state.pending) {
            Ok(v) => v,
            Err(arc) => (*arc).clone(),
        };
        let mut out: Vec<IndexBlock> = Vec::with_capacity(pending.len() + 1);
        for arc_block in pending {
            out.push(Arc::try_unwrap(arc_block).unwrap_or_else(|arc| (*arc).clone()));
        }
        if let Some(ip) = state.in_progress {
            out.push(Arc::try_unwrap(ip).unwrap_or_else(|arc| (*arc).clone()));
        }
        out
    }
}
