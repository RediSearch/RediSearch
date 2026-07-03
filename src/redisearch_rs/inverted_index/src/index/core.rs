/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::snapshot::InvertedIndexSnapshot;
use super::unique_id::IndexUniqueId;
use crate::BlockCapacity;
use crate::{
    Encoder, IdDelta,
    controlled_cursor::ControlledCursor,
    debug::{BlockSummary, Summary},
};
use ffi::{IndexFlags, IndexFlags_Index_HasMultiValue};
use index_result::RSIndexResult;
use rqe_core::DocId;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    sync::{Arc, LazyLock},
};
use thin_vec::ThinVec;

/// Shared, immutable empty `sealed` region. Every index with nothing sealed yet — the
/// common case for small / single-block terms — points its `sealed` `Arc` here instead of
/// allocating its own refcount box, saving a per-index heap allocation across the many
/// inverted indexes a keyspace holds. `sealed` is only ever replaced wholesale (never
/// mutated in place), so sharing one empty instance is safe; GC/`add_record` swap in a
/// fresh `Arc` the moment they actually seal a block.
static EMPTY_SEALED: LazyLock<Arc<[IndexBlock]>> = LazyLock::new(|| Arc::from([]));

/// An `Arc::clone` of the shared empty `sealed` region — see [`EMPTY_SEALED`].
pub(crate) fn empty_sealed() -> Arc<[IndexBlock]> {
    Arc::clone(&EMPTY_SEALED)
}

/// An inverted index is a data structure that maps terms to their occurrences in documents. It is
/// used to efficiently search for documents that contain specific terms.
///
/// Block storage is split across two direct fields:
/// - [`Self::sealed`] — compacted blocks owned via a single [`Arc<[IndexBlock]>`]; only
///   GC writes here.
/// - [`Self::pending`] — full blocks plus the currently-written tail (the last slot),
///   each wrapped in an [`Arc`] so the snapshot's shallow Vec clone shares the underlying
///   data with the index. Writes go to the tail copy-on-write (see [`Self::add_record`]).
///
/// Both are protected by the spec write lock (C side) plus `&mut self` (Rust side)
/// for mutations. Readers take a snapshot under the spec read lock — see
/// [`Self::snapshot`].
#[derive(Debug)]
pub struct InvertedIndex<E> {
    /// Compacted, immutable blocks stored inline in a single `Arc` allocation
    /// ([`Arc<[IndexBlock]>`] — refcount header immediately followed by the block slice,
    /// no separate backing buffer). GC's `apply_gc` replaces this [`Arc`] under
    /// `&mut self`; readers' snapshots share the same allocation via refcount clone.
    pub(crate) sealed: Arc<[IndexBlock]>,

    /// Full blocks plus the currently-written tail block, in insertion order. The **last**
    /// slot is the writable tail: `add_record` appends into it (copy-on-write when a
    /// snapshot shares it) and pushes a fresh block when it fills up. GC drains this into
    /// `sealed` on compaction, leaving the compacted tail as the sole remaining slot.
    /// Snapshots `.clone()` this Vec (a shallow copy of the `Arc` pointer slots).
    pub(crate) pending: ThinVec<Arc<IndexBlock>, BlockCapacity>,

    /// Number of unique documents in the index. This is not the total number of entries, but rather the
    /// number of unique documents that have been indexed.
    pub(crate) n_unique_docs: u32,

    /// The flags of this index. This is used to determine the type of index and how it should be
    /// handled.
    pub(crate) flags: IndexFlags,

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

/// The strong/weak refcount header that prefixes the `T` inside every `Arc<T>` heap
/// allocation. Two pointer-sized atomics — 16 bytes on a 64-bit target. Used by
/// [`InvertedIndex::memory_usage`] and [`InvertedIndex::add_record`] (mem_growth) to
/// keep their accounting consistent.
pub(crate) const ARC_HEADER_BYTES: usize = std::mem::size_of::<usize>() * 2;

/// Memory cost of one `Arc<IndexBlock>` heap allocation when an `in_progress` block
/// rolls over into `pending`: the `Arc` refcount header plus the inline `IndexBlock`.
/// **Does not** include the `Vec<Arc<IndexBlock>>` slot the rollover may consume —
/// that's tracked separately because `Vec::push` grows by amortized capacity, not
/// one slot at a time. See [`InvertedIndex::add_record`].
pub(crate) const PER_ROLLOVER_HEAP_BYTES: usize = IndexBlock::STACK_SIZE + ARC_HEADER_BYTES;

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

impl<E: Encoder> InvertedIndex<E> {
    /// Create a new inverted index with the given encoder. The encoder is used to write new
    /// entries to the index.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            sealed: empty_sealed(),
            pending: ThinVec::new(),
            n_unique_docs: 0,
            flags,
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

        // Every block goes into `pending`; the last one is the writable tail.
        let pending: ThinVec<Arc<IndexBlock>, BlockCapacity> =
            blocks.into_iter().map(Arc::new).collect();

        Self {
            sealed: empty_sealed(),
            pending,
            n_unique_docs,
            flags,
            unique_id: IndexUniqueId::next(),
            _encoder: Default::default(),
        }
    }

    /// The memory size of the index in bytes.
    ///
    /// Counts every heap allocation reachable from `self`: the single [`Arc<[IndexBlock]>`]
    /// allocation for `sealed` (refcount header + the inline block slice), the heap of the
    /// directly-owned `pending: Vec<Arc<IndexBlock>>` plus each `Arc<IndexBlock>`'s
    /// allocation, and every block's `buffer` capacity. The fat-pointer length of `sealed`
    /// and the `pending` pointer are part of `size_of::<Self>()`.
    pub fn memory_usage(&self) -> usize {
        // `size_of::<Self>` covers the direct fields: the `Arc<[IndexBlock]>` fat pointer
        // (data ptr + len), the `ThinVec<Arc<IndexBlock>>` pointer, the counters/flags,
        // unique_id and PhantomData. Heap-allocated portions are added below.
        let stack = std::mem::size_of::<Self>();

        // `sealed: Arc<[IndexBlock]>` — one allocation holding the refcount header
        // immediately followed by the `[IndexBlock]` slice (no separate ThinVec buffer or
        // header). The empty shared singleton has `len == 0`, so this is just the header.
        let sealed_arc = ARC_HEADER_BYTES + self.sealed.len() * IndexBlock::STACK_SIZE;
        let sealed_buffers: usize = self.sealed.iter().map(|b| b.buffer.capacity()).sum();

        // `pending: ThinVec<Arc<IndexBlock>, BlockCapacity>` — direct field. `mem_usage()`
        // covers the ThinVec heap (its len/cap header plus the `Arc<IndexBlock>` slots);
        // the pointer-sized ThinVec stack field is already in `stack`.
        let pending_vec_heap = self.pending.mem_usage();
        let pending_blocks: usize = self
            .pending
            .iter()
            .map(|arc_b| ARC_HEADER_BYTES + IndexBlock::STACK_SIZE + arc_b.buffer.capacity())
            .sum();

        stack + sealed_arc + sealed_buffers + pending_vec_heap + pending_blocks
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
    /// The write mutates `self.in_progress` in place — no buffer clone, no `Arc`
    /// allocation, no Vec clone — in the common case where we're appending to the
    /// current block. On roll-over the in_progress block is moved into a fresh
    /// [`Arc<IndexBlock>`] and pushed onto `self.pending` (O(1) amortized).
    ///
    /// Safety: `&mut self` guarantees no concurrent writer in Rust; the spec write lock
    /// at the FFI boundary guarantees no concurrent reader (readers eagerly clone
    /// `pending` + `in_progress` at snapshot time under the spec read lock, so they only
    /// see quiescent states). If a future story lifts the spec lock, this must be
    /// reworked.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<AddRecordOutcome> {
        let doc_id = record.doc_id;

        let same_doc = match (
            E::ALLOW_DUPLICATES,
            self.pending
                .last()
                .map(|b| b.last_doc_id == doc_id)
                .unwrap_or(false),
        ) {
            (true, true) => true,
            (false, true) => {
                // Encoder doesn't allow duplicates — skip.
                return Ok(AddRecordOutcome::default());
            }
            (_, false) => false,
        };

        let start_new_block = match self.pending.last() {
            None => true,
            Some(tail) => !same_doc && tail.num_entries >= E::RECOMMENDED_BLOCK_ENTRIES,
        };

        let mut mem_growth: usize = 0;
        let mut blocks_added: u32 = 0;
        // Count of fresh tail blocks pushed onto `pending`. Each is an `Arc<IndexBlock>`
        // heap allocation (`PER_ROLLOVER_HEAP_BYTES` = refcount header + inline IndexBlock).
        let mut new_blocks: u32 = 0;
        // Snapshot pending's ThinVec heap size now; we'll diff at the end to charge for any
        // amortized slot allocation a push triggered (0 on a no-realloc push).
        let pending_mem_before = self.pending.mem_usage();

        if start_new_block {
            self.pending.push(Arc::new(IndexBlock::new(doc_id)));
            new_blocks += 1;
            blocks_added += 1;
        } else {
            // Copy-on-write: if a live snapshot still shares the tail block, clone it into a
            // fresh uniquely-owned `Arc` before mutating, so the snapshot keeps observing the
            // frozen tail. This is the deferred COW cost — paid on the first write after a
            // read snapshotted the tail, rather than on every snapshot (as the old
            // `in_progress` deep-copy did).
            let tail = self.pending.last_mut().expect("pending is non-empty here");
            if Arc::get_mut(tail).is_none() {
                *tail = Arc::new((**tail).clone());
            }
        }

        // Try to encode into the tail block (uniquely owned after the push/COW above). If
        // the encoder reports the delta is too big for its format, push a fresh tail and
        // encode delta=0 there.
        let mut delta_too_big = false;
        {
            let working_block = Arc::get_mut(self.pending.last_mut().expect("ensured above"))
                .expect("tail is uniquely owned after push/COW");
            let delta_base = E::delta_base(working_block);
            debug_assert!(
                doc_id >= delta_base,
                "documents should be encoded in the order of their IDs"
            );
            let delta = doc_id.wrapping_sub(delta_base);

            match E::Delta::from_u64(delta) {
                Some(delta) => {
                    let buf_cap = working_block.buffer.capacity();
                    E::encode(working_block.writer(), delta, record)?;
                    let buf_growth = working_block.buffer.capacity() - buf_cap;
                    mem_growth += buf_growth;

                    debug_assert!(working_block.num_entries.saturating_add(1) < u16::MAX);
                    working_block.num_entries += 1;
                    working_block.last_doc_id = doc_id;
                }
                None => {
                    delta_too_big = true;
                }
            }
        }

        if delta_too_big {
            // The tail block can't hold this delta — push a fresh tail and encode delta=0.
            self.pending.push(Arc::new(IndexBlock::new(doc_id)));
            new_blocks += 1;
            blocks_added += 1;

            let working_block = Arc::get_mut(self.pending.last_mut().expect("just pushed"))
                .expect("freshly pushed tail is uniquely owned");
            let buf_cap = working_block.buffer.capacity();
            E::encode(working_block.writer(), E::Delta::zero(), record)?;
            let buf_growth = working_block.buffer.capacity() - buf_cap;
            mem_growth += buf_growth;

            working_block.num_entries += 1;
            working_block.last_doc_id = doc_id;
        }

        // Per new tail block:
        // - One `Arc<IndexBlock>` heap allocation: refcount header + inline `IndexBlock`
        //   (`PER_ROLLOVER_HEAP_BYTES`). Unlike the old design (where the first block lived
        //   inline in `in_progress`), every block — including the first — is now an `Arc`.
        // - Plus the *actual* pending `ThinVec` heap delta, which reflects amortized growth
        //   (0 bytes when a push fit existing capacity, header + several slots on realloc).
        //   Matches what `memory_usage()` reports via `self.pending.mem_usage()`.
        //
        // (A copy-on-write clone of a shared tail swaps one `Arc<IndexBlock>` for a
        // same-sized one, so it doesn't change the index's own memory usage — the retained
        // old block is charged to the snapshot, not here.)
        mem_growth += (new_blocks as usize) * PER_ROLLOVER_HEAP_BYTES;
        let pending_cap_growth = self.pending.mem_usage() - pending_mem_before;
        mem_growth += pending_cap_growth;

        if !same_doc {
            self.n_unique_docs += 1;
        } else {
            self.flags |= IndexFlags_Index_HasMultiValue;
        }

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
        // The last block is the writable tail (last of `pending`), or the tail of `sealed`
        // when `pending` is empty (e.g. right after a GC that compacted everything).
        self.pending
            .last()
            .map(|arc| arc.last_doc_id)
            .or_else(|| self.sealed.last().map(|b| b.last_doc_id))
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
        let snap = self.snapshot();
        Summary {
            number_of_docs: self.n_unique_docs,
            number_of_entries: self.n_unique_docs as usize,
            last_doc_id: snap.last_block().map(|b| b.last_doc_id).unwrap_or(0),
            flags: self.flags as _,
            number_of_blocks: snap.block_count(),
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

    /// Take an owned [`InvertedIndexSnapshot`] of the current block storage: a refcount
    /// clone of `sealed` and a shallow Vec clone of `pending` (the Arcs — including the
    /// writable tail — share underlying block data). Both are captured together so the
    /// snapshot is internally consistent; the caller must hold the spec read lock so no
    /// concurrent writer/GC can interleave. No block data is copied here — a later write
    /// to the tail copies-on-write instead.
    pub fn snapshot(&self) -> InvertedIndexSnapshot {
        InvertedIndexSnapshot::new(Arc::clone(&self.sealed), self.pending.clone())
    }

    /// Returns the unique identifier for this index instance. This ID is assigned once at
    /// construction time and never changes. Used to detect the ABA problem in cursor
    /// revalidation.
    pub const fn unique_id(&self) -> IndexUniqueId {
        self.unique_id
    }

    /// Return a snapshot of all blocks as an owned `Vec<IndexBlock>`. Used by tests that
    /// want to inspect or compare the full block list; production code should call
    /// [`Self::snapshot`] and walk the returned [`InvertedIndexSnapshot`] instead.
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
    /// extract its blocks into an owned form. Tries to avoid cloning by `Arc::try_unwrap` —
    /// since the consumer owns the index, the Arcs are usually unique.
    pub(crate) fn into_blocks_owned(self) -> Vec<IndexBlock> {
        let mut out: Vec<IndexBlock> = Vec::with_capacity(self.sealed.len() + self.pending.len());
        // `sealed: Arc<[IndexBlock]>` — you can't move blocks out of a shared slice, so
        // clone each. In practice `sealed` is empty here (the only caller builds a
        // throwaway index via `add_record`, whose blocks all live in `pending`), so this
        // clones nothing.
        out.extend(self.sealed.iter().cloned());
        for arc_block in self.pending {
            out.push(Arc::try_unwrap(arc_block).unwrap_or_else(|arc| (*arc).clone()));
        }
        out
    }
}
