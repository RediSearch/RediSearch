/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, sync::Arc};

use super::{IndexReader, NumericReader, TermReader};
use crate::{
    DecodedBy, Decoder, Encoder, HasInnerIndex, InvertedIndex, NumericDecoder, TermDecoder,
    index::snapshot::InvertedIndexSnapshot, index::unique_id::IndexUniqueId,
    opaque::OpaqueEncoding,
};
use ffi::{IndexFlags, IndexFlags_Index_HasMultiValue};
use index_result::RSIndexResult;
use rqe_core::DocId;

/// Reader that is able to read the records from an [`InvertedIndex`].
///
/// Reads go through an [`InvertedIndexSnapshot`], not the index's block fields directly.
/// The snapshot is taken at construction (or after [`Self::reset`]) and owned by the
/// reader for its lifetime, so concurrent writers and GC don't observe-or-tear the
/// reader's view. The `'index` lifetime continues to bind result-buffer references —
/// see the safety notes on [`Self::cursor_at`].
pub struct IndexReaderCore<'index, E> {
    /// The inverted index that is being read from. Held for ABA detection
    /// ([`Self::points_to_ii`]), flag/`unique_docs` queries, and the GC marker. Block
    /// data is read from [`Self::snapshot`], not from the index's `sealed` /
    /// `pending` / `in_progress` fields.
    pub(crate) ii: &'index InvertedIndex<E>,

    /// Snapshot of the index's block storage taken at construction (refreshed on
    /// [`Self::reset`]). All block-data reads go through this; the reader never touches
    /// the index's block fields directly during iteration, keeping reads lock-free.
    snapshot: InvertedIndexSnapshot,

    /// Logical index of the current block in [`Self::snapshot`]. The index is flat
    /// across the snapshot's three regions (sealed → pending → in_progress).
    pub(crate) current_block_idx: usize,

    /// Byte offset within the current block's buffer. We don't store a [`Cursor`]
    /// directly because its borrow lifetime would be tied to the snapshot's block
    /// (which is owned by `self.snapshot`); instead, we reconstruct a cursor at the
    /// start of each decode call via [`Self::cursor_at`].
    current_position: u64,

    /// The last document ID that was read. Used as the base for delta decoding.
    pub(crate) last_doc_id: DocId,

    /// The unique ID of the inverted index when this reader was created. Used together
    /// with pointer comparison in [`Self::points_to_ii`] to detect the ABA problem.
    ii_unique_id: IndexUniqueId,
}

// Automatically implemented if the IndexReaderCore uses a NumericDecoder.
impl<'index, E: DecodedBy<Decoder = D>, D: Decoder + NumericDecoder> NumericReader<'index>
    for IndexReaderCore<'index, E>
{
}

/// Automatically implemented if the IndexReaderCore uses a TermDecoder.
impl<'index, E: DecodedBy<Decoder = D> + OpaqueEncoding, D: Decoder + TermDecoder>
    TermReader<'index> for IndexReaderCore<'index, E>
where
    E::Storage: HasInnerIndex<E>,
{
    fn points_to_the_same_opaque_index(&self, opaque: &crate::opaque::InvertedIndex) -> bool {
        let storage = E::from_opaque(opaque);
        let ii = storage.inner_index();
        self.points_to_ii(ii)
    }
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> IndexReader<'index>
    for IndexReaderCore<'index, E>
{
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        loop {
            let block = match self.snapshot.block_ref(self.current_block_idx) {
                Some(b) => b,
                None => return Ok(false),
            };

            // If we're at (or past) the end of this block's buffer, advance to the next
            // block if any.
            if self.current_position >= block.buffer.len() as u64 {
                let next_idx = self.current_block_idx + 1;
                if next_idx >= self.snapshot.block_count() {
                    return Ok(false);
                }
                self.set_current_block(next_idx);
                continue;
            }

            let base = D::base_id(block, self.last_doc_id);
            // SAFETY: see `cursor_at`.
            let mut cursor = unsafe { Self::cursor_at(&block.buffer, self.current_position) };
            D::decode(&mut cursor, base, result)?;
            self.current_position = cursor.position();
            self.last_doc_id = result.doc_id;
            return Ok(true);
        }
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        if !self.skip_to(doc_id) {
            return Ok(false);
        }

        let block = self
            .snapshot
            .block_ref(self.current_block_idx)
            .expect("skip_to placed the cursor on a valid block");
        let base = D::base_id(block, self.last_doc_id);
        // SAFETY: see `cursor_at`.
        let mut cursor = unsafe { Self::cursor_at(&block.buffer, self.current_position) };
        let success = D::seek(&mut cursor, base, doc_id, result)?;
        self.current_position = cursor.position();

        if success {
            self.last_doc_id = result.doc_id;
        }

        Ok(success)
    }

    fn skip_to(&mut self, doc_id: DocId) -> bool {
        let total = self.snapshot.block_count();
        if total == 0 {
            return false;
        }

        let current = self
            .snapshot
            .block_ref(self.current_block_idx)
            .expect("current_block_idx must point to a valid block");
        if current.last_doc_id >= doc_id {
            return true;
        }

        // doc_id is past the last block's range — definitely not present.
        // SAFETY: total > 0 was checked above.
        let last = self.snapshot.last_block().unwrap();
        if last.last_doc_id < doc_id {
            return false;
        }

        // Fast path: is the very next block the answer?
        let search_start = self.current_block_idx + 1;
        if let Some(next) = self.snapshot.block_ref(search_start)
            && next.last_doc_id >= doc_id
        {
            self.set_current_block(search_start);
            return true;
        }

        // General search across the snapshot's three regions.
        let idx = self.snapshot.find_block_for_doc_id(search_start, doc_id);
        debug_assert!(idx < total, "we verified above that doc_id is in range");
        self.set_current_block(idx);
        true
    }

    fn reset(&mut self) {
        // Take a fresh snapshot so the caller can see writes/GC that happened since this
        // reader was last positioned.
        //
        // NOTE: replacing `self.snapshot` drops the previous one. Any
        // `RSIndexResult<'index>` previously yielded from `next_record`/`seek_record`
        // and not yet dropped will be left holding dangling slices. Safe Rust does
        // not enforce this — see the safety doc on `cursor_at` and tracking ticket
        // MOD-16148. Today's call sites (`next_record` in the query iterators)
        // consume each result inside one loop iteration and only call `reset`
        // between iterations, so the invariant holds in practice.
        self.snapshot = self.ii.snapshot();
        self.current_block_idx = 0;
        self.current_position = 0;
        self.last_doc_id = self
            .snapshot
            .first_block()
            .map(|b| b.first_doc_id)
            .unwrap_or(0);
    }

    fn unique_docs(&self) -> u64 {
        self.ii.unique_docs() as u64
    }

    fn has_duplicates(&self) -> bool {
        self.ii.flags() & IndexFlags_Index_HasMultiValue > 0
    }

    fn flags(&self) -> IndexFlags {
        self.ii.flags()
    }

    fn needs_revalidation(&self) -> bool {
        // The snapshot is a frozen point-in-time view; the reader only needs to rewind it
        // when it no longer reflects the index's storage. Crucially, appends do *not*
        // require a rewind: `add_record` only extends the live index beyond the snapshot
        // (new `pending`/`in_progress` blocks), never mutating the blocks the snapshot
        // captured. GC is the only thing that rewrites existing blocks — and it does so by
        // replacing `InvertedIndex::sealed` wholesale. So the snapshot is stale exactly
        // when the index's current `sealed` `Arc` is no longer the one we captured.
        //
        // Keying off appends here (the old block-count / tail-entry checks) was correct
        // but forced a re-snapshot on every write, which under churn turns each resumed
        // read into a rewind + re-seek. Pointer-identity on `sealed` lets a reader keep its
        // valid view across concurrent appends and only pay for an actual compaction.
        !Arc::ptr_eq(self.snapshot.sealed_arc(), &self.ii.sealed)
    }

    fn refresh_buffer_pointers(&mut self) {
        // The snapshot's `IndexBlock` buffers are immutable clones, so they can't be
        // reallocated under us — there's nothing to refresh. (Pre-snapshot this method
        // existed to handle in-place buffer growth on writes; that mode is gone.) Callers
        // that need to see new data should `reset()` instead.
    }
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> IndexReaderCore<'index, E> {
    /// Create a new index reader for `ii`, taking a snapshot of its block storage now.
    /// The reader sees a frozen view of the index for its lifetime; calling
    /// [`Self::reset`] refreshes the snapshot.
    pub(crate) fn new(ii: &'index InvertedIndex<E>) -> Self {
        let snapshot = ii.snapshot();
        let (current_block_idx, current_position, last_doc_id) = match snapshot.first_block() {
            Some(first) => (0, 0, first.first_doc_id),
            None => (0, 0, 0),
        };

        Self {
            ii,
            snapshot,
            current_block_idx,
            current_position,
            last_doc_id,
            ii_unique_id: ii.unique_id(),
        }
    }

    /// Check if this reader is reading from the given index by comparing both their pointers
    /// and unique IDs. The dual check prevents the ABA problem: if the original index is
    /// freed and a new one is allocated at the same address, the unique IDs will differ.
    pub fn points_to_ii(&self, index: &InvertedIndex<E>) -> bool {
        std::ptr::eq(self.ii, index) && self.ii_unique_id == index.unique_id()
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by
    /// the C tests to trigger a revalidation. Also refreshes the snapshot from the new
    /// index, since the previous snapshot was tied to the old one.
    ///
    /// # Caller obligation (unenforced)
    /// Like [`Self::reset`], swapping replaces `self.snapshot`. Any
    /// `RSIndexResult<'index>` previously yielded by this reader will be left with
    /// dangling slices into the dropped snapshot — safe Rust does not catch this.
    /// Drop every outstanding result before calling. Tracked in MOD-16148 for a
    /// borrow-checker-enforced fix.
    pub fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        std::mem::swap(&mut self.ii, index);
        self.ii_unique_id = self.ii.unique_id();
        self.snapshot = self.ii.snapshot();
        self.current_block_idx = 0;
        self.current_position = 0;
        self.last_doc_id = self
            .snapshot
            .first_block()
            .map(|b| b.first_doc_id)
            .unwrap_or(0);
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.ii
    }

    /// Set the current active block to the given logical index in the snapshot.
    fn set_current_block(&mut self, index: usize) {
        debug_assert!(
            index < self.snapshot.block_count(),
            "block index should stay in bounds"
        );
        self.current_block_idx = index;
        self.current_position = 0;
        if let Some(block) = self.snapshot.block_ref(index) {
            self.last_doc_id = block.first_doc_id;
        }
    }

    /// Construct a [`Cursor`] over a snapshot block's buffer, with its position set.
    ///
    /// # Safety
    ///
    /// The returned cursor has a fictitious `'index` lifetime. The actual lifetime of
    /// the borrowed buffer slice is that of the `IndexBlock` inside `self.snapshot`,
    /// which is owned by the reader (`self`).
    ///
    /// **The borrow checker does not enforce the soundness invariant below.** Slices
    /// in an `RSIndexResult<'index>` decoded from the returned cursor become dangling
    /// the moment `self.snapshot` is replaced. The caller MUST drop every
    /// previously-yielded `RSIndexResult<'index>` before calling [`Self::reset`] or
    /// [`Self::swap_index`]. RediSearch's natural "decode → consume result → decode
    /// again" loop preserves this; bespoke callers must too.
    ///
    /// Snapshot-block buffers are immutable `Vec<u8>` clones produced by the writer, so
    /// they cannot be reallocated under us while the snapshot is held.
    ///
    /// Tracked: [MOD-16148] proposes tying the result lifetime to a per-call reader
    /// borrow so the borrow checker enforces this contract.
    #[inline(always)]
    unsafe fn cursor_at(buffer: &[u8], position: u64) -> Cursor<&'index [u8]> {
        // SAFETY: lifetime extension; see the function-level safety comment.
        let extended: &'index [u8] = unsafe { std::mem::transmute::<&[u8], &'index [u8]>(buffer) };
        let mut cursor = Cursor::new(extended);
        cursor.set_position(position);
        cursor
    }

    /// Number of blocks visible in the reader's current snapshot.
    #[cfg(test)]
    pub(crate) fn snapshot_block_count(&self) -> usize {
        self.snapshot.block_count()
    }
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReaderCore<'_, E> {
        IndexReaderCore::new(self)
    }
}
