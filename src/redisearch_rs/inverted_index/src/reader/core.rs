/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, sync::atomic};

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
/// Block-data access goes through [`Self::snapshot`] — a borrowed view of the index
/// captured at construction (refreshed by [`Self::reset`] and [`Self::swap_index`]).
/// The follow-up storage refactor will change `snapshot` from a borrowed wrapper into
/// an owned one without changing the reader's call sites.
pub struct IndexReaderCore<'index, E> {
    /// The inverted index that is being read from.
    pub(crate) ii: &'index InvertedIndex<E>,

    /// Snapshot of the index's block storage. Currently borrows from `ii`; the follow-up
    /// storage refactor swaps in an owned snapshot.
    snapshot: InvertedIndexSnapshot<'index>,

    /// The current position in the block that is being read from.
    current_buffer: Cursor<&'index [u8]>,

    /// The index of the current block in the snapshot. Used to track which block we're
    /// reading from, especially when the current buffer is empty and we need to advance.
    pub(crate) current_block_idx: usize,

    /// The last document ID that was read from the index. This is used to determine the base
    /// document ID for delta calculations.
    pub(crate) last_doc_id: DocId,

    /// The marker of the inverted index when this reader last read from it. This is used to
    /// detect if the index has been modified since the last read, in which case the reader
    /// should be reset.
    pub(crate) gc_marker: u32,

    /// The unique ID of the inverted index when this reader was created. Used together with
    /// pointer comparison in [`Self::points_to_ii`] to detect the ABA problem: if the original
    /// index is freed and a new one is allocated at the same address, the unique IDs will differ.
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
        // Check if the current buffer is empty or the end of the buffer has been reached
        if self.current_buffer.get_ref().len() as u64 <= self.current_buffer.position() {
            if self.current_block_idx + 1 >= self.snapshot.block_count() {
                // No more blocks to read from
                return Ok(false);
            };

            self.set_current_block(self.current_block_idx + 1);
        }

        let block = self
            .snapshot
            .block_ref(self.current_block_idx)
            .expect("current_block_idx must point to a valid block");
        let base = D::base_id(block, self.last_doc_id);
        D::decode(&mut self.current_buffer, base, result)?;

        self.last_doc_id = result.doc_id;

        Ok(true)
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
        let success = D::seek(&mut self.current_buffer, base, doc_id, result)?;

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

        // SAFETY: total > 0 was checked above.
        let last = self.snapshot.last_block().unwrap();
        if last.last_doc_id < doc_id {
            return false;
        }

        // Fast path: is the very next block the answer?
        let search_start = self.current_block_idx + 1;
        if let Some(next_block) = self.snapshot.block_ref(search_start)
            && next_block.last_doc_id >= doc_id
        {
            self.set_current_block(search_start);
            return true;
        }

        let idx = self.snapshot.find_block_for_doc_id(search_start, doc_id);
        debug_assert!(idx < total, "we verified above that doc_id is in range");
        self.set_current_block(idx);
        true
    }

    fn reset(&mut self) {
        self.snapshot = self.ii.snapshot();
        if self.snapshot.block_count() > 0 {
            self.set_current_block(0);
        } else {
            self.current_buffer = Cursor::new(&[]);
            self.last_doc_id = 0;
        }

        self.gc_marker = self.ii.gc_marker.load(atomic::Ordering::Relaxed);
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
        self.gc_marker != self.ii.gc_marker.load(atomic::Ordering::Relaxed)
    }

    fn refresh_buffer_pointers(&mut self) {
        // Re-snapshot first — the borrowed snapshot may be stale (e.g. the index's
        // backing Vec was reallocated under us via raw-pointer mutation in C-side
        // code). Refreshing gives us a current view of `ii.blocks`.
        self.snapshot = self.ii.snapshot();
        if let Some(current_block) = self.snapshot.block_ref(self.current_block_idx) {
            // Update the cursor to point to the current position in the refreshed buffer
            let position = self.current_buffer.position();
            self.current_buffer = Cursor::new(&current_block.buffer);
            self.current_buffer.set_position(position);
        }
    }
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> IndexReaderCore<'index, E> {
    /// Create a new index reader that reads from the given [`InvertedIndex`].
    ///
    /// # Panic
    /// This function will panic if the inverted index is empty.
    pub(crate) fn new(ii: &'index InvertedIndex<E>) -> Self {
        let snapshot = ii.snapshot();
        let (current_buffer, last_doc_id) = if let Some(first_block) = snapshot.first_block() {
            (
                Cursor::new(first_block.buffer.as_ref()),
                first_block.first_doc_id,
            )
        } else {
            (Cursor::new(&[] as &[u8]), 0)
        };

        Self {
            ii,
            snapshot,
            current_buffer,
            current_block_idx: 0,
            last_doc_id,
            gc_marker: ii.gc_marker.load(atomic::Ordering::Relaxed),
            ii_unique_id: ii.unique_id(),
        }
    }

    /// Check if this reader is reading from the given index by comparing both their pointers and
    /// unique IDs. The dual check prevents the ABA problem: if the original index is freed and a
    /// new one is allocated at the same address, the unique IDs will differ.
    pub fn points_to_ii(&self, index: &InvertedIndex<E>) -> bool {
        std::ptr::eq(self.ii, index) && self.ii_unique_id == index.unique_id()
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation. Also refreshes the snapshot to point at the new index.
    pub fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        std::mem::swap(&mut self.ii, index);
        self.ii_unique_id = self.ii.unique_id();
        self.snapshot = self.ii.snapshot();
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.ii
    }

    /// Set the current active block to the given index in the snapshot.
    fn set_current_block(&mut self, index: usize) {
        debug_assert!(
            index < self.snapshot.block_count(),
            "block index should stay in bounds"
        );

        self.current_block_idx = index;
        let current_block = self
            .snapshot
            .block_ref(self.current_block_idx)
            .expect("debug_assert above bounded the index");
        self.last_doc_id = current_block.first_doc_id;
        self.current_buffer = Cursor::new(&current_block.buffer);
    }
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReaderCore<'_, E> {
        IndexReaderCore::new(self)
    }
}
