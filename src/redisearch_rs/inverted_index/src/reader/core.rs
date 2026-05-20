/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, marker::PhantomData, ptr::NonNull, sync::atomic};

use ref_mode::{Active, Ref, SharedPtr, Suspended};

use super::{
    IndexReader, NumericReader, PointsToOpaqueIndex, RefreshOutcome, ResumableReader,
    SuspendableReader, TermReader,
};
use crate::{
    DecodedBy, Decoder, Encoder, HasInnerIndex, InvertedIndex, NumericDecoder, TermDecoder,
    index::unique_id::IndexUniqueId, opaque::OpaqueEncoding,
};
use ffi::{IndexFlags, IndexFlags_Index_HasMultiValue};
use index_result::RSIndexResult;
use rqe_core::DocId;

/// Reader that is able to read the records from an [`InvertedIndex`].
///
/// Parameterised over a [`Ref`] mode:
///
/// - With [`Active<'index>`], the pointers in this struct are real `&'index` references
///   into the index and the [`IndexReader`] trait is implemented — see
///   [`IndexReaderCore`] for that instantiation.
/// - With [`Suspended`], the pointers are inert raw
///   pointers — the struct is a passive carrier across a lock release/reacquire
///   cycle. It will be re-promoted to [`Active`] under the read lock before any
///   reading happens.
///
/// Both instantiations have identical memory layout (`#[repr(C)]` +
/// [`SharedPtr`] is `#[repr(transparent)]` over `NonNull`), so converting from
/// `Active` to `Suspended` and back is a zero-cost transmute.
#[repr(C)]
pub struct RawIndexReaderCore<Rf: Ref, E> {
    /// The inverted index that is being read from.
    pub(crate) ii: SharedPtr<Rf, InvertedIndex<E>>,

    /// The buffer of the current block. In [`Active`] mode this is a real
    /// `&'index [u8]` into the block buffer; in [`Suspended`]
    /// mode it is a raw pointer that may be stale and is refreshed when
    /// re-promoting to [`Active`].
    buf: SharedPtr<Rf, [u8]>,

    /// The current read position into [`Self::buf`].
    buf_pos: u64,

    /// The index of the current block in the `blocks` vector. This is used to keep track of
    /// which block we are currently reading from, especially when the current buffer is empty and we
    /// need to move to the next block.
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

    /// Keeps `E` in the type signature even though it only appears through `ii`.
    _phantom: PhantomData<E>,
}

/// Alias for an [`Active`] [`RawIndexReaderCore`] — the only instantiation
/// that can actually read records from the underlying index.
pub type IndexReaderCore<'index, E> = RawIndexReaderCore<Active<'index>, E>;

impl<Rf: Ref, E: Encoder> RawIndexReaderCore<Rf, E> {
    /// Check if this reader is reading from the given index by comparing both their pointers and
    /// unique IDs. The dual check prevents the ABA problem: if the original index is freed and a
    /// new one is allocated at the same address, the unique IDs will differ.
    ///
    /// Mode-independent: neither the raw `ii` pointer nor the cached
    /// `ii_unique_id` requires dereferencing the [`Suspended`] pointer,
    /// so leaves (`Tag`, `Term`, …) can call this from their suspended-side
    /// `should_abort` checks.
    pub fn points_to_ii(&self, index: &InvertedIndex<E>) -> bool {
        std::ptr::eq(self.ii.as_raw(), index) && self.ii_unique_id == index.unique_id()
    }
}

/// `IndexReaderCore<Active<'a>, E>` suspends to `IndexReaderCore<Suspended, E>`.
impl<'a, E> SuspendableReader for RawIndexReaderCore<Active<'a>, E> {
    type Suspended = RawIndexReaderCore<Suspended, E>;
}

/// Inverse of the above: `RawIndexReaderCore<Suspended, E>` resumes to
/// `IndexReaderCore<Active<'a>, E>` at any index lifetime `'a`.
impl<E: 'static> ResumableReader for RawIndexReaderCore<Suspended, E>
where
    for<'a> RawIndexReaderCore<Active<'a>, E>: IndexReader<'a>,
{
    type Resumed<'a> = RawIndexReaderCore<Active<'a>, E>;

    fn refresh_pointers(&mut self) -> RefreshOutcome {
        // SAFETY: The caller holds the IndexSpec's read lock (documented
        // invariant on [`ResumableReader::refresh_pointers`]). The
        // `InvertedIndex` struct that `self.ii` points to is owned by the
        // spec and its address is stable for the spec's lifetime — GC
        // mutates the index in place rather than replacing the struct.
        // The borrow ends with this function so it cannot escape and is
        // not aliased with any mutable reference (the read lock excludes
        // writers).
        let ii: &InvertedIndex<E> = unsafe { &*self.ii.as_raw() };

        let current_gc = ii.gc_marker.load(atomic::Ordering::Relaxed);
        if current_gc != self.gc_marker {
            // GC ran while we were suspended. The cached block offset is
            // stale; refreshing the buffer pointer alone would not be
            // enough, and would in fact mislead the caller into thinking
            // the offset is still valid. Leave `self.buf` as it is (its
            // value is about to be overwritten by `rewind` on the active
            // side) and report that a re-seek is needed.
            return RefreshOutcome::NeedsReseek {
                last_doc_id: self.last_doc_id,
            };
        }

        if !ii.blocks.is_empty() && self.current_block_idx < ii.blocks.len() {
            let current_block = &ii.blocks[self.current_block_idx];
            // Write a fresh `Suspended` pointer to the current block's
            // buffer. We do *not* dereference `self.buf` first — its
            // previous value may be dangling if the block buffer was
            // reallocated by a non-GC operation (e.g. an append landing
            // on a different allocator slot).
            let new_buf: NonNull<[u8]> = NonNull::from(current_block.buffer.as_slice());
            self.buf = SharedPtr::from_non_null(new_buf);
        }

        RefreshOutcome::Ok
    }
}

// Automatically implemented if the IndexReaderCore uses a NumericDecoder.
impl<'index, E: DecodedBy<Decoder = D> + 'index, D: Decoder + NumericDecoder> NumericReader<'index>
    for RawIndexReaderCore<Active<'index>, E>
{
}

/// Mode-independent: any `RawIndexReaderCore<Rf, E>` whose encoding `E`
/// satisfies the term-decoder bounds can resolve an opaque
/// [`InvertedIndex`](crate::opaque::InvertedIndex) and compare against
/// its own `ii` pointer. The body uses only mode-independent helpers
/// ([`E::from_opaque`](crate::DecodedBy) +
/// [`Self::points_to_ii`](RawIndexReaderCore::points_to_ii)), so it
/// works in [`Active`] and [`Suspended`] modes alike — leaves (`Term`)
/// call this from their suspended-side `should_abort` checks without
/// upgrading the reader.
impl<Rf: Ref, E: DecodedBy<Decoder = D> + OpaqueEncoding, D: Decoder + TermDecoder>
    PointsToOpaqueIndex for RawIndexReaderCore<Rf, E>
where
    E::Storage: HasInnerIndex<E>,
{
    fn points_to_the_same_opaque_index(&self, opaque: &crate::opaque::InvertedIndex) -> bool {
        let storage = E::from_opaque(opaque);
        let ii = storage.inner_index();
        self.points_to_ii(ii)
    }
}

/// Automatically implemented if the IndexReaderCore uses a TermDecoder.
/// The [`PointsToOpaqueIndex`] supertrait is satisfied via the
/// mode-independent impl above.
impl<'index, E: DecodedBy<Decoder = D> + OpaqueEncoding + 'index, D: Decoder + TermDecoder>
    TermReader<'index> for RawIndexReaderCore<Active<'index>, E>
where
    E::Storage: HasInnerIndex<E>,
{
}

impl<'index, E: DecodedBy<Decoder = D> + 'index, D: Decoder> IndexReader<'index>
    for RawIndexReaderCore<Active<'index>, E>
{
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        // Check if the current buffer is empty or the end of the buffer has been reached
        if (self.buf.get().len() as u64) <= self.buf_pos {
            if self.current_block_idx + 1 >= self.ii.get().blocks.len() {
                // No more blocks to read from
                return Ok(false);
            };

            self.set_current_block(self.current_block_idx + 1);
        }

        let ii = self.ii.get();
        let base = D::base_id(&ii.blocks[self.current_block_idx], self.last_doc_id);
        let mut cursor = Cursor::new(self.buf.get());
        cursor.set_position(self.buf_pos);
        D::decode(&mut cursor, base, result)?;
        self.buf_pos = cursor.position();

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

        let ii = self.ii.get();
        let base = D::base_id(&ii.blocks[self.current_block_idx], self.last_doc_id);
        let mut cursor = Cursor::new(self.buf.get());
        cursor.set_position(self.buf_pos);
        let success = D::seek(&mut cursor, base, doc_id, result)?;
        self.buf_pos = cursor.position();

        if success {
            self.last_doc_id = result.doc_id;
        }

        Ok(success)
    }

    fn skip_to(&mut self, doc_id: DocId) -> bool {
        let ii = self.ii.get();
        if ii.blocks.is_empty() {
            return false;
        }

        if ii.blocks[self.current_block_idx].last_doc_id >= doc_id {
            // We are already in the correct block
            return true;
        }

        // SAFETY: it is safe to unwrap because we checked that the blocks are not empty when
        // creating the reader.
        if ii.blocks.last().unwrap().last_doc_id < doc_id {
            // The document ID is greater than the last document ID in the index
            return false;
        }

        // Check if the very next block is correct before doing a binary search. This is a small
        // optimization for the common case where we are skipping to the next block.
        let search_start = self.current_block_idx + 1;
        if let Some(next_block) = ii.blocks.get(search_start)
            && next_block.last_doc_id >= doc_id
        {
            self.set_current_block(search_start);
            return true;
        }

        // Binary search to find the correct block index
        let relative_idx = ii.blocks[search_start..]
            .binary_search_by_key(&doc_id, |b| b.last_doc_id)
            .unwrap_or_else(|insertion_point| insertion_point);

        self.set_current_block(search_start + relative_idx);

        true
    }

    fn reset(&mut self) {
        let ii = self.ii.get();
        if !ii.blocks.is_empty() {
            self.set_current_block(0);
        } else {
            self.buf = SharedPtr::from_ref(&[]);
            self.buf_pos = 0;
            self.last_doc_id = 0;
        }

        self.gc_marker = self.ii.get().gc_marker.load(atomic::Ordering::Relaxed);
    }

    fn unique_docs(&self) -> u64 {
        self.ii.get().unique_docs() as u64
    }

    fn has_duplicates(&self) -> bool {
        self.ii.get().flags() & IndexFlags_Index_HasMultiValue > 0
    }

    fn flags(&self) -> IndexFlags {
        self.ii.get().flags()
    }

    fn needs_revalidation(&self) -> bool {
        self.gc_marker != self.ii.get().gc_marker.load(atomic::Ordering::Relaxed)
    }

    fn refresh_buffer_pointers(&mut self) {
        let ii = self.ii.get();
        if !ii.blocks.is_empty() && self.current_block_idx < ii.blocks.len() {
            let current_block = &ii.blocks[self.current_block_idx];
            // Update the cursor to point to the current position in the refreshed buffer
            self.buf = SharedPtr::from_ref(current_block.buffer.as_slice());
        }
    }
}

impl<'index, E: DecodedBy<Decoder = D> + 'index, D: Decoder> RawIndexReaderCore<Active<'index>, E> {
    /// Create a new index reader that reads from the given [`InvertedIndex`].
    ///
    /// # Panic
    /// This function will panic if the inverted index is empty.
    pub(crate) fn new(ii: &'index InvertedIndex<E>) -> Self {
        let (buf, last_doc_id) = if let Some(first_block) = ii.blocks.first() {
            (
                SharedPtr::from_ref(first_block.buffer.as_slice()),
                first_block.first_doc_id,
            )
        } else {
            (SharedPtr::from_ref(&[][..]), 0)
        };

        Self {
            ii: SharedPtr::from_ref(ii),
            buf,
            buf_pos: 0,
            current_block_idx: 0,
            last_doc_id,
            gc_marker: ii.gc_marker.load(atomic::Ordering::Relaxed),
            ii_unique_id: ii.unique_id(),
            _phantom: PhantomData,
        }
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation.
    pub const fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        let current = self.ii.get();
        let new_ii = std::mem::replace(index, current);
        self.ii = SharedPtr::from_ref(new_ii);
        self.ii_unique_id = new_ii.unique_id();
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.ii.get()
    }

    /// Set the current active block to the given index
    fn set_current_block(&mut self, index: usize) {
        let ii = self.ii.get();
        debug_assert!(index < ii.blocks.len(), "block index should stay in bounds");

        self.current_block_idx = index;
        let current_block = &ii.blocks[self.current_block_idx];
        self.last_doc_id = current_block.first_doc_id;
        self.buf = SharedPtr::from_ref(current_block.buffer.as_slice());
        self.buf_pos = 0;
    }
}

impl<E: Encoder + DecodedBy> InvertedIndex<E> {
    /// Create a new [`IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReaderCore<'_, E> {
        IndexReaderCore::new(self)
    }
}
