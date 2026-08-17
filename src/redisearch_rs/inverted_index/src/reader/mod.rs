/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod core;
mod field_mask;
mod geo;
mod numeric;

use ffi::IndexFlags;
use index_result::RSIndexResult;
use rqe_core::{DocId, FieldMask};

pub use self::core::{IndexReaderCore, RawIndexReaderCore};
pub use field_mask::FilterMaskReader;
pub use geo::FilterGeoReader;
pub use numeric::{FilterNumericReader, NumericFilter};

/// Outcome of [`ResumableReader::refresh_pointers`].
///
/// Communicates to the iterator's `resume` body whether the reader's
/// cached buffer pointer was successfully refreshed in place, or whether
/// a garbage-collection cycle invalidated the position and the iterator
/// must rewind + re-seek after being promoted back to [`Active`](ref_mode::Active).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// The buffer pointer was refreshed without intervening GC. The
    /// reader's position is still valid; the caller can cast Suspended →
    /// Active and resume reading at the same offset.
    Ok,
    /// Either GC ran while suspended, or the current block buffer was
    /// relocated by a non-GC reallocation (an append that outgrew its
    /// allocation). Either way the cached block offset — and any
    /// `RSOffsetSlice` the iterator's result borrows from that buffer — is no
    /// longer valid. The caller must promote to Active, rewind, and re-seek to
    /// the last document it returned before suspending, which re-decodes the
    /// current document and rebuilds every borrowed slice against the live
    /// buffer.
    NeedsReseek,
}

/// A reader is something which knows how to read / decode the records from an [`InvertedIndex`](crate::InvertedIndex).
pub trait IndexReader<'index> {
    /// Read the next record from the index into `result`. If there are no more records to read,
    /// then `false` is returned.
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool>;

    /// Seek to the first record whose ID is higher or equal to the given document ID and put it
    /// on `recult`. If the end of the index is reached before finding the document ID, then `false`
    /// is returned.
    fn seek_record(
        &mut self,
        doc_id: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool>;

    /// Skip forward to the block containing the given document ID. Returns false if the end of the
    /// index was reached and true otherwise.
    fn skip_to(&mut self, doc_id: DocId) -> bool;

    /// Reset the reader to the beginning of the index.
    fn reset(&mut self);

    /// Return the number of unique documents in the underlying index.
    fn unique_docs(&self) -> u64;

    /// Returns true if the underlying index has duplicate document IDs.
    fn has_duplicates(&self) -> bool;

    /// Get the flags of the underlying index
    fn flags(&self) -> IndexFlags;

    /// Check if the underlying index has been modified since the last time this reader read from it.
    /// If it has, then the reader should be reset before reading from it again.
    fn needs_revalidation(&self) -> bool;

    /// Refresh buffer pointers in case blocks were reallocated without GC changes
    fn refresh_buffer_pointers(&mut self);
}

/// Type-level mapping from an Active reader to its Suspended counterpart.
///
/// Each reader type that contains [`ref_mode::Active`] somewhere in its type
/// parameters declares its matching `Suspended` shape via this trait. The
/// suspend/resume work in `rqe_iterators` uses it to express the `Suspended`
/// associated type of an iterator generically, e.g.
/// `InvIndIterator<Active<'a>, R, E>` has
/// `Suspended = InvIndIterator<Suspended, R::Suspended, E>` for any
/// `R: SuspendableReader`.
///
/// The whole-allocation reinterpretation between the two layouts happens at the
/// iterator level (the inverted-index iterator's `suspend`/`resume` in the
/// `rqe_iterators` crate); this trait is the type-level half of that mechanism.
///
/// # Safety
///
/// Implementers must guarantee that `Self` and [`Self::Suspended`] are
/// **layout-compatible**: identical size and alignment, with every field at the
/// same offset, so a `Box<Self>` may be reinterpreted as a `Box<Self::Suspended>`
/// via a same-allocation pointer cast. The inverted-index iterator relies on this
/// to move between [`Active`](ref_mode::Active) and [`Suspended`](ref_mode::Suspended)
/// modes without reallocating (which would dangle external pointers into the
/// iterator interior). The idiomatic way to satisfy this is to make both types
/// the *same* `#[repr(C)]` generic differing only in the [`Ref`](ref_mode::Ref) mode, whose only
/// mode-dependent fields are `#[repr(transparent)]`-over-`NonNull`
/// [`SharedPtr`](ref_mode::SharedPtr)s — those have identical layout in every mode.
pub unsafe trait SuspendableReader {
    /// The matching reader type carrying [`ref_mode::Suspended`] instead of
    /// [`ref_mode::Active`].
    type Suspended;
}

/// Inverse of [`SuspendableReader`]: type-level mapping from a Suspended
/// reader to its Active counterpart at a given index lifetime.
///
/// Together with [`SuspendableReader`], implementers of this trait form a
/// bijection between Active and Suspended reader types — used by the
/// `RQESuspendedIterator` impl on the suspended side of inverted-index
/// iterators (in `rqe_iterators`) to express the [`Resumed`](Self::Resumed)
/// associated type generically.
///
/// The `'static` bound reflects that a suspended reader carries no live
/// references into the index.
///
/// # Safety
///
/// Implementers must guarantee that `Self` and [`Self::Resumed`] (for every
/// lifetime `'a`) are **layout-compatible** in the sense described on
/// [`SuspendableReader`] — this is the inverse direction of the same
/// same-allocation reinterpretation the iterator performs on resume.
pub unsafe trait ResumableReader: 'static {
    /// The matching active reader type, parameterised by the index
    /// lifetime under which the suspended reader is being resumed.
    type Resumed<'a>: IndexReader<'a> + SuspendableReader<Suspended = Self> + 'a;

    /// Refresh any reader-internal pointers that may have been invalidated
    /// while suspended, *without* leaving the [`Suspended`](ref_mode::Suspended)
    /// type-state.
    ///
    /// Returns whether the iterator's last-known position is still usable
    /// after the refresh, or whether garbage collection ran and the caller
    /// must rewind + re-seek after promoting back to [`Active`](ref_mode::Active).
    ///
    /// # Why this method exists on the suspended side
    ///
    /// The active form of a reader holds `SharedPtr<Active<'a>, [u8]>`
    /// fields whose `'a` validity guarantee may have been broken by a GC
    /// cycle that reallocated the underlying block buffer. Promoting
    /// Suspended → Active *before* refreshing those pointers is a
    /// validity hazard: any code path inside the refresh that materializes
    /// a `&'a [u8]` borrow from the stale field (even through
    /// [`SharedPtr::get`](ref_mode::SharedPtr::get)) is UB.
    ///
    /// Doing the refresh on the suspended form sidesteps this entirely:
    /// `SharedPtr<Suspended, _>` exposes no safe deref, so the refresh
    /// has to either write a fresh pointer directly or upgrade fields
    /// one at a time under a documented invariant. Only after every
    /// `Rf`-dependent field has been refreshed does the iterator perform
    /// the whole-box `Box<Suspended> → Box<Active<'a>>` cast.
    ///
    /// # Safety
    ///
    /// For the duration of the call the caller must guarantee that no concurrent
    /// writer or GC cycle mutates, relocates, or frees the pointed-to inverted
    /// index — i.e. access to it is effectively unique (or shared read-only with
    /// no aliasing writer). The implementation reads the index's GC marker and
    /// block buffers through the reader's stored raw index pointer; a concurrent
    /// mutation would be a data race. This trait does not depend on `index_spec`,
    /// so the obligation is expressed as `unsafe` rather than by threading a guard
    /// type. In practice the sole caller — `RQESuspendedIterator::resume` (in
    /// `rqe_iterators`) — satisfies it by holding the [`IndexSpec`](ffi::IndexSpec)
    /// read lock (witnessed by its `&IndexSpecReadGuard` parameter), which excludes
    /// writers and GC for the duration of the call.
    unsafe fn refresh_pointers(&mut self) -> RefreshOutcome;
}

/// Marker trait for readers producing numeric values.
pub trait NumericReader<'index>: IndexReader<'index> {}

/// Check whether a reader is linked to the same
/// [`InvertedIndex`](crate::opaque::InvertedIndex) as an externally-held
/// opaque pointer.
///
/// Extracted from [`TermReader`] so it can be implemented for both the
/// [`Active`](ref_mode::Active) and [`Suspended`](ref_mode::Suspended) reader
/// forms; leaves (`Term`, FFI enums) call it from their suspended-side
/// `should_abort` checks during `RQESuspendedIterator::resume`.
pub trait PointsToOpaqueIndex {
    /// Check if this reader's underlying index points to the same one
    /// contained in the given opaque [`InvertedIndex`](crate::opaque::InvertedIndex).
    fn points_to_the_same_opaque_index(&self, opaque: &crate::opaque::InvertedIndex) -> bool;
}

/// Trait for readers producing term values.
pub trait TermReader<'index>: IndexReader<'index> + PointsToOpaqueIndex {}

/// Filter to apply when reading from an index. Entries which don't match the filter will not be
/// returned by the reader.
#[cheadergen::config(prefix_with_name, rename = "IndexDecoderCtx")]
#[repr(u8)]
#[derive(Debug)]
pub enum ReadFilter<'numeric_filter> {
    /// No filter, all entries are accepted
    None,

    /// Accepts entries matching this field mask
    FieldMask(FieldMask),

    /// Accepts entries matching this numeric filter
    Numeric(&'numeric_filter NumericFilter),
}
