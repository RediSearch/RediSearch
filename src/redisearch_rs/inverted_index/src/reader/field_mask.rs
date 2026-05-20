/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::{
    IndexReader, IndexReaderCore, RefreshOutcome, ResumableReader, SuspendableReader, TermReader,
};
use crate::{
    DecodedBy, Decoder, HasInnerIndex, InvertedIndex, TermDecoder, opaque::OpaqueEncoding,
};
use ffi::IndexFlags;
use index_result::RSIndexResult;
use rqe_core::{DocId, FieldMask};

/// A reader that filters out records that do not match a given field mask. It is used to
/// filter records in an index based on their field mask, allowing only those that match the
/// specified mask to be returned.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** When the inner reader `IR` is
///    layout-compatible with its suspended form (invariant 1 on
///    [`RawIndexReaderCore`](crate::RawIndexReaderCore)), so is
///    `FilterMaskReader<IR>`: it is `#[repr(C)]` and `IR` is its only
///    mode-dependent field. This is the layout compatibility the
///    [`SuspendableReader`]/[`ResumableReader`] contract requires. Enforced by
///    the `const _` proof below.
#[repr(C)]
pub struct FilterMaskReader<IR> {
    /// Mask which a record needs to match to be valid
    mask: FieldMask,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

// Compile-time proof of invariant 1 on `FilterMaskReader`, for a representative
// concrete inner reader. The inner reader's own layout compatibility is invariant
// 1 on `RawIndexReaderCore`.
const _: () = {
    use crate::RawIndexReaderCore;
    use crate::codec::doc_ids_only::DocIdsOnly;
    use ref_mode::{Active, Suspended};
    use std::mem::{align_of, offset_of, size_of};
    type A = FilterMaskReader<RawIndexReaderCore<Active<'static>, DocIdsOnly>>;
    type S = FilterMaskReader<RawIndexReaderCore<Suspended, DocIdsOnly>>;
    assert!(offset_of!(A, mask) == offset_of!(S, mask));
    assert!(offset_of!(A, inner) == offset_of!(S, inner));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, IR: IndexReader<'index>> FilterMaskReader<IR> {
    /// Create a new filter mask reader with the given mask and inner iterator
    pub const fn new(mask: FieldMask, inner: IR) -> Self {
        Self { mask, inner }
    }
}

/// `FilterMaskReader<IR>` suspends to `FilterMaskReader<IR::Suspended>`.
///
/// SAFETY: layout compatibility is invariant 1 on [`FilterMaskReader`] (const
/// proof there), given `IR`'s own layout compatibility.
unsafe impl<IR: SuspendableReader> SuspendableReader for FilterMaskReader<IR> {
    type Suspended = FilterMaskReader<IR::Suspended>;
}

/// Inverse of the above: `FilterMaskReader<RS>` resumes to
/// `FilterMaskReader<RS::Resumed<'a>>` for any `RS: ResumableReader`.
///
/// SAFETY: layout compatibility is invariant 1 on [`FilterMaskReader`] (const
/// proof there).
unsafe impl<RS: ResumableReader> ResumableReader for FilterMaskReader<RS>
where
    for<'a> Self: 'static,
    for<'a> FilterMaskReader<RS::Resumed<'a>>: IndexReader<'a>,
{
    type Resumed<'a> = FilterMaskReader<RS::Resumed<'a>>;

    unsafe fn refresh_pointers(&mut self) -> RefreshOutcome {
        // SAFETY: our caller upholds `ResumableReader::refresh_pointers`'s
        // read-lock obligation, which we forward unchanged to the inner reader.
        unsafe { self.inner.refresh_pointers() }
    }
}

impl<'index, IR: IndexReader<'index>> IndexReader<'index> for FilterMaskReader<IR> {
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        loop {
            let success = self.inner.next_record(result)?;

            if !success {
                return Ok(false);
            }

            if result.field_mask & self.mask > 0 {
                return Ok(true);
            }
        }
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let success = self.inner.seek_record(doc_id, result)?;

        if !success {
            return Ok(false);
        }

        if result.field_mask & self.mask > 0 {
            Ok(true)
        } else {
            self.next_record(result)
        }
    }

    fn skip_to(&mut self, doc_id: DocId) -> bool {
        self.inner.skip_to(doc_id)
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn unique_docs(&self) -> u64 {
        self.inner.unique_docs()
    }

    fn has_duplicates(&self) -> bool {
        self.inner.has_duplicates()
    }

    fn flags(&self) -> IndexFlags {
        self.inner.flags()
    }

    fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }

    fn refresh_buffer_pointers(&mut self) {
        self.inner.refresh_buffer_pointers();
    }
}

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> FilterMaskReader<IndexReaderCore<'index, E>> {
    /// Check if the underlying index has been modified since the last time this reader read from it.
    /// If it has, then the reader should be reset before reading from it again.
    pub fn needs_revalidation(&self) -> bool {
        self.inner.needs_revalidation()
    }

    /// Check if this reader is reading from the given index
    pub fn is_index(&self, index: &InvertedIndex<E>) -> bool {
        self.inner.points_to_ii(index)
    }

    /// Swap the inverted index of the reader with the supplied index. This is only used by the C
    /// tests to trigger a revalidation.
    pub const fn swap_index(&mut self, index: &mut &'index InvertedIndex<E>) {
        self.inner.swap_index(index);
    }

    /// Get the internal index of the reader. This is only used by some C tests.
    pub const fn internal_index(&self) -> &InvertedIndex<E> {
        self.inner.internal_index()
    }
}

/// Automatically implemented if the IndexReaderCore uses a TermDecoder.
impl<'index, E: DecodedBy<Decoder = D> + OpaqueEncoding + 'index, D: Decoder + TermDecoder>
    TermReader<'index> for FilterMaskReader<IndexReaderCore<'index, E>>
where
    E::Storage: HasInnerIndex<E>,
{
    fn points_to_the_same_opaque_index(&self, opaque: &crate::opaque::InvertedIndex) -> bool {
        self.inner.points_to_the_same_opaque_index(opaque)
    }
}
