/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::{IndexReader, IndexReaderCore, TermReader};
use crate::{
    DecodedBy, Decoder, HasInnerIndex, InvertedIndex, RSIndexResult, TermDecoder,
    opaque::OpaqueEncoding,
};
use ffi::{IndexFlags, t_docId, t_fieldMask};

/// A reader that filters out records that do not match a given field mask. It is used to
/// filter records in an index based on their field mask, allowing only those that match the
/// specified mask to be returned.
pub struct FilterMaskReader<IR> {
    /// Mask which a record needs to match to be valid
    mask: t_fieldMask,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

impl<'index, IR: IndexReader<'index>> FilterMaskReader<IR> {
    /// Create a new filter mask reader with the given mask and inner iterator
    pub const fn new(mask: t_fieldMask, inner: IR) -> Self {
        Self { mask, inner }
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
        doc_id: t_docId,
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

    fn skip_to(&mut self, doc_id: t_docId) -> bool {
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
impl<'index, E: DecodedBy<Decoder = D> + OpaqueEncoding, D: Decoder + TermDecoder>
    TermReader<'index> for FilterMaskReader<IndexReaderCore<'index, E>>
where
    E::Storage: HasInnerIndex<E>,
{
    fn points_to_the_same_opaque_index(&self, opaque: &crate::opaque::InvertedIndex) -> bool {
        self.inner.points_to_the_same_opaque_index(opaque)
    }
}
