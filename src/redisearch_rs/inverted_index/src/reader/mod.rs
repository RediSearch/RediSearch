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

use crate::RSIndexResult;
use ffi::{IndexFlags, t_docId, t_fieldMask};

pub use self::core::*;
pub use field_mask::FilterMaskReader;
pub use geo::FilterGeoReader;
pub use numeric::{FilterNumericReader, NumericFilter};

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
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool>;

    /// Skip forward to the block containing the given document ID. Returns false if the end of the
    /// index was reached and true otherwise.
    fn skip_to(&mut self, doc_id: t_docId) -> bool;

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

/// Marker trait for readers producing numeric values.
pub trait NumericReader<'index>: IndexReader<'index> {}

/// Trait for readers producing term values.
pub trait TermReader<'index>: IndexReader<'index> {
    /// Check if this reader's underlying index points to the same one
    /// contained in the given opaque [`InvertedIndex`](crate::opaque::InvertedIndex).
    fn points_to_the_same_opaque_index(&self, opaque: &crate::opaque::InvertedIndex) -> bool;
}

/// Filter to apply when reading from an index. Entries which don't match the filter will not be
/// returned by the reader.
/// cbindgen:prefix-with-name=true
#[repr(u8)]
#[derive(Debug)]
pub enum ReadFilter<'numeric_filter> {
    /// No filter, all entries are accepted
    None,

    /// Accepts entries matching this field mask
    FieldMask(t_fieldMask),

    /// Accepts entries matching this numeric filter
    Numeric(&'numeric_filter NumericFilter),
}
