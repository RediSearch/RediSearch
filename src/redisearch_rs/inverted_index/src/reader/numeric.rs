/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_void;

use super::{
    IndexReader, IndexReaderCore, NumericReader, RefreshOutcome, ResumableReader, SuspendableReader,
};
use crate::{DecodedBy, Decoder, InvertedIndex};
use ffi::{FieldSpec, IndexFlags};
use index_result::RSIndexResult;
use rqe_core::DocId;

/// Filter details to apply to numeric values
#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[cheadergen::config(export, rename_all = "camelCase")]
pub struct NumericFilter {
    /// The field specification which this filter is acting on
    pub field_spec: *const FieldSpec,

    /// Beginning of the range
    pub min: f64,

    /// End of the range
    pub max: f64,

    /// Geo filter, if any
    pub geo_filter: *const c_void,

    /// Range includes the min value
    pub min_inclusive: bool,

    /// Range includes the max value
    pub max_inclusive: bool,

    /// Order of SORTBY (ascending/descending)
    pub ascending: bool,

    /// Minimum number of results needed
    pub limit: usize,

    /// Number of results to skip
    pub offset: usize,
}

impl Default for NumericFilter {
    fn default() -> Self {
        Self {
            min: 0.0,
            max: f64::MAX,
            min_inclusive: true,
            max_inclusive: true,
            field_spec: std::ptr::null(),
            geo_filter: std::ptr::null(),
            ascending: true,
            limit: 0,
            offset: 0,
        }
    }
}

impl NumericFilter {
    /// Check if this is a numeric filter (and not a geo filter)
    pub const fn is_numeric_filter(&self) -> bool {
        self.geo_filter.is_null()
    }

    /// Check if the given value is in the range specified by this filter
    #[inline(always)]
    pub fn value_in_range(&self, value: f64) -> bool {
        let min_ok = value > self.min || (self.min_inclusive && value == self.min);
        let max_ok = value < self.max || (self.max_inclusive && value == self.max);

        min_ok && max_ok
    }
}

/// A reader that filters out records that do not match a given numeric filter. It is used to
/// filter records in an index based on their numeric value, allowing only those that match the
/// specified filter to be returned.
///
/// This should only be wrapped around readers that return numeric records.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** When the inner reader `IR` is
///    layout-compatible with its suspended form (invariant 1 on
///    [`RawIndexReaderCore`](crate::RawIndexReaderCore)), so is
///    `FilterNumericReader<IR>`: it is `#[repr(C)]` and `IR` is its only
///    mode-dependent field. This is the layout compatibility the
///    [`SuspendableReader`]/[`ResumableReader`] contract requires. Enforced by
///    the `const _` proof below.
#[repr(C)]
pub struct FilterNumericReader<IR> {
    /// The numeric filter that is used to filter the records.
    filter: NumericFilter,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

// Compile-time proof of invariant 1 on `FilterNumericReader`, for a
// representative concrete numeric-encoded inner reader. The inner reader's own
// layout compatibility is invariant 1 on `RawIndexReaderCore`.
const _: () = {
    use crate::RawIndexReaderCore;
    use crate::codec::numeric::Numeric;
    use ref_mode::{Active, Suspended};
    use std::mem::{align_of, offset_of, size_of};
    type A = FilterNumericReader<RawIndexReaderCore<Active<'static>, Numeric>>;
    type S = FilterNumericReader<RawIndexReaderCore<Suspended, Numeric>>;
    assert!(offset_of!(A, filter) == offset_of!(S, filter));
    assert!(offset_of!(A, inner) == offset_of!(S, inner));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, IR: NumericReader<'index>> FilterNumericReader<IR> {
    /// Create a new filter numeric reader with the given filter and inner iterator.
    pub const fn new(filter: NumericFilter, inner: IR) -> Self {
        Self { filter, inner }
    }
}

/// `FilterNumericReader<IR>` suspends to `FilterNumericReader<IR::Suspended>`
/// — only the inner reader switches modes.
///
/// SAFETY: layout compatibility is invariant 1 on [`FilterNumericReader`] (const
/// proof there), given `IR`'s own layout compatibility.
unsafe impl<IR: SuspendableReader> SuspendableReader for FilterNumericReader<IR> {
    type Suspended = FilterNumericReader<IR::Suspended>;
}

/// Inverse of the above: `FilterNumericReader<RS>` resumes to
/// `FilterNumericReader<RS::Resumed<'a>>` for any `RS: ResumableReader`. The
/// `IndexReader<'a>` bound requires `RS::Resumed<'a>: NumericReader<'a>`, which
/// the resumed core reader provides.
///
/// SAFETY: layout compatibility is invariant 1 on [`FilterNumericReader`] (const
/// proof there).
unsafe impl<RS: ResumableReader> ResumableReader for FilterNumericReader<RS>
where
    for<'a> Self: 'static,
    for<'a> FilterNumericReader<RS::Resumed<'a>>: IndexReader<'a>,
{
    type Resumed<'a> = FilterNumericReader<RS::Resumed<'a>>;

    unsafe fn refresh_pointers(&mut self) -> RefreshOutcome {
        // SAFETY: our caller upholds `ResumableReader::refresh_pointers`'s
        // read-lock obligation, which we forward unchanged to the inner reader.
        unsafe { self.inner.refresh_pointers() }
    }
}

impl<'index, E> FilterNumericReader<IndexReaderCore<'index, E>> {
    /// Get the numeric filter used by this reader.
    pub const fn filter(&self) -> &NumericFilter {
        &self.filter
    }
}

impl<'index, IR: NumericReader<'index>> IndexReader<'index> for FilterNumericReader<IR> {
    /// Get the next record from the inner reader that matches the numeric filter.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true when this function is called.
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        loop {
            let success = self.inner.next_record(result)?;

            if !success {
                return Ok(false);
            }

            // SAFETY: the caller must ensure the result is numeric
            let value = unsafe { result.as_numeric_unchecked() };

            if self.filter.value_in_range(value) {
                return Ok(true);
            }
        }
    }

    /// Seek to the record with the given document ID in the inner reader that matches the numeric filter.
    ///
    /// # Safety
    ///
    /// 1. `result.is_numeric()` must be true when this function is called.
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

        // SAFETY: the caller must ensure the result is numeric
        let value = unsafe { result.as_numeric_unchecked() };

        if self.filter.value_in_range(value) {
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

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder>
    FilterNumericReader<IndexReaderCore<'index, E>>
{
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

/// A [`FilterNumericReader`] wrapping a [`NumericReader`] is also a [`NumericReader`].
impl<'index, IR: NumericReader<'index>> NumericReader<'index> for FilterNumericReader<IR> {}
