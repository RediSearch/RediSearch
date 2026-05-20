/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::{
    IndexReader, IndexReaderCore, NumericFilter, NumericReader, RefreshOutcome, ResumableReader,
    SuspendableReader,
};
use crate::{DecodedBy, Decoder, InvertedIndex};
use ffi::{GeoFilter, IndexFlags};
use index_result::RSIndexResult;
use rqe_core::DocId;

// Manually define some C functions, because we'll create a circular dependency if we use the FFI
// crate to make them automatically.
unsafe extern "C" {
    /// Checks if a value (distance) is within the given geo filter.
    ///
    /// # Safety
    /// The [`GeoFilter`] should not be null and a valid instance
    unsafe fn isWithinRadius(gf: *const GeoFilter, d: f64, distance: *mut f64) -> bool;
}

/// A reader that filters out records that do not match a given geo filter. It is used to
/// filter records in an index based on their geo location, allowing only those that match the
/// specified geo filter to be returned.
///
/// This should only be wrapped around readers that return numeric records.
///
/// `#[repr(C)]` so that, once `IR` is layout-compatible across `Active`/`Suspended`
/// instantiations of its inner [`RawIndexReaderCore`](crate::RawIndexReaderCore),
/// the whole `FilterGeoReader` is too.
#[repr(C)]
pub struct FilterGeoReader<IR> {
    /// Numeric filter with a geo filter set to which a record needs to match to be valid.
    /// `filter.geo_filter` is rebound at construction to point at our owned
    /// [`geo_filter`](Self::geo_filter), so consumers of [`filter`](Self::filter)
    /// never observe the caller's (potentially short-lived) `GeoFilter`.
    filter: NumericFilter,

    /// Owned heap-allocated copy of the geo filter. Boxed so its address is
    /// stable across moves of `Self`, which lets `filter.geo_filter` safely
    /// alias it for the reader's lifetime.
    geo_filter: Box<GeoFilter>,

    /// The inner reader that will be used to read the records from the index.
    inner: IR,
}

impl<'index, IR: NumericReader<'index>> FilterGeoReader<IR> {
    /// Create a new filter geo reader with the given numeric filter and inner iterator
    ///
    /// # Safety
    /// The caller should ensure the `geo_filter` pointer in the numeric filter is set and a valid
    /// pointer to a `GeoFilter` struct at the time of construction; the
    /// reader copies the geo filter onto the heap, so the original need only
    /// outlive this call.
    pub fn new(mut filter: NumericFilter, inner: IR) -> Self {
        debug_assert!(
            !filter.geo_filter.is_null(),
            "FilterGeoReader needs the geo filter to be set on the numeric filter"
        );

        // SAFETY: we just asserted the filter is set and the caller is to ensure it is a valid
        // `GeoFilter` instance. We copy the geo filter onto the heap so we own it for the
        // reader's lifetime.
        let geo_filter = Box::new(unsafe { *(filter.geo_filter as *const GeoFilter) });

        // Rebind `filter.geo_filter` to our owned heap copy. The box's address is stable across
        // moves of `Self`, so this pointer stays valid for as long as the reader is alive.
        filter.geo_filter = std::ptr::from_ref::<GeoFilter>(&geo_filter).cast();

        Self {
            filter,
            geo_filter,
            inner,
        }
    }
}

/// `FilterGeoReader<IR>` suspends to `FilterGeoReader<IR::Suspended>` —
/// only the inner reader switches modes.
impl<IR: SuspendableReader> SuspendableReader for FilterGeoReader<IR> {
    type Suspended = FilterGeoReader<IR::Suspended>;
}

/// Inverse of the above: `FilterGeoReader<RS>` resumes to
/// `FilterGeoReader<RS::Resumed<'a>>` for any `RS: ResumableReader`.
impl<RS: ResumableReader> ResumableReader for FilterGeoReader<RS>
where
    for<'a> FilterGeoReader<RS::Resumed<'a>>: IndexReader<'a>,
{
    type Resumed<'a> = FilterGeoReader<RS::Resumed<'a>>;

    fn refresh_pointers(&mut self) -> RefreshOutcome {
        self.inner.refresh_pointers()
    }
}

impl<'index, E> FilterGeoReader<IndexReaderCore<'index, E>> {
    /// Get the numeric filter used by this reader.
    pub const fn filter(&self) -> &NumericFilter {
        &self.filter
    }
}

impl<'index, IR: NumericReader<'index>> IndexReader<'index> for FilterGeoReader<IR> {
    /// Get the next record from the inner reader that matches the geo filter.
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
            let value = unsafe { result.as_numeric_unchecked_mut() };

            // SAFETY: we know the filter is not a null pointer since we hold a reference to it
            let in_radius = unsafe { isWithinRadius(&*self.geo_filter, *value, value) };

            if in_radius {
                return Ok(true);
            }
        }
    }

    /// Seek to the record with the given document ID in the inner reader that matches the geo filter.
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
        let value = unsafe { result.as_numeric_unchecked_mut() };

        // SAFETY: we know the filter is not a null pointer since we hold a reference to it
        let in_radius = unsafe { isWithinRadius(&*self.geo_filter, *value, value) };

        if in_radius {
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

impl<'index, E: DecodedBy<Decoder = D>, D: Decoder> FilterGeoReader<IndexReaderCore<'index, E>> {
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

/// A [`FilterGeoReader`] wrapping a [`NumericReader`] is also a [`NumericReader`].
impl<'index, IR: NumericReader<'index>> NumericReader<'index> for FilterGeoReader<IR> {}
