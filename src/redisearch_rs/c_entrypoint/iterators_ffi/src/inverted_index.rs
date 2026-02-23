/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use field::{FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{
    FilterGeoReader, FilterNumericReader, IndexReader, IndexReaderCore, NumericFilter,
    NumericReader, RSIndexResult, t_docId,
};
use rqe_iterators::{FieldExpirationChecker, inverted_index::Numeric};
use rqe_iterators_interop::RQEIteratorWrapper;

/// Wrapper around different numeric reader types to avoid generics in FFI code.
enum NumericIndexReader<'index> {
    Uncompressed(IndexReaderCore<'index, inverted_index::numeric::Numeric>),
    Compressed(IndexReaderCore<'index, inverted_index::numeric::NumericFloatCompression>),
}

impl<'index> IndexReader<'index> for NumericIndexReader<'index> {
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.next_record(result),
            NumericIndexReader::Compressed(reader) => reader.next_record(result),
        }
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.seek_record(doc_id, result),
            NumericIndexReader::Compressed(reader) => reader.seek_record(doc_id, result),
        }
    }

    #[inline(always)]
    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.skip_to(doc_id),
            NumericIndexReader::Compressed(reader) => reader.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn reset(&mut self) {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.reset(),
            NumericIndexReader::Compressed(reader) => reader.reset(),
        }
    }

    #[inline(always)]
    fn unique_docs(&self) -> u64 {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.unique_docs(),
            NumericIndexReader::Compressed(reader) => reader.unique_docs(),
        }
    }

    #[inline(always)]
    fn has_duplicates(&self) -> bool {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.has_duplicates(),
            NumericIndexReader::Compressed(reader) => reader.has_duplicates(),
        }
    }

    #[inline(always)]
    fn flags(&self) -> ffi::IndexFlags {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.flags(),
            NumericIndexReader::Compressed(reader) => reader.flags(),
        }
    }

    #[inline(always)]
    fn needs_revalidation(&self) -> bool {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.needs_revalidation(),
            NumericIndexReader::Compressed(reader) => reader.needs_revalidation(),
        }
    }

    #[inline(always)]
    fn refresh_buffer_pointers(&mut self) {
        match self {
            NumericIndexReader::Uncompressed(reader) => reader.refresh_buffer_pointers(),
            NumericIndexReader::Compressed(reader) => reader.refresh_buffer_pointers(),
        }
    }
}

impl<'index> NumericReader<'index> for NumericIndexReader<'index> {}

/// Enum holding either a numeric or geo iterator variant.
/// This allows all iterator types to share the same iterator wrapper structure.
enum IteratorVariant<'index> {
    /// Numeric iterator without a filter (uses the reader directly).
    Numeric(Numeric<'index, NumericIndexReader<'index>, FieldExpirationChecker>),
    /// Numeric iterator with a user filter applied.
    NumericFiltered(
        Numeric<
            'index,
            FilterNumericReader<'index, NumericIndexReader<'index>>,
            FieldExpirationChecker,
        >,
    ),
    /// Geo iterator (always has a filter).
    Geo(
        Numeric<
            'index,
            FilterGeoReader<'index, NumericIndexReader<'index>>,
            FieldExpirationChecker,
        >,
    ),
}

/// Wrapper around the actual Numeric iterator.
/// Needed as we need to keep the `filter` pointer around so it can be returned in
/// [`NumericInvIndIterator_GetNumericFilter`].
struct NumericIterator<'index> {
    /// The user numeric filter, or None if no filter was provided.
    filter: Option<NonNull<NumericFilter>>,
    /// The iterator variant (numeric or geo).
    iterator: IteratorVariant<'index>,
}

impl<'index> NumericIterator<'index> {
    /// Get the flags from the underlying reader.
    fn flags(&self) -> ffi::IndexFlags {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.reader().flags(),
            IteratorVariant::NumericFiltered(iter) => iter.reader().flags(),
            IteratorVariant::Geo(iter) => iter.reader().flags(),
        }
    }

    /// Get the range minimum value for profiling.
    const fn range_min(&self) -> f64 {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.range_min(),
            IteratorVariant::NumericFiltered(iter) => iter.range_min(),
            IteratorVariant::Geo(iter) => iter.range_min(),
        }
    }

    /// Get the range maximum value for profiling.
    const fn range_max(&self) -> f64 {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.range_max(),
            IteratorVariant::NumericFiltered(iter) => iter.range_max(),
            IteratorVariant::Geo(iter) => iter.range_max(),
        }
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for NumericIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.current(),
            IteratorVariant::NumericFiltered(iter) => iter.current(),
            IteratorVariant::Geo(iter) => iter.current(),
        }
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.read(),
            IteratorVariant::NumericFiltered(iter) => iter.read(),
            IteratorVariant::Geo(iter) => iter.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: u64,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.skip_to(doc_id),
            IteratorVariant::NumericFiltered(iter) => iter.skip_to(doc_id),
            IteratorVariant::Geo(iter) => iter.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.revalidate(),
            IteratorVariant::NumericFiltered(iter) => iter.revalidate(),
            IteratorVariant::Geo(iter) => iter.revalidate(),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.rewind(),
            IteratorVariant::NumericFiltered(iter) => iter.rewind(),
            IteratorVariant::Geo(iter) => iter.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.num_estimated(),
            IteratorVariant::NumericFiltered(iter) => iter.num_estimated(),
            IteratorVariant::Geo(iter) => iter.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> u64 {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.last_doc_id(),
            IteratorVariant::NumericFiltered(iter) => iter.last_doc_id(),
            IteratorVariant::Geo(iter) => iter.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.at_eof(),
            IteratorVariant::NumericFiltered(iter) => iter.at_eof(),
            IteratorVariant::Geo(iter) => iter.at_eof(),
        }
    }
}

#[unsafe(no_mangle)]
/// Creates a new numeric inverted index iterator for querying numeric fields.
///
/// # Parameters
///
/// * `idx` - Pointer to the inverted index to query.
/// * `sctx` - Pointer to the Redis search context for expiration checking.
/// * `field_ctx` - Pointer to the field filter context (field index and expiration predicate).
/// * `flt` - Optional pointer to a numeric filter for value filtering (can be NULL).
/// * `rt` - Optional pointer to the numeric range tree for revalidation (can be NULL).
/// * `range_min` - Minimum value of the numeric range.
/// * `range_max` - Maximum value of the numeric range.
///
/// # Returns
///
/// A pointer to a `QueryIterator` that can be used from C code.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `idx` must be a valid pointer to a numeric `InvertedIndex` and cannot be NULL.
/// 2. `idx` must remain valid for the lifetime of the returned iterator.
/// 3. `sctx` must be a valid pointer to a `RedisSearchCtx` and cannot be NULL.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
/// 5. `field_ctx` must be a valid pointer to a `FieldFilterContext` and cannot be NULL.
/// 6. `field_ctx.field` must be a field index (tag == FieldMaskOrIndex_Index), not a field mask.
///    Numeric queries require a specific field index.
/// 7. If `flt` is not NULL, it must be a valid pointer to a `NumericFilter` and must
///    remain valid for the lifetime of the returned iterator.
/// 8. If `rt` is not NULL, it must be a valid pointer to a `NumericRangeTree` and must
///    remain valid for the lifetime of the returned iterator.
/// 9. `range_min` is smaller or equal to `range_max`.
pub unsafe extern "C" fn NewInvIndIterator_NumericQuery(
    idx: *const ffi::InvertedIndex,
    sctx: *const ffi::RedisSearchCtx,
    field_ctx: *const FieldFilterContext,
    flt: *const NumericFilter,
    rt: *const ffi::NumericRangeTree,
    range_min: f64,
    range_max: f64,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!field_ctx.is_null(), "field_ctx must not be null");

    let idx: *const inverted_index_ffi::InvertedIndex = idx.cast();
    // SAFETY: 1. guarantees idx is valid and non-null
    let ii_ref = unsafe { &*idx };

    // Get field index and predicate from field context
    // SAFETY: 5. guarantees field_ctx is valid and non-null
    let field_ctx = unsafe { &*field_ctx };

    let field_index = match field_ctx.field {
        field::FieldMaskOrIndex::Index(index) => index,
        field::FieldMaskOrIndex::Mask(_) => {
            // SAFETY: Guaranteed by safety requirement 6.
            panic!("Numeric queries require a field index, not a field mask");
        }
    };

    // SAFETY: 3.
    let sctx = unsafe { NonNull::new_unchecked(sctx as *mut _) };
    let filter = NonNull::new(flt as *mut NumericFilter);
    let range_tree = NonNull::new(rt as *mut _);

    let reader = match ii_ref {
        inverted_index_ffi::InvertedIndex::Numeric(entries_tracking_index) => {
            NumericIndexReader::Uncompressed(entries_tracking_index.reader())
        }
        inverted_index_ffi::InvertedIndex::NumericFloatCompression(entries_tracking_index) => {
            NumericIndexReader::Compressed(entries_tracking_index.reader())
        }
        // SAFETY: 1. guarantees that the inverted index is numeric.
        _ => panic!("Unsupported inverted index type"),
    };

    // Create the expiration checker
    // Note: The caller guarantees sctx is valid and non-null (see safety contract in new())
    let expiration_checker = FieldExpirationChecker::new(
        sctx,
        FieldFilterContext {
            field: FieldMaskOrIndex::Index(field_index),
            predicate: field_ctx.predicate,
        },
        reader.flags(),
    );

    let iterator = match filter {
        Some(filter) => {
            // SAFETY: 7.
            let filter_ref = unsafe { filter.as_ref() };
            if !filter_ref.geo_filter.is_null() {
                // Geo filter
                let filter_reader = FilterGeoReader::new(filter_ref, reader);
                // SAFETY: 8. guarantees `range_tree` validity for the iterator's lifetime.
                let iter = unsafe {
                    Numeric::new(
                        filter_reader,
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                NumericIterator {
                    filter: Some(filter),
                    iterator: IteratorVariant::Geo(iter),
                }
            } else {
                // Numeric filter (no geo)
                let filter_reader = FilterNumericReader::new(filter_ref, reader);
                // SAFETY: 8. guarantees `range_tree` validity for the iterator's lifetime.
                let iter = unsafe {
                    Numeric::new(
                        filter_reader,
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                NumericIterator {
                    filter: Some(filter),
                    iterator: IteratorVariant::NumericFiltered(iter),
                }
            }
        }
        None => {
            // No filter - use the reader directly
            // SAFETY: 8. guarantees `range_tree` validity for the iterator's lifetime.
            let iter = unsafe {
                Numeric::new(
                    reader,
                    expiration_checker,
                    range_tree,
                    Some(range_min),
                    Some(range_max),
                )
            };
            NumericIterator {
                filter: None,
                iterator: IteratorVariant::Numeric(iter),
            }
        }
    };

    RQEIteratorWrapper::boxed_new(ffi::IteratorType_INV_IDX_NUMERIC_ITERATOR, iterator)
}

/// Gets the flags of the underlying IndexReader from a numeric inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to a `QueryIterator`.
/// 2. If `it` iterator type is IteratorType_INV_IDX_NUMERIC_ITERATOR, it has been created using `NewInvIndIterator_NumericQuery`.
/// 3. If `it` has a different iterator type, its `reader` field must be a valid non-NULL pointer to an `IndexReader`.
///
/// # Returns
///
/// The flags of the `IndexReader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvIndIterator_GetReaderFlags(
    it: *const ffi::InvIndIterator,
) -> ffi::IndexFlags {
    debug_assert!(!it.is_null());

    // SAFETY: 1.
    let it_ref = unsafe { &*it };

    match it_ref.base.type_ {
        ffi::IteratorType_INV_IDX_NUMERIC_ITERATOR => {
            // SAFETY: the numeric iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        _ => {
            // C iterator
            let reader: *mut inverted_index_ffi::IndexReader = it_ref.reader.cast();
            // SAFETY: 3.
            let reader_ref = unsafe { &*reader };
            reader_ref.flags()
        }
    }
}

/// Gets the numeric filter from a numeric inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `NumericInvIndIterator` created by `NewInvIndIterator_NumericQuery`.
///
/// # Returns
///
/// A pointer to the numeric filter, or NULL if no filter was provided when creating the iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetNumericFilter(
    it: *const ffi::NumericInvIndIterator,
) -> *const ffi::NumericFilter {
    debug_assert!(!it.is_null());

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };

    // Return a pointer to the pinned filter, or NULL if no filter was provided
    // SAFETY: The filter is pinned and has a stable address for the lifetime of the iterator
    // Both types have the same #[repr(C)] layout so we can cast the pointer
    wrapper
        .inner
        .filter
        .map(|f| f.as_ptr() as *const ffi::NumericFilter)
        .unwrap_or(std::ptr::null())
}

/// Gets the minimum range value for profiling a numeric iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `QueryIterator` created by `NewInvIndIterator_NumericQuery`.
///
/// # Returns
///
/// The minimum range value from the filter, or negative infinity if no filter was provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetProfileRangeMin(
    it: *const ffi::NumericInvIndIterator,
) -> f64 {
    debug_assert!(!it.is_null());

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };
    wrapper.inner.range_min()
}

/// Gets the maximum range value for profiling a numeric iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `QueryIterator` created by `NewInvIndIterator_NumericQuery`.
///
/// # Returns
///
/// The maximum range value from the filter, or positive infinity if no filter was provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetProfileRangeMax(
    it: *const ffi::NumericInvIndIterator,
) -> f64 {
    debug_assert!(!it.is_null());

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };
    wrapper.inner.range_max()
}

/// Swap the inverted index of an inverted index iterator. This is only used by C tests
/// to trigger revalidation on the iterator's underlying reader.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to an `InvIndIterator`.
/// 2. If `it` is a C iterator, its `reader` field must be a valid non-NULL
///    pointer to an `IndexReader`.
/// 3. `ii` must be a valid non-NULL pointer to an `InvertedIndex` whose type matches the
///    iterator's underlying index type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvIndIterator_Rs_SwapIndex(
    it: *mut ffi::InvIndIterator,
    ii: *const ffi::InvertedIndex,
) {
    debug_assert!(!it.is_null());
    debug_assert!(!ii.is_null());

    // SAFETY: 1.
    let it_ref = unsafe { &*it };

    match it_ref.base.type_ {
        ffi::IteratorType_INV_IDX_NUMERIC_ITERATOR => {
            unimplemented!(
                "Numeric iterators use revision ID for revalidation, not index swapping"
            );
        }
        _ => {
            // C iterator
            let reader: *mut inverted_index_ffi::IndexReader = it_ref.reader.cast();
            // SAFETY: 2. guarantees reader is valid.
            let reader_ref = unsafe { &mut *reader };
            let ii: *const inverted_index_ffi::InvertedIndex = ii.cast();
            // SAFETY: 3. guarantees ii is valid and matching.
            let ii_ref = unsafe { &*ii };
            reader_ref.swap_index(ii_ref);
        }
    }
}
