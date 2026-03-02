/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{
    FilterMaskReader, IndexReader, IndexReaderCore, RSIndexResult, RSQueryTerm, TermReader,
    doc_ids_only::DocIdsOnly, fields_offsets, fields_only, freqs_fields, freqs_offsets, freqs_only,
    full, offsets_only, raw_doc_ids_only::RawDocIdsOnly, t_docId,
};
use rqe_iterators::{FieldExpirationChecker, inverted_index::Term};
use rqe_iterators_interop::RQEIteratorWrapper;

/// Wrapper around different term reader types to avoid generics in FFI code.
///
/// Handles all term-compatible encoding types. Types with field mask tracking use
/// [`FilterMaskReader`] to filter records by field mask, while types without field
/// mask data use the bare [`IndexReaderCore`].
enum TermIndexReader<'index> {
    // FieldMaskTrackingIndex types (with FilterMaskReader)
    Full(FilterMaskReader<IndexReaderCore<'index, full::Full>>),
    FullWide(FilterMaskReader<IndexReaderCore<'index, full::FullWide>>),
    FreqsFields(FilterMaskReader<IndexReaderCore<'index, freqs_fields::FreqsFields>>),
    FreqsFieldsWide(FilterMaskReader<IndexReaderCore<'index, freqs_fields::FreqsFieldsWide>>),
    FieldsOnly(FilterMaskReader<IndexReaderCore<'index, fields_only::FieldsOnly>>),
    FieldsOnlyWide(FilterMaskReader<IndexReaderCore<'index, fields_only::FieldsOnlyWide>>),
    FieldsOffsets(FilterMaskReader<IndexReaderCore<'index, fields_offsets::FieldsOffsets>>),
    FieldsOffsetsWide(FilterMaskReader<IndexReaderCore<'index, fields_offsets::FieldsOffsetsWide>>),
    // InvertedIndexInner types (without FilterMaskReader)
    FreqsOnly(IndexReaderCore<'index, freqs_only::FreqsOnly>),
    OffsetsOnly(IndexReaderCore<'index, offsets_only::OffsetsOnly>),
    FreqsOffsets(IndexReaderCore<'index, freqs_offsets::FreqsOffsets>),
    DocIdsOnly(IndexReaderCore<'index, DocIdsOnly>),
    RawDocIdsOnly(IndexReaderCore<'index, RawDocIdsOnly>),
}

macro_rules! term_ir_dispatch {
    ($self:expr, $method:ident $(, $args:expr)*) => {
        match $self {
            TermIndexReader::Full(r) => r.$method($($args),*),
            TermIndexReader::FullWide(r) => r.$method($($args),*),
            TermIndexReader::FreqsFields(r) => r.$method($($args),*),
            TermIndexReader::FreqsFieldsWide(r) => r.$method($($args),*),
            TermIndexReader::FieldsOnly(r) => r.$method($($args),*),
            TermIndexReader::FieldsOnlyWide(r) => r.$method($($args),*),
            TermIndexReader::FieldsOffsets(r) => r.$method($($args),*),
            TermIndexReader::FieldsOffsetsWide(r) => r.$method($($args),*),
            TermIndexReader::FreqsOnly(r) => r.$method($($args),*),
            TermIndexReader::OffsetsOnly(r) => r.$method($($args),*),
            TermIndexReader::FreqsOffsets(r) => r.$method($($args),*),
            TermIndexReader::DocIdsOnly(r) => r.$method($($args),*),
            TermIndexReader::RawDocIdsOnly(r) => r.$method($($args),*),
        }
    };
}

impl<'index> IndexReader<'index> for TermIndexReader<'index> {
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        term_ir_dispatch!(self, next_record, result)
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        term_ir_dispatch!(self, seek_record, doc_id, result)
    }

    #[inline(always)]
    fn skip_to(&mut self, doc_id: t_docId) -> bool {
        term_ir_dispatch!(self, skip_to, doc_id)
    }

    #[inline(always)]
    fn reset(&mut self) {
        term_ir_dispatch!(self, reset)
    }

    #[inline(always)]
    fn unique_docs(&self) -> u64 {
        term_ir_dispatch!(self, unique_docs)
    }

    #[inline(always)]
    fn has_duplicates(&self) -> bool {
        term_ir_dispatch!(self, has_duplicates)
    }

    #[inline(always)]
    fn flags(&self) -> ffi::IndexFlags {
        term_ir_dispatch!(self, flags)
    }

    #[inline(always)]
    fn needs_revalidation(&self) -> bool {
        term_ir_dispatch!(self, needs_revalidation)
    }

    #[inline(always)]
    fn refresh_buffer_pointers(&mut self) {
        term_ir_dispatch!(self, refresh_buffer_pointers)
    }
}

impl<'index> TermReader<'index> for TermIndexReader<'index> {
    fn points_to_the_same_opaque_index(
        &self,
        opaque: &inverted_index::opaque::InvertedIndex,
    ) -> bool {
        term_ir_dispatch!(self, points_to_the_same_opaque_index, opaque)
    }
}

/// Creates a new term inverted index iterator for querying term fields.
///
/// # Parameters
///
/// * `idx` - Pointer to the inverted index to query.
/// * `sctx` - Pointer to the Redis search context.
/// * `field_mask_or_index` - Field mask or field index to filter on.
/// * `term` - Pointer to the query term. Ownership is transferred to the iterator.
/// * `weight` - Weight to apply to the term results.
///
/// # Returns
///
/// A pointer to a `QueryIterator` that can be used from C code.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `idx` must be a valid pointer to a term `InvertedIndex` and cannot be NULL.
/// 2. `idx` must remain valid between `revalidate()` calls, since the revalidation
///    mechanism detects when the index has been replaced via `Redis_OpenInvertedIndex()`
///    pointer comparison.
/// 3. `sctx` must be a valid pointer to a `RedisSearchCtx` and cannot be NULL.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
/// 5. `term` must be a valid pointer to a heap-allocated `RSQueryTerm` (e.g. created by
///    `NewQueryTerm`) and cannot be NULL. Ownership is transferred to the iterator.
#[allow(improper_ctypes_definitions)] // `field_mask_or_index` contains `t_fieldMask` (u128)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewInvIndIterator_TermQuery_Rs(
    idx: *const ffi::InvertedIndex,
    sctx: *const ffi::RedisSearchCtx,
    field_mask_or_index: FieldMaskOrIndex,
    term: *mut RSQueryTerm,
    weight: f64,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!term.is_null(), "term must not be null");

    let idx_ffi: *const inverted_index_ffi::InvertedIndex = idx.cast();
    // SAFETY: 1. guarantees idx is valid and non-null
    let ii_ref = unsafe { &*idx_ffi };

    // Determine the field mask for reader filtering.
    // If a mask is given, filter by that mask. Otherwise, use ALL (index fields are filtered
    // differently via the expiration checker).
    let mask = match field_mask_or_index {
        FieldMaskOrIndex::Mask(m) => m,
        FieldMaskOrIndex::Index(_) => ffi::RS_FIELDMASK_ALL,
    };

    // Create the appropriate reader based on the encoding type
    let reader = match ii_ref {
        inverted_index_ffi::InvertedIndex::Full(ii) => TermIndexReader::Full(ii.reader(mask)),
        inverted_index_ffi::InvertedIndex::FullWide(ii) => {
            TermIndexReader::FullWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FreqsFields(ii) => {
            TermIndexReader::FreqsFields(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FreqsFieldsWide(ii) => {
            TermIndexReader::FreqsFieldsWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOnly(ii) => {
            TermIndexReader::FieldsOnly(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOnlyWide(ii) => {
            TermIndexReader::FieldsOnlyWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOffsets(ii) => {
            TermIndexReader::FieldsOffsets(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOffsetsWide(ii) => {
            TermIndexReader::FieldsOffsetsWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FreqsOnly(ii) => TermIndexReader::FreqsOnly(ii.reader()),
        inverted_index_ffi::InvertedIndex::OffsetsOnly(ii) => {
            TermIndexReader::OffsetsOnly(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::FreqsOffsets(ii) => {
            TermIndexReader::FreqsOffsets(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::DocIdsOnly(ii) => {
            TermIndexReader::DocIdsOnly(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::RawDocIdsOnly(ii) => {
            TermIndexReader::RawDocIdsOnly(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::Numeric(_)
        | inverted_index_ffi::InvertedIndex::NumericFloatCompression(_) => panic!(
            "Unsupported inverted index type for term query (numeric indices are not supported)",
        ),
    };

    // SAFETY: 3.
    let sctx = unsafe { NonNull::new_unchecked(sctx as *mut _) };

    // SAFETY: 5. guarantees term is a heap-allocated RSQueryTerm
    let term = unsafe { Box::from_raw(term) };

    // Create the expiration checker
    let expiration_checker = FieldExpirationChecker::new(
        sctx,
        FieldFilterContext {
            field: field_mask_or_index,
            predicate: FieldExpirationPredicate::Default,
        },
        reader.flags(),
    );

    // SAFETY: All preconditions for `Term::new` are upheld by this function's
    // own safety contract (valid reader, valid sctx, valid term).
    let iterator = unsafe { Term::new(reader, sctx, term, weight, expiration_checker) };

    RQEIteratorWrapper::boxed_new(ffi::IteratorType_INV_IDX_TERM_ITERATOR, iterator)
}
