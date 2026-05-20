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
use index_result::{RSIndexResult, RSQueryTerm};
use inverted_index::{
    FilterMaskReader, IndexReader, PointsToOpaqueIndex, RawIndexReaderCore, RefreshOutcome,
    ResumableReader, SuspendableReader, TermReader, doc_ids_only::DocIdsOnly, fields_offsets,
    fields_only, freqs_fields, freqs_offsets, freqs_only, full, offsets_only,
    raw_doc_ids_only::RawDocIdsOnly,
};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::{FieldExpirationChecker, inverted_index::Term};

/// Wrapper around different term reader types to avoid generics in FFI code,
/// parameterised over the [`Ref`] mode `Rf`. See [`TermIndexReader`] for the
/// [`Active`] instantiation (the live reader implementing [`IndexReader`]);
/// `RawTermIndexReader<Suspended>` is its passive carrier across a lock
/// release/reacquire cycle (implementing [`ResumableReader`]).
///
/// Handles all term-compatible encoding types. Types with field mask tracking use
/// [`FilterMaskReader`] to filter records by field mask, while types without field
/// mask data use the bare [`RawIndexReaderCore`].
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawTermIndexReader<Active<'index>>`
///    and `RawTermIndexReader<Suspended>` are layout-identical, so the owning
///    `RawInvIndIterator` can suspend/resume by a same-allocation reinterpretation
///    — the [`SuspendableReader`]/[`ResumableReader`] contract. This holds because
///    it is a single `#[repr(C)]` generic whose `Rf` flows only through the
///    per-variant [`RawIndexReaderCore`]/[`FilterMaskReader`] payloads, each itself
///    layout-compatible (invariant 1 on those types). Enforced by the `const _`
///    proof below.
#[repr(C)]
pub(super) enum RawTermIndexReader<Rf: Ref> {
    // FieldMaskTrackingIndex types (with FilterMaskReader)
    Full(FilterMaskReader<RawIndexReaderCore<Rf, full::Full>>),
    FullWide(FilterMaskReader<RawIndexReaderCore<Rf, full::FullWide>>),
    FreqsFields(FilterMaskReader<RawIndexReaderCore<Rf, freqs_fields::FreqsFields>>),
    FreqsFieldsWide(FilterMaskReader<RawIndexReaderCore<Rf, freqs_fields::FreqsFieldsWide>>),
    FieldsOnly(FilterMaskReader<RawIndexReaderCore<Rf, fields_only::FieldsOnly>>),
    FieldsOnlyWide(FilterMaskReader<RawIndexReaderCore<Rf, fields_only::FieldsOnlyWide>>),
    FieldsOffsets(FilterMaskReader<RawIndexReaderCore<Rf, fields_offsets::FieldsOffsets>>),
    FieldsOffsetsWide(FilterMaskReader<RawIndexReaderCore<Rf, fields_offsets::FieldsOffsetsWide>>),
    // InvertedIndexInner types (without FilterMaskReader)
    FreqsOnly(RawIndexReaderCore<Rf, freqs_only::FreqsOnly>),
    OffsetsOnly(RawIndexReaderCore<Rf, offsets_only::OffsetsOnly>),
    FreqsOffsets(RawIndexReaderCore<Rf, freqs_offsets::FreqsOffsets>),
    DocIdsOnly(RawIndexReaderCore<Rf, DocIdsOnly>),
    RawDocIdsOnly(RawIndexReaderCore<Rf, RawDocIdsOnly>),
}

/// Active-form alias of [`RawTermIndexReader`] — the live term reader.
pub(super) type TermIndexReader<'index> = RawTermIndexReader<Active<'index>>;

// Compile-time proof of invariant 1 on `RawTermIndexReader`: its `Active` and
// `Suspended` instantiations are layout-identical. As an enum we assert size and
// alignment equality; the per-variant payloads are layout-compatible by their own
// invariant 1, so a divergence here would be a build error.
const _: () = {
    use std::mem::{align_of, size_of};
    type A = RawTermIndexReader<Active<'static>>;
    type S = RawTermIndexReader<Suspended>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Dispatch a method call to the reader held by the current variant. Works for
/// any `Rf` because every variant carries the same name across modes.
macro_rules! term_ir_dispatch {
    ($self:expr, $method:ident $(, $args:expr)*) => {
        match $self {
            RawTermIndexReader::Full(r) => r.$method($($args),*),
            RawTermIndexReader::FullWide(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsFields(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsFieldsWide(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOnlyWide(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOffsets(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOffsetsWide(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::OffsetsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsOffsets(r) => r.$method($($args),*),
            RawTermIndexReader::DocIdsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::RawDocIdsOnly(r) => r.$method($($args),*),
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
        doc_id: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        term_ir_dispatch!(self, seek_record, doc_id, result)
    }

    #[inline(always)]
    fn skip_to(&mut self, doc_id: DocId) -> bool {
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

/// Resolve an opaque index and compare it against the current variant's reader.
/// Implemented for every `Rf` so it is callable from the suspended-side
/// `should_abort` check as well as the active reader.
impl<Rf: Ref> PointsToOpaqueIndex for RawTermIndexReader<Rf> {
    fn points_to_the_same_opaque_index(
        &self,
        opaque: &inverted_index::opaque::InvertedIndex,
    ) -> bool {
        term_ir_dispatch!(self, points_to_the_same_opaque_index, opaque)
    }
}

impl<'index> TermReader<'index> for TermIndexReader<'index> {}

/// `TermIndexReader<'index>` suspends to `RawTermIndexReader<Suspended>` — the
/// same generic enum with the ref mode weakened.
///
/// SAFETY: layout compatibility is invariant 1 on [`RawTermIndexReader`] (const
/// proof there).
unsafe impl<'index> SuspendableReader for TermIndexReader<'index> {
    type Suspended = RawTermIndexReader<Suspended>;
}

/// Inverse of the above: `RawTermIndexReader<Suspended>` resumes to
/// `TermIndexReader<'a>` at any index lifetime `'a`. `refresh_pointers` forwards
/// to the suspended reader held by the current variant.
///
/// SAFETY: layout compatibility is invariant 1 on [`RawTermIndexReader`] (const
/// proof there).
unsafe impl ResumableReader for RawTermIndexReader<Suspended> {
    type Resumed<'a> = TermIndexReader<'a>;

    unsafe fn refresh_pointers(&mut self) -> RefreshOutcome {
        // SAFETY: our caller upholds `ResumableReader::refresh_pointers`'s
        // no-concurrent-aliasing obligation, which we forward unchanged to the
        // per-variant suspended reader.
        unsafe { term_ir_dispatch!(self, refresh_pointers) }
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewInvIndIterator_TermQuery(
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
        FieldMaskOrIndex::Index(_) => RS_FIELDMASK_ALL,
    };

    // Create the appropriate reader based on the encoding type
    let reader = match ii_ref {
        inverted_index_ffi::InvertedIndex::Full(ii) => RawTermIndexReader::Full(ii.reader(mask)),
        inverted_index_ffi::InvertedIndex::FullWide(ii) => {
            RawTermIndexReader::FullWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FreqsFields(ii) => {
            RawTermIndexReader::FreqsFields(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FreqsFieldsWide(ii) => {
            RawTermIndexReader::FreqsFieldsWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOnly(ii) => {
            RawTermIndexReader::FieldsOnly(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOnlyWide(ii) => {
            RawTermIndexReader::FieldsOnlyWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOffsets(ii) => {
            RawTermIndexReader::FieldsOffsets(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FieldsOffsetsWide(ii) => {
            RawTermIndexReader::FieldsOffsetsWide(ii.reader(mask))
        }
        inverted_index_ffi::InvertedIndex::FreqsOnly(ii) => RawTermIndexReader::FreqsOnly(ii.reader()),
        inverted_index_ffi::InvertedIndex::OffsetsOnly(ii) => {
            RawTermIndexReader::OffsetsOnly(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::FreqsOffsets(ii) => {
            RawTermIndexReader::FreqsOffsets(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::DocIdsOnly(ii) => {
            RawTermIndexReader::DocIdsOnly(ii.reader())
        }
        inverted_index_ffi::InvertedIndex::RawDocIdsOnly(ii) => {
            RawTermIndexReader::RawDocIdsOnly(ii.reader())
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

    // SAFETY: The caller guarantees `sctx` points to a valid `RedisSearchCtx`
    // with a valid `spec`, both remaining valid for the iterator's lifetime.
    let expiration_checker = unsafe {
        FieldExpirationChecker::new(
            sctx,
            FieldFilterContext {
                field: field_mask_or_index,
                predicate: FieldExpirationPredicate::Default,
            },
            reader.flags(),
        )
    };

    // SAFETY: All preconditions for `Term::new` are upheld by this function's
    // own safety contract (valid reader, valid sctx, valid term).
    let iterator = unsafe { Term::new(reader, sctx, term, weight, expiration_checker) };

    RQEIteratorWrapper::boxed_new(iterator)
}
