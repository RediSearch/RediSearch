/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod bindings {
    #![allow(non_snake_case)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(unsafe_op_in_unsafe_fn)]
    #![allow(improper_ctypes)]
    #![allow(dead_code)]
    #![allow(clippy::ptr_offset_with_cast)]
    #![allow(clippy::useless_transmute)]
    #![allow(clippy::missing_const_for_fn)]

    use ffi::{NumericFilter, t_fieldIndex, t_fieldMask};
    use field::{FieldFilterContext, FieldMaskOrIndex};
    use inverted_index::t_docId;

    // Type aliases for C bindings - types without lifetimes for C interop
    pub type RSIndexResult = inverted_index::RSIndexResult<'static>;
    pub type RSOffsetVector = inverted_index::RSOffsetVector<'static>;
    pub type IndexDecoderCtx = inverted_index::ReadFilter<'static>;

    // Type alias to Rust defined inverted index types
    pub use inverted_index_ffi::{
        InvertedIndex_Free, InvertedIndex_WriteEntryGeneric, InvertedIndex_WriteNumericEntry,
        NewInvertedIndex_Ex,
    };
    pub type InvertedIndex = inverted_index_ffi::InvertedIndex;
    pub type IndexReader = inverted_index_ffi::IndexReader<'static>;

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreByteOffsets,
    IndexFlags_Index_StoreFieldFlags, IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric,
    IndexFlags_Index_StoreTermOffsets,
};
use bindings::{IteratorStatus, ValidateStatus};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{NumericFilter, RSIndexResult};
use std::ptr;

// Direct C benchmark functions that eliminate FFI overhead
// by implementing the benchmark loop entirely in C
unsafe extern "C" {
    /// Benchmark wildcard iterator read operations directly in C
    /// Returns the number of iterations performed and total time in nanoseconds
    fn benchmark_wildcard_read_direct_c(
        max_id: u64,
        iterations_out: *mut u64,
        time_ns_out: *mut u64,
    );

    /// Benchmark wildcard iterator skip_to operations directly in C
    /// Returns the number of iterations performed and total time in nanoseconds
    fn benchmark_wildcard_skip_to_direct_c(
        max_id: u64,
        step: u64,
        iterations_out: *mut u64,
        time_ns_out: *mut u64,
    );
}

/// Simple wrapper around the C `QueryIterator` type.
/// All methods are inlined to avoid the overhead when benchmarking.
pub struct QueryIterator(*mut bindings::QueryIterator);

impl QueryIterator {
    #[inline(always)]
    pub fn new_empty() -> Self {
        Self(unsafe { bindings::NewEmptyIterator() })
    }

    #[inline(always)]
    pub unsafe fn new_numeric(
        ii: *mut bindings::InvertedIndex,
        filter: Option<&NumericFilter>,
    ) -> Self {
        let field_ctx = FieldFilterContext {
            field: FieldMaskOrIndex::index_invalid(),
            predicate: FieldExpirationPredicate::Default,
        };
        let flt = filter
            .map(|filter| filter as *const NumericFilter as *const _)
            .unwrap_or_default();

        Self(unsafe {
            bindings::NewInvIndIterator_NumericQuery(
                ii,
                ptr::null(),
                &field_ctx as _,
                flt,
                ptr::null(),
                0.0,
                std::f64::MAX,
            )
        })
    }

    #[inline(always)]
    pub unsafe fn new_term(ii: *mut bindings::InvertedIndex) -> Self {
        Self(unsafe {
            bindings::NewInvIndIterator_TermQuery(
                ii,
                ptr::null(),
                FieldMaskOrIndex::mask_all(),
                ptr::null_mut(),
                1.0,
            )
        })
    }

    #[inline(always)]
    pub fn num_estimated(&self) -> usize {
        unsafe { (*self.0).NumEstimated.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn at_eof(&self) -> bool {
        unsafe { (*self.0).atEOF }
    }

    #[inline(always)]
    pub fn last_doc_id(&self) -> u64 {
        unsafe { (*self.0).lastDocId }
    }

    #[inline(always)]
    pub fn read(&self) -> IteratorStatus {
        unsafe { (*self.0).Read.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn skip_to(&self, doc_id: u64) -> IteratorStatus {
        unsafe { (*self.0).SkipTo.unwrap()(self.0, doc_id) }
    }

    #[inline(always)]
    pub fn rewind(&self) {
        unsafe { (*self.0).Rewind.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn revalidate(&self) -> ValidateStatus {
        unsafe { (*self.0).Revalidate.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn free(&self) {
        unsafe { (*self.0).Free.unwrap()(self.0) }
    }

    #[inline(always)]
    pub fn current(&self) -> Option<&RSIndexResult<'static>> {
        let current = unsafe { (*self.0).current };
        unsafe { current.as_ref() }
    }
}

/// Direct C benchmark results
#[derive(Debug, Clone)]
pub struct DirectBenchmarkResult {
    pub iterations: u64,
    pub time_ns: u64,
}

impl QueryIterator {
    /// Run direct C benchmark for wildcard read operations
    pub fn benchmark_wildcard_read_direct(max_id: u64) -> DirectBenchmarkResult {
        let mut iterations = 0u64;
        let mut time_ns = 0u64;
        unsafe {
            benchmark_wildcard_read_direct_c(max_id, &mut iterations, &mut time_ns);
        }
        DirectBenchmarkResult {
            iterations,
            time_ns,
        }
    }

    /// Run direct C benchmark for wildcard skip_to operations
    pub fn benchmark_wildcard_skip_to_direct(max_id: u64, step: u64) -> DirectBenchmarkResult {
        let mut iterations = 0u64;
        let mut time_ns = 0u64;
        unsafe {
            benchmark_wildcard_skip_to_direct_c(max_id, step, &mut iterations, &mut time_ns);
        }
        DirectBenchmarkResult {
            iterations,
            time_ns,
        }
    }
}

/// Simple wrapper around the C InvertedIndex.
/// All methods are inlined to avoid the overhead when benchmarking.
pub struct InvertedIndex(pub *mut bindings::InvertedIndex);

impl InvertedIndex {
    #[inline(always)]
    pub fn new(flags: bindings::IndexFlags) -> Self {
        let mut memsize = 0;
        let ptr = bindings::NewInvertedIndex_Ex(flags, false, false, &mut memsize);
        Self(ptr)
    }

    #[inline(always)]
    pub fn write_numeric_entry(&self, doc_id: u64, value: f64) {
        unsafe {
            bindings::InvertedIndex_WriteNumericEntry(self.0, doc_id, value);
        }
    }

    /// `term_ptr` and `offsets` must be valid for the lifetime of the index.
    #[inline(always)]
    pub fn write_term_entry(
        &self,
        doc_id: u64,
        freq: u32,
        field_mask: u32,
        term_ptr: *mut ::ffi::RSQueryTerm,
        offsets: &[u8],
    ) {
        let record = RSIndexResult::term_with_term_ptr(
            term_ptr,
            inverted_index::RSOffsetVector::with_data(offsets.as_ptr() as _, offsets.len() as _),
            doc_id,
            field_mask as u128,
            freq,
        );
        unsafe {
            bindings::InvertedIndex_WriteEntryGeneric(self.0, &record as *const _ as *mut _);
        }
    }

    #[inline(always)]
    pub fn iterator_numeric(&self, filter: Option<&NumericFilter>) -> QueryIterator {
        unsafe { QueryIterator::new_numeric(self.0, filter) }
    }

    #[inline(always)]
    pub fn iterator_term(&self) -> QueryIterator {
        unsafe { QueryIterator::new_term(self.0) }
    }
}

impl Drop for InvertedIndex {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { bindings::InvertedIndex_Free(self.0) };
    }
}

/// A builder for creating `QueryTerm` instances.
///
/// Use [`QueryTermBuilder::allocate`] to create a new instance
/// on the heap.
#[allow(unused)]
pub(crate) struct QueryTermBuilder<'a> {
    pub(crate) token: &'a str,
    pub(crate) idf: f64,
    pub(crate) id: i32,
    pub(crate) flags: u32,
    pub(crate) bm25_idf: f64,
}

impl<'a> QueryTermBuilder<'a> {
    /// Creates a new instance of `RSQueryTerm` on the heap.
    /// It returns a raw pointer to the allocated `RSQueryTerm`.
    ///
    /// The caller is responsible for freeing the allocated memory
    /// using [`Term_Free`](ffi::Term_Free).
    #[allow(unused)]
    pub(crate) fn allocate(self) -> *mut ffi::RSQueryTerm {
        let Self {
            token,
            idf,
            id,
            flags,
            bm25_idf,
        } = self;
        let token = ffi::RSToken {
            str_: token.as_ptr() as *mut _,
            len: token.len(),
            _bitfield_align_1: Default::default(),
            _bitfield_1: Default::default(),
            __bindgen_padding_0: Default::default(),
        };
        let token_ptr = Box::into_raw(Box::new(token));
        let query_term = unsafe { ffi::NewQueryTerm(token_ptr as *mut _, id) };

        // Now that NewQueryTerm copied tok->str into ret->str,
        // the temporary token struct is no longer needed.
        unsafe {
            drop(Box::from_raw(token_ptr));
        }

        // Patch the fields we can't set via the constructor
        unsafe { (*query_term).idf = idf };
        unsafe { (*query_term).bm25_idf = bm25_idf };
        unsafe { (*query_term).flags = flags };

        query_term
    }
}

#[cfg(test)]
// `miri` can't handle FFI.
#[cfg(not(miri))]
mod tests {
    use super::*;
    use bindings::{
        IndexFlags_Index_StoreNumeric, IteratorStatus_ITERATOR_EOF,
        IteratorStatus_ITERATOR_NOTFOUND, IteratorStatus_ITERATOR_OK, ValidateStatus_VALIDATE_OK,
    };

    #[test]
    fn empty_iterator() {
        let it = QueryIterator::new_empty();
        assert_eq!(it.num_estimated(), 0);
        assert!(it.at_eof());

        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);
        assert_eq!(it.skip_to(1), IteratorStatus_ITERATOR_EOF);

        it.rewind();
        assert!(it.at_eof());
        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);

        assert_eq!(it.revalidate(), ValidateStatus_VALIDATE_OK);

        it.free();
    }

    #[test]
    fn numeric_iterator_full() {
        let ii = InvertedIndex::new(IndexFlags_Index_StoreNumeric);
        ii.write_numeric_entry(1, 1.0);
        ii.write_numeric_entry(10, 10.0);
        ii.write_numeric_entry(100, 100.0);

        let it = unsafe { QueryIterator::new_numeric(ii.0, None) };
        assert_eq!(it.num_estimated(), 3);

        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);
        assert!(it.at_eof());

        it.rewind();
        assert_eq!(it.skip_to(10), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.skip_to(20), IteratorStatus_ITERATOR_NOTFOUND);

        it.rewind();
        assert_eq!(it.revalidate(), ValidateStatus_VALIDATE_OK);

        it.free();
    }

    #[test]
    fn numeric_iterator_filter() {
        let ii = InvertedIndex::new(IndexFlags_Index_StoreNumeric);
        for i in 1..=10 {
            ii.write_numeric_entry(i, i as f64);
        }

        let filter = NumericFilter {
            min: 2.0,
            max: 5.0,
            min_inclusive: true,
            max_inclusive: false,
            ..Default::default()
        };

        let it = unsafe { QueryIterator::new_numeric(ii.0, Some(&filter)) };

        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.current().unwrap().doc_id, 2);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.current().unwrap().doc_id, 3);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.current().unwrap().doc_id, 4);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);

        it.rewind();
        assert_eq!(it.skip_to(2), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.skip_to(5), IteratorStatus_ITERATOR_EOF);

        it.rewind();
        assert_eq!(it.revalidate(), ValidateStatus_VALIDATE_OK);

        it.free();
    }

    #[test]
    fn term_full_iterator() {
        let ii = InvertedIndex::new(
            IndexFlags_Index_StoreFreqs
                | IndexFlags_Index_StoreTermOffsets
                | IndexFlags_Index_StoreFieldFlags
                | IndexFlags_Index_StoreByteOffsets,
        );

        let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        const TEST_STR: &str = "term";
        let term = || {
            QueryTermBuilder {
                token: TEST_STR,
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            }
            .allocate()
        };

        ii.write_term_entry(1, 1, 1, term(), &offsets);
        ii.write_term_entry(10, 1, 1, term(), &offsets);
        ii.write_term_entry(100, 1, 1, term(), &offsets);

        let it = unsafe { QueryIterator::new_term(ii.0) };
        assert_eq!(it.num_estimated(), 3);

        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.read(), IteratorStatus_ITERATOR_EOF);
        assert!(it.at_eof());

        it.rewind();
        assert_eq!(it.skip_to(10), IteratorStatus_ITERATOR_OK);
        assert_eq!(it.skip_to(20), IteratorStatus_ITERATOR_NOTFOUND);

        it.rewind();
        assert_eq!(it.revalidate(), ValidateStatus_VALIDATE_OK);

        it.free();
    }
}
