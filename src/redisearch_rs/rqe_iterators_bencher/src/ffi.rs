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
    use inverted_index::{FieldMaskOrIndex, t_docId};

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
use ffi::RedisModule_Alloc;
use inverted_index::RSIndexResult;

use crate::ffi::bindings::Metric_VECTOR_DISTANCE;

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
    pub fn new_id_list(vec: Vec<u64>) -> Self {
        // Convert the Rust vector to use C allocation because the C iterator takes ownership of the array
        let len = vec.len();
        let data =
            unsafe { RedisModule_Alloc.unwrap()(len * std::mem::size_of::<u64>()) as *mut u64 };
        unsafe {
            std::ptr::copy_nonoverlapping(vec.as_ptr(), data, len);
        }
        Self(unsafe { bindings::NewIdListIterator(data, len as u64, 1f64) })
    }
    #[inline(always)]
    pub fn new_metric(vec: Vec<u64>, metric_data: Vec<f64>) -> Self {
        let len = vec.len();
        let data =
            unsafe { RedisModule_Alloc.unwrap()(len * std::mem::size_of::<u64>()) as *mut u64 };
        let m_data =
            unsafe { RedisModule_Alloc.unwrap()(len * std::mem::size_of::<f64>()) as *mut f64 };
        unsafe {
            std::ptr::copy_nonoverlapping(vec.as_ptr(), data, len);
            std::ptr::copy_nonoverlapping(metric_data.as_ptr(), m_data, len);
        }
        Self(unsafe { bindings::NewMetricIterator(data, m_data, len, Metric_VECTOR_DISTANCE) })
    }
    #[inline(always)]
    pub fn new_wildcard(max_id: u64, num_docs: usize) -> Self {
        Self(unsafe { bindings::NewWildcardIterator_NonOptimized(max_id, num_docs, 1f64) })
    }

    #[inline(always)]
    pub unsafe fn new_numeric_full(ii: *mut bindings::InvertedIndex) -> Self {
        Self(unsafe { bindings::NewInvIndIterator_NumericFull(ii) })
    }

    #[inline(always)]
    pub unsafe fn new_term_full(ii: *mut bindings::InvertedIndex) -> Self {
        Self(unsafe { bindings::NewInvIndIterator_TermFull(ii) })
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
    pub fn current(&self) -> *mut RSIndexResult<'static> {
        unsafe { (*self.0).current }
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
    pub fn iterator_numeric_full(&self) -> QueryIterator {
        unsafe { QueryIterator::new_numeric_full(self.0) }
    }

    #[inline(always)]
    pub fn iterator_term_full(&self) -> QueryIterator {
        unsafe { QueryIterator::new_term_full(self.0) }
    }
}

impl Drop for InvertedIndex {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { bindings::InvertedIndex_Free(self.0) };
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
    fn numeric_full_iterator() {
        let ii = InvertedIndex::new(IndexFlags_Index_StoreNumeric);
        ii.write_numeric_entry(1, 1.0);
        ii.write_numeric_entry(10, 10.0);
        ii.write_numeric_entry(100, 100.0);

        let it = unsafe { QueryIterator::new_numeric_full(ii.0) };
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
    fn term_full_iterator() {
        let ii = InvertedIndex::new(
            IndexFlags_Index_StoreFreqs
                | IndexFlags_Index_StoreTermOffsets
                | IndexFlags_Index_StoreFieldFlags
                | IndexFlags_Index_StoreByteOffsets,
        );

        let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        const TEST_STR: &str = "term";
        let test_str_ptr = TEST_STR.as_ptr() as *mut _;
        let term = Box::new(ffi::RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        });
        let term = Box::into_raw(term);

        ii.write_term_entry(1, 1, 1, term, &offsets);
        ii.write_term_entry(10, 1, 1, term, &offsets);
        ii.write_term_entry(100, 1, 1, term, &offsets);

        let it = unsafe { QueryIterator::new_term_full(ii.0) };
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

        let _ = unsafe { Box::from_raw(term) };
    }
}
