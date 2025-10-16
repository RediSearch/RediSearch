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

    use ffi::{NumericFilter, t_fieldMask};
    use inverted_index::t_docId;

    // Type aliases for C bindings - types without lifetimes for C interop
    pub type RSIndexResult = inverted_index::RSIndexResult<'static>;
    pub type RSOffsetVector = inverted_index::RSOffsetVector<'static>;

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

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

#[cfg(test)]
// `miri` can't handle FFI.
#[cfg(not(miri))]
mod tests {
    use super::*;
    use bindings::{IteratorStatus_ITERATOR_EOF, ValidateStatus_VALIDATE_OK};

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
}
