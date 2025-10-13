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

use std::ffi::c_void;

use bindings::{IteratorStatus, ValidateStatus};
use ffi::{RedisModule_Alloc, RedisModule_Free};
use inverted_index::RSIndexResult;

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

    /// Benchmark optional iterator read operations directly in C
    /// Returns the number of iterations performed and total time in nanoseconds
    fn benchmark_optional_read_direct_c(
        max_id: u64,
        iterations_out: *mut u64,
        time_ns_out: *mut u64,
    );

    /// Benchmark optional iterator skip_to operations directly in C
    /// Returns the number of iterations performed and total time in nanoseconds
    fn benchmark_optional_skip_to_direct_c(
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
    pub fn new_wildcard(max_id: u64, num_docs: usize) -> Self {
        Self(unsafe { bindings::NewWildcardIterator_NonOptimized(max_id, num_docs, 1f64) })
    }

    #[inline(always)]
    pub fn new_optional_full_child(max_id: u64, weight: f64) -> Self {
        let child =
            unsafe { bindings::NewWildcardIterator_NonOptimized(max_id, max_id as usize, 1f64) };
        let query_eval_ctx = new_redis_search_ctx(max_id);
        let it = unsafe { bindings::NewOptionalIterator(child, query_eval_ctx, weight) };
        free_redis_search_ctx(query_eval_ctx);
        Self(it)
    }

    #[inline(always)]
    pub fn new_optional_virtual_only(max_id: u64, weight: f64) -> Self {
        let child = std::ptr::null_mut();
        let query_eval_ctx = new_redis_search_ctx(max_id);
        let it = unsafe { bindings::NewOptionalIterator(child, query_eval_ctx, weight) };
        free_redis_search_ctx(query_eval_ctx);
        Self(it)
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

fn new_redis_search_ctx(max_id: u64) -> *mut bindings::QueryEvalCtx {
    let query_eval_ctx = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<bindings::QueryEvalCtx>())
            as *mut bindings::QueryEvalCtx
    };
    let doc_table = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<bindings::DocTable>())
            as *mut bindings::DocTable
    };
    let search_ctx = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<bindings::RedisSearchCtx>())
            as *mut bindings::RedisSearchCtx
    };
    let spec = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<bindings::IndexSpec>())
            as *mut bindings::IndexSpec
    };
    unsafe {
        (*doc_table).maxSize = max_id;
    }
    unsafe {
        (*doc_table).maxDocId = max_id;
    }
    unsafe {
        (*search_ctx).spec = spec;
    }
    unsafe {
        (*query_eval_ctx).docTable = doc_table;
    }
    unsafe {
        (*query_eval_ctx).sctx = search_ctx;
    }
    query_eval_ctx
}

fn free_redis_search_ctx(ctx: *mut bindings::QueryEvalCtx) {
    unsafe {
        RedisModule_Free.unwrap()((*(*ctx).sctx).spec as *mut c_void);
    };
    unsafe {
        RedisModule_Free.unwrap()((*ctx).sctx as *mut c_void);
    };
    unsafe {
        RedisModule_Free.unwrap()((*ctx).docTable as *mut c_void);
    };
    unsafe {
        RedisModule_Free.unwrap()(ctx as *mut c_void);
    };
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

    /// Run direct C benchmark for optional read operations
    pub fn benchmark_optional_read_direct(max_id: u64) -> DirectBenchmarkResult {
        let mut iterations = 0u64;
        let mut time_ns = 0u64;
        unsafe {
            benchmark_optional_read_direct_c(max_id, &mut iterations, &mut time_ns);
        }
        DirectBenchmarkResult {
            iterations,
            time_ns,
        }
    }

    /// Run direct C benchmark for optional skip_to operations
    pub fn benchmark_optional_skip_to_direct(max_id: u64, step: u64) -> DirectBenchmarkResult {
        let mut iterations = 0u64;
        let mut time_ns = 0u64;
        unsafe {
            benchmark_optional_skip_to_direct_c(max_id, step, &mut iterations, &mut time_ns);
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
