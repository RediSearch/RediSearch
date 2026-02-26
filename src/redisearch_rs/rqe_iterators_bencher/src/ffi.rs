/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub use ffi::{
    IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreByteOffsets,
    IndexFlags_Index_StoreFieldFlags, IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric,
    IndexFlags_Index_StoreTermOffsets, IteratorStatus, IteratorStatus_ITERATOR_OK,
    RedisModule_Alloc, RedisModule_Free, ValidateStatus,
};
use inverted_index::{RSIndexResult, t_docId};
use query_term::RSQueryTerm;
use std::{ffi::c_void, ptr};

// Direct C benchmark functions that eliminate FFI overhead
// by implementing the benchmark loop entirely in C
unsafe extern "C" {
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
pub struct QueryIterator(*mut ffi::QueryIterator);

impl QueryIterator {
    #[inline(always)]
    pub fn new_optional_full_child_wildcard(max_id: u64, weight: f64) -> Self {
        let child = iterators_ffi::wildcard::NewWildcardIterator_NonOptimized(max_id, 1f64);
        Self::new_optional_with_child(max_id, weight, child)
    }

    #[inline(always)]
    pub fn new_optional_id_list(max_id: u64, weight: f64, ids: Vec<u64>) -> Self {
        let Self(child) = Self::new_id_list(ids);
        Self::new_optional_with_child(max_id, weight, child)
    }

    #[inline(always)]
    fn new_optional_with_child(max_id: u64, weight: f64, child: *mut ffi::QueryIterator) -> Self {
        let query_eval_ctx = new_redis_search_ctx(max_id);
        let it = unsafe { ffi::NewOptionalIterator(child, query_eval_ctx, weight) };
        free_redis_search_ctx(query_eval_ctx);
        Self(it)
    }

    #[inline(always)]
    pub fn new_optional_virtual_only(max_id: u64, weight: f64) -> Self {
        let child = std::ptr::null_mut();
        let query_eval_ctx = new_redis_search_ctx(max_id);
        let it = unsafe { ffi::NewOptionalIterator(child, query_eval_ctx, weight) };
        free_redis_search_ctx(query_eval_ctx);
        Self(it)
    }

    /// Create an empty iterator (returns no results).
    #[inline(always)]
    pub fn new_empty() -> Self {
        Self(iterators_ffi::empty::NewEmptyIterator())
    }

    /// Create an ID list iterator from a vector of sorted document IDs.
    #[inline(always)]
    pub fn new_id_list(ids: Vec<u64>) -> Self {
        let num = ids.len() as u64;
        let ids_ptr = if num > 0 {
            let ptr = unsafe {
                RedisModule_Alloc.unwrap()(num as usize * std::mem::size_of::<u64>()) as *mut u64
            };
            unsafe {
                std::ptr::copy_nonoverlapping(ids.as_ptr(), ptr, num as usize);
            }
            ptr
        } else {
            ptr::null_mut()
        };

        Self(unsafe { iterators_ffi::id_list::NewSortedIdListIterator(ids_ptr, num, 1.0) })
    }

    /// Create a non-optimized NOT iterator with the given child and max_doc_id.
    /// This creates a minimal QueryEvalCtx to ensure the C code creates a non-optimized version.
    #[inline(always)]
    pub fn new_not_non_optimized(child: Self, max_doc_id: u64, weight: f64) -> Self {
        // Create a minimal QueryEvalCtx that will NOT trigger optimization
        // The C code checks: optimized = q && q->sctx && q->sctx->spec && q->sctx->spec->rule && q->sctx->spec->rule->index_all
        // By zeroing everything, we ensure spec->rule is NULL, so optimized = false
        let query_eval_ctx = new_redis_search_ctx(max_doc_id);
        let timeout = ffi::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        let it =
            unsafe { ffi::NewNotIterator(child.0, max_doc_id, weight, timeout, query_eval_ctx) };

        free_redis_search_ctx(query_eval_ctx);
        Self(it)
    }

    #[inline(always)]
    pub unsafe fn new_term(ii: *mut ffi::InvertedIndex) -> Self {
        let term: *mut ffi::RSQueryTerm = Box::into_raw(RSQueryTerm::new(b"term", 1, 0)).cast();
        Self(unsafe {
            let field_mask_ffi = ffi::FieldMaskOrIndex {
                __bindgen_anon_2: ffi::FieldMaskOrIndex__bindgen_ty_2 {
                    mask_tag: 1, // FieldMaskOrIndex_Mask = 1
                    __bindgen_padding_0: 0,
                    mask: ffi::RS_FIELDMASK_ALL,
                },
            };

            ffi::NewInvIndIterator_TermQuery(ii, ptr::null(), field_mask_ffi, term, 1.0)
        })
    }

    /// Creates a new intersection iterator from child ID list iterators.
    ///
    /// # Arguments
    /// * `children_ids` - A slice of vectors, each containing sorted document IDs for a child iterator
    /// * `weight` - The weight for the intersection result
    #[inline(always)]
    pub fn new_intersection(children_ids: &[Vec<t_docId>], weight: f64) -> Self {
        let num_children = children_ids.len();

        // Allocate array of child iterator pointers using RedisModule_Alloc
        let children_ptr = unsafe {
            RedisModule_Alloc.unwrap()(
                num_children * std::mem::size_of::<*mut ffi::QueryIterator>(),
            ) as *mut *mut ffi::QueryIterator
        };

        for (i, ids) in children_ids.iter().enumerate() {
            // Allocate and copy IDs using RedisModule_Alloc (required by NewSortedIdListIterator)
            let ids_ptr = unsafe {
                RedisModule_Alloc.unwrap()(ids.len() * std::mem::size_of::<t_docId>())
                    as *mut t_docId
            };
            unsafe {
                std::ptr::copy_nonoverlapping(ids.as_ptr(), ids_ptr, ids.len());
            }

            // Create child iterator
            let child = unsafe {
                iterators_ffi::id_list::NewSortedIdListIterator(ids_ptr, ids.len() as u64, 1.0)
            };
            unsafe {
                *children_ptr.add(i) = child;
            }
        }

        // Create intersection iterator (takes ownership of children array)
        Self(unsafe {
            ffi::NewIntersectionIterator(
                children_ptr,
                num_children,
                -1,    // max_slop: -1 means no slop validation
                false, // in_order
                weight,
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
        unsafe { current.cast::<RSIndexResult>().as_ref() }
    }
}

fn new_redis_search_ctx(max_id: u64) -> *mut ffi::QueryEvalCtx {
    let query_eval_ctx = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<ffi::QueryEvalCtx>())
            as *mut ffi::QueryEvalCtx
    };
    unsafe {
        (*query_eval_ctx) = std::mem::zeroed();
    }
    let doc_table = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<ffi::DocTable>()) as *mut ffi::DocTable
    };
    unsafe {
        (*doc_table) = std::mem::zeroed();
    }
    let search_ctx = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<ffi::RedisSearchCtx>())
            as *mut ffi::RedisSearchCtx
    };
    unsafe {
        (*search_ctx) = std::mem::zeroed();
    }
    let spec = unsafe {
        RedisModule_Alloc.unwrap()(std::mem::size_of::<ffi::IndexSpec>()) as *mut ffi::IndexSpec
    };
    unsafe {
        (*spec) = std::mem::zeroed();
    }
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

fn free_redis_search_ctx(ctx: *mut ffi::QueryEvalCtx) {
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

/// Simple wrapper around the C InvertedIndex.
/// All methods are inlined to avoid the overhead when benchmarking.
pub struct InvertedIndex(pub *mut ffi::InvertedIndex);

impl InvertedIndex {
    #[inline(always)]
    pub fn new(flags: ffi::IndexFlags) -> Self {
        let mut memsize = 0;
        let ptr = inverted_index_ffi::NewInvertedIndex_Ex(flags, false, false, &mut memsize);
        Self(ptr.cast())
    }

    #[inline(always)]
    pub fn write_numeric_entry(&self, doc_id: u64, value: f64) {
        unsafe {
            inverted_index_ffi::InvertedIndex_WriteNumericEntry(self.0.cast(), doc_id, value);
        }
    }

    /// `term_ptr` and `offsets` must be valid for the lifetime of the index.
    #[inline(always)]
    pub fn write_term_entry(
        &self,
        doc_id: u64,
        freq: u32,
        field_mask: u32,
        term: Option<Box<RSQueryTerm>>,
        offsets: &[u8],
    ) {
        let record = RSIndexResult::with_term(
            term,
            inverted_index::RSOffsetSlice::from_slice(offsets),
            doc_id,
            field_mask as u128,
            freq,
        );
        unsafe {
            inverted_index_ffi::InvertedIndex_WriteEntryGeneric(
                self.0.cast(),
                &record as *const _ as *mut _,
            );
        }
    }

    #[inline(always)]
    pub fn iterator_term(&self) -> QueryIterator {
        unsafe { QueryIterator::new_term(self.0) }
    }
}

impl Drop for InvertedIndex {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { inverted_index_ffi::InvertedIndex_Free(self.0.cast()) };
    }
}

#[cfg(test)]
// `miri` can't handle FFI.
#[cfg(not(miri))]
mod tests {
    use super::*;
    use ffi::{
        IteratorStatus_ITERATOR_EOF, IteratorStatus_ITERATOR_NOTFOUND, IteratorStatus_ITERATOR_OK,
        ValidateStatus_VALIDATE_OK,
    };

    #[test]
    fn term_full_iterator() {
        let ii = InvertedIndex::new(
            IndexFlags_Index_StoreFreqs
                | IndexFlags_Index_StoreTermOffsets
                | IndexFlags_Index_StoreFieldFlags
                | IndexFlags_Index_StoreByteOffsets,
        );

        let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let term = || {
            let mut t = RSQueryTerm::new(b"term", 1, 0);
            t.set_idf(5.0);
            t.set_bm25_idf(10.0);
            Some(t)
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
