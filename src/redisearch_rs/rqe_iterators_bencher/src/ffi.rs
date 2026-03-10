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
use std::{ffi::c_void, ptr};

/// Simple wrapper around the C `QueryIterator` type.
/// All methods are inlined to avoid the overhead when benchmarking.
pub struct QueryIterator(*mut ffi::QueryIterator);

impl QueryIterator {
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

    /// Creates a new missing-field inverted index iterator via the C path.
    ///
    /// # Safety
    ///
    /// `idx` must be a valid pointer to a DocIdsOnly inverted index.
    /// `sctx` must be a valid pointer to a `RedisSearchCtx` with valid `spec`
    /// and `missingFieldDict`.
    /// `field_index` must be a valid index into `sctx.spec.fields`.
    #[inline(always)]
    pub unsafe fn new_missing(
        idx: *const ffi::InvertedIndex,
        sctx: *const ffi::RedisSearchCtx,
        field_index: ffi::t_fieldIndex,
    ) -> Self {
        Self(unsafe { ffi::NewInvIndIterator_MissingQuery(idx, sctx, field_index) })
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
