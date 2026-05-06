/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub use ffi::{
    IndexFlags, IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreByteOffsets,
    IndexFlags_Index_StoreFieldFlags, IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric,
    IndexFlags_Index_StoreTermOffsets, IteratorStatus, IteratorStatus_ITERATOR_OK,
    RedisModule_Alloc, RedisModule_Free, ValidateStatus, t_docId,
};
use inverted_index::{RSIndexResult, RSQueryTerm};
use iterators_ffi::intersection::NewIntersectionIterator;
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

    /// Give up ownership of the raw pointer without calling `Free`.
    ///
    /// Used when passing the iterator to a C function that takes ownership
    /// (e.g. `NewIntersectionIterator`), so that the C side is responsible for freeing it.
    #[inline(always)]
    pub const fn into_raw(self) -> *mut ffi::QueryIterator {
        self.0
    }

    /// Create a C intersection from two pre-built term `QueryIterator`s.
    ///
    /// Takes ownership of both children — they will be freed when the intersection is freed.
    #[inline(always)]
    pub fn new_intersection_from_term_its(
        child1: Self,
        child2: Self,
        max_slop: i32,
        in_order: bool,
    ) -> Self {
        let children_ptr = unsafe {
            RedisModule_Alloc.unwrap()(2 * std::mem::size_of::<*mut ffi::QueryIterator>())
                as *mut *mut ffi::QueryIterator
        };
        unsafe {
            *children_ptr.add(0) = child1.into_raw();
            *children_ptr.add(1) = child2.into_raw();
        }
        Self(unsafe { NewIntersectionIterator(children_ptr, 2, max_slop, in_order, 1.0) })
    }

    #[inline(always)]
    pub unsafe fn new_term(ii: *mut ffi::InvertedIndex, sctx: *const ffi::RedisSearchCtx) -> Self {
        let term = Box::into_raw(RSQueryTerm::new("term", 1, 0));
        Self(unsafe {
            iterators_ffi::inverted_index::NewInvIndIterator_TermQuery(
                ii.cast_const(),
                sctx,
                field::FieldMaskOrIndex::Mask(ffi::RS_FIELDMASK_ALL),
                term,
                1.0,
            )
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
            NewIntersectionIterator(
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
    pub fn revalidate(&self, spec: *mut ffi::IndexSpec) -> ValidateStatus {
        unsafe { (*self.0).Revalidate.unwrap()(self.0, spec) }
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

/// Create a minimal zeroed `RedisSearchCtx` with a valid `IndexSpec`.
///
/// The caller must call [`free_search_ctx`] to free the memory.
fn new_search_ctx() -> *mut ffi::RedisSearchCtx {
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
        (*search_ctx).spec = spec;
    }
    search_ctx
}

fn free_search_ctx(sctx: *mut ffi::RedisSearchCtx) {
    unsafe {
        RedisModule_Free.unwrap()((*sctx).spec as *mut c_void);
        RedisModule_Free.unwrap()(sctx as *mut c_void);
    }
}

/// Simple wrapper around the C InvertedIndex.
/// All methods are inlined to avoid the overhead when benchmarking.
pub struct InvertedIndex {
    pub ii: *mut ffi::InvertedIndex,
    sctx: *mut ffi::RedisSearchCtx,
}

impl Drop for InvertedIndex {
    fn drop(&mut self) {
        unsafe { inverted_index_ffi::InvertedIndex_Free(self.ii.cast()) };
        free_search_ctx(self.sctx);
    }
}

impl InvertedIndex {
    #[inline(always)]
    pub fn new(flags: ffi::IndexFlags) -> Self {
        let mut memsize = 0;
        let ptr = inverted_index_ffi::NewInvertedIndex_Ex(flags, false, false, &mut memsize);
        Self {
            ii: ptr.cast(),
            sctx: new_search_ctx(),
        }
    }

    #[inline(always)]
    pub fn write_numeric_entry(&self, doc_id: u64, value: f64) {
        unsafe {
            inverted_index_ffi::InvertedIndex_WriteNumericEntry(self.ii.cast(), doc_id, value);
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
        let offsets = inverted_index::RSOffsetSlice::from_slice(offsets);
        let record = RSIndexResult::build_term()
            .borrowed_record(term, offsets)
            .doc_id(doc_id)
            .field_mask(field_mask as u128)
            .frequency(freq)
            .build();
        unsafe {
            inverted_index_ffi::InvertedIndex_WriteEntryGeneric(
                self.ii.cast(),
                &record as *const _ as *mut _,
            );
        }
    }

    #[inline(always)]
    pub fn iterator_term(&self) -> QueryIterator {
        unsafe { QueryIterator::new_term(self.ii, self.sctx) }
    }
}
