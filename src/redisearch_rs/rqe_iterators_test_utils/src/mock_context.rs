/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{IndexSpec, QueryEvalCtx, RedisSearchCtx, SchemaRule, t_docId};
use numeric_range_tree::NumericRangeTree;

/// Mock search context creating fake objects for testing.
/// It can be used to test expiration but not validation.
/// Use [`TestContext`](crate::TestContext) instead to test revalidation.
///
/// Uses raw pointers for storage to avoid Stacked Borrows violations.
/// Box would claim Unique ownership which gets invalidated when the library
/// code creates references through the pointer chain (e.g., `sctx.spec.as_ref()`).
/// Raw pointers don't participate in borrow tracking, so they're compatible
/// with the library's reference creation.
pub struct MockContext {
    rule: *mut SchemaRule,
    spec: *mut IndexSpec,
    sctx: *mut RedisSearchCtx,
    qctx: *mut QueryEvalCtx,
    numeric_range_tree: *mut NumericRangeTree,
    tag_index: *mut ffi::TagIndex,
}

impl Drop for MockContext {
    fn drop(&mut self) {
        // Deallocate all the structs using the global allocator directly.
        // We can't use Box::from_raw because that would create a Box with a Unique
        // tag, but the memory's borrow stack may have been modified by SharedReadOnly
        // tags from the library code's reference creation.
        unsafe {
            std::alloc::dealloc(
                self.rule as *mut u8,
                std::alloc::Layout::new::<SchemaRule>(),
            );
            std::alloc::dealloc(self.spec as *mut u8, std::alloc::Layout::new::<IndexSpec>());
            std::alloc::dealloc(
                self.sctx as *mut u8,
                std::alloc::Layout::new::<RedisSearchCtx>(),
            );
            std::alloc::dealloc(
                self.qctx as *mut u8,
                std::alloc::Layout::new::<QueryEvalCtx>(),
            );
            let _ = Box::from_raw(self.numeric_range_tree);
            std::alloc::dealloc(
                self.tag_index as *mut u8,
                std::alloc::Layout::new::<ffi::TagIndex>(),
            );
        }
    }
}

impl MockContext {
    pub fn new(max_doc_id: t_docId, num_docs: usize) -> Self {
        // Allocate each struct using Box::into_raw to get raw pointers.
        // We store raw pointers (not Boxes) because the library code creates
        // references through the pointer chain which would invalidate Box's
        // Unique ownership tag under Stacked Borrows.

        // Create boxes and immediately convert to raw pointers
        let rule_ptr = Box::into_raw(Box::new(unsafe { std::mem::zeroed::<SchemaRule>() }));
        let spec_ptr = Box::into_raw(Box::new(unsafe { std::mem::zeroed::<IndexSpec>() }));
        let sctx_ptr = Box::into_raw(Box::new(unsafe { std::mem::zeroed::<RedisSearchCtx>() }));
        let qctx_ptr = Box::into_raw(Box::new(unsafe { std::mem::zeroed::<QueryEvalCtx>() }));
        let numeric_range_tree_ptr = Box::into_raw(Box::new(NumericRangeTree::new(false)));
        // SAFETY: TagIndex is a C struct where all-zeros is a valid representation.
        let tag_index_ptr: *mut ffi::TagIndex = unsafe {
            let ptr = std::alloc::alloc_zeroed(std::alloc::Layout::new::<ffi::TagIndex>());
            assert!(!ptr.is_null(), "allocation failed");
            ptr.cast()
        };

        // Initialize all structs through raw pointers
        unsafe {
            // Initialize SchemaRule
            (*rule_ptr).index_all = false;

            // Initialize IndexSpec
            (*spec_ptr).rule = rule_ptr;
            (*spec_ptr).monitorDocumentExpiration = true; // Only depends on API availability, so always true
            (*spec_ptr).monitorFieldExpiration = true; // Only depends on API availability, so always true
            (*spec_ptr).docs.maxDocId = max_doc_id;
            (*spec_ptr).docs.size = if num_docs > 0 {
                num_docs
            } else {
                max_doc_id as usize
            };
            (*spec_ptr).stats.scoring.numDocuments = (*spec_ptr).docs.size;

            // Initialize RedisSearchCtx
            (*sctx_ptr).spec = spec_ptr;

            // Initialize QueryEvalCtx
            (*qctx_ptr).sctx = sctx_ptr;
            (*qctx_ptr).docTable = std::ptr::addr_of_mut!((*spec_ptr).docs);

            // Store raw pointers directly (don't convert back to Box)
            Self {
                rule: rule_ptr,
                spec: spec_ptr,
                sctx: sctx_ptr,
                qctx: qctx_ptr,
                numeric_range_tree: numeric_range_tree_ptr,
                tag_index: tag_index_ptr,
            }
        }
    }

    pub const fn numeric_range_tree(&self) -> NonNull<NumericRangeTree> {
        NonNull::new(self.numeric_range_tree).expect("NumericRangeTree should not be null")
    }

    /// Get the search context from the TestContext.
    pub const fn sctx(&self) -> NonNull<ffi::RedisSearchCtx> {
        NonNull::new(self.sctx).expect("RedisSearchCtx should not be null")
    }

    /// Get the index spec pointer from the [`MockContext`].
    pub const fn spec(&self) -> NonNull<ffi::IndexSpec> {
        NonNull::new(self.spec).expect("IndexSpec should not be null")
    }

    /// Get the query evaluation context from the [`MockContext`].
    pub const fn qctx(&self) -> NonNull<ffi::QueryEvalCtx> {
        NonNull::new(self.qctx).expect("QueryEvalCtx should not be null")
    }

    /// Set [`SchemaRule::index_all`]
    ///
    /// # Safety
    ///
    /// Must not be called while any iterator created from this context is
    /// still alive, as it mutates the spec through a raw pointer.
    pub unsafe fn set_index_all(&self, value: bool) {
        // SAFETY: Caller guarantees no iterators from this context are alive,
        // so the write does not race.
        unsafe { (*self.rule).index_all = value };
    }

    /// Set [`IndexSpec::diskSpec`] to point to the given disk index spec.
    ///
    /// Pass `std::ptr::null_mut()` to clear the field (making the spec appear
    /// to have no disk index).
    ///
    /// # Safety
    ///
    /// 1. Must not be called while any iterator created from this context is
    ///    still alive, as it mutates the spec through a raw pointer.
    /// 2. `disk_spec`, when non-null, must remain valid for as long as
    ///    iterators created from this context are alive.
    pub unsafe fn set_disk_spec(&self, disk_spec: *mut ffi::RedisSearchDiskIndexSpec) {
        // SAFETY: Caller guarantees no iterators from this context are alive (1),
        // so the write does not race.
        unsafe { (*self.spec).diskSpec = disk_spec };
    }

    /// Get a zeroed [`TagIndex`](ffi::TagIndex) pointer for basic (non-revalidation) tests.
    pub const fn tag_index(&self) -> NonNull<ffi::TagIndex> {
        NonNull::new(self.tag_index).expect("TagIndex should not be null")
    }
}
