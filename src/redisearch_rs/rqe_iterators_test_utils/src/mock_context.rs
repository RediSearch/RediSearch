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
    #[allow(dead_code)] // Holds allocation that spec.rule points to
    rule: *mut SchemaRule,
    spec: *mut IndexSpec,
    sctx: *mut RedisSearchCtx,
    #[allow(dead_code)]
    qctx: *mut QueryEvalCtx,
    numeric_range_tree: *mut NumericRangeTree,
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
}
