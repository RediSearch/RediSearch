/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lightweight mock for [`ffi::RSQueryNode`] that avoids linking any C code
//! beyond the `array_*` helpers needed for children arrays.

use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ffi::c_char,
    ptr::NonNull,
};

use inverted_index::NumericFilter;
use query_types::QueryNodeType;
use rqe_core::DocId;

/// Owns a heap-allocated [`ffi::RSQueryNode`].
///
/// Uses raw pointers for storage to avoid Stacked Borrows violations (see
/// `rqe_iterators_test_utils::MockContext` for the rationale).
///
/// Auxiliary allocations (e.g. a dummy `FieldSpec`) are kept alive via
/// `_aux` and freed on drop.
pub struct MockQueryNode {
    node: *mut ffi::RSQueryNode,
    /// Auxiliary heap allocations that must outlive the node.
    _aux: Vec<AuxAlloc>,
}

/// A type-erased heap allocation freed on drop.
struct AuxAlloc {
    ptr: *mut u8,
    layout: Layout,
}

impl Drop for AuxAlloc {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with `alloc_zeroed` using `layout`.
        unsafe { dealloc(self.ptr, self.layout) }
    }
}

/// Allocate a zeroed instance of `T` on the heap and return a raw pointer.
///
/// The returned [`AuxAlloc`] must be stored to keep the allocation alive.
fn alloc_zeroed_aux<T>() -> (*mut T, AuxAlloc) {
    let layout = Layout::new::<T>();
    // SAFETY: layout is non-zero-sized for any struct with fields.
    let ptr = unsafe { alloc_zeroed(layout) };
    assert!(!ptr.is_null());
    (ptr.cast::<T>(), AuxAlloc { ptr, layout })
}

impl Drop for MockQueryNode {
    fn drop(&mut self) {
        // SAFETY: `self.node` is a valid, owned allocation. If `children` is
        // non-null it was allocated via `array_new_sz` and must be freed with
        // `array_free`.
        unsafe {
            let children = (*self.node).children;
            if !children.is_null() {
                ffi::array_free(children.cast());
            }
            dealloc(self.node.cast(), Layout::new::<ffi::RSQueryNode>());
        }
    }
}

impl MockQueryNode {
    pub fn new(type_: QueryNodeType) -> Self {
        // SAFETY: zeroed allocation is valid for `RSQueryNode` (all-zeros is a
        // valid bit pattern for the C struct); pointer is non-null-checked.
        unsafe {
            let node = alloc_zeroed(Layout::new::<ffi::RSQueryNode>()).cast::<ffi::RSQueryNode>();
            assert!(!node.is_null());
            (*node).type_ = type_;

            let mut aux = Vec::new();

            // Some node types contain pointers that `as_enum` dereferences
            // unconditionally.  Set them to valid zeroed allocations so the
            // mock can be used without extra setup.
            let union_ptr = &raw mut (*node).__bindgen_anon_1;
            match type_ {
                QueryNodeType::Tag => {
                    let (fs, alloc) = alloc_zeroed_aux::<ffi::FieldSpec>();
                    aux.push(alloc);
                    (*union_ptr.cast::<ffi::QueryTagNode>()).fs = fs;
                }
                QueryNodeType::Missing => {
                    let (fs, alloc) = alloc_zeroed_aux::<ffi::FieldSpec>();
                    aux.push(alloc);
                    (*union_ptr.cast::<ffi::QueryMissingNode>()).field = fs;
                }
                _ => {}
            }

            Self { node, _aux: aux }
        }
    }

    pub fn as_non_null(&self) -> NonNull<ffi::RSQueryNode> {
        NonNull::new(self.node).expect("node should not be null")
    }

    pub fn as_ptr(&self) -> *mut ffi::RSQueryNode {
        self.node
    }

    pub fn opts_mut(&mut self) -> &mut ffi::QueryNodeOptions {
        // SAFETY: `self.node` is a valid, exclusively-owned allocation.
        unsafe { &mut (*self.node).opts }
    }

    /// Set the `nf` field of the numeric-node union variant.
    pub fn set_numeric_filter(&mut self, nf: *mut NumericFilter) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Numeric so the `nn` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            (*union_ptr.cast::<ffi::QueryNumericNode>()).nf = nf.cast();
        }
    }

    /// Set the `gf` field of the geo-node union variant.
    pub fn set_geo_filter(&mut self, gf: *mut ffi::GeoFilter) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Geo so the `gn` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            (*union_ptr.cast::<ffi::QueryGeofilterNode>()).gf = gf;
        }
    }

    /// Set the `prefix` and `suffix` fields of the prefix-node union variant.
    pub fn set_prefix_mode(&mut self, prefix: bool, suffix: bool) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Prefix so the `pfx` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            let pfx = &mut *union_ptr.cast::<ffi::QueryPrefixNode>();
            pfx.prefix = prefix;
            pfx.suffix = suffix;
        }
    }

    /// Set the fields of the lex-range-node union variant.
    pub fn set_lex_range(
        &mut self,
        begin: *mut c_char,
        include_begin: bool,
        end: *mut c_char,
        include_end: bool,
    ) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is LexRange so the `lxrng` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            let lx = &mut *union_ptr.cast::<ffi::QueryLexRangeNode>();
            lx.begin = begin;
            lx.includeBegin = include_begin;
            lx.end = end;
            lx.includeEnd = include_end;
        }
    }

    /// Set the `exact` field of the phrase-node union variant.
    pub fn set_phrase_exact(&mut self, exact: i32) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Phrase so the `pn` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            (*union_ptr.cast::<ffi::QueryPhraseNode>()).exact = exact;
        }
    }

    /// Set the `str`/`len` fields of the token-node union variant
    /// ([`ffi::RSToken`]).
    ///
    /// The caller must keep `str_` valid for `len` bytes for as long as the
    /// node is used (e.g. by owning the backing buffer alongside the node).
    /// The token's `flags`/`expanded` bitfields keep their zeroed defaults.
    pub fn set_token(&mut self, str_: *mut c_char, len: usize) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Token so the `tn` (`RSToken`) variant is
        // active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            let tok = &mut *union_ptr.cast::<ffi::RSToken>();
            tok.str_ = str_;
            tok.len = len;
        }
    }

    /// Allocate a children array and populate it with the given child pointers.
    pub fn set_children(&mut self, children: &[*mut ffi::RSQueryNode]) {
        // SAFETY: `array_new_sz` allocates a tracked array with the given
        // length; `copy_nonoverlapping` fills the data region. `self.node` is
        // valid and exclusively owned.
        unsafe {
            let arr = ffi::array_new_sz(
                std::mem::size_of::<*mut ffi::RSQueryNode>() as u16,
                0,
                children.len() as u32,
            )
            .cast::<*mut ffi::RSQueryNode>();
            assert!(!arr.is_null());
            std::ptr::copy_nonoverlapping(children.as_ptr(), arr, children.len());
            (*self.node).children = arr;
        }
    }

    /// Set the fields of the IDs-node union variant.
    ///
    /// `keys` and `doc_ids` must outlive this `MockQueryNode`.
    pub fn set_ids(&mut self, keys: *const ffi::sds, doc_ids: *mut DocId, len: usize) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Ids so the `fn` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            let ids = &mut *union_ptr.cast::<ffi::QueryIdFilterNode>();
            ids.keys = keys;
            ids.docIds = doc_ids;
            ids.len = len;
        }
    }

    /// Set the `field` pointer of the missing-node union variant.
    ///
    /// `field` must outlive this `MockQueryNode`.
    pub fn set_missing_field(&mut self, field: *const ffi::FieldSpec) {
        // SAFETY: `self.node` is valid and exclusively owned; the caller
        // guarantees the node type is Missing so the `miss` variant is active.
        unsafe {
            let union_ptr = &raw mut (*self.node).__bindgen_anon_1;
            (*union_ptr.cast::<ffi::QueryMissingNode>()).field = field.cast_mut();
        }
    }
}
