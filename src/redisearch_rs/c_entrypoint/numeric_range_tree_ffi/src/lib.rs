/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bindings for NumericRangeTree.
//!
//! This module exposes the Rust NumericRangeTree implementation to C code
//! through a C-compatible interface.

use std::ptr;

use ffi::t_docId;
use inverted_index::{FilterGeoReader, FilterNumericReader, NumericFilter};
use inverted_index_ffi::IndexReader;
use numeric_range_tree::{NumericRange, NumericRangeNode, NumericRangeTree, NumericRangeTreeIterator};

/// Result of an add operation on the NumericRangeTree.
/// This matches the C struct NRN_AddRv.
#[repr(C)]
pub struct NRN_AddRv {
    /// Change in memory size
    pub sz: i32,
    /// Number of records affected
    pub num_records: i32,
    /// Whether the tree structure changed
    pub changed: i32,
    /// Change in number of ranges
    pub num_ranges: i32,
    /// Change in number of leaves
    pub num_leaves: i32,
}

impl From<numeric_range_tree::AddResult> for NRN_AddRv {
    fn from(result: numeric_range_tree::AddResult) -> Self {
        Self {
            sz: result.size_change as i32,
            num_records: result.num_records as i32,
            changed: if result.changed { 1 } else { 0 },
            num_ranges: result.num_ranges as i32,
            num_leaves: result.num_leaves as i32,
        }
    }
}

/// Creates a new NumericRangeTree.
///
/// # Returns
///
/// A pointer to the newly created tree. The caller is responsible for
/// freeing it with [`NumericRangeTree_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewNumericRangeTree() -> *mut NumericRangeTree {
    Box::into_raw(Box::new(NumericRangeTree::new()))
}

/// Frees a NumericRangeTree and all its resources.
///
/// # Safety
///
/// - `tree` must be a valid pointer returned by [`NewNumericRangeTree`].
/// - After calling this function, `tree` must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Free(tree: *mut NumericRangeTree) {
    if !tree.is_null() {
        // SAFETY: The caller guarantees that `tree` is a valid pointer.
        drop(unsafe { Box::from_raw(tree) });
    }
}

/// Adds a value to the NumericRangeTree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
///
/// # Arguments
///
/// * `tree` - Pointer to the tree
/// * `doc_id` - The document ID
/// * `value` - The numeric value to add
/// * `is_multi` - Non-zero if this is a multi-value field (allows duplicate doc IDs)
///
/// # Returns
///
/// The result of the add operation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Add(
    tree: *mut NumericRangeTree,
    doc_id: t_docId,
    value: f64,
    is_multi: std::ffi::c_int,
) -> NRN_AddRv {
    if tree.is_null() {
        return NRN_AddRv {
            sz: 0,
            num_records: 0,
            changed: 0,
            num_ranges: 0,
            num_leaves: 0,
        };
    }

    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    let tree = unsafe { &mut *tree };
    match tree.add(doc_id, value, is_multi != 0) {
        Ok(result) => result.into(),
        Err(_) => NRN_AddRv {
            sz: 0,
            num_records: 0,
            changed: 0,
            num_ranges: 0,
            num_leaves: 0,
        },
    }
}

/// Finds all ranges that match the given filter.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
/// - `filter` must be a valid pointer to a NumericFilter.
/// - The returned Vector must be freed by the caller.
///
/// # Returns
///
/// A C vector containing pointers to matching NumericRange objects.
/// The caller must free the vector but NOT the ranges (they are owned by the tree).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Find(
    tree: *const NumericRangeTree,
    filter: *const NumericFilter,
) -> *mut Vec<*const NumericRange> {
    if tree.is_null() || filter.is_null() {
        return ptr::null_mut();
    }

    // SAFETY: We verified tree is not null and caller guarantees it's valid.
    let tree = unsafe { &*tree };
    // SAFETY: We verified filter is not null and caller guarantees it's valid.
    let filter = unsafe { &*filter };

    let ranges = tree.find(filter);
    let ptrs: Vec<*const NumericRange> = ranges.into_iter().map(|r| r as *const _).collect();

    if ptrs.is_empty() {
        return ptr::null_mut();
    }

    Box::into_raw(Box::new(ptrs))
}

/// Frees a vector returned by NumericRangeTree_Find.
///
/// # Safety
///
/// - `vec` must be a valid pointer returned by [`NumericRangeTree_Find`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_FreeVector(vec: *mut Vec<*const NumericRange>) {
    if !vec.is_null() {
        // SAFETY: The caller guarantees that `vec` is a valid pointer.
        drop(unsafe { Box::from_raw(vec) });
    }
}

/// Gets the number of elements in a vector returned by NumericRangeTree_Find.
///
/// # Safety
///
/// - `vec` must be a valid pointer returned by [`NumericRangeTree_Find`], or null.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_VectorSize(vec: *const Vec<*const NumericRange>) -> usize {
    if vec.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `vec` is a valid pointer.
    unsafe { (*vec).len() }
}

/// Gets an element from a vector returned by NumericRangeTree_Find.
///
/// # Safety
///
/// - `vec` must be a valid pointer returned by [`NumericRangeTree_Find`].
/// - `index` must be less than the vector's size.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_VectorGet(
    vec: *const Vec<*const NumericRange>,
    index: usize,
) -> *const NumericRange {
    if vec.is_null() {
        return ptr::null();
    }
    // SAFETY: The caller guarantees that `vec` is a valid pointer and index is in bounds.
    let vec = unsafe { &*vec };
    if index < vec.len() {
        vec[index]
    } else {
        ptr::null()
    }
}

/// Trims empty leaves from the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_TrimEmptyLeaves(tree: *mut NumericRangeTree) -> NRN_AddRv {
    if tree.is_null() {
        return NRN_AddRv {
            sz: 0,
            num_records: 0,
            changed: 0,
            num_ranges: 0,
            num_leaves: 0,
        };
    }

    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    let tree = unsafe { &mut *tree };
    tree.trim_empty_leaves().into()
}

/// Gets the cardinality of a NumericRange.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_GetCardinality(range: *const NumericRange) -> usize {
    if range.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `range` is a valid pointer.
    unsafe { (*range).cardinality_uncached() }
}

/// Gets the memory usage of the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericIndexType_MemUsage(tree: *const NumericRangeTree) -> usize {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).memory_usage() }
}

/// Tree iterator wrapper for C.
pub struct NumericRangeTreeIteratorWrapper<'a> {
    iter: NumericRangeTreeIterator<'a>,
}

/// Creates a new iterator for the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
/// - The tree must outlive the iterator.
///
/// # Returns
///
/// A pointer to the iterator. The caller must free it with [`NumericRangeTreeIterator_Free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_New(
    tree: *const NumericRangeTree,
) -> *mut NumericRangeTreeIteratorWrapper<'static> {
    if tree.is_null() {
        return ptr::null_mut();
    }

    // SAFETY: The caller guarantees that `tree` is a valid pointer and will outlive the iterator.
    // We transmute the lifetime to 'static because C doesn't have lifetime tracking.
    // The caller is responsible for ensuring the tree outlives the iterator.
    let tree = unsafe { &*tree };
    let iter = tree.iter();
    let wrapper = NumericRangeTreeIteratorWrapper { iter };

    // SAFETY: We transmute the lifetime to 'static. The caller must ensure the tree outlives
    // the iterator.
    Box::into_raw(Box::new(unsafe {
        std::mem::transmute::<NumericRangeTreeIteratorWrapper<'_>, NumericRangeTreeIteratorWrapper<'static>>(wrapper)
    }))
}

/// Gets the next node from the iterator.
///
/// # Safety
///
/// - `iter` must be a valid pointer to an iterator.
///
/// # Returns
///
/// A pointer to the next node, or null if there are no more nodes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_Next(
    iter: *mut NumericRangeTreeIteratorWrapper<'static>,
) -> *const NumericRangeNode {
    if iter.is_null() {
        return ptr::null();
    }

    // SAFETY: The caller guarantees that `iter` is a valid pointer.
    let iter = unsafe { &mut *iter };
    iter.iter.next().map_or(ptr::null(), |n| n as *const _)
}

/// Frees a tree iterator.
///
/// # Safety
///
/// - `iter` must be a valid pointer returned by [`NumericRangeTreeIterator_New`], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_Free(
    iter: *mut NumericRangeTreeIteratorWrapper<'static>,
) {
    if !iter.is_null() {
        // SAFETY: The caller guarantees that `iter` is a valid pointer.
        drop(unsafe { Box::from_raw(iter) });
    }
}

/// Checks if a node is a leaf.
///
/// # Safety
///
/// - `node` must be a valid pointer to a NumericRangeNode.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeNode_IsLeaf(node: *const NumericRangeNode) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: The caller guarantees that `node` is a valid pointer.
    unsafe { (*node).is_leaf() }
}

/// Gets the range from a node, if present.
///
/// # Safety
///
/// - `node` must be a valid pointer to a NumericRangeNode.
///
/// # Returns
///
/// A pointer to the range, or null if the node has no range.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeNode_GetRange(node: *const NumericRangeNode) -> *const NumericRange {
    if node.is_null() {
        return ptr::null();
    }
    // SAFETY: The caller guarantees that `node` is a valid pointer.
    unsafe { (*node).range.as_ref().map_or(ptr::null(), |r| r as *const _) }
}

/// Gets the min value of a range.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRange_GetMinVal(range: *const NumericRange) -> f64 {
    if range.is_null() {
        return f64::INFINITY;
    }
    // SAFETY: The caller guarantees that `range` is a valid pointer.
    unsafe { (*range).min_val }
}

/// Gets the max value of a range.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRange_GetMaxVal(range: *const NumericRange) -> f64 {
    if range.is_null() {
        return f64::NEG_INFINITY;
    }
    // SAFETY: The caller guarantees that `range` is a valid pointer.
    unsafe { (*range).max_val }
}

/// Gets the number of entries in a range.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRange_GetNumEntries(range: *const NumericRange) -> usize {
    if range.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `range` is a valid pointer.
    unsafe { (*range).num_entries() }
}

/// Gets the number of unique docs in a range.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRange_GetNumDocs(range: *const NumericRange) -> u32 {
    if range.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `range` is a valid pointer.
    unsafe { (*range).num_docs() }
}

/// Gets the inverted index size of a range.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_GetInvertedIndexSize(range: *const NumericRange) -> usize {
    if range.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `range` is a valid pointer.
    unsafe { (*range).inverted_index_size() }
}

/// Gets the number of ranges in the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetNumRanges(tree: *const NumericRangeTree) -> usize {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).num_ranges() }
}

/// Gets the number of leaves in the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetNumLeaves(tree: *const NumericRangeTree) -> usize {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).num_leaves() }
}

/// Gets the number of entries in the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetNumEntries(tree: *const NumericRangeTree) -> usize {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).num_entries() }
}

/// Gets the revision ID of the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetRevisionId(tree: *const NumericRangeTree) -> u32 {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).revision_id() }
}

/// Gets the unique ID of the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetUniqueId(tree: *const NumericRangeTree) -> u32 {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).unique_id() }
}

/// Gets the last doc ID added to the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetLastDocId(tree: *const NumericRangeTree) -> t_docId {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).last_doc_id() }
}

/// Gets the inverted indexes size of the tree.
///
/// # Safety
///
/// - `tree` must be a valid pointer to a NumericRangeTree.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn NumericRangeTree_GetInvertedIndexesSize(tree: *const NumericRangeTree) -> usize {
    if tree.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees that `tree` is a valid pointer.
    unsafe { (*tree).inverted_indexes_size() }
}

/// Creates an IndexReader for a NumericRange's inverted index entries.
///
/// This function allows C code to create an IndexReader to iterate over
/// the entries in a NumericRange. The filter parameter can be used to
/// filter entries by numeric value or geo coordinates.
///
/// # Safety
///
/// - `range` must be a valid pointer to a NumericRange.
/// - If `filter` is non-null, it must be a valid pointer to a NumericFilter.
/// - The returned IndexReader must be freed using `IndexReader_Free` from inverted_index_ffi.
/// - The NumericRange must outlive the returned IndexReader.
///
/// # Returns
///
/// A pointer to a new IndexReader, or null if `range` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_CreateReader(
    range: *const NumericRange,
    filter: *const NumericFilter,
) -> *mut IndexReader<'static> {
    if range.is_null() {
        return ptr::null_mut();
    }

    // SAFETY: The caller guarantees that `range` is a valid pointer.
    let range = unsafe { &*range };

    let reader = if filter.is_null() {
        // No filter - create a basic numeric reader
        IndexReader::Numeric(range.entries.reader())
    } else {
        // SAFETY: The caller guarantees that `filter` is a valid pointer.
        let filter = unsafe { &*filter };

        if filter.is_numeric_filter() {
            // Numeric range filter
            IndexReader::NumericFiltered(FilterNumericReader::new(filter, range.entries.reader()))
        } else {
            // Geo filter
            IndexReader::NumericGeoFiltered(FilterGeoReader::new(filter, range.entries.reader()))
        }
    };

    // SAFETY: We transmute the lifetime to 'static. The caller must ensure the range outlives
    // the reader.
    Box::into_raw(Box::new(unsafe {
        std::mem::transmute::<IndexReader<'_>, IndexReader<'static>>(reader)
    }))
}
