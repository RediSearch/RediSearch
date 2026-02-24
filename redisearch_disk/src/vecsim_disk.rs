/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

//! FFI bindings to the vecsim_disk C++ library.
//! After creation, use standard VecSimIndex_* functions for all operations.

use std::ffi::{c_char, c_void};

use speedb::{
    MergeOperands,
    merge_operator::{MergeFn, MergeResult},
};

/// Storage handles passed to C++. Mirrors `SpeeDBHandles` from vecsim_disk_api.h.
/// The C++ side copies these pointer values, so this struct can be stack-allocated.
/// The underlying database and column family must outlive the vector index.
#[repr(C)]
pub struct SpeeDBHandles {
    pub db: *mut c_void,
    pub cf: *mut c_void,
}

// Linking handled by CMake
unsafe extern "C" {
    #[link_name = "VecSimDisk_CreateIndex"]
    pub fn VecSimDisk_CreateIndex(params: *const c_void) -> *mut c_void;

    #[link_name = "VecSimDisk_FreeIndex"]
    pub fn VecSimDisk_FreeIndex(index: *mut c_void);

    #[link_name = "VecSimDisk_EdgeListMerge"]
    fn VecSimDisk_EdgeListMerge(
        existing_value: *const c_char,
        existing_value_len: usize,
        operands_list: *const *const c_char,
        operands_len_list: *const usize,
        num_operands: usize,
        result_len: *mut usize,
    ) -> *mut c_char;

    #[link_name = "VecSimDisk_EdgeListPartialMerge"]
    fn VecSimDisk_EdgeListPartialMerge(
        operands_list: *const *const c_char,
        operands_len_list: *const usize,
        num_operands: usize,
        result_len: *mut usize,
        success: *mut i32,
    ) -> *mut c_char;

    #[link_name = "VecSimDisk_FreeMergeResult"]
    fn VecSimDisk_FreeMergeResult(result: *mut c_char);
}

/// Wrapper for the C++ EdgeListMergeOperator.
///
/// This calls the C++ implementation via FFI, ensuring consistency
/// between the Rust merge operator used for column family creation
/// and the C++ code that uses the merge operations.
#[non_exhaustive]
pub struct EdgeListMergeOperator;

impl EdgeListMergeOperator {
    /// Create a full merge function that wraps the C++ EdgeListMergeOperator::FullMergeV2.
    ///
    /// Full merge combines an existing value with operands to produce a final value.
    /// The output is raw edge bytes (no operation prefix).
    pub fn full_merge_fn() -> impl MergeFn + Clone {
        move |_key: &[u8],
              existing_value: Option<&[u8]>,
              operand_list: &MergeOperands|
              -> Option<MergeResult> {
            // Build pointer and length arrays in a single pass (avoids intermediate Vec<&[u8]>)
            let (operand_ptrs, operand_lens): (Vec<*const c_char>, Vec<usize>) = operand_list
                .iter()
                .map(|o| (o.as_ptr() as *const c_char, o.len()))
                .unzip();

            let mut result_len: usize = 0;

            // SAFETY: We're calling the C++ function with valid pointers.
            // The C++ function copies the data, so our slices only need to be valid for the call.
            let result_ptr = unsafe {
                VecSimDisk_EdgeListMerge(
                    existing_value.map_or(std::ptr::null(), |v| v.as_ptr() as *const c_char),
                    existing_value.map_or(0, |v| v.len()),
                    operand_ptrs.as_ptr(),
                    operand_lens.as_ptr(),
                    operand_ptrs.len(),
                    &mut result_len,
                )
            };

            if result_ptr.is_null() {
                return None;
            }

            // Copy result and free the C++ allocated buffer
            // SAFETY: result_ptr points to result_len valid bytes allocated by C++
            let result = unsafe { std::slice::from_raw_parts(result_ptr as *const u8, result_len) };
            let result_vec = result.to_vec();

            // SAFETY: result_ptr was allocated by VecSimDisk_EdgeListMerge
            unsafe {
                VecSimDisk_FreeMergeResult(result_ptr);
            }

            Some((result_vec, None))
        }
    }

    /// Create a partial merge function that wraps the C++ EdgeListMergeOperator::PartialMerge.
    ///
    /// Partial merge combines operands without an existing value - used during compaction
    /// to reduce the number of operands before a full merge. Unlike full merge, partial
    /// merge preserves the operand format (with 'A'/'D' prefix) so the result can be
    /// used as an operand in subsequent merges.
    ///
    /// Returns `None` if partial merge cannot be performed (e.g., different operation types),
    /// which tells SpeedB to fall back to full merge.
    pub fn partial_merge_fn() -> impl MergeFn + Clone {
        move |_key: &[u8],
              _existing_value: Option<&[u8]>,
              operand_list: &MergeOperands|
              -> Option<MergeResult> {
            // Partial merge requires at least 2 operands
            if operand_list.len() < 2 {
                return None;
            }

            // Build pointer and length arrays
            let (operand_ptrs, operand_lens): (Vec<*const c_char>, Vec<usize>) = operand_list
                .iter()
                .map(|o| (o.as_ptr() as *const c_char, o.len()))
                .unzip();

            let mut result_len: usize = 0;
            let mut success: i32 = 0;

            // SAFETY: We're calling the C++ function with valid pointers.
            // The C++ function copies the data, so our slices only need to be valid for the call.
            let result_ptr = unsafe {
                VecSimDisk_EdgeListPartialMerge(
                    operand_ptrs.as_ptr(),
                    operand_lens.as_ptr(),
                    operand_ptrs.len(),
                    &mut result_len,
                    &mut success,
                )
            };

            // success=0 means partial merge cannot be performed (not an error)
            if success == 0 || result_ptr.is_null() {
                return None;
            }

            // Copy result and free the C++ allocated buffer
            // SAFETY: result_ptr points to result_len valid bytes allocated by C++
            let result = unsafe { std::slice::from_raw_parts(result_ptr as *const u8, result_len) };
            let result_vec = result.to_vec();

            // SAFETY: result_ptr was allocated by VecSimDisk_EdgeListPartialMerge
            unsafe {
                VecSimDisk_FreeMergeResult(result_ptr);
            }

            Some((result_vec, None))
        }
    }
}
