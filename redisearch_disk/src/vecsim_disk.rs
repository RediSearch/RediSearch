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

use std::ffi::c_void;

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
}
