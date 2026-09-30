/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for the VecSim enum metadata of `vector_index.h`, backed by
//! [`vecsim::names`].
//!
//! Every returned string is static and must not be freed. NULL (or zero, for
//! sizes) stands for an argument outside its enum.

use std::ffi::{CStr, c_char};
use std::ptr;

use ffi::{VecSearchMode, VecSimAlgo, VecSimMetric, VecSimOptionMode, VecSimQuantType};
use ffi::{VecSimSvsQuantBits, VecSimType};
use vecsim::names;

const fn to_c(name: Option<&'static CStr>) -> *const c_char {
    match name {
        Some(name) => name.as_ptr(),
        None => ptr::null(),
    }
}

/// Returns the name of a vector element type. See [`names::type_name`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimType_ToString(ty: VecSimType) -> *const c_char {
    to_c(names::type_name(ty))
}

/// Returns the size in bytes of one vector element. See [`names::type_size`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimType_sizeof(ty: VecSimType) -> usize {
    match names::type_size(ty) {
        Some(size) => size,
        None => 0,
    }
}

/// Returns the name of a distance metric. See [`names::metric_name`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimMetric_ToString(metric: VecSimMetric) -> *const c_char {
    to_c(names::metric_name(metric))
}

/// Returns the name of an index algorithm. See [`names::algorithm_name`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimAlgorithm_ToString(algo: VecSimAlgo) -> *const c_char {
    to_c(names::algorithm_name(algo))
}

/// Returns the name of a vector query strategy. See [`names::search_mode_name`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimSearchMode_ToString(mode: VecSearchMode) -> *const c_char {
    to_c(names::search_mode_name(mode))
}

/// Returns the name of an HNSW quantization scheme. See
/// [`names::hnsw_compression_name`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimHnswCompression_ToString(quant: VecSimQuantType) -> *const c_char {
    to_c(names::hnsw_compression_name(quant))
}

/// Returns the name of the SVS quantization scheme that runs on this machine for
/// `bits`. See [`names::svs_compression_name`].
#[unsafe(no_mangle)]
pub extern "C" fn VecSimSvsCompression_ToString(bits: VecSimSvsQuantBits) -> *const c_char {
    // SAFETY: `isLVQSupported` only inspects the CPU and has no preconditions.
    let lvq_supported = unsafe { ffi::isLVQSupported() };
    to_c(names::svs_compression_name(bits, lvq_supported))
}

/// Returns the name of an SVS `USE_SEARCH_HISTORY` setting. See
/// [`names::search_history_name`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSimSearchHistory_ToString(option: VecSimOptionMode) -> *const c_char {
    to_c(names::search_history_name(option))
}

/// Returns whether `bits` is an SVS LeanVec quantization scheme. See
/// [`names::is_leanvec_compression`].
#[unsafe(no_mangle)]
pub const extern "C" fn VecSim_IsLeanVecCompressionType(bits: VecSimSvsQuantBits) -> bool {
    names::is_leanvec_compression(bits)
}
