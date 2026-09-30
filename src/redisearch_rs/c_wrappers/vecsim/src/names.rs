/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Display names and element sizes of the VecSim enums.
//!
//! The names are the spellings `FT.INFO` and `FT.PROFILE` report. Most come from the
//! `VECSIM_*` constants of `vector_index.h`, which the `FT.CREATE` parser matches
//! against too, so both directions share one spelling.
//!
//! Every function takes the raw C enum value, which may lie outside the enum, and
//! returns [`None`] for such a value.

#![expect(
    non_upper_case_globals,
    reason = "the match patterns are bindgen's enum constants"
)]

use std::ffi::CStr;

use ffi::{
    VECSIM_ALGORITHM_BF, VECSIM_ALGORITHM_HNSW, VECSIM_ALGORITHM_SVS, VECSIM_ALGORITHM_TIERED,
    VECSIM_LEANVEC_4X8, VECSIM_LEANVEC_8X8, VECSIM_LVQ_4, VECSIM_LVQ_4X4, VECSIM_LVQ_4X8,
    VECSIM_LVQ_8, VECSIM_LVQ_SCALAR, VECSIM_METRIC_COSINE, VECSIM_METRIC_IP, VECSIM_METRIC_L2,
    VECSIM_NO_COMPRESSION, VECSIM_SQ8, VECSIM_TYPE_BFLOAT16, VECSIM_TYPE_FLOAT16,
    VECSIM_TYPE_FLOAT32, VECSIM_TYPE_FLOAT64, VECSIM_TYPE_INT8, VECSIM_TYPE_INT32,
    VECSIM_TYPE_INT64, VECSIM_TYPE_UINT8, VECSIM_USE_SEARCH_HISTORY_DEFAULT,
    VECSIM_USE_SEARCH_HISTORY_OFF, VECSIM_USE_SEARCH_HISTORY_ON, VecSearchMode,
    VecSearchMode_EMPTY_MODE, VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES,
    VecSearchMode_HYBRID_BATCHES_TO_ADHOC_BF, VecSearchMode_RANGE_QUERY,
    VecSearchMode_STANDARD_KNN, VecSimAlgo, VecSimAlgo_VecSimAlgo_BF,
    VecSimAlgo_VecSimAlgo_HNSWLIB, VecSimAlgo_VecSimAlgo_SVS, VecSimAlgo_VecSimAlgo_TIERED,
    VecSimMetric, VecSimMetric_VecSimMetric_Cosine, VecSimMetric_VecSimMetric_IP,
    VecSimMetric_VecSimMetric_L2, VecSimOptionMode, VecSimOptionMode_VecSimOption_AUTO,
    VecSimOptionMode_VecSimOption_DISABLE, VecSimOptionMode_VecSimOption_ENABLE, VecSimQuantType,
    VecSimQuantType_VecSimQuant_NONE, VecSimQuantType_VecSimQuant_SQ8, VecSimSvsQuantBits,
    VecSimSvsQuantBits_VecSimSvsQuant_4, VecSimSvsQuantBits_VecSimSvsQuant_4x4,
    VecSimSvsQuantBits_VecSimSvsQuant_4x8, VecSimSvsQuantBits_VecSimSvsQuant_4x8_LeanVec,
    VecSimSvsQuantBits_VecSimSvsQuant_8, VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec,
    VecSimSvsQuantBits_VecSimSvsQuant_NONE, VecSimType, VecSimType_VecSimType_BFLOAT16,
    VecSimType_VecSimType_FLOAT16, VecSimType_VecSimType_FLOAT32, VecSimType_VecSimType_FLOAT64,
    VecSimType_VecSimType_INT8, VecSimType_VecSimType_INT32, VecSimType_VecSimType_INT64,
    VecSimType_VecSimType_UINT8,
};

/// Converts a `#define`d C string, as bindgen emits it, to a [`CStr`].
const fn c_str(bytes: &'static [u8]) -> &'static CStr {
    match CStr::from_bytes_with_nul(bytes) {
        Ok(name) => name,
        Err(_) => panic!("not a nul-terminated string"),
    }
}

/// The name of a vector element type.
pub const fn type_name(ty: VecSimType) -> Option<&'static CStr> {
    Some(match ty {
        VecSimType_VecSimType_FLOAT32 => const { c_str(VECSIM_TYPE_FLOAT32) },
        VecSimType_VecSimType_FLOAT64 => const { c_str(VECSIM_TYPE_FLOAT64) },
        VecSimType_VecSimType_FLOAT16 => const { c_str(VECSIM_TYPE_FLOAT16) },
        VecSimType_VecSimType_BFLOAT16 => const { c_str(VECSIM_TYPE_BFLOAT16) },
        VecSimType_VecSimType_UINT8 => const { c_str(VECSIM_TYPE_UINT8) },
        VecSimType_VecSimType_INT8 => const { c_str(VECSIM_TYPE_INT8) },
        VecSimType_VecSimType_INT32 => const { c_str(VECSIM_TYPE_INT32) },
        VecSimType_VecSimType_INT64 => const { c_str(VECSIM_TYPE_INT64) },
        _ => return None,
    })
}

/// The size in bytes of one vector element of type `ty`.
pub const fn type_size(ty: VecSimType) -> Option<usize> {
    Some(match ty {
        VecSimType_VecSimType_FLOAT32 => size_of::<f32>(),
        VecSimType_VecSimType_FLOAT64 => size_of::<f64>(),
        VecSimType_VecSimType_FLOAT16 | VecSimType_VecSimType_BFLOAT16 => size_of::<u16>(),
        VecSimType_VecSimType_UINT8 => size_of::<u8>(),
        VecSimType_VecSimType_INT8 => size_of::<i8>(),
        VecSimType_VecSimType_INT32 => size_of::<i32>(),
        VecSimType_VecSimType_INT64 => size_of::<i64>(),
        _ => return None,
    })
}

/// The name of a distance metric.
pub const fn metric_name(metric: VecSimMetric) -> Option<&'static CStr> {
    Some(match metric {
        VecSimMetric_VecSimMetric_IP => const { c_str(VECSIM_METRIC_IP) },
        VecSimMetric_VecSimMetric_L2 => const { c_str(VECSIM_METRIC_L2) },
        VecSimMetric_VecSimMetric_Cosine => const { c_str(VECSIM_METRIC_COSINE) },
        _ => return None,
    })
}

/// The name of an index algorithm.
pub const fn algorithm_name(algo: VecSimAlgo) -> Option<&'static CStr> {
    Some(match algo {
        VecSimAlgo_VecSimAlgo_BF => const { c_str(VECSIM_ALGORITHM_BF) },
        VecSimAlgo_VecSimAlgo_HNSWLIB => const { c_str(VECSIM_ALGORITHM_HNSW) },
        VecSimAlgo_VecSimAlgo_TIERED => const { c_str(VECSIM_ALGORITHM_TIERED) },
        VecSimAlgo_VecSimAlgo_SVS => const { c_str(VECSIM_ALGORITHM_SVS) },
        _ => return None,
    })
}

/// The name of the strategy a vector query ran with, as `FT.PROFILE` reports it.
pub const fn search_mode_name(mode: VecSearchMode) -> Option<&'static CStr> {
    Some(match mode {
        VecSearchMode_EMPTY_MODE => c"EMPTY_MODE",
        VecSearchMode_STANDARD_KNN => c"STANDARD_KNN",
        VecSearchMode_HYBRID_ADHOC_BF => c"HYBRID_ADHOC_BF",
        VecSearchMode_HYBRID_BATCHES => c"HYBRID_BATCHES",
        VecSearchMode_HYBRID_BATCHES_TO_ADHOC_BF => c"HYBRID_BATCHES_TO_ADHOC_BF",
        VecSearchMode_RANGE_QUERY => c"RANGE_QUERY",
        _ => return None,
    })
}

/// The name of an HNSW quantization scheme.
pub const fn hnsw_compression_name(quant: VecSimQuantType) -> Option<&'static CStr> {
    Some(match quant {
        VecSimQuantType_VecSimQuant_NONE => const { c_str(VECSIM_NO_COMPRESSION) },
        VecSimQuantType_VecSimQuant_SQ8 => const { c_str(VECSIM_SQ8) },
        _ => return None,
    })
}

/// Whether `bits` is one of the SVS LeanVec quantization schemes.
pub const fn is_leanvec_compression(bits: VecSimSvsQuantBits) -> bool {
    matches!(
        bits,
        VecSimSvsQuantBits_VecSimSvsQuant_4x8_LeanVec
            | VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec
    )
}

/// The name of the SVS quantization scheme that runs for a configured `bits`.
///
/// Without LVQ support (`lvq_supported`), VecSim falls back to scalar quantization
/// for every scheme other than none, so that is the name reported for any other
/// value of `bits`, valid or not.
pub const fn svs_compression_name(
    bits: VecSimSvsQuantBits,
    lvq_supported: bool,
) -> Option<&'static CStr> {
    if bits == VecSimSvsQuantBits_VecSimSvsQuant_NONE {
        return Some(const { c_str(VECSIM_NO_COMPRESSION) });
    }
    if !lvq_supported {
        return Some(const { c_str(VECSIM_LVQ_SCALAR) });
    }
    Some(match bits {
        VecSimSvsQuantBits_VecSimSvsQuant_4 => const { c_str(VECSIM_LVQ_4) },
        VecSimSvsQuantBits_VecSimSvsQuant_8 => const { c_str(VECSIM_LVQ_8) },
        VecSimSvsQuantBits_VecSimSvsQuant_4x4 => const { c_str(VECSIM_LVQ_4X4) },
        VecSimSvsQuantBits_VecSimSvsQuant_4x8 => const { c_str(VECSIM_LVQ_4X8) },
        VecSimSvsQuantBits_VecSimSvsQuant_4x8_LeanVec => const { c_str(VECSIM_LEANVEC_4X8) },
        VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec => const { c_str(VECSIM_LEANVEC_8X8) },
        _ => return None,
    })
}

/// The name of an SVS `USE_SEARCH_HISTORY` setting.
pub const fn search_history_name(option: VecSimOptionMode) -> Option<&'static CStr> {
    Some(match option {
        VecSimOptionMode_VecSimOption_ENABLE => const { c_str(VECSIM_USE_SEARCH_HISTORY_ON) },
        VecSimOptionMode_VecSimOption_DISABLE => const { c_str(VECSIM_USE_SEARCH_HISTORY_OFF) },
        VecSimOptionMode_VecSimOption_AUTO => const { c_str(VECSIM_USE_SEARCH_HISTORY_DEFAULT) },
        _ => return None,
    })
}
