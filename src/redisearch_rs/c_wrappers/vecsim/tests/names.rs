/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ffi::CStr;

use ffi::{
    VecSearchMode_EMPTY_MODE, VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES,
    VecSearchMode_HYBRID_BATCHES_TO_ADHOC_BF, VecSearchMode_RANGE_QUERY,
    VecSearchMode_STANDARD_KNN, VecSimAlgo_VecSimAlgo_BF, VecSimAlgo_VecSimAlgo_HNSWLIB,
    VecSimAlgo_VecSimAlgo_SVS, VecSimAlgo_VecSimAlgo_TIERED, VecSimMetric_VecSimMetric_Cosine,
    VecSimMetric_VecSimMetric_IP, VecSimMetric_VecSimMetric_L2, VecSimOptionMode_VecSimOption_AUTO,
    VecSimOptionMode_VecSimOption_DISABLE, VecSimOptionMode_VecSimOption_ENABLE,
    VecSimQuantType_VecSimQuant_NONE, VecSimQuantType_VecSimQuant_SQ8,
    VecSimSvsQuantBits_VecSimSvsQuant_4, VecSimSvsQuantBits_VecSimSvsQuant_4x4,
    VecSimSvsQuantBits_VecSimSvsQuant_4x8, VecSimSvsQuantBits_VecSimSvsQuant_4x8_LeanVec,
    VecSimSvsQuantBits_VecSimSvsQuant_8, VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec,
    VecSimSvsQuantBits_VecSimSvsQuant_NONE, VecSimSvsQuantBits_VecSimSvsQuant_Scalar,
    VecSimType_VecSimType_BFLOAT16, VecSimType_VecSimType_FLOAT16, VecSimType_VecSimType_FLOAT32,
    VecSimType_VecSimType_FLOAT64, VecSimType_VecSimType_INT8, VecSimType_VecSimType_INT32,
    VecSimType_VecSimType_INT64, VecSimType_VecSimType_UINT8,
};
use vecsim::names;

/// Not a value of any VecSim enum.
const INVALID: u32 = 0xdead;

/// Asserts `name(value) == expected` for every `(value, expected)` pair, then that
/// `name` rejects [`INVALID`].
fn assert_names(name: fn(u32) -> Option<&'static CStr>, table: &[(u32, &CStr)]) {
    for &(value, expected) in table {
        assert_eq!(name(value), Some(expected), "value {value}");
    }
    assert_eq!(name(INVALID), None);
}

#[test]
fn element_types() {
    let table = [
        (VecSimType_VecSimType_FLOAT32, c"FLOAT32", 4),
        (VecSimType_VecSimType_FLOAT64, c"FLOAT64", 8),
        (VecSimType_VecSimType_BFLOAT16, c"BFLOAT16", 2),
        (VecSimType_VecSimType_FLOAT16, c"FLOAT16", 2),
        (VecSimType_VecSimType_INT8, c"INT8", 1),
        (VecSimType_VecSimType_UINT8, c"UINT8", 1),
        (VecSimType_VecSimType_INT32, c"INT32", 4),
        (VecSimType_VecSimType_INT64, c"INT64", 8),
    ];
    for (ty, name, size) in table {
        assert_eq!(names::type_name(ty), Some(name));
        assert_eq!(names::type_size(ty), Some(size), "{name:?}");
    }
    assert_eq!(names::type_name(INVALID), None);
    assert_eq!(names::type_size(INVALID), None);
}

#[test]
fn metrics_and_algorithms() {
    assert_names(
        names::metric_name,
        &[
            (VecSimMetric_VecSimMetric_L2, c"L2"),
            (VecSimMetric_VecSimMetric_IP, c"IP"),
            (VecSimMetric_VecSimMetric_Cosine, c"COSINE"),
        ],
    );
    assert_names(
        names::algorithm_name,
        &[
            (VecSimAlgo_VecSimAlgo_BF, c"FLAT"),
            (VecSimAlgo_VecSimAlgo_HNSWLIB, c"HNSW"),
            (VecSimAlgo_VecSimAlgo_TIERED, c"TIERED"),
            (VecSimAlgo_VecSimAlgo_SVS, c"SVS-VAMANA"),
        ],
    );
}

#[test]
fn search_modes() {
    assert_names(
        names::search_mode_name,
        &[
            (VecSearchMode_EMPTY_MODE, c"EMPTY_MODE"),
            (VecSearchMode_STANDARD_KNN, c"STANDARD_KNN"),
            (VecSearchMode_HYBRID_ADHOC_BF, c"HYBRID_ADHOC_BF"),
            (VecSearchMode_HYBRID_BATCHES, c"HYBRID_BATCHES"),
            (
                VecSearchMode_HYBRID_BATCHES_TO_ADHOC_BF,
                c"HYBRID_BATCHES_TO_ADHOC_BF",
            ),
            (VecSearchMode_RANGE_QUERY, c"RANGE_QUERY"),
        ],
    );
}

#[test]
fn hnsw_options() {
    assert_names(
        names::hnsw_compression_name,
        &[
            (VecSimQuantType_VecSimQuant_NONE, c"NO_COMPRESSION"),
            (VecSimQuantType_VecSimQuant_SQ8, c"SQ8"),
        ],
    );
    assert_names(
        names::search_history_name,
        &[
            (VecSimOptionMode_VecSimOption_ENABLE, c"ON"),
            (VecSimOptionMode_VecSimOption_DISABLE, c"OFF"),
            (VecSimOptionMode_VecSimOption_AUTO, c"DEFAULT"),
        ],
    );
}

#[test]
fn svs_compression_with_lvq() {
    assert_names(
        |bits| names::svs_compression_name(bits, true),
        &[
            (VecSimSvsQuantBits_VecSimSvsQuant_NONE, c"NO_COMPRESSION"),
            (VecSimSvsQuantBits_VecSimSvsQuant_4, c"LVQ4"),
            (VecSimSvsQuantBits_VecSimSvsQuant_8, c"LVQ8"),
            (VecSimSvsQuantBits_VecSimSvsQuant_4x4, c"LVQ4x4"),
            (VecSimSvsQuantBits_VecSimSvsQuant_4x8, c"LVQ4x8"),
            (VecSimSvsQuantBits_VecSimSvsQuant_4x8_LeanVec, c"LeanVec4x8"),
            (VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec, c"LeanVec8x8"),
        ],
    );
    // No schema option selects plain scalar quantization.
    assert_eq!(
        names::svs_compression_name(VecSimSvsQuantBits_VecSimSvsQuant_Scalar, true),
        None
    );
}

/// Without LVQ, VecSim runs scalar quantization for any requested scheme.
#[test]
fn svs_compression_without_lvq() {
    assert_eq!(
        names::svs_compression_name(VecSimSvsQuantBits_VecSimSvsQuant_NONE, false),
        Some(c"NO_COMPRESSION")
    );
    for bits in [
        VecSimSvsQuantBits_VecSimSvsQuant_4,
        VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec,
        INVALID,
    ] {
        assert_eq!(
            names::svs_compression_name(bits, false),
            Some(c"GlobalSQ8"),
            "bits {bits}"
        );
    }
}

#[test]
fn leanvec_schemes() {
    assert!(names::is_leanvec_compression(
        VecSimSvsQuantBits_VecSimSvsQuant_4x8_LeanVec
    ));
    assert!(names::is_leanvec_compression(
        VecSimSvsQuantBits_VecSimSvsQuant_8x8_LeanVec
    ));
    assert!(!names::is_leanvec_compression(
        VecSimSvsQuantBits_VecSimSvsQuant_4x8
    ));
    assert!(!names::is_leanvec_compression(INVALID));
}
