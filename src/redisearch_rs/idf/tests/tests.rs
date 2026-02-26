/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use approx::{assert_abs_diff_eq, assert_ulps_eq};
use idf::{calculate_idf, calculate_idf_bm25};
use rstest::rstest;

// Miri interprets floating-point operations using a software implementation
// that can produce results slightly different from hardware FPUs.
//
// - `calculate_idf` extracts the IEEE 754 exponent directly (no float math),
//   so its results are exact and identical under Miri.
// - `calculate_idf_bm25` uses `ln`, a continuous function whose Miri results
//   can differ by a few ULPs. These tests use `approx` tolerances accordingly.

#[test]
fn idf_basic() {
    // 100 total docs, term appears in 10 → logb(1.0 + 101/10) = logb(11.1) = 3.0
    assert_abs_diff_eq!(calculate_idf(100, 10), 3.0);
}

#[test]
fn idf_term_docs_zero_treated_as_one() {
    // term_docs = 0 should behave like term_docs = 1
    assert_abs_diff_eq!(calculate_idf(100, 0), calculate_idf(100, 1));
}

#[test]
fn idf_total_docs_zero() {
    // 0 total docs, term in 1 doc → logb(1.0 + 1/1) = logb(2.0) = 1.0
    assert_abs_diff_eq!(calculate_idf(0, 1), 1.0);
}

#[test]
fn idf_both_zero() {
    // 0 total, 0 term → treated as (0, 1) → logb(1.0 + 1/1) = 1.0
    assert_abs_diff_eq!(calculate_idf(0, 0), 1.0);
}

#[test]
fn idf_single_doc() {
    // 1 total, 1 term → logb(1.0 + 2/1) = logb(3.0) = 1.0
    assert_abs_diff_eq!(calculate_idf(1, 1), 1.0);
}

#[test]
fn idf_rare_term_has_higher_score() {
    assert!(calculate_idf(1000, 1) > calculate_idf(1000, 500));
}

#[test]
fn idf_monotonically_decreasing_with_term_docs() {
    let total = 10_000;
    let mut prev = calculate_idf(total, 1);
    for term in [2, 10, 100, 1000, 5000, 10_000] {
        let current = calculate_idf(total, term);
        assert!(
            current <= prev,
            "IDF should decrease as term_docs increases: \
             idf({total}, {}) = {prev} > idf({total}, {term}) = {current}",
            term - 1,
        );
        prev = current;
    }
}

#[test]
fn idf_known_values() {
    // logb(1.0 + (1000+1)/1) = logb(1002.0) = floor(log2(1002)) = floor(9.968...) = 9
    assert_abs_diff_eq!(calculate_idf(1000, 1), 9.0);
    // logb(1.0 + (1000+1)/500) = logb(3.002) = floor(log2(3.002)) = floor(1.586...) = 1
    assert_abs_diff_eq!(calculate_idf(1000, 500), 1.0);
    // logb(1.0 + (1000+1)/1000) = logb(2.001) = floor(log2(2.001)) = floor(1.000...) = 1
    assert_abs_diff_eq!(calculate_idf(1000, 1000), 1.0);
}

/// Values computed from the original C implementation of `CalculateIDF`.
#[rstest]
#[case(0, 0, 1.0)]
#[case(0, 1, 1.0)]
#[case(1, 0, 1.0)]
#[case(1, 1, 1.0)]
#[case(10, 5, 1.0)]
#[case(100, 1, 6.0)]
#[case(100, 50, 1.0)]
#[case(100, 100, 1.0)]
#[case(1000, 1, 9.0)]
#[case(1000, 500, 1.0)]
#[case(10_000, 10_000, 1.0)]
#[case(1_000_000, 1, 19.0)]
fn idf_matches_c_reference(
    #[case] total_docs: usize,
    #[case] term_docs: usize,
    #[case] expected: f64,
) {
    assert_abs_diff_eq!(calculate_idf(total_docs, term_docs), expected);
}

#[test]
fn bm25_idf_basic() {
    // ln(1.0 + (100 - 10 + 0.5) / (10 + 0.5)) = ln(1.0 + 90.5/10.5) = ln(9.619...)
    assert_abs_diff_eq!(calculate_idf_bm25(100, 10), 2.2635, epsilon = 0.001);
}

#[test]
fn bm25_idf_all_docs_contain_term() {
    // ln(1.0 + (100 - 100 + 0.5) / (100 + 0.5)) = ln(1.0 + 0.5/100.5)
    let result = calculate_idf_bm25(100, 100);
    assert!(result > 0.0, "BM25 IDF should always be positive");
    assert!(result < 0.01);
}

#[test]
fn bm25_idf_term_docs_exceeds_total_docs() {
    // When term_docs > total_docs, clamp total_docs = term_docs
    assert_abs_diff_eq!(calculate_idf_bm25(5, 10), calculate_idf_bm25(10, 10));
}

#[test]
fn bm25_idf_both_zero() {
    // total=0, term=0 → ln(1.0 + 0.5/0.5) = ln(2.0)
    assert_abs_diff_eq!(
        calculate_idf_bm25(0, 0),
        2.0_f64.ln(),
        epsilon = 4.0 * f64::EPSILON,
    );
}

#[test]
fn bm25_idf_rare_term_scores_higher() {
    assert!(calculate_idf_bm25(1000, 1) > calculate_idf_bm25(1000, 500));
}

#[test]
fn bm25_idf_monotonically_decreasing() {
    let total = 10_000;
    let mut prev = calculate_idf_bm25(total, 1);
    for term in [2, 10, 100, 1000, 5000, 10_000] {
        let current = calculate_idf_bm25(total, term);
        assert!(
            current <= prev,
            "BM25 IDF should decrease as term_docs increases"
        );
        prev = current;
    }
}

#[test]
fn bm25_idf_always_non_negative() {
    let cases = [(0, 0), (1, 1), (100, 100), (50, 100), (1000, 1000)];
    for (total, term) in cases {
        assert!(
            calculate_idf_bm25(total, term) >= 0.0,
            "BM25 IDF should be non-negative for ({total}, {term})"
        );
    }
}

/// Values computed from the original C implementation of `CalculateIDF_BM25`,
/// with the float-precision intermediate arithmetic corrected to f64.
#[rstest]
#[case(0, 0, f64::from_bits(4604418534313441775))]
#[case(0, 1, f64::from_bits(4598854039415085969))]
#[case(1, 0, f64::from_bits(4608922133940812271))]
#[case(1, 1, f64::from_bits(4598854039415085969))]
#[case(10, 5, f64::from_bits(4604418534313441775))]
#[case(100, 1, f64::from_bits(4616425669059920044))]
#[case(100, 50, f64::from_bits(4604418534313441775))]
#[case(100, 100, f64::from_bits(4572371728709057247))]
#[case(1000, 1, f64::from_bits(4619008071662370528))]
#[case(1000, 500, f64::from_bits(4604418534313441775))]
#[case(10_000, 10_000, f64::from_bits(4542502969032277634))]
#[case(1_000_000, 1, f64::from_bits(4623738803079082246))]
#[case(5, 10, f64::from_bits(4586865061838308779))] // term_docs > total_docs
fn bm25_idf_matches_c_reference(
    #[case] total_docs: usize,
    #[case] term_docs: usize,
    #[case] expected: f64,
) {
    assert_ulps_eq!(
        calculate_idf_bm25(total_docs, term_docs),
        expected,
        max_ulps = 4,
    );
}
