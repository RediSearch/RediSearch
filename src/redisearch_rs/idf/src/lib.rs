/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [Inverse Document Frequency] (IDF) computation for search scoring.
//!
//! IDF measures how important a term is across a corpus of documents.
//! Terms that appear in fewer documents receive a higher IDF score,
//! meaning they are more discriminative for search ranking.
//!
//! Two variants are provided:
//! - [`calculate_idf`]: A custom IDF formula using the binary exponent
//!   (equivalent to C's `logb`).
//! - [`calculate_idf_bm25`]: The standard [BM25] IDF formula.
//!
//! [Inverse Document Frequency]: https://en.wikipedia.org/wiki/Tf%E2%80%93idf#Inverse_document_frequency
//! [BM25]: https://en.wikipedia.org/wiki/Okapi_BM25

/// Computes the Inverse Document Frequency (IDF) for a term.
///
/// Uses the binary exponent of the frequency ratio, equivalent to C's `logb`:
///
/// ```text
/// IDF = logb(1.0 + (total_docs + 1) / max(term_docs, 1))
/// ```
///
/// The `total_docs + 1` offset accounts for the step-wise nature of `logb`,
/// which returns `floor(log2(x))` for positive values.
///
/// # Examples
///
/// ```
/// # use idf::calculate_idf;
/// // A rare term in a large corpus has a high IDF.
/// assert!(calculate_idf(1000, 1) > calculate_idf(1000, 500));
///
/// // A term appearing in zero documents is treated as appearing in one.
/// assert_eq!(calculate_idf(100, 0), calculate_idf(100, 1));
/// ```
#[inline]
#[must_use]
pub fn calculate_idf(total_docs: usize, term_docs: usize) -> f64 {
    let term_docs = if term_docs == 0 { 1 } else { term_docs };
    let value = 1.0 + (total_docs + 1) as f64 / term_docs as f64;
    // `logb` returns the binary exponent, i.e. `floor(log2(x))` for positive
    // normal floating-point values.
    value.log2().floor()
}

/// Computes the IDF component of the [BM25] scoring algorithm.
///
/// Uses the standard BM25 IDF formula:
///
/// ```text
/// IDF_BM25 = ln(1.0 + (total_docs - term_docs + 0.5) / (term_docs + 0.5))
/// ```
///
/// When `total_docs < term_docs` (which can transiently happen during
/// deletions/updates before garbage collection), `total_docs` is clamped to
/// `term_docs` to prevent unsigned underflow.
///
/// [BM25]: https://en.wikipedia.org/wiki/Okapi_BM25
///
/// # Examples
///
/// ```
/// # use idf::calculate_idf_bm25;
/// // A rare term has a higher BM25 IDF than a common term.
/// assert!(calculate_idf_bm25(1000, 1) > calculate_idf_bm25(1000, 500));
///
/// // When term_docs exceeds total_docs, total_docs is clamped.
/// let a = calculate_idf_bm25(5, 10);
/// let b = calculate_idf_bm25(10, 10);
/// assert!((a - b).abs() < f64::EPSILON);
/// ```
#[inline]
#[must_use]
pub fn calculate_idf_bm25(total_docs: usize, term_docs: usize) -> f64 {
    let total_docs = total_docs.max(term_docs);
    let total = total_docs as f64;
    let term = term_docs as f64;
    (1.0 + (total - term + 0.5) / (term + 0.5)).ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Several tests below are ignored under Miri (`#[cfg_attr(miri, ignore)]`).
    //
    // Miri interprets floating-point operations using a software implementation
    // that can produce results slightly different from hardware FPUs. For example,
    // `f64::log2(2.0)` may return a value marginally below `1.0` under Miri,
    // causing `floor()` to yield `0.0` instead of the expected `1.0`. Similarly,
    // `f64::ln` results can differ in the last ULP, breaking exact equality
    // assertions.
    //
    // Since this crate contains no `unsafe` code, Miri cannot catch any
    // additional bugs here — the ignored tests are purely about floating-point
    // precision, not memory safety.

    #[test]
    fn idf_basic() {
        // 100 total docs, term appears in 10 → logb(1.0 + 101/10) = logb(11.1) = 3.0
        assert_eq!(calculate_idf(100, 10), 3.0);
    }

    #[test]
    fn idf_term_docs_zero_treated_as_one() {
        // term_docs = 0 should behave like term_docs = 1
        assert_eq!(calculate_idf(100, 0), calculate_idf(100, 1));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn idf_total_docs_zero() {
        // 0 total docs, term in 1 doc → logb(1.0 + 1/1) = logb(2.0) = 1.0
        assert_eq!(calculate_idf(0, 1), 1.0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn idf_both_zero() {
        // 0 total, 0 term → treated as (0, 1) → logb(1.0 + 1/1) = 1.0
        assert_eq!(calculate_idf(0, 0), 1.0);
    }

    #[test]
    fn idf_single_doc() {
        // 1 total, 1 term → logb(1.0 + 2/1) = logb(3.0) = 1.0
        assert_eq!(calculate_idf(1, 1), 1.0);
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
        assert_eq!(calculate_idf(1000, 1), 9.0);
        // logb(1.0 + (1000+1)/500) = logb(3.002) = floor(log2(3.002)) = floor(1.586...) = 1
        assert_eq!(calculate_idf(1000, 500), 1.0);
        // logb(1.0 + (1000+1)/1000) = logb(2.001) = floor(log2(2.001)) = floor(1.000...) = 1
        assert_eq!(calculate_idf(1000, 1000), 1.0);
    }

    /// Values computed from the original C implementation of `CalculateIDF`.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn idf_matches_c_reference() {
        // (total_docs, term_docs) => expected IDF
        let cases: &[(usize, usize, f64)] = &[
            (0, 0, 1.0),
            (0, 1, 1.0),
            (1, 0, 1.0),
            (1, 1, 1.0),
            (10, 5, 1.0),
            (100, 1, 6.0),
            (100, 50, 1.0),
            (100, 100, 1.0),
            (1000, 1, 9.0),
            (1000, 500, 1.0),
            (10_000, 10_000, 1.0),
            (1_000_000, 1, 19.0),
        ];
        for &(total, term, expected) in cases {
            assert_eq!(
                calculate_idf(total, term),
                expected,
                "Mismatch for calculate_idf({total}, {term})"
            );
        }
    }

    #[test]
    fn bm25_idf_basic() {
        // ln(1.0 + (100 - 10 + 0.5) / (10 + 0.5)) = ln(1.0 + 90.5/10.5) = ln(9.619...)
        let result = calculate_idf_bm25(100, 10);
        assert!((result - 2.2635).abs() < 0.001);
    }

    #[test]
    fn bm25_idf_all_docs_contain_term() {
        // ln(1.0 + (100 - 100 + 0.5) / (100 + 0.5)) = ln(1.0 + 0.5/100.5)
        let result = calculate_idf_bm25(100, 100);
        assert!(result > 0.0, "BM25 IDF should always be positive");
        assert!(result < 0.01);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn bm25_idf_term_docs_exceeds_total_docs() {
        // When term_docs > total_docs, clamp total_docs = term_docs
        assert_eq!(calculate_idf_bm25(5, 10), calculate_idf_bm25(10, 10));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn bm25_idf_both_zero() {
        // total=0, term=0 → ln(1.0 + 0.5/0.5) = ln(2.0)
        assert!((calculate_idf_bm25(0, 0) - 2.0_f64.ln()).abs() < f64::EPSILON);
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
    #[test]
    #[cfg_attr(miri, ignore)]
    fn bm25_idf_matches_c_reference() {
        // (total_docs, term_docs) => expected BM25 IDF
        let cases: &[(usize, usize, f64)] = &[
            (0, 0, f64::from_bits(4604418534313441775)),
            (0, 1, f64::from_bits(4598854039415085969)),
            (1, 0, f64::from_bits(4608922133940812271)),
            (1, 1, f64::from_bits(4598854039415085969)),
            (10, 5, f64::from_bits(4604418534313441775)),
            (100, 1, f64::from_bits(4616425669059920044)),
            (100, 50, f64::from_bits(4604418534313441775)),
            (100, 100, f64::from_bits(4572371728709057247)),
            (1000, 1, f64::from_bits(4619008071662370528)),
            (1000, 500, f64::from_bits(4604418534313441775)),
            (10_000, 10_000, f64::from_bits(4542502969032277634)),
            (1_000_000, 1, f64::from_bits(4623738803079082246)),
            (5, 10, f64::from_bits(4586865061838308779)), // term_docs > total_docs
        ];
        for &(total, term, expected) in cases {
            assert_eq!(
                calculate_idf_bm25(total, term),
                expected,
                "Mismatch for calculate_idf_bm25({total}, {term})"
            );
        }
    }
}
