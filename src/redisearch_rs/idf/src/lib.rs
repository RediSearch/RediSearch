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

/// Extracts the unbiased binary exponent from an IEEE 754 `f64`, equivalent
/// to C's `logb` for positive normal values: returns `floor(log2(value))`.
///
/// Unlike `value.log2().floor()`, this operates on the bit representation
/// and is therefore exact — it cannot be off by one due to floating-point
/// rounding.
///
/// # Panics
///
/// Panics in debug mode if `value` is not positive and normal (i.e. zero,
/// subnormal, infinite, or NaN).
#[inline]
fn ilogb(value: f64) -> i32 {
    debug_assert!(
        value.is_normal() && value.is_sign_positive(),
        "ilogb requires a positive normal f64, got {value}"
    );
    // IEEE 754 double: bits [62:52] hold the biased exponent (bias = 1023).
    ((value.to_bits() >> 52) as i32 & 0x7FF) - 1023
}

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
pub fn calculate_idf(total_docs: usize, term_docs: usize) -> f64 {
    let term_docs = if term_docs == 0 { 1 } else { term_docs };
    let value = 1.0 + (total_docs + 1) as f64 / term_docs as f64;
    // Extract the binary exponent directly from the IEEE 754 representation,
    // equivalent to C's `logb`. This is exact — unlike `log2().floor()`, it
    // cannot be off by one when the value is close to a power of two.
    ilogb(value) as f64
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
pub fn calculate_idf_bm25(total_docs: usize, term_docs: usize) -> f64 {
    let total_docs = total_docs.max(term_docs);
    let total = total_docs as f64;
    let term = term_docs as f64;
    (1.0 + (total - term + 0.5) / (term + 0.5)).ln()
}
