/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer exposing the [`idf`] crate's IDF computation functions to C.

/// Computes the Inverse Document Frequency (IDF) for a term.
///
/// See [`idf::calculate_idf`] for details.
#[unsafe(no_mangle)]
pub extern "C" fn CalculateIDF(total_docs: usize, term_docs: usize) -> f64 {
    idf::calculate_idf(total_docs, term_docs)
}

/// Computes the BM25 IDF for a term.
///
/// See [`idf::calculate_idf_bm25`] for details.
#[unsafe(no_mangle)]
pub extern "C" fn CalculateIDF_BM25(total_docs: usize, term_docs: usize) -> f64 {
    idf::calculate_idf_bm25(total_docs, term_docs)
}
