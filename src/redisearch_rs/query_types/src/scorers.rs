/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Canonical names of the built-in scorers.

/// TF-IDF scorer (`TFIDF`). Uses term proximity, so it needs term offsets.
#[cheadergen::config(export)]
pub const TFIDF_SCORER_NAME: &str = "TFIDF";

/// Document-normalized TF-IDF scorer (`TFIDF.DOCNORM`).
#[cheadergen::config(export)]
pub const TFIDF_DOCNORM_SCORER_NAME: &str = "TFIDF.DOCNORM";

/// Disjunction-max scorer (`DISMAX`).
#[cheadergen::config(export)]
pub const DISMAX_SCORER_NAME: &str = "DISMAX";

/// Legacy BM25 scorer (`BM25`).
#[cheadergen::config(export)]
pub const BM25_SCORER_NAME: &str = "BM25";

/// Standard BM25 scorer (`BM25STD`). The default scorer.
#[cheadergen::config(export)]
pub const BM25_STD_SCORER_NAME: &str = "BM25STD";

/// Standard BM25 scorer with tanh normalization (`BM25STD.TANH`).
#[cheadergen::config(export)]
pub const BM25_STD_NORMALIZED_TANH_SCORER_NAME: &str = "BM25STD.TANH";

/// Standard BM25 scorer with max normalization (`BM25STD.NORM`).
#[cheadergen::config(export)]
pub const BM25_STD_NORMALIZED_MAX_SCORER_NAME: &str = "BM25STD.NORM";

/// Document-score scorer (`DOCSCORE`).
#[cheadergen::config(export)]
pub const DOCSCORE_SCORER: &str = "DOCSCORE";

/// Hamming-distance scorer (`HAMMING`).
#[cheadergen::config(export)]
pub const HAMMINGDISTANCE_SCORER: &str = "HAMMING";

/// Name of the scorer used when a query does not request one explicitly.
///
/// This is the standard BM25 scorer. It is spelled out as a literal, matching
/// [`BM25_STD_SCORER_NAME`], because the C header generator only exports literal
/// string constants.
#[cheadergen::config(export)]
pub const DEFAULT_SCORER_NAME: &str = "BM25STD";
