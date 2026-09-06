/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Canonical names of the built-in scorers, and the [`BuiltInScorer`] enum over them.

use std::ffi::CStr;

use strum::VariantArray;

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

/// A built-in scorer, identified by its canonical name.
///
/// Parse a name into a [`BuiltInScorer`] with [`str::parse`]/[`FromStr`]; a name that
/// is not one of the built-ins (a custom scorer) yields [`UnknownScorer`].
///
/// The [`Default`] is [`Bm25Std`](BuiltInScorer::Bm25Std), the scorer applied when a
/// query requests none (see [`DEFAULT_SCORER_NAME`]).
///
/// [`FromStr`]: std::str::FromStr
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, VariantArray)]
pub enum BuiltInScorer {
    /// TF-IDF ([`TFIDF_SCORER_NAME`]).
    TfIdf,
    /// Document-normalized TF-IDF ([`TFIDF_DOCNORM_SCORER_NAME`]).
    TfIdfDocNorm,
    /// Legacy BM25 ([`BM25_SCORER_NAME`]).
    Bm25,
    /// Disjunction-max ([`DISMAX_SCORER_NAME`]).
    DisMax,
    /// Standard BM25 ([`BM25_STD_SCORER_NAME`]). The default scorer.
    #[default]
    Bm25Std,
    /// Standard BM25 with tanh normalization ([`BM25_STD_NORMALIZED_TANH_SCORER_NAME`]).
    Bm25StdTanh,
    /// Standard BM25 with max normalization ([`BM25_STD_NORMALIZED_MAX_SCORER_NAME`]).
    Bm25StdNorm,
    /// Document score ([`DOCSCORE_SCORER`]).
    DocScore,
    /// Hamming distance ([`HAMMINGDISTANCE_SCORER`]).
    Hamming,
}

impl BuiltInScorer {
    /// Resolve a scorer from its raw name bytes, or [`None`] for a name that is
    /// not one of the built-ins.
    fn from_name_bytes(bytes: &[u8]) -> Option<BuiltInScorer> {
        BuiltInScorer::VARIANTS
            .iter()
            .copied()
            .find(|scorer| scorer.name().as_bytes() == bytes)
    }

    /// Resolve a scorer from a NUL-terminated C string.
    pub fn from_c_str(name: &CStr) -> Option<BuiltInScorer> {
        BuiltInScorer::from_name_bytes(name.to_bytes())
    }

    /// The scorer's canonical name.
    pub const fn name(self) -> &'static str {
        match self {
            BuiltInScorer::TfIdf => TFIDF_SCORER_NAME,
            BuiltInScorer::TfIdfDocNorm => TFIDF_DOCNORM_SCORER_NAME,
            BuiltInScorer::Bm25 => BM25_SCORER_NAME,
            BuiltInScorer::DisMax => DISMAX_SCORER_NAME,
            BuiltInScorer::Bm25Std => BM25_STD_SCORER_NAME,
            BuiltInScorer::Bm25StdTanh => BM25_STD_NORMALIZED_TANH_SCORER_NAME,
            BuiltInScorer::Bm25StdNorm => BM25_STD_NORMALIZED_MAX_SCORER_NAME,
            BuiltInScorer::DocScore => DOCSCORE_SCORER,
            BuiltInScorer::Hamming => HAMMINGDISTANCE_SCORER,
        }
    }

    /// Whether this scorer needs term offset data.
    ///
    /// The BM25STD family, [`DisMax`](BuiltInScorer::DisMax),
    /// [`DocScore`](BuiltInScorer::DocScore) and [`Hamming`](BuiltInScorer::Hamming) score
    /// without term proximity, so they don't need offsets. TF-IDF (both
    /// variants) and legacy [`Bm25`](BuiltInScorer::Bm25) use proximity scoring and do.
    pub const fn needs_offsets(self) -> bool {
        match self {
            BuiltInScorer::Bm25Std
            | BuiltInScorer::Bm25StdTanh
            | BuiltInScorer::Bm25StdNorm
            | BuiltInScorer::DisMax
            | BuiltInScorer::DocScore
            | BuiltInScorer::Hamming => false,
            BuiltInScorer::TfIdf | BuiltInScorer::TfIdfDocNorm | BuiltInScorer::Bm25 => true,
        }
    }
}

/// Whether a node's phrase/slop options force term offsets on their own,
/// regardless of the scorer.
///
/// A phrase/slop constraint — a non-negative `max_slop` or an `in_order`
/// requirement — needs offsets to filter matches by term position. When this
/// returns `false` the options impose no such requirement and the scorer alone
/// decides (see [`BuiltInScorer::needs_offsets`]).
pub const fn slop_forces_offsets(max_slop: i32, in_order: i32) -> bool {
    max_slop >= 0 || in_order != 0
}

/// The scorer a query requested, before any default is applied.
///
/// This reports the query's stated intent as-is; it never substitutes a
/// default. The [`Custom`](RequestedScorer::Custom) name borrows from the
/// source that produced it (typically the query's search options).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedScorer<'name> {
    /// The query set no scorer of its own. The caller applies its own default.
    Unset,
    /// The query set a name that is not one of the built-ins (a custom scorer),
    /// carried here as its raw NUL-terminated name. It cannot be resolved to a
    /// [`BuiltInScorer`], so the caller should handle it conservatively (e.g. as
    /// needing term offsets) or look it up among the registered extension
    /// scorers by name.
    Custom(&'name CStr),
    /// The query set a recognized built-in [`BuiltInScorer`].
    BuiltIn(BuiltInScorer),
}

/// Error returned by [`BuiltInScorer::from_str`](std::str::FromStr::from_str) when the
/// name is not one of the built-in scorers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown scorer name: {name:?}")]
pub struct UnknownScorer {
    /// The unrecognized scorer name that was requested.
    pub name: String,
}

impl std::str::FromStr for BuiltInScorer {
    type Err = UnknownScorer;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        BuiltInScorer::from_name_bytes(s.as_bytes())
            .ok_or_else(|| UnknownScorer { name: s.to_owned() })
    }
}
