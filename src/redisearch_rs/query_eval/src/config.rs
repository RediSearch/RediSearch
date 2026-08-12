/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Configuration threaded through query evaluation.

use rqe_iterators::IteratorsConfig;

use crate::scorers::BuiltInScorer;

/// Global configuration values consumed by the query evaluator.
///
/// These are snapshotted from the process-wide configuration once, at the FFI
/// entry point, and threaded through evaluation as an explicit parameter rather
/// than read from global state deep inside the dispatcher.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Whether numeric inverted indexes use the compressed on-disk encoding.
    pub numeric_compress: bool,
    /// Whether an intersection prioritizes its union children when ordering its
    /// sub-iterators for evaluation.
    pub prioritize_intersect_union_children: bool,
    /// The configured default scorer, applied to a query that sets no scorer of
    /// its own.
    ///
    /// [`None`] when no built-in default is configured (either unset or a custom
    /// scorer name), in which case a query with no scorer of its own is treated
    /// as a custom scorer.
    pub default_scorer: Option<BuiltInScorer>,
    /// Minimum pattern length (in bytes) a prefix/suffix/contains query must
    /// reach before it is expanded; shorter patterns produce no matches.
    pub min_term_prefix: u32,
    /// Maximum number of terms a prefix/suffix/contains query expands to before
    /// the trie walk stops and a warning is recorded.
    pub max_prefix_expansions: usize,
    /// Minimum number of children for a union iterator to use a heap-based
    /// implementation instead of a flat linear scan.
    pub min_union_iter_heap: usize,
}

impl Default for Config {
    fn default() -> Self {
        // Reuse the shared iterator-config defaults so the prefix-expansion knobs
        // keep a single source of truth.
        let IteratorsConfig {
            min_term_prefix,
            max_prefix_expansions,
            min_union_iter_heap,
            ..
        } = IteratorsConfig::default();
        let max_prefix_expansions = max_prefix_expansions as usize;
        let min_union_iter_heap = min_union_iter_heap as usize;

        Self {
            numeric_compress: false,
            prioritize_intersect_union_children: false,
            default_scorer: None,
            min_term_prefix,
            max_prefix_expansions,
            min_union_iter_heap,
        }
    }
}
