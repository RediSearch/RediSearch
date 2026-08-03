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
    /// Minimum number of children for a union iterator to use a heap-based
    /// implementation instead of a flat linear scan.
    pub min_union_iter_heap: usize,
}

impl Default for Config {
    fn default() -> Self {
        // Reuse the shared iterator-config default so the union-heap threshold
        // keeps a single source of truth.
        let IteratorsConfig {
            min_union_iter_heap,
            ..
        } = IteratorsConfig::default();
        let min_union_iter_heap = min_union_iter_heap as usize;

        Self {
            numeric_compress: false,
            prioritize_intersect_union_children: false,
            default_scorer: None,
            min_union_iter_heap,
        }
    }
}
