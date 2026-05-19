/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Configuration parameters related to query execution.

/// Configuration parameters related to the query execution.
#[cheadergen::config(export, rename_all = "camelCase")]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct IteratorsConfig {
    /// The maximal number of expansions we allow for a prefix. Default: 200
    pub max_prefix_expansions: u32,
    /// The minimal number of characters we allow expansion for in a prefix
    /// search. Default: 2
    pub min_term_prefix: u32,
    /// The minimal word length to stem. Default: 4
    pub min_stem_length: u32,
    /// Minimum number of children for a union iterator to use a heap-based
    /// implementation instead of a flat (linear scan) one. Default: 20
    pub min_union_iter_heap: u32,
}

impl Default for IteratorsConfig {
    fn default() -> Self {
        Self {
            max_prefix_expansions: 200,
            min_term_prefix: 2,
            min_stem_length: 4,
            min_union_iter_heap: 20,
        }
    }
}
