/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Why [`CompoundIndex::new`](crate::CompoundIndex::new) rejected a list of levels.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// No routing level: the index would be a single terminal.
    #[error("a compound index needs at least one routing level")]
    NoLevels,
    /// More routing levels than [`MAX_LEVELS`](crate::MAX_LEVELS).
    #[error("a compound index supports at most {max} routing levels, got {got}")]
    TooManyLevels {
        /// The limit, [`MAX_LEVELS`](crate::MAX_LEVELS).
        max: usize,
        /// The number of levels given.
        got: usize,
    },
}

/// Inserting a path, or a batch of them, would give a node more children than its level's
/// [`max_cardinality`](crate::TagLevel::max_cardinality). Returned by
/// [`CompoundIndex::get_or_insert_with`](crate::CompoundIndex::get_or_insert_with) and
/// [`CompoundIndex::check_paths`](crate::CompoundIndex::check_paths); nothing is inserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("level {level} already holds its maximum of {max} distinct values under this path")]
pub struct CardinalityExceeded {
    /// The position of the level.
    pub level: usize,
    /// The level's limit.
    pub max: usize,
}

/// Why a [`CompoundIndex::collect_terminals`](crate::CompoundIndex::collect_terminals) walk
/// failed. It never returns part of the terminals.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CollectError {
    /// The walk reached more than the allowed number of terminals.
    #[error("the query reaches more than {max} terminals")]
    TooManyTerminals {
        /// The limit the walk was given.
        max: usize,
    },
    /// The walk's [`TimeoutChecker`](timeout::TimeoutChecker) reported a timeout.
    #[error("the query timed out while walking the index")]
    TimedOut,
}
