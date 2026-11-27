/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::num::NonZeroUsize;

/// A routing level of a [`CompoundIndex`](crate::CompoundIndex): its children are keyed by an
/// exact tag value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TagLevel {
    max_cardinality: Option<NonZeroUsize>,
}

impl TagLevel {
    /// A tag level with no limit on the number of distinct values.
    pub const fn new() -> Self {
        Self {
            max_cardinality: None,
        }
    }

    /// A tag level whose every node holds at most `max` distinct values.
    ///
    /// The limit is per node, not per level: two parents may each hold `max` children. That is
    /// what bounds the fan-out of a walk through this level.
    pub const fn with_max_cardinality(max: NonZeroUsize) -> Self {
        Self {
            max_cardinality: Some(max),
        }
    }

    /// The per-node limit set by [`TagLevel::with_max_cardinality`], if any.
    pub const fn max_cardinality(&self) -> Option<NonZeroUsize> {
        self.max_cardinality
    }
}
