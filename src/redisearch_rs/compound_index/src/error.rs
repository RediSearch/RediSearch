/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Why [`BucketLevel::new`](crate::BucketLevel::new) rejected a list of boundaries.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BucketsError {
    /// Fewer than two boundaries, so no bucket.
    #[error("a bucket level needs at least 2 boundaries, got {got}")]
    TooFewBoundaries {
        /// The number of boundaries given.
        got: usize,
    },
    /// More buckets than [`MAX_BUCKETS`](crate::MAX_BUCKETS).
    #[error("a bucket level supports at most {max} buckets, got {got}")]
    TooManyBuckets {
        /// The limit, [`MAX_BUCKETS`](crate::MAX_BUCKETS).
        max: usize,
        /// The number of buckets the boundaries define.
        got: usize,
    },
    /// A boundary is NaN or infinite.
    #[error("bucket boundary {index} is not a finite number")]
    NotFinite {
        /// The position of the boundary.
        index: usize,
    },
    /// A boundary is not greater than the one before it.
    #[error("bucket boundary {index} is not greater than the previous one")]
    NotIncreasing {
        /// The position of the boundary.
        index: usize,
    },
}

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

/// Why a path of [`Key`](crate::Key)s does not route through the index.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// The path does not have one key per level.
    #[error("expected one key per level ({expected}), got {got}")]
    WrongLength {
        /// The number of levels.
        expected: usize,
        /// The number given.
        got: usize,
    },
    /// A tag key on a bucket level, or a numeric key on a tag level.
    #[error("the key for level {level} does not match the level's type")]
    KeyMismatch {
        /// The position of the level.
        level: usize,
    },
    /// A numeric key outside every bucket of its level, or NaN.
    #[error("the value for level {level} is outside the level's buckets")]
    OutsideBuckets {
        /// The position of the level.
        level: usize,
    },
}

/// Why [`CompoundIndex::get_or_insert_with`](crate::CompoundIndex::get_or_insert_with)
/// rejected a path. A rejected insert leaves the index unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InsertError {
    /// The path itself is invalid.
    #[error(transparent)]
    Path(#[from] PathError),
    /// The path needs a new child under a node that already holds its level's
    /// [`max_cardinality`](crate::TagLevel::max_cardinality) children.
    #[error("level {level} already holds its maximum of {max} distinct values under this path")]
    CardinalityExceeded {
        /// The position of the level.
        level: usize,
        /// The level's limit.
        max: usize,
    },
}

/// Why [`CompoundIndex::collect_terminals`](crate::CompoundIndex::collect_terminals) rejected a
/// query.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CollectError {
    /// The query does not have one constraint per level.
    #[error("expected one constraint per level ({expected}), got {got}")]
    WrongLength {
        /// The number of levels.
        expected: usize,
        /// The number given.
        got: usize,
    },
    /// A tag constraint on a bucket level, or a bucket range on a tag level.
    #[error("the constraint for level {level} does not match the level's type")]
    ConstraintMismatch {
        /// The position of the level.
        level: usize,
    },
    /// A [`BucketRange`](crate::BucketRange) that covers part of a bucket, or has a NaN end.
    #[error("the range for level {level} must cover whole buckets")]
    UnalignedBucketRange {
        /// The position of the level.
        level: usize,
    },
    /// The walk reached more than the allowed number of terminals.
    #[error("the query reaches more than {max} terminals")]
    TooManyTerminals {
        /// The limit the walk was given.
        max: usize,
    },
    /// The walk entered more than the allowed number of nodes.
    #[error("the query enters more than {max} index nodes")]
    TooManyNodes {
        /// The limit the walk was given.
        max: usize,
    },
}
