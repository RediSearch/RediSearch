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
    TooFewBoundaries { got: usize },
    /// A boundary is NaN or infinite.
    #[error("bucket boundary {index} is not a finite number")]
    NotFinite { index: usize },
    /// A boundary is not greater than the one before it.
    #[error("bucket boundary {index} is not greater than the previous one")]
    NotIncreasing { index: usize },
}

/// Why [`CompoundIndex::new`](crate::CompoundIndex::new) rejected a list of levels.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// No routing level: the index would be a single terminal.
    #[error("a compound index needs at least one routing level")]
    NoLevels,
    /// More routing levels than [`MAX_LEVELS`](crate::MAX_LEVELS).
    #[error("a compound index supports at most {max} routing levels, got {got}")]
    TooManyLevels { max: usize, got: usize },
}

/// Why a path of [`Key`](crate::Key)s does not route through the index.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// The path does not have one key per level.
    #[error("expected one key per level ({expected}), got {got}")]
    WrongLength { expected: usize, got: usize },
    /// A tag key on a bucket level, or a numeric key on a tag level.
    #[error("the key for level {level} does not match the level's type")]
    KeyMismatch { level: usize },
    /// A numeric key outside every bucket of its level, or NaN.
    #[error("the value for level {level} is outside the level's buckets")]
    OutsideBuckets { level: usize },
}

/// Why [`CompoundIndex::terminal_or_insert_with`](crate::CompoundIndex::terminal_or_insert_with)
/// rejected a path. A rejected insert leaves the index unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InsertError {
    /// The path itself is invalid.
    #[error(transparent)]
    Path(#[from] PathError),
    /// The path needs a new child under a node that already holds its level's
    /// [`max_cardinality`](crate::TagLevel::max_cardinality) children.
    #[error("level {level} already holds its maximum of {max} distinct values under this path")]
    CardinalityExceeded { level: usize, max: usize },
}

/// Why [`CompoundIndex::collect_terminals`](crate::CompoundIndex::collect_terminals) rejected a
/// query.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CollectError {
    /// The query does not have one constraint per level.
    #[error("expected one constraint per level ({expected}), got {got}")]
    WrongLength { expected: usize, got: usize },
    /// A tag constraint on a bucket level, or a bucket range on a tag level.
    #[error("the constraint for level {level} does not match the level's type")]
    ConstraintMismatch { level: usize },
    /// A [`BucketRange`](crate::BucketRange) end that is not on a bucket boundary.
    #[error("the range for level {level} must start and end on bucket boundaries")]
    UnalignedBucketRange { level: usize },
    /// The walk reached more than the allowed number of terminals.
    #[error("the query reaches more than {max} terminals")]
    TooManyTerminals { max: usize },
}
