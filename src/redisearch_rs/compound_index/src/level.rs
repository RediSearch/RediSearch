/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::num::NonZeroUsize;

use crate::BucketsError;

/// The most buckets a [`BucketLevel`] accepts.
///
/// Every node on a bucket level holds one slot per bucket, used or not, and a walk over the
/// level reads every slot. The bound keeps both the memory of a node and the work of a walk over
/// it predictable.
pub const MAX_BUCKETS: usize = 1024;

/// A routing level of a [`CompoundIndex`](crate::CompoundIndex).
#[derive(Debug, Clone, PartialEq)]
pub enum Level {
    /// Routes an element by its exact tag value.
    Tag(TagLevel),
    /// Routes an element by the fixed numeric bucket its value falls in.
    Buckets(BucketLevel),
}

/// A level whose children are keyed by an exact tag value.
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

/// A level whose children are fixed numeric buckets.
///
/// Boundaries `b₀ < b₁ < … < bₙ` define the half-open buckets `[b₀, b₁) … [bₙ₋₁, bₙ)`.
/// Buckets never split, so a bucket level behaves like an equality level with
/// [`BucketLevel::num_buckets`] possible children.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketLevel {
    boundaries: Box<[f64]>,
}

impl BucketLevel {
    /// Builds a bucket level from its boundaries.
    ///
    /// The boundaries must be finite and strictly increasing, and define between one and
    /// [`MAX_BUCKETS`] buckets.
    pub fn new(boundaries: Vec<f64>) -> Result<Self, BucketsError> {
        if boundaries.len() < 2 {
            return Err(BucketsError::TooFewBoundaries {
                got: boundaries.len(),
            });
        }
        if boundaries.len() - 1 > MAX_BUCKETS {
            return Err(BucketsError::TooManyBuckets {
                max: MAX_BUCKETS,
                got: boundaries.len() - 1,
            });
        }
        if let Some(index) = boundaries.iter().position(|b| !b.is_finite()) {
            return Err(BucketsError::NotFinite { index });
        }
        if let Some(index) = boundaries.windows(2).position(|w| w[0] >= w[1]) {
            return Err(BucketsError::NotIncreasing { index: index + 1 });
        }
        Ok(Self {
            boundaries: boundaries.into_boxed_slice(),
        })
    }

    /// The boundaries, in increasing order.
    pub fn boundaries(&self) -> &[f64] {
        &self.boundaries
    }

    /// The number of buckets: one fewer than the number of boundaries.
    pub const fn num_buckets(&self) -> usize {
        self.boundaries.len() - 1
    }

    /// The bucket `value` falls in, or [`None`] when it is outside `[b₀, bₙ)` or NaN.
    pub fn bucket_of(&self, value: f64) -> Option<usize> {
        let first = self.boundaries[0];
        let last = self.boundaries[self.boundaries.len() - 1];
        // Written so that NaN, which fails every comparison, lands on `None`.
        if !(value >= first && value < last) {
            return None;
        }
        Some(self.boundaries.partition_point(|b| *b <= value) - 1)
    }
}
