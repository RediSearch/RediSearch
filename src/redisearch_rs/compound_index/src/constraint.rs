/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::Bound;

/// What a query asks of one routing [`Level`](crate::Level).
///
/// [`CompoundIndex::collect_terminals`](crate::CompoundIndex::collect_terminals) takes one
/// constraint per level. The tag variants apply to a [`TagLevel`](crate::TagLevel),
/// [`Constraint::BucketRange`] to a [`BucketLevel`](crate::BucketLevel), and [`Constraint::Any`]
/// to either.
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint<'a> {
    /// Every child of the level.
    Any,
    /// The children whose tag value is in the set. Repeated values count once.
    TagIn(Vec<&'a [u8]>),
    /// The children whose tag value is not in the set.
    TagNotIn(Vec<&'a [u8]>),
    /// The children whose tag value starts with the prefix.
    TagPrefix(&'a [u8]),
    /// The buckets inside a range of values.
    BucketRange(BucketRange),
}

/// A numeric range on a [`BucketLevel`](crate::BucketLevel), with ends like a numeric query
/// range: inclusive (`[lo`), exclusive (`(lo`), or unbounded (`-inf`, `+inf`).
///
/// The range selects the buckets lying wholly inside it. A bucket only partly inside it would
/// hold elements outside the range, and the index keeps no value below a bucket level to filter
/// them out, so such a range is rejected with
/// [`CollectError::UnalignedBucketRange`](crate::CollectError::UnalignedBucketRange). Over the
/// buckets `[2022, 2024)` and `[2024, 2026)`, `[2022 (2024]` selects the first bucket, while
/// `[2022 2024]` is rejected: it includes 2024, the first value of the second bucket. An end that
/// is NaN is rejected too, and an empty range selects no bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketRange {
    /// The lower end of the range.
    pub start: Bound<f64>,
    /// The upper end of the range.
    pub end: Bound<f64>,
}
