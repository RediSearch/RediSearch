/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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

/// The half-open range of values `[start, end)` on a [`BucketLevel`](crate::BucketLevel).
///
/// Each end must sit on a bucket boundary or outside the buckets (at or below the first boundary,
/// or at or above the last), so that the range covers whole buckets. A bucket
/// only partly inside the range would hold elements outside it, and the index keeps no value
/// below a bucket level to filter them out.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketRange {
    /// The inclusive start of the range.
    pub start: f64,
    /// The exclusive end of the range.
    pub end: f64,
}
