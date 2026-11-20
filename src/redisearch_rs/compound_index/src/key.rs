/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// One element's value on one routing [`Level`](crate::Level).
///
/// A path is a slice of keys, one per level, in the index's level order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Key<'a> {
    /// The value of a [`TagLevel`](crate::TagLevel), already normalised by the caller (case
    /// folding). One element has exactly one value per level.
    Tag(&'a [u8]),
    /// The value of a [`BucketLevel`](crate::BucketLevel). The level decides which bucket it
    /// falls in.
    Numeric(f64),
}
