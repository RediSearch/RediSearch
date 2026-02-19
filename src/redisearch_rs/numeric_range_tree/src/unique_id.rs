/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unique identifier for [`NumericRangeTree`](crate::NumericRangeTree) instances.

use std::sync::atomic::{AtomicU32, Ordering};

/// Global counter for unique tree IDs.
static UNIQUE_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Unique identifier for a [`NumericRangeTree`](crate::NumericRangeTree) instance.
///
/// Generated from a global atomic counter.
/// Two distinct trees are guaranteed to have different IDs (until the
/// counter wraps).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct TreeUniqueId(u32);

impl TreeUniqueId {
    /// Allocate the next unique ID from the global counter.
    pub(crate) fn next() -> Self {
        Self(UNIQUE_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for TreeUniqueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TreeUniqueId> for u32 {
    fn from(id: TreeUniqueId) -> Self {
        id.0
    }
}
