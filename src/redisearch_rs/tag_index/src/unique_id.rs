/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unique identifier for [`TagIndex`](crate::TagIndex) instances.

use std::sync::atomic::{AtomicU32, Ordering};

/// Global counter for unique tag index IDs.
static UNIQUE_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Unique identifier for a [`TagIndex`](crate::TagIndex) instance.
///
/// Generated from a global atomic counter, so that a tag index dropped and
/// recreated for the same field never carries its predecessor's ID: the fork GC
/// compares the ID it scanned in the child against the live index's, and applies
/// its deltas only when they match.
///
/// Two distinct indexes are guaranteed to have different IDs (until the
/// counter wraps).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct TagUniqueId(u32);

impl TagUniqueId {
    /// Allocate the next unique ID from the global counter.
    pub(crate) fn next() -> Self {
        Self(UNIQUE_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl From<TagUniqueId> for u32 {
    fn from(id: TagUniqueId) -> Self {
        id.0
    }
}
