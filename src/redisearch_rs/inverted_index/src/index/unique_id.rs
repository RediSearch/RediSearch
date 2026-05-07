/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unique identifier for [`InvertedIndex`](crate::InvertedIndex) instances.

use std::sync::atomic::{AtomicU32, Ordering};

/// Global counter for unique inverted index IDs.
static UNIQUE_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Unique identifier for an [`InvertedIndex`](crate::InvertedIndex) instance.
///
/// Generated from a global atomic counter and assigned at construction time.
/// Used together with pointer comparison to detect the ABA problem: when an
/// index is freed and a new one is allocated at the same address, the unique
/// IDs will differ, allowing cursors to detect the replacement.
///
/// Two distinct indexes are guaranteed to have different IDs (until the
/// counter wraps).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct IndexUniqueId(u32);

impl IndexUniqueId {
    /// Allocate the next unique ID from the global counter.
    pub(crate) fn next() -> Self {
        Self(UNIQUE_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}
