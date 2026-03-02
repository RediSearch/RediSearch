/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod core;
pub mod opaque;
mod with_entries;
mod with_mask;

pub use self::core::*;
pub use with_entries::EntriesTrackingIndex;
pub use with_mask::FieldMaskTrackingIndex;

/// Types that contain or wrap an [`InvertedIndex<E>`] and can provide a
/// reference to the underlying index.
pub trait HasInnerIndex<E> {
    /// Get a reference to the underlying [`InvertedIndex`].
    fn inner_index(&self) -> &InvertedIndex<E>;
}

impl<E> HasInnerIndex<E> for InvertedIndex<E> {
    fn inner_index(&self) -> &InvertedIndex<E> {
        self
    }
}

impl<E: crate::Encoder> HasInnerIndex<E> for FieldMaskTrackingIndex<E> {
    fn inner_index(&self) -> &InvertedIndex<E> {
        self.inner()
    }
}
