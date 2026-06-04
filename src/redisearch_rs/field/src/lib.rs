/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_core::{FieldIndex, FieldMask, RS_FIELDMASK_ALL, RS_INVALID_FIELD_INDEX};

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
#[cheadergen::config(prefix_with_name)]
/// Type representing either a field mask or field index.
pub enum FieldMaskOrIndex {
    /// For textual fields, allows to host multiple field indices at once.
    Index(FieldIndex) = 0,
    /// For the other fields, allows a single field to be referenced.
    Mask(FieldMask) = 1,
}

impl FieldMaskOrIndex {
    /// Creates a new [`FieldMaskOrIndex::Index`] with an invalid index.
    pub const fn index_invalid() -> Self {
        Self::Index(RS_INVALID_FIELD_INDEX)
    }

    /// Creates a new [`FieldMaskOrIndex::Mask`] covering all masks.
    pub const fn mask_all() -> Self {
        Self::Mask(RS_FIELDMASK_ALL)
    }
}

/// Field expiration predicate used when checking fields.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(C)]
#[cheadergen::config(export, prefix_with_name, rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FieldExpirationPredicate {
    /// one of the fields need to be valid.
    Default = 0,
    /// one of the fields need to be expired for the entry to be considered missing.
    Missing = 1,
}

impl FieldExpirationPredicate {
    /// Returns the raw value of the expiration predicate.
    pub const fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Field filter context used when querying fields.
#[derive(Debug)]
#[repr(C)]
#[cheadergen::config(export)]
pub struct FieldFilterContext {
    /// the field mask or index to filter on.
    pub field: FieldMaskOrIndex,
    /// our field expiration predicate.
    pub predicate: FieldExpirationPredicate,
}
