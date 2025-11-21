/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{RS_FIELDMASK_ALL, RS_INVALID_FIELD_INDEX, t_fieldIndex, t_fieldMask};

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
/// cbindgen:prefix-with-name=true
/// Type representing either a field mask or field index.
pub enum FieldMaskOrIndex {
    /// For textual fields, allows to host multiple field indices at once.
    Index(t_fieldIndex) = 0,
    /// For the other fields, allows a single field to be referenced.
    Mask(t_fieldMask) = 1,
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
#[derive(Debug)]
#[repr(C)]
/// cbindgen:prefix-with-name
/// cbindgen:rename-all=ScreamingSnakeCase
pub enum FieldExpirationPredicate {
    /// one of the fields need to be valid.
    Default = 0,
    /// one of the fields need to be expired for the entry to be considered missing.
    Missing = 1,
}

/// Field filter context used when querying fields.
#[derive(Debug)]
#[repr(C)]
pub struct FieldFilterContext {
    /// the field mask or index to filter on.
    pub field: FieldMaskOrIndex,
    /// our field expiration predicate.
    pub predicate: FieldExpirationPredicate,
}
