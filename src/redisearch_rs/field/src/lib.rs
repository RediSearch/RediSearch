/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{t_fieldIndex, t_fieldMask};

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
