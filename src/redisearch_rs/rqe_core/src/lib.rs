/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Fundamental type aliases shared across Rust and C code.

#[cheadergen::config(export, rename = "t_docId")]
pub type DocId = u64;

#[cheadergen::config(export, rename = "t_fieldIndex")]
pub type FieldIndex = u16;

#[cfg(target_pointer_width = "64")]
#[cheadergen::config(skip, rename = "t_fieldMask")] // architecture specific
pub type FieldMask = u128;

#[cfg(target_pointer_width = "32")]
#[cheadergen::config(skip, rename = "t_fieldMask")] // architecture specific
pub type FieldMask = u64;

#[cfg(target_pointer_width = "64")]
pub const RS_FIELDMASK_ALL: FieldMask = u128::MAX;

#[cfg(target_pointer_width = "32")]
pub const RS_FIELDMASK_ALL: FieldMask = u64::MAX;

pub const RS_INVALID_FIELD_INDEX: FieldIndex = 0xFFFF;
