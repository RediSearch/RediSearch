/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Fundamental type aliases shared across Rust and C code.

#![expect(non_camel_case_types, reason = "FFI types must match their C names")]

#[cheadergen::config(export)]
pub type t_docId = u64;

#[cheadergen::config(export)]
pub type t_fieldIndex = u16;

#[cfg(target_pointer_width = "64")]
#[cheadergen::config(skip)] // architecture specific
pub type t_fieldMask = u128;

#[cfg(target_pointer_width = "32")]
#[cheadergen::config(skip)] // architecture specific
pub type t_fieldMask = u64;

#[cfg(target_pointer_width = "64")]
pub const RS_FIELDMASK_ALL: t_fieldMask = u128::MAX;

#[cfg(target_pointer_width = "32")]
pub const RS_FIELDMASK_ALL: t_fieldMask = u64::MAX;

pub const RS_INVALID_FIELD_INDEX: t_fieldIndex = 0xFFFF;
