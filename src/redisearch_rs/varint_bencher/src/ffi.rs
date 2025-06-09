/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module provides FFI bindings for the C varint functions used in benchmarking.
//! The build script generates either real C bindings or dummy stubs depending on library availability.

#![allow(non_snake_case)]
#![allow(improper_ctypes_definitions)]

// We hide the generated bindings in a separate module and reduce warnings.
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(improper_ctypes)]
#[allow(dead_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(clippy::ptr_offset_with_cast)]
#[allow(clippy::upper_case_acronyms)]
#[allow(clippy::useless_transmute)]
#[allow(clippy::multiple_unsafe_ops_per_block)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::{
    Buffer, BufferReader, BufferWriter, MAX_VARINT_LEN, NewBufferReader, NewVarintVectorWriter,
    ReadVarint, ReadVarintFieldMask, ReadVarintFieldMaskNonInline, ReadVarintNonInline, VVW_Free,
    VVW_Truncate, VVW_Write, VarintVectorWriter, WriteVarint, WriteVarintFieldMask, t_fieldMask,
};
