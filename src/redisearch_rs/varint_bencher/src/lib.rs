/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types and functions for benchmarking varint operations.
//!
//! This crate benchmarks the performance of Rust varint implementation
//! against the original C implementation to validate performance characteristics.

#![allow(non_upper_case_globals)]

use std::ffi::c_void;

// Force the compiler to link the symbols defined in `redis_mock`,
// since they are required by `libvarint.a`.
extern crate redis_mock;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

pub use bencher::VarintBencher;

pub mod bencher;
pub mod c_varint;
pub mod ffi;

// Convenient type aliases for varint operations.
pub type FieldMask = encode_decode::FieldMask;

// Re-export C functions from FFI module for easier access in benchmarks
pub use ffi::{
    NewVarintVectorWriter, VVW_Free, VVW_Truncate, VVW_Write, WriteVarint, WriteVarintFieldMask,
};

// Export C types
pub use ffi::VarintVectorWriter;
