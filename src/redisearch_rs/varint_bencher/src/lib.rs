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
//! to validate performance characteristics and memory efficiency.

pub use bencher::VarintBencher;

pub mod bencher;
