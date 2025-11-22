/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#![feature(generic_const_exprs)]
#![allow(incomplete_features)]

//! HyperLogLog implementation for Redisearch in Rust

mod hll;

pub use hll::HLL;
