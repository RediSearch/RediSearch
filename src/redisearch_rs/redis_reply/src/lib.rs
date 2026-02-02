/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// The Redis module API exposes functions via static mutable function pointers.
// Accessing these pointers and calling through them is inherently two unsafe operations,
// but they represent a single semantic operation (call the Redis function).
#![expect(clippy::multiple_unsafe_ops_per_block)]

//! Redis reply abstraction for building Redis protocol responses.
//!
//! This crate provides ergonomic wrappers around the Redis module reply functions,
//! eliminating repetitive `.expect()` calls and manual length tracking for arrays and maps.
//!
//! # Example
//!
//! ```ignore
//! // SAFETY: ctx is a valid Redis module context
//! let mut replier = unsafe { Replier::new(ctx) };
//! let mut arr = replier.array();
//! arr.long_long(tree.num_ranges() as i64);
//! arr.long_long(tree.num_entries() as i64);
//! // Length is automatically set when `arr` is dropped
//! ```

mod array;
mod map;
mod replier;

pub use array::ArrayBuilder;
pub use map::MapBuilder;
pub use replier::{RedisModuleCtx, Replier};
