/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The COLLECT reducer aggregates rows within each group, with optional field
//! projection, sorting, and limiting.
//!
//! Two variants share the same C-visible vtable layout through
//! [`CollectCommon`]:
//!
//! - [`ShardCollectReducer`] runs on each data shard, projecting configured
//!   field values from every row into an `Array<Map>` payload that the
//!   coordinator can unpack.
//! - [`CoordCollectReducer`] runs on the coordinator node, unpacking the
//!   per-shard payloads collected under the `__SOURCE__` key into a single
//!   flat array.
//!
//! Configuration is parsed in C and passed to Rust via the per-variant
//! factory entry points in `reducers_ffi`.

pub mod common;
pub mod coord;
pub mod shard;

pub use common::CollectCommon;
pub use coord::{CoordCollectCtx, CoordCollectReducer};
pub use shard::{ShardCollectCtx, ShardCollectReducer};
