/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! COLLECT reducer variants for shard execution and coordinator merge.
//!
//! C parses the reducer arguments and constructs the appropriate variant via
//! `reducers_ffi`.

pub(crate) mod common;
pub mod coord;
pub mod shard;

pub use coord::{CoordCollectCtx, CoordCollectReducer};
pub use shard::{ShardCollectCtx, ShardCollectReducer};
