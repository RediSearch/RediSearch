/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrappers around the `VecSim*` C API exposed by `deps/VectorSimilarity`.
//!
//! The wrappers cover the surface needed by the top-K hybrid query source:
//! single-shot top-K queries, batch iteration, and the two adhoc-BF execution
//! paths (RAM with tiered-index shared locks, disk with a preprocessed
//! `VecSimAdhocBfCtx`). Each owned VecSim handle becomes a Rust struct that
//! frees the handle in `Drop`, and the lock ordering between a
//! `VecSimQueryReply` and its iterator is encoded in [`ReplyResults`].
//!
//! The non-owning [`IndexRef`] is the entry point; everything else is
//! produced from it.

mod batch;
mod index;
mod params;
mod reply;

pub use batch::BatchIterator;
pub use index::{AdhocBfCtx, IndexRef, QueryVector, SharedLockGuard};
pub use params::{QueryError, ReplyOrder};
pub use reply::{QueryReply, ReplyResults};
