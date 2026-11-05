/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe Rust implementation of RediSearch's index garbage collection.
//!
//! [`scanners`] holds the per-index-kind collection and application logic,
//! shared by every GC policy. [`fork`] holds what is specific to the forking
//! policy: the pipe, its framing, and the child/parent orchestration. Redis-facing concerns (the
//! `ForkGC_*` C ABI, `RedisModule_*` API calls, process-exit handling) live
//! in the `index_gc_ffi` crate, which is a thin trampoline on top of this
//! one.

pub mod fork;
pub mod handle;
pub mod scanners;
pub mod spec_access;
pub mod stats;

pub use fork::doc_existence::DocExistence;
pub use fork::frame::Frame;
pub use fork::pipe::ForkPipe;
pub use handle::{GcError, HandleOutcome};
pub use stats::GcApplyStats;
