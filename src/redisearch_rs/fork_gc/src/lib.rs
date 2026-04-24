/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe Rust implementation of RediSearch's Fork GC.
//!
//! This crate hosts the protocol- and algorithm-level logic that runs in
//! the forked child and the parent process. Redis-facing concerns (the
//! `FGC_*` C ABI, `RedisModule_*` API calls, process-exit handling) live
//! in the `fork_gc_ffi` crate, which is a thin trampoline on top of this
//! one.

pub mod fork_gc;
pub mod util;
pub mod writer;

pub use fork_gc::ForkGC;
