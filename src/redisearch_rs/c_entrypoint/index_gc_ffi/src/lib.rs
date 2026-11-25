/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI entrypoint for the index GC module.
//!
//! Functions in this crate are called from `src/gc/*.c`. This layer is a
//! pure FFI trampoline: it translates raw C inputs into safe Rust types and
//! delegates everything else — including Redis-specific failure handling —
//! to the `index_gc` crate.

mod fork;
mod thread;
mod util;
