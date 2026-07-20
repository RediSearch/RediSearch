/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the `tag_index` crate, driving `TagIndex` (and the
//! query iterators it builds) through the public API only.

mod create;
mod disk;
mod filtered_iteration;
mod indexing;

// Building the query iterators goes through the FFI-backed mock search
// context, which miri cannot execute.
#[cfg(not(miri))]
mod reader;
