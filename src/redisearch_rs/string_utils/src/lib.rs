/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared string utility functions.
//!
//! These are pure-Rust replacements for C helpers that were previously
//! implemented using `libnu` for Unicode operations.

pub mod runes;
pub mod tag;
pub mod unicode;
