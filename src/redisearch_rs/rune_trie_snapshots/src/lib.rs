/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot fixtures capturing the C rune-trie's observable behavior.
//!
//! All tests live under `tests/integration/`. This lib is scaffolding so
//! `build.rs` can emit the `libredisearch_all.a` link directive (gated on
//! the `unittest` dev-dep self-loop in `Cargo.toml`). Removing the lib
//! breaks that plumbing. See `numeric_range_tree` for the same pattern.
