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
//! The integration tests drive the C trie (linked from `libredisearch_all.a`)
//! through a fixed set of scenarios and assert against `insta` snapshots. The
//! resulting `.snap` files are committed and serve as the oracle for a future
//! Rust port — once the port exists, the same scenarios can be retargeted at
//! the Rust implementation and any divergence surfaces as a snapshot diff.
//!
//! # Why this lib is empty
//!
//! All meaningful code lives under `tests/integration/`. This lib exists
//! purely as scaffolding for the `libredisearch_all.a` link plumbing:
//!
//! - `build.rs` emits `cargo::rustc-link-lib=static:-bundle=redisearch_all`
//!   (gated on the `unittest` feature, via [`build_utils::bind_foreign_c_symbols`]).
//!   That directive can only be carried into the dependency graph through a
//!   Rust artifact's rmeta — i.e. this crate's rlib.
//! - The dev-dep self-loop in `Cargo.toml`
//!   (`rune_trie_snapshots = { path = ".", features = ["unittest"] }`)
//!   activates the `unittest` feature when the integration tests are built.
//!   A self-loop requires a lib target to point at.
//! - `tests/integration/main.rs` then has `extern crate rune_trie_snapshots;`
//!   to force rustc to keep this rlib in the link graph — without that the
//!   integration target references no item from the lib, the rlib is
//!   discarded as unused, and the recorded link directive disappears with it.
//!
//! Removing this lib would mean replumbing all three points. The convention
//! matches `numeric_range_tree`, the workspace's reference example for the
//! "Rust harness driving the C side via the combined static library" pattern.
