/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `redisearch.so` — the core Redis module, built as a `cdylib` so that `rustc`
//! is the final linker and `-C prefer-dynamic` is honoured.
//!
//! It links:
//! - `libsearch_shared.so` (dynamically) — the single shared instance of the
//!   subgraph (`rqe_iterators::SEARCH_ENTERPRISE_ITERATORS`, etc.).
//! - dynamic `libstd` — one allocator / std across core and the disk plugin.
//! - `libredisearch_all.a` (statically, via `build.rs`) — the C search core.
//! - the `redisearch_rs` c_entrypoint hub — the `*_ffi` `extern "C"` symbols the
//!   C core calls.
//!
//! It carries NO speedb / disk / vecsim code. On RoF it `dlopen`s the sibling
//! `redisearch_disk.so` through the strong forwarders in [`disk_forwarder`].

// Keep the shared umbrella dylib as a link edge (single-instance subgraph).
use search_shared as _;
// Keep the core FFI hub: forces the `*_ffi` extern "C" symbols into redisearch.so.
use redisearch_rs as _;

pub mod disk_forwarder;
pub mod module_entry;
