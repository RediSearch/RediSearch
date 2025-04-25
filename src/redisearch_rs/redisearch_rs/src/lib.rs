//! `redisearch_rs` is the entrypoint for all the modules, on the C side, who need to consume functionality
//! that's implemented in Rust.
//!
//! It exposes an FFI module for each workspace crate that must be consumed (directly) by the C code.
// The feature flag is needed to disable this module when linking `redisearch_rs.a` against the C code,
// since including it would cause a conflict between `deps/triemap.c` and the symbols defined
// in the `trie` module—they satisfy the same header file, `deps/triemap.h`.
// We will enable it unconditionally once `deps/triemap.c` is removed in favour of the `trie` module.
#[cfg(feature = "trie")]
pub mod trie;

#[cfg(feature = "iterator")]
pub mod iterator;

/// Registers the Redis module allocator as the global allocator for the application.
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;
