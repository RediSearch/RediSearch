//! A trie map implementation with minimal memory footprint.
//!
//! Check [`TrieMap`]'s documentation for more details.

mod node;
mod trie;
mod utils;

#[cfg(feature = "ffi")]
/// FFI bindings to invoke [`TrieMap`] methods from C code.
pub mod ffi;
pub use trie::{Iter, TrieMap};

/// Registers the Redis module allocator
/// as the global allocator for the application.
/// Disabled in tests.
#[cfg(all(feature = "redis_allocator", not(test)))]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;
