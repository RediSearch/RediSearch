//! A trie map implementation with minimal memory footprint.
//!
//! Check [`TrieMap`]'s documentation for more details.

mod node;
mod trie;
mod utils;

pub use trie::{Iter, TrieMap};

#[cfg(feature = "ffi")]
pub mod ffi;

/// Registers the Redis module allocator
/// as the global allocator for the application.
/// Disabled in tests.
#[cfg(all(feature = "redis_allocator", not(test)))]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;
