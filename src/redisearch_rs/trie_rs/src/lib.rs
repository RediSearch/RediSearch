#[cfg(feature = "ffi")]
pub mod ffi;

pub mod trie;

pub mod iter;

/// Registers the Redis module allocator
/// as the global allocator for the application.
/// Disabled in tests.
#[cfg(all(feature = "redis_allocator", not(test)))]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;
