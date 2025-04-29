mod buffer;
pub mod varint;
pub use buffer::*;

/// Registers the Redis module allocator
/// as the global allocator for the application.
/// Disabled in tests.
#[cfg(all(feature = "redis_allocator", not(test)))]
#[global_allocator]
static REDIS_MODULE_ALLOCATOR: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;

/// Rust implementation of `t_fieldMask` from `redisearch.h`
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub type FieldMask = u128;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub type FieldMask = u64;
