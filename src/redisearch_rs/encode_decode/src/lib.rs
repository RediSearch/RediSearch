mod buffer;
pub mod varint;
pub use buffer::*;

/// Rust implementation of `t_fieldMask` from `redisearch.h`
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub type FieldMask = u128;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub type FieldMask = u64;
