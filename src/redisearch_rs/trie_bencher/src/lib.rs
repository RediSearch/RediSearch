//! Supporting types and functions for benchmarking trie operations.
use std::{ffi::c_void, ptr::NonNull};

pub use bencher::OperationBencher;

pub mod bencher;
pub mod corpus;
pub mod ffi;
mod redis_allocator;

// Convenient aliases for the trie types that are being benchmarked.
pub use ffi::CTrieMap;
pub type RustTrieMap = trie_rs::trie::TrieMap<NonNull<c_void>>;
