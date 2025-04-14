//! Supporting types and functions for benchmarking trie operations.
use std::{ffi::c_void, ptr::NonNull};

pub use bencher::OperationBencher;

pub mod bencher;
mod c_map;
pub mod corpus;

// Convenient aliases for the trie types that are being benchmarked.
pub use c_map::CTrieMap;
pub type RustTrieMap = trie_rs::trie::TrieMap<NonNull<c_void>>;
