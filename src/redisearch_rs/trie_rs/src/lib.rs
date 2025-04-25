//! A trie map implementation with minimal memory footprint.
//!
//! Check [`TrieMap`]'s documentation for more details.

mod node;
mod trie;
mod utils;

pub use trie::{Iter, TrieMap};
