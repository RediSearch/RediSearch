#![allow(dead_code, unused_imports)]

use std::{ffi::c_char, marker::PhantomData, ops::Deref, ptr::NonNull, usize};

use branching::BranchingNode;
use header::{AllocationHeader, NodeKind};
use leaf::LeafNode;

mod branching;
mod header;
mod leaf;
mod node;
mod trie;

pub use trie::TrieMap;

pub trait ToCCharArray<const N: usize> {
    /// Convenience method to convert a byte array to a C-compatible character array.
    fn c_chars(self) -> [c_char; N];
}

impl<const N: usize> ToCCharArray<N> for [u8; N] {
    #![allow(dead_code)]
    fn c_chars(self) -> [c_char; N] {
        self.map(|b| b as c_char)
    }
}
