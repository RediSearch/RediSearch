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

pub(crate) fn to_string_lossy(label: &[c_char]) -> String {
    let slice = label.iter().map(|&c| c as u8).collect::<Vec<_>>();
    String::from_utf8_lossy(&slice).into_owned()
}
