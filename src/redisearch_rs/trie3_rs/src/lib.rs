#![allow(dead_code, unused_imports)]

use std::{ffi::c_char, marker::PhantomData, ops::Deref, ptr::NonNull, usize};

use header::NodeHeader;

mod header;
mod layout;
mod node;
mod trie;
mod utils;

pub use trie::TrieMap;
