use std::ffi::c_char;

use crate::node::Node;

#[derive(Clone, PartialEq, Eq)]
pub struct TrieMap<T> {
    root: Node<T>,
}

impl<T> TrieMap<T> {
    pub fn new() -> Self {
        Self {
            root: Node::root(),
        }
    }

    pub fn insert(&mut self, key: &[c_char], data: T) -> Option<T> {
        self.root.add_child(key, data)
    }

    pub fn remove(&mut self, _key: &[c_char]) -> Option<T> {
        // todo:
        None
    }

    // todo: shouldn't be mutable
    pub fn find(&mut self, key: &[c_char]) -> Option<&T> {
        self.root.find(key)
    }
}