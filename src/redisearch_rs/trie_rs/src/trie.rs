/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    iter::{IntoValues, Iter, LendingIter, PrefixesIter, Values, filter::VisitAll},
    node::Node,
    utils::strip_prefix,
};
use std::fmt;

#[derive(Clone, PartialEq, Eq)]
/// A trie data structure that maps keys of type `&[u8]` to values.
pub struct TrieMap<Data> {
    /// The root node of the trie.
    root: Option<Node<Data>>,
    /// The number of unique keys stored in this map.
    n_unique_keys: usize,
}

impl<Data> Default for TrieMap<Data> {
    fn default() -> Self {
        Self {
            root: None,
            n_unique_keys: 0,
        }
    }
}

impl<Data> TrieMap<Data> {
    /// Create a new (empty) [`TrieMap`].
    ///
    /// # Allocations
    ///
    /// No allocation is performed on creation.
    /// Memory is allocated only when the first insertion occurs.
    pub fn new() -> Self {
        Default::default()
    }

    /// Insert a key-value pair into the trie.
    ///
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[u8], data: Data) -> Option<Data> {
        let mut old_data = None;
        self.insert_with(key, |curr_data| {
            old_data = curr_data;
            data
        });
        old_data
    }

    /// Remove an entry from the trie.
    ///
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, key: &[u8]) -> Option<Data> {
        // If there's no root, there's nothing to remove.
        let root = self.root.as_mut()?;

        // The key is not in the trie if the root's label is not a
        // prefix of the key.
        let suffix = strip_prefix(key, root.label())?;

        // If the root turns out to be the node that needs removal,
        // we check whether it has any children. If it doesn't, we can
        // simply remove the root node. If it does, we remove the root's
        // data and attempt to merge the children.
        let data = if suffix.is_empty() {
            if root.n_children() == 0 {
                self.root.take().and_then(|mut n| n.data_mut().take())
            } else {
                let data = root.data_mut().take();
                root.merge_child_if_possible();
                data
            }
        } else {
            // The node we need to remove is deeper in the trie.
            let data = root.remove_descendant(suffix);
            // After removing the child, we attempt to merge the child into the root.
            root.merge_child_if_possible();
            data
        };
        if data.is_some() {
            self.n_unique_keys -= 1;
        }
        data
    }

    /// Get a reference to the value associated with a key.
    ///
    /// Returns `None` if there is no entry for the key.
    pub fn find(&self, key: &[u8]) -> Option<&Data> {
        self.root.as_ref().and_then(|n| n.find(key))
    }

    /// Get a reference to the subtree associated with a key prefix.
    /// Returns `None` if the key prefix is not present.
    fn find_root_for_prefix(&self, key: &[u8]) -> Option<(&Node<Data>, Vec<u8>)> {
        self.root.as_ref().and_then(|n| n.find_root_for_prefix(key))
    }

    /// Insert an entry into the trie.
    ///
    /// The value is obtained by calling the provided callback function.
    /// If the key already exists, the existing value is passed to the callback,
    /// otherwise `f(None)` is inserted.
    pub fn insert_with<F>(&mut self, key: &[u8], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        let mut has_cardinality_increased = false;
        let wrapped_f = |old_data: Option<Data>| {
            if old_data.is_none() {
                has_cardinality_increased = true;
            }
            f(old_data)
        };
        match &mut self.root {
            None => {
                let data = wrapped_f(None);
                self.root = Some(Node::new_leaf(key, Some(data)));
            }
            Some(root) => root.insert_or_replace_with(key, wrapped_f),
        }

        if has_cardinality_increased {
            self.n_unique_keys += 1;
        }
    }

    /// Get the memory usage of the trie in bytes.
    /// Includes the memory usage of the root node on the stack.
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.root.as_ref().map(|r| r.mem_usage()).unwrap_or(0)
    }

    /// The number of unique keys stored in this map.
    pub fn n_unique_keys(&self) -> usize {
        self.n_unique_keys
    }

    /// Compute the number of nodes in the trie.
    pub fn n_nodes(&self) -> usize {
        match &self.root {
            Some(r) => 1 + r.n_descendants(),
            None => 0,
        }
    }

    /// Iterate over the entries, in lexicographical key order.
    pub fn iter(&self) -> Iter<'_, Data, VisitAll> {
        Iter::new(self.root.as_ref(), vec![])
    }

    /// Iterate over all trie entries whose key is a prefix of `target`.
    pub fn prefixes_iter<'a>(&'a self, target: &'a [u8]) -> PrefixesIter<'a, Data> {
        PrefixesIter::new(self.root.as_ref(), target)
    }

    /// Iterate over the entries that start with the given prefix, in lexicographical key order.
    pub fn prefixed_iter(&self, prefix: &[u8]) -> Iter<'_, Data, VisitAll> {
        match self.find_root_for_prefix(prefix) {
            Some((subroot, subroot_prefix)) => Iter::new(Some(subroot), subroot_prefix),
            None => Iter::empty(),
        }
    }

    /// Iterate over the entries, borrowing the current key from the iterator, in lexicographical key order.
    pub fn lending_iter(&self) -> LendingIter<'_, Data, VisitAll> {
        self.iter().into()
    }

    /// Iterate over the entries that start with the given prefix, borrowing the current key from the iterator,
    /// in lexicographical key order.
    pub fn prefixed_lending_iter(&self, prefix: &[u8]) -> LendingIter<'_, Data, VisitAll> {
        self.prefixed_iter(prefix).into()
    }

    /// Iterate over references to the values stored in this trie, in lexicographical key order.
    ///
    /// It won't yield the corresponding keys.
    pub fn values(&self) -> Values<'_, Data> {
        Values::new(self.root.as_ref())
    }

    /// Iterate over the values stored in this trie, in lexicographical key order.
    ///
    /// It won't yield the corresponding keys.
    pub fn into_values(self) -> IntoValues<Data> {
        IntoValues::new(self.root)
    }

    /// Iterate over the values stored in this trie, in lexicographical key order.
    ///
    /// It will only yield the values associated with keys that start with the given prefix.
    /// It won't yield the corresponding keys.
    pub fn prefixed_values(&self, prefix: &[u8]) -> Values<'_, Data> {
        match self.find_root_for_prefix(prefix) {
            Some((root, _)) => Values::new(Some(root)),
            None => Values::new(None),
        }
    }
}

impl<Data: std::fmt::Debug> std::fmt::Debug for TrieMap<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(r) => r.fmt(f),
            None => f.write_str("(empty)"),
        }
    }
}
