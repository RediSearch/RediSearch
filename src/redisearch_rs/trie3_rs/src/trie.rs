use crate::node::Node;
use std::{cmp::Ordering, ffi::c_char, fmt};

#[derive(Clone)]
/// A trie data structure that maps keys of type `&[c_char]` to values.
pub struct TrieMap<Data> {
    /// The root node of the trie.
    root: Option<Node<Data>>,
}

impl<Data> TrieMap<Data> {
    /// Create a new [`TrieMap`].
    pub fn new() -> Self {
        Self { root: None }
    }

    /// Insert a key-value pair into the trie.
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[c_char], data: Data) -> Option<Data> {
        let mut old_data = None;
        self.insert_with(key, |curr_data| {
            old_data = curr_data;
            data
        });
        old_data
    }

    /// Remove an entry from the trie.
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, key: &[c_char]) -> Option<Data> {
        // If there's no root, there's nothing to remove.
        let root = self.root.as_mut()?;

        let suffix = key.strip_prefix(root.label())?;

        // If the root turns out to be the node that needs removal,
        // we check whether it has any children. If it doesn't, we can
        // simply remove the root node. If it does, we remove the root's
        // data and attempt to merge the children.
        if suffix.is_empty() {
            if root.n_children() == 0 {
                self.root.take().and_then(|mut n| n.data_mut().take())
            } else {
                let data = root.data_mut().take();
                root.merge_child_if_possible();
                data
            }
        } else {
            // If the root is not the node that needs removal, we delegate traverse
            // the trie given the suffix of the key.
            let data = root.remove_child(suffix);
            // After removing the child, we attempt to merge the child into the root.
            root.merge_child_if_possible();
            data
        }
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the no entry for the key is present.
    pub fn find(&self, key: &[c_char]) -> Option<&Data> {
        self.root.as_ref().and_then(|n| n.find(key))
    }

    /// Insert an entry into the trie. The value is obtained by calling the
    /// provided callback function. If the key already exists, the existing
    /// value is passed to the callback, otherwise `None` is passed.
    pub fn insert_with<F>(&mut self, key: &[c_char], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        match &mut self.root {
            None => {
                let data = f(None);
                self.root = Some(Node::new_leaf(key, Some(data)));
            }
            Some(root) => root.insert_or_replace_with(key, f),
        }
    }

    /// Get the memory usage of the trie in bytes.
    /// Includes the memory usage of the root node on the stack.
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.root.as_ref().map(|r| r.mem_usage()).unwrap_or(0)
    }

    pub fn n_nodes(&self) -> usize {
        1 + self.root.as_ref().map_or(0, |r| r.n_nodes())
    }

    /// Get an iterator over the map entries in order of keys.
    pub fn iter(&self) -> Iter<'_, Data> {
        Iter::new(self.root.as_ref())
    }
}

/// Iterates over the entries of a [`TrieMap`] in lexicographical order
/// of the keys.
pub struct Iter<'tm, Data> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<(&'tm Node<Data>, bool)>,
    /// Labels of the parent nodes, used to reconstruct the key.
    prefixes: Vec<&'tm [c_char]>,
}

impl<'tm, Data> Iter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`].
    fn new(root: Option<&'tm Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().map(|node| (node, false)).collect(),
            prefixes: Vec::new(),
        }
    }
}

impl<'tm, Data> Iterator for Iter<'tm, Data> {
    type Item = (Vec<c_char>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (node, was_visited) = self.stack.pop()?;

        if !was_visited {
            let data = node.data();
            self.stack.push((node, true));

            for child in node.children().iter().rev() {
                self.stack.push((child, false));
            }

            self.prefixes.push(node.label());
            if let Some(data) = data {
                // Combine the labels of the parent nodes and the current node,
                // thereby reconstructing the key.
                let label = self
                    .prefixes
                    .iter()
                    .flat_map(|p| p.iter())
                    .copied()
                    .collect();
                return Some((label, data));
            }
        } else {
            self.prefixes.pop();
        }
        self.next()
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

#[cfg(test)]
mod test {
    use std::{ffi::c_void, ptr::NonNull};

    use proptest::prelude::Arbitrary;

    use crate::utils::{ToCCharArray as _, to_string_lossy};

    use super::*;

    /// Forwards to `insta::assert_debug_snapshot!`,
    /// but is disabled in Miri, as snapshot testing
    /// involves file I/O, which is not supported in Miri.
    macro_rules! assert_debug_snapshot {
        ($($arg:tt)*) => {
            #[cfg(not(miri))]
            insta::assert_debug_snapshot!($($arg)*);
        };
    }

    #[test]
    fn test_trie_child_additions() {
        // A minimal case identified by `arbitrary` that used to cause
        // an invalid reference to uninitialized data (UB!).
        let mut trie = TrieMap::new();
        trie.insert(&b"notcxw".c_chars(), 0);
        assert_debug_snapshot!(trie, @r###""notcxw" (0)"###);
        trie.insert(&b"ul".c_chars(), 1);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳n–––"notcxw" (0)
          ↳u–––"ul" (1)
        "###);
        trie.insert(&b"vsvaah".c_chars(), 2);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳n–––"notcxw" (0)
          ↳u–––"ul" (1)
          ↳v–––"vsvaah" (2)
        "###);
        trie.insert(&b"kunjrn".c_chars(), 3);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳k–––"kunjrn" (3)
          ↳n–––"notcxw" (0)
          ↳u–––"ul" (1)
          ↳v–––"vsvaah" (2)
        "###);
    }

    #[test]
    fn test_trie_insertions() {
        let mut trie = TrieMap::new();
        trie.insert(&b"bike".c_chars(), 0);
        assert_debug_snapshot!(trie, @r###""bike" (0)"###);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"cool".c_chars()), None);

        trie.insert(&b"biker".c_chars(), 1);
        assert_debug_snapshot!(trie, @r###"
        "bike" (0)
          ↳r–––"r" (1)
        "###);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"cool".c_chars()), None);

        trie.insert(&b"bis".c_chars(), 2);
        assert_debug_snapshot!(trie, @r###"
        "bi" (-)
          ↳k–––"ke" (0)
                ↳r–––"r" (1)
          ↳s–––"s" (2)
        "###);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), None);

        trie.insert(&b"cool".c_chars(), 3);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳b–––"bi" (-)
                ↳k–––"ke" (0)
                      ↳r–––"r" (1)
                ↳s–––"s" (2)
          ↳c–––"cool" (3)
        "###);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), Some(&3));

        trie.insert(&b"bi".c_chars(), 4);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳b–––"bi" (4)
                ↳k–––"ke" (0)
                      ↳r–––"r" (1)
                ↳s–––"s" (2)
          ↳c–––"cool" (3)
        "###);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), Some(&3));
        assert_eq!(trie.find(&b"bi".c_chars()), Some(&4));

        assert_eq!(trie.remove(&b"cool".c_chars()), Some(3));
        assert_debug_snapshot!(trie, @r###"
        "bi" (4)
          ↳k–––"ke" (0)
                ↳r–––"r" (1)
          ↳s–––"s" (2)
        "###);
        assert_eq!(trie.remove(&b"cool".c_chars()), None);

        assert_eq!(trie.remove(&b"bike".c_chars()), Some(0));
        assert_debug_snapshot!(trie, @r###"
        "bi" (4)
          ↳k–––"ker" (1)
          ↳s–––"s" (2)
        "###);
        assert_eq!(trie.remove(&b"bike".c_chars()), None);

        assert_eq!(trie.remove(&b"biker".c_chars()), Some(1));
        assert_debug_snapshot!(trie, @r###"
        "bi" (4)
          ↳s–––"s" (2)
        "###);
        assert_eq!(trie.remove(&b"biker".c_chars()), None);

        assert_eq!(trie.remove(&b"bi".c_chars()), Some(4));
        assert_debug_snapshot!(trie, @r#"
        "bis" (2)
        "#);
        assert_eq!(trie.remove(&b"bi".c_chars()), None);
    }

    #[test]
    /// Tests what happens when the label you want
    /// to insert is already present.
    fn test_trie_replace() {
        let mut trie = TrieMap::new();
        trie.insert(&b";".c_chars(), 256);
        assert_debug_snapshot!(trie, @r###"";" (256)"###);

        trie.insert(&b";".c_chars(), 0);
        assert_debug_snapshot!(trie, @r###"
        ";" (0)
        "###);
    }

    #[test]
    /// Tests what happens when the data attached to nodes
    /// has a non-trivial `Drop` implementation.
    fn test_trie_with_non_copy_data() {
        let mut trie = TrieMap::new();
        trie.insert(&b";".c_chars(), NonNull::<c_void>::dangling());
        assert_debug_snapshot!(trie, @r###"";" (0x1)"###);
    }

    #[test]
    /// Verify that the cloned trie has an independent
    /// copy of the data—i.e. no double-free on drop.
    fn test_trie_clone() {
        let mut trie = TrieMap::new();
        trie.insert(&b";".c_chars(), NonNull::<c_void>::dangling());
        assert_debug_snapshot!(trie, @r###"";" (0x1)"###);
        let cloned = trie.clone();
        assert_debug_snapshot!(cloned, @r###"";" (0x1)"###);
    }

    #[test]
    /// Tests whether the trie merges nodes
    /// correctly upon removal of entries.
    fn test_trie_merge() {
        let mut trie = TrieMap::new();
        trie.insert(&b"a".c_chars(), 0);
        assert_debug_snapshot!(trie, @r###""a" (0)"###);

        trie.insert(&b"ab".c_chars(), 1);
        assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"b" (1)
        "###);

        trie.insert(&b"abcd".c_chars(), 2);
        assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"b" (1)
                ↳c–––"cd" (2)
        "###);

        assert_eq!(trie.remove(&b"ab".c_chars()), Some(1));
        assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"bcd" (2)
        "###);

        trie.insert(&b"abce".c_chars(), 3);
        assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"bc" (-)
                ↳d–––"d" (2)
                ↳e–––"e" (3)
        "###);

        assert_eq!(trie.remove(&b"abcd".c_chars()), Some(2));
        assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"bce" (3)
        "###);
    }

    #[derive(proptest_derive::Arbitrary, Debug)]
    #[cfg(not(miri))]
    /// Enum representing operations that can be performed on a trie.
    /// Used for in the proptest below.
    enum TrieOperation<Data> {
        Insert(
            #[proptest(strategy = "proptest::collection::vec(97..122 as c_char, 0..10)")]
            Vec<c_char>,
            Data,
        ),
        Remove(
            #[proptest(strategy = "proptest::collection::vec(97..122 as c_char, 0..10)")]
            Vec<c_char>,
        ),
    }

    // Disable the proptest when testing with Miri,
    // as proptest accesses the file system, which is not supported Miri
    #[cfg(not(miri))]
    proptest::proptest! {
        #[test]
        /// Check whether the trie behaves like a [`std::collections::BTreeMap<Vec<c_char>, _>`]
        /// when inserting and removing elements. We can use the `proptest` crate to generate random
        /// operations and check that the trie behaves identically to the `BTreeMap`.
        fn sanity_check(ops: Vec<TrieOperation<i32>>) {
            let mut triemap = TrieMap::new();
            let mut btreemap = std::collections::BTreeMap::new();

            for op in ops {
                match op {
                    TrieOperation::Insert(k, v) => {
                        triemap.insert(&k, v);
                        btreemap.insert(k, v);
                    }
                    TrieOperation::Remove(k) => {
                        triemap.remove(&k);
                        btreemap.remove(&k);
                    },
                }
            }

            let trie_entries = triemap.iter().collect::<Vec<_>>();
            let hash_entries = btreemap.iter().map(|(label, data)| (label.clone(), data)).collect::<Vec<_>>();
            assert_eq!(trie_entries, hash_entries, "TrieMap and BTreeMap should report the same entries");
        }
    }
}
