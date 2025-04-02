use crate::node::{Either, Node};
use std::{cmp::Ordering, ffi::c_char, fmt};

#[derive(Clone, PartialEq, Eq)]
/// A trie data structure that maps keys of type `&[c_char]` to values.
pub struct TrieMap<T> {
    /// The root node of the trie.
    root: Node<T>,
}

impl<T> TrieMap<T> {
    /// Create a new [`TrieMap`].
    pub fn new() -> Self {
        Self { root: Node::root() }
    }

    /// Insert a key-value pair into the trie.
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[c_char], data: T) -> Option<T> {
        self.root.add_child(key, data)
    }

    /// Remove an entry from the trie.
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, _key: &[c_char]) -> Option<T> {
        todo!()
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the no entry for the key is present.
    pub fn find(&self, key: &[c_char]) -> Option<&T> {
        self.root.find(key)
    }

    // /// Get the memory usage of the trie in bytes.
    // /// Includes the memory usage of the root node on the stack.
    // pub fn mem_usage(&self) -> usize {
    //     std::mem::size_of::<Self>() + self.root.mem_usage()
    // }

    // pub fn num_nodes(&self) -> usize {
    //     1 + self.root.num_nodes()
    // }

    /// Get an iterator over the map entries in order of keys.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter::new(&self.root)
    }
}

impl<T: fmt::Debug> fmt::Debug for TrieMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.root)
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
    fn new(root: &'tm Node<Data>) -> Self {
        Self {
            stack: vec![(root, false)],
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
            self.prefixes.push(&node.label());

            if let Either::Right(branching) = node.cast_ref() {
                let iter = branching.children().iter().rev();
                let skip_first = branching.has_empty_labeled_child();
                for (i, child) in iter.enumerate() {
                    if skip_first && i == 0 {
                        continue;
                    }
                    self.stack.push((&child, false));
                }
            }

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::ToCCharArray;

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
    fn test_trie() {
        let mut trie = TrieMap::new();
        trie.insert(&b"bike".c_chars(), 0);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"cool".c_chars()), None);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳––––"bike" (0)
        "###);

        trie.insert(&b"biker".c_chars(), 1);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"cool".c_chars()), None);
        assert_debug_snapshot!(trie, @r#"
        "bike" (0)
          ↳––––"r" (1)
        "#);

        trie.insert(&b"bis".c_chars(), 2);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), None);
        assert_debug_snapshot!(trie, @r#"
        "bi" (-)
          ↳––––"ke" (0)
                ↳––––"r" (1)
          ↳––––"s" (2)
        "#);

        trie.insert(&b"cool".c_chars(), 3);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), Some(&3));
        assert_debug_snapshot!(trie, @r#"
        "" (-)
          ↳––––"bi" (-)
                ↳––––"ke" (0)
                      ↳––––"r" (1)
                ↳––––"s" (2)
          ↳––––"cool" (3)
        "#);

        trie.insert(&b"bi".c_chars(), 4);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), Some(&3));
        assert_eq!(trie.find(&b"bi".c_chars()), Some(&4));
        assert_debug_snapshot!(trie, @r#"
        "" (-)
          ↳––––"bi" (4)
                ↳––––"ke" (0)
                      ↳––––"r" (1)
                ↳––––"s" (2)
          ↳––––"cool" (3)
        "#);

        // assert_eq!(trie.remove(&b"cool".c_chars()), Some(3));
        // assert_debug_snapshot!(trie, @r#"
        // "bi" (4)
        //   ↳––––"ke" (0)
        //         ↳––––"r" (1)
        //   ↳––––"s" (2)
        // "#);
        // assert_eq!(trie.remove(&b"cool".c_chars()), None);

        // assert_eq!(trie.remove(&b"bike".c_chars()), Some(0));
        // assert_debug_snapshot!(trie, @r#"
        // "bi" (4)
        //   ↳––––"ker" (1)
        //   ↳––––"s" (2)
        // "#);
        // assert_eq!(trie.remove(&b"bike".c_chars()), None);

        // assert_eq!(trie.remove(&b"biker".c_chars()), Some(1));
        // assert_debug_snapshot!(trie, @r#"
        // "bi" (4)
        //   ↳––––"s" (2)
        // "#);
        // assert_eq!(trie.remove(&b"biker".c_chars()), None);

        // assert_eq!(trie.remove(&b"bi".c_chars()), Some(4));
        // assert_debug_snapshot!(trie, @r#"
        // "bis" (2)
        // "#);
        // assert_eq!(trie.remove(&b"bi".c_chars()), None);
    }

    #[test]
    /// Tests whether the trie merges nodes
    /// correctly upon removal of entries.
    fn test_trie_merge() {
        let mut trie = TrieMap::new();
        trie.insert(&b"a".c_chars(), 0);
        assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳––––"a" (0)
        "###);

        trie.insert(&b"ab".c_chars(), 1);
        trie.insert(&b"abcd".c_chars(), 2);
        assert_debug_snapshot!(trie, @r#"
        "a" (0)
          ↳––––"b" (1)
                ↳––––"cd" (2)
        "#);

        // assert_eq!(trie.remove(&b"ab".c_chars()), Some(1));
        // assert_debug_snapshot!(trie, @r#"
        // "a" (0)
        //   ↳––––"bcd" (2)
        // "#);

        // trie.insert(&b"abce".c_chars(), 3);
        // assert_debug_snapshot!(trie, @r#"
        // "a" (0)
        //   ↳––––"bc" (-)
        //         ↳––––"d" (2)
        //         ↳––––"e" (3)
        // "#);

        // assert_eq!(trie.remove(&b"abcd".c_chars()), Some(2));
        // assert_debug_snapshot!(trie, @r#"
        // "a" (0)
        //   ↳––––"bce" (3)
        // "#);
    }

    #[derive(proptest_derive::Arbitrary, Debug)]
    #[cfg(not(miri))]
    /// Enum representing operations that can be performed on a trie.
    /// Used for in the proptest below.
    enum TrieOperation<Data> {
        Insert(Vec<c_char>, Data),
        // Remove(Vec<c_char>),
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
            let mut hashmap = std::collections::BTreeMap::new();

            for op in ops {
                match op {
                    TrieOperation::Insert(k, v) => {
                        triemap.insert(&k, v);
                        hashmap.insert(k, v);
                    }
                    // TrieOperation::Remove(k) => {
                    //     triemap.remove(&k);
                    //     hashmap.remove(&k);
                    // },
                }
            }

            let trie_entries = triemap.iter().collect::<Vec<_>>();
            let hash_entries = hashmap.iter().map(|(label, data)| (label.clone(), data)).collect::<Vec<_>>();
            assert_eq!(trie_entries, hash_entries);
        }
    }
}
