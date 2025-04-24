//! Different iterators to traverse a [`TrieMap`](crate::TrieMap).
pub mod filter;
mod iter_;
mod lending;
mod values;

pub use iter_::Iter;
pub use lending::LendingIter;
pub use values::Values;

#[cfg(test)]
mod test {
    #[cfg(not(miri))]
    proptest::proptest! {
        #[test]
        /// Test whether the [`super::Iter`] iterator yields the same results as the BTreeMap entries iterator.
        fn test_iter(entries: std::collections::BTreeMap<Vec<std::ffi::c_char>,i32> ) {
            let mut trie = crate::trie::TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<std::ffi::c_char>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();
            let btree_entries: Vec<(Vec<std::ffi::c_char>, i32)> = entries.iter().map(|(k, v)| (k.clone(), *v)).collect();

            assert_eq!(trie_entries, btree_entries);
        }

        #[test]
        /// Test whether the [`super::Values`] iterator yields the same results as the BTreeMap values iterator.
        fn test_values(entries: std::collections::BTreeMap<Vec<std::ffi::c_char>,i32> ) {
            let mut trie = crate::trie::TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }

            let trie_values: Vec<i32> = trie.values().copied().collect();
            let btree_values: Vec<i32> = entries.into_values().collect();

            assert_eq!(trie_values, btree_values);
        }
    }
}
