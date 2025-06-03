/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_void, ptr::NonNull};
use trie_rs::TrieMap;

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
fn test_counters_on_empty_tries() {
    let trie = TrieMap::<u64>::new();
    assert_eq!(trie.n_nodes(), 0);
    assert_eq!(trie.n_unique_keys(), 0);
}

#[test]
fn test_trie_child_additions() {
    // A minimal case identified by `arbitrary` that used to cause
    // an invalid reference to uninitialized data (UB!).
    let mut trie = TrieMap::new();
    trie.insert(b"notcxw", 0);
    assert_debug_snapshot!(trie, @r###""notcxw" (0)"###);
    trie.insert(b"ul", 1);
    assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳n–––"notcxw" (0)
          ↳u–––"ul" (1)
        "###);
    trie.insert(b"vsvaah", 2);
    assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳n–––"notcxw" (0)
          ↳u–––"ul" (1)
          ↳v–––"vsvaah" (2)
        "###);
    trie.insert(b"kunjrn", 3);
    assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳k–––"kunjrn" (3)
          ↳n–––"notcxw" (0)
          ↳u–––"ul" (1)
          ↳v–––"vsvaah" (2)
        "###);
}

#[test]
#[should_panic(
    expected = "The label length is 65536, which exceeds the maximum allowed length, 65535"
)]
fn test_excessively_long_label() {
    let mut trie = TrieMap::new();
    trie.insert(&[1; u16::MAX as usize + 1], 0);
}

#[test]
fn test_trie_insertions() {
    let mut trie = TrieMap::new();
    trie.insert(b"bike", 0);
    assert_debug_snapshot!(trie, @r###""bike" (0)"###);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"cool"), None);

    trie.insert(b"biker", 1);
    assert_debug_snapshot!(trie, @r###"
        "bike" (0)
          ↳r–––"r" (1)
        "###);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"cool"), None);

    trie.insert(b"bis", 2);
    assert_debug_snapshot!(trie, @r###"
        "bi" (-)
          ↳k–––"ke" (0)
                ↳r–––"r" (1)
          ↳s–––"s" (2)
        "###);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"bis"), Some(&2));
    assert_eq!(trie.find(b"cool"), None);

    trie.insert(b"cool", 3);
    assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳b–––"bi" (-)
                ↳k–––"ke" (0)
                      ↳r–––"r" (1)
                ↳s–––"s" (2)
          ↳c–––"cool" (3)
        "###);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"bis"), Some(&2));
    assert_eq!(trie.find(b"cool"), Some(&3));

    trie.insert(b"bi", 4);
    assert_debug_snapshot!(trie, @r###"
        "" (-)
          ↳b–––"bi" (4)
                ↳k–––"ke" (0)
                      ↳r–––"r" (1)
                ↳s–––"s" (2)
          ↳c–––"cool" (3)
        "###);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"bis"), Some(&2));
    assert_eq!(trie.find(b"cool"), Some(&3));
    assert_eq!(trie.find(b"bi"), Some(&4));

    assert_eq!(trie.n_nodes(), 6);

    assert_eq!(trie.remove(b"cool"), Some(3));
    assert_debug_snapshot!(trie, @r###"
        "bi" (4)
          ↳k–––"ke" (0)
                ↳r–––"r" (1)
          ↳s–––"s" (2)
        "###);
    assert_eq!(trie.remove(b"cool"), None);

    assert_eq!(trie.remove(b"bike"), Some(0));
    assert_debug_snapshot!(trie, @r###"
        "bi" (4)
          ↳k–––"ker" (1)
          ↳s–––"s" (2)
        "###);
    assert_eq!(trie.remove(b"bike"), None);

    assert_eq!(trie.remove(b"biker"), Some(1));
    assert_debug_snapshot!(trie, @r###"
        "bi" (4)
          ↳s–––"s" (2)
        "###);
    assert_eq!(trie.remove(b"biker"), None);

    assert_eq!(trie.remove(b"bi"), Some(4));
    assert_debug_snapshot!(trie, @r#"
        "bis" (2)
        "#);
    assert_eq!(trie.remove(b"bi"), None);
}

#[test]
/// Tests what happens when the label you want
/// to insert is already present.
fn test_trie_replace() {
    let mut trie = TrieMap::new();
    trie.insert(b";", 256);
    assert_debug_snapshot!(trie, @r###"";" (256)"###);

    trie.insert(b";", 0);
    assert_debug_snapshot!(trie, @r###"
        ";" (0)
        "###);
}

#[test]
/// Tests what happens when the data attached to nodes
/// has a non-trivial `Drop` implementation.
fn test_trie_with_non_copy_data() {
    let mut trie = TrieMap::new();
    trie.insert(b";", NonNull::<c_void>::dangling());
    assert_debug_snapshot!(trie, @r###"";" (0x1)"###);
}

#[test]
/// Verify that the cloned trie has an independent identical
/// copy of the data—i.e. no double-free on drop.
fn test_trie_clone() {
    let mut trie = TrieMap::new();
    trie.insert(b";", NonNull::<c_void>::dangling());
    trie.insert(b";hey", NonNull::<c_void>::dangling());
    assert_debug_snapshot!(trie, @r###"
    ";" (0x1)
      ↳h–––"hey" (0x1)
    "###);
    let cloned = trie.clone();
    assert_debug_snapshot!(cloned, @r###"
    ";" (0x1)
      ↳h–––"hey" (0x1)
    "###);
    assert_eq!(trie, cloned);
}

#[test]
/// Tests whether the trie merges nodes
/// correctly upon removal of entries.
fn test_trie_merge() {
    let mut trie = TrieMap::new();
    trie.insert(b"a", 0);
    assert_debug_snapshot!(trie, @r###""a" (0)"###);

    trie.insert(b"ab", 1);
    assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"b" (1)
        "###);

    trie.insert(b"abcd", 2);
    assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"b" (1)
                ↳c–––"cd" (2)
        "###);

    assert_eq!(trie.remove(b"ab"), Some(1));
    assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"bcd" (2)
        "###);

    trie.insert(b"abce", 3);
    assert_debug_snapshot!(trie, @r###"
        "a" (0)
          ↳b–––"bc" (-)
                ↳d–––"d" (2)
                ↳e–––"e" (3)
        "###);

    assert_eq!(trie.remove(b"abcd"), Some(2));
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
        #[proptest(strategy = "proptest::collection::vec(97..122 as u8, 0..10)")] Vec<u8>,
        Data,
    ),
    Remove(#[proptest(strategy = "proptest::collection::vec(97..122 as u8, 0..10)")] Vec<u8>),
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
        assert_eq!(triemap.n_unique_keys(), trie_entries.len());
    }
}
