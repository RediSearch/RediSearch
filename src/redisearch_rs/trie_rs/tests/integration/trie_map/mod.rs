/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod iter;

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
    assert_debug_snapshot!(trie, @r#""notcxw" (0)"#);
    trie.insert(b"ul", 1);
    assert_debug_snapshot!(trie, @r#"
    "" (-)
      ↳n–––"notcxw" (0)
      ↳u–––"ul" (1)
    "#);
    trie.insert(b"vsvaah", 2);
    assert_debug_snapshot!(trie, @r#"
    "" (-)
      ↳n–––"notcxw" (0)
      ↳u–––"ul" (1)
      ↳v–––"vsvaah" (2)
    "#);
    trie.insert(b"kunjrn", 3);
    assert_debug_snapshot!(trie, @r#"
    "" (-)
      ↳k–––"kunjrn" (3)
      ↳n–––"notcxw" (0)
      ↳u–––"ul" (1)
      ↳v–––"vsvaah" (2)
    "#);
}

fn assert_long_key_operations(length: usize, prefill: Option<&[u8]>) {
    let mut trie = TrieMap::new();
    if let Some(key) = prefill {
        trie.insert(key, 0);
    }
    let key = vec![b'a'; length];
    let mut neighbour = key.clone();
    neighbour[length / 2] = b'b';
    let prefix = &key[..length / 2];
    assert_eq!(trie.insert(&key, 1), None);
    assert_eq!(trie.insert(&neighbour, 2), None);
    assert_eq!(trie.insert(prefix, 3), None);
    assert_eq!(trie.find(&key), Some(&1));
    assert_eq!(trie.find(&neighbour), Some(&2));
    assert_eq!(trie.find(prefix), Some(&3));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
    let mut expected = vec![
        (key.clone(), 1),
        (neighbour.clone(), 2),
        (prefix.to_vec(), 3),
    ];
    if let Some(key) = prefill {
        expected.push((key.to_vec(), 0));
    }
    expected.sort();
    assert_eq!(
        trie.iter().map(|(k, v)| (k, *v)).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        trie.clone()
            .iter()
            .map(|(k, v)| (k, *v))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(trie.remove(prefix), Some(3));
    assert_eq!(trie.remove(&neighbour), Some(2));
    assert_eq!(trie.find(&key), Some(&1));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
    assert_eq!(trie.remove(&key), Some(1));
    if let Some(key) = prefill {
        assert_eq!(trie.remove(key), Some(0));
    }
    assert_eq!(trie.n_unique_keys(), 0);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
}

macro_rules! long_key_tests {
    ($module:ident, $length:expr) => {
        mod $module {
            use super::assert_long_key_operations;

            #[test]
            fn empty_trie() {
                assert_long_key_operations($length, None);
            }

            #[test]
            fn unrelated_prefix() {
                assert_long_key_operations($length, Some(b"unrelated"));
            }

            #[test]
            fn shared_prefix() {
                assert_long_key_operations($length, Some(b"aab"));
            }
        }
    };
}

long_key_tests!(long_keys_at_label_limit, u16::MAX as usize);
long_key_tests!(long_keys_past_label_limit, u16::MAX as usize + 1);
long_key_tests!(long_keys_observed_gc_length, 65541);
long_key_tests!(long_keys_multiple_chunks, 2 * u16::MAX as usize + 1);

#[test]
fn removing_long_keys_reclaims_empty_chunks() {
    for length in [u16::MAX as usize + 1, 2 * u16::MAX as usize + 1] {
        for survivor in [None, Some(b"unrelated".as_slice()), Some(b"a".as_slice())] {
            let mut trie = TrieMap::new();
            let empty_memory = trie.mem_usage();
            if let Some(key) = survivor {
                trie.insert(key, 0);
            }
            let baseline_memory = trie.mem_usage();

            for byte in b'b'..=b'd' {
                let mut key = vec![byte; length];
                key[0] = b'a';
                assert_eq!(trie.insert(&key, 1), None);
                assert_eq!(trie.remove(&key), Some(1));
                assert_eq!(trie.remove(&key), None);
                assert_eq!(trie.mem_usage(), baseline_memory);
                assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
                assert_eq!(trie.n_unique_keys(), usize::from(survivor.is_some()));
                if let Some(key) = survivor {
                    assert_eq!(trie.find(key), Some(&0));
                    assert_eq!(trie.iter().collect::<Vec<_>>(), vec![(key.to_vec(), &0)]);
                } else {
                    assert_eq!(trie.iter().next(), None);
                }
            }

            if let Some(key) = survivor {
                assert_eq!(trie.remove(key), Some(0));
            }
            assert_eq!(trie.mem_usage(), empty_memory);
        }
    }
}

#[test]
fn compaction_counts_accept_a_key_longer_than_one_node_label() {
    let key = vec![b'x'; 65541];
    let mut counts = trie_rs::TrieCount::new();
    counts.increment(&key, 1);
    counts.increment(&key, 2);
    counts.increment(b"short", 4);
    assert_eq!(counts.get(&key), Some(3));
    assert_eq!(counts.n_unique_keys(), 2);
    assert_eq!(
        counts.iter().collect::<Vec<_>>(),
        vec![(b"short".to_vec(), 4), (key, 3)]
    );
}

#[test]
fn test_trie_insertions() {
    let mut trie = TrieMap::new();
    trie.insert(b"bike", 0);
    assert_debug_snapshot!(trie, @r#""bike" (0)"#);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"cool"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    trie.insert(b"biker", 1);
    assert_debug_snapshot!(trie, @r#"
    "bike" (0)
      ↳r–––"r" (1)
    "#);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"cool"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    trie.insert(b"bis", 2);
    assert_debug_snapshot!(trie, @r#"
    "bi" (-)
      ↳k–––"ke" (0)
            ↳r–––"r" (1)
      ↳s–––"s" (2)
    "#);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"bis"), Some(&2));
    assert_eq!(trie.find(b"cool"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    trie.insert(b"cool", 3);
    assert_debug_snapshot!(trie, @r#"
    "" (-)
      ↳b–––"bi" (-)
            ↳k–––"ke" (0)
                  ↳r–––"r" (1)
            ↳s–––"s" (2)
      ↳c–––"cool" (3)
    "#);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"bis"), Some(&2));
    assert_eq!(trie.find(b"cool"), Some(&3));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    trie.insert(b"bi", 4);
    assert_debug_snapshot!(trie, @r#"
    "" (-)
      ↳b–––"bi" (4)
            ↳k–––"ke" (0)
                  ↳r–––"r" (1)
            ↳s–––"s" (2)
      ↳c–––"cool" (3)
    "#);
    assert_eq!(trie.find(b"bike"), Some(&0));
    assert_eq!(trie.find(b"biker"), Some(&1));
    assert_eq!(trie.find(b"bis"), Some(&2));
    assert_eq!(trie.find(b"cool"), Some(&3));
    assert_eq!(trie.find(b"bi"), Some(&4));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    assert_eq!(trie.n_nodes(), 6);

    assert_eq!(trie.remove(b"cool"), Some(3));
    assert_debug_snapshot!(trie, @r#"
    "bi" (4)
      ↳k–––"ke" (0)
            ↳r–––"r" (1)
      ↳s–––"s" (2)
    "#);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    assert_eq!(trie.remove(b"cool"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    assert_eq!(trie.remove(b"bike"), Some(0));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
    assert_debug_snapshot!(trie, @r#"
    "bi" (4)
      ↳k–––"ker" (1)
      ↳s–––"s" (2)
    "#);

    assert_eq!(trie.remove(b"bike"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    assert_eq!(trie.remove(b"biker"), Some(1));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
    assert_debug_snapshot!(trie, @r#"
    "bi" (4)
      ↳s–––"s" (2)
    "#);

    assert_eq!(trie.remove(b"biker"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());

    assert_eq!(trie.remove(b"bi"), Some(4));
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
    assert_debug_snapshot!(trie, @r#""bis" (2)"#);

    assert_eq!(trie.remove(b"bi"), None);
    assert_eq!(trie.mem_usage(), trie.recursive_mem_usage());
}

#[test]
/// Tests what happens when the label you want
/// to insert is already present.
fn test_trie_replace() {
    let mut trie = TrieMap::new();
    trie.insert(b";", 256);
    assert_debug_snapshot!(trie, @r#"";" (256)"#);

    trie.insert(b";", 0);
    assert_debug_snapshot!(trie, @r#"";" (0)"#);
}

#[test]
/// Tests what happens when the data attached to nodes
/// has a non-trivial `Drop` implementation.
fn test_trie_with_non_copy_data() {
    let mut trie = TrieMap::new();
    trie.insert(b";", NonNull::<c_void>::dangling());
    assert_debug_snapshot!(trie, @r#"";" (0x1)"#);
}

#[test]
/// Verify that the cloned trie has an independent identical
/// copy of the data—i.e. no double-free on drop.
fn test_trie_clone() {
    let mut trie = TrieMap::new();
    trie.insert(b";", NonNull::<c_void>::dangling());
    trie.insert(b";hey", NonNull::<c_void>::dangling());
    assert_debug_snapshot!(trie, @r#"
    ";" (0x1)
      ↳h–––"hey" (0x1)
    "#);
    let cloned = trie.clone();
    assert_debug_snapshot!(cloned, @r#"
    ";" (0x1)
      ↳h–––"hey" (0x1)
    "#);
    assert_eq!(trie, cloned);
}

#[test]
/// Tests whether the trie merges nodes
/// correctly upon removal of entries.
fn test_trie_merge() {
    let mut trie = TrieMap::new();
    trie.insert(b"a", 0);
    assert_debug_snapshot!(trie, @r#""a" (0)"#);

    trie.insert(b"ab", 1);
    assert_debug_snapshot!(trie, @r#"
    "a" (0)
      ↳b–––"b" (1)
    "#);

    trie.insert(b"abcd", 2);
    assert_debug_snapshot!(trie, @r#"
    "a" (0)
      ↳b–––"b" (1)
            ↳c–––"cd" (2)
    "#);

    assert_eq!(trie.remove(b"ab"), Some(1));
    assert_debug_snapshot!(trie, @r#"
    "a" (0)
      ↳b–––"bcd" (2)
    "#);

    trie.insert(b"abce", 3);
    assert_debug_snapshot!(trie, @r#"
    "a" (0)
      ↳b–––"bc" (-)
            ↳d–––"d" (2)
            ↳e–––"e" (3)
    "#);

    assert_eq!(trie.remove(b"abcd"), Some(2));
    assert_debug_snapshot!(trie, @r#"
    "a" (0)
      ↳b–––"bce" (3)
    "#);
}

#[derive(proptest_derive::Arbitrary, Debug)]
#[cfg(not(miri))]
/// Enum representing operations that can be performed on a trie.
/// Used for in the proptest below.
enum TrieOperation<Data> {
    Insert(
        #[proptest(strategy = "proptest::collection::vec(97u8..122, 0..10)")] Vec<u8>,
        Data,
    ),
    Remove(#[proptest(strategy = "proptest::collection::vec(97u8..122, 0..10)")] Vec<u8>),
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
            assert_eq!(triemap.n_unique_keys(), triemap.iter().count());
            assert_eq!(triemap.mem_usage(), triemap.recursive_mem_usage());
        }

        let trie_entries = triemap.iter().collect::<Vec<_>>();
        let hash_entries = btreemap.iter().map(|(label, data)| (label.clone(), data)).collect::<Vec<_>>();
        assert_eq!(trie_entries, hash_entries, "TrieMap and BTreeMap should report the same entries");
        assert_eq!(triemap.n_unique_keys(), trie_entries.len());
    }
}
