/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`LendingStrIter`] is the same traversal as the [`Iterator`] view of each
//! [`StrTrieMap`] iterator, so the two must agree entry for entry. Agreement
//! alone cannot catch a key the traversal assembled wrongly — both views would
//! report it identically — so the walks that assemble one from a filtered
//! traversal are also pinned against literal keys.

use trie_rs::str_trie_map::{
    StrTrieMap,
    iter::{ContainsIter, LendingStrIter, RangeBoundary, RangeFilter, SuffixedIter},
};

fn seeded() -> StrTrieMap<i32> {
    let mut trie = StrTrieMap::new();
    for (i, key) in ["apple", "applesauce", "banana", "grape", "pineapple"]
        .into_iter()
        .enumerate()
    {
        trie.insert(key, i as i32);
    }
    trie
}

/// Drain a lending iterator, copying each lent key so the result can be
/// compared with [`owned`].
fn lent<'tm, I: LendingStrIter<'tm, Data = i32>>(mut iter: I) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    while let Some((key, value)) = iter.next_borrowed() {
        out.push((key.to_owned(), *value));
    }
    out
}

fn owned<'tm>(iter: impl Iterator<Item = (String, &'tm i32)>) -> Vec<(String, i32)> {
    iter.map(|(key, value)| (key, *value)).collect()
}

#[test]
fn lending_and_owning_views_agree() {
    let trie = seeded();
    let range = RangeFilter {
        min: Some(RangeBoundary::included("b")),
        max: Some(RangeBoundary::excluded("q")),
    };

    assert_eq!(lent(trie.iter()), owned(trie.iter()));
    assert_eq!(
        lent(trie.prefixed_iter("apple")),
        owned(trie.prefixed_iter("apple"))
    );
    assert_eq!(
        lent(trie.suffixed_iter("apple")),
        owned(trie.suffixed_iter("apple"))
    );
    assert_eq!(
        lent(trie.contains_iter("nan")),
        owned(trie.contains_iter("nan"))
    );
    assert_eq!(
        lent(trie.case_insensitive_iter("APPLE")),
        owned(trie.case_insensitive_iter("APPLE"))
    );
    assert_eq!(
        lent(trie.wildcard_iter("*apple")),
        owned(trie.wildcard_iter("*apple"))
    );
    assert_eq!(
        lent(trie.fuzzy_iter("aple", 1)),
        owned(trie.fuzzy_iter("aple", 1))
    );
    assert_eq!(lent(trie.range_iter(range)), owned(trie.range_iter(range)));

    // A walk that finds nothing must lend nothing, rather than lend the key
    // its traversal stopped on.
    assert_eq!(lent(trie.prefixed_iter("kiwi")), Vec::new());
    assert_eq!(lent(SuffixedIter::<i32>::empty()), Vec::new());
    assert_eq!(lent(ContainsIter::<i32>::empty()), Vec::new());
}

/// The filtering walks pick their key out of a traversal that also visits
/// non-matching keys, so the key each one lends is pinned here rather than
/// only compared against the view that shares that traversal.
#[test]
fn filtering_walks_lend_the_key_they_matched_on() {
    let trie = seeded();

    assert_eq!(
        lent(trie.suffixed_iter("apple")),
        vec![("apple".to_owned(), 0), ("pineapple".to_owned(), 4)]
    );
    assert_eq!(
        lent(trie.wildcard_iter("*apple")),
        vec![("apple".to_owned(), 0), ("pineapple".to_owned(), 4)]
    );
    assert_eq!(
        lent(trie.fuzzy_iter("aple", 1)),
        vec![("apple".to_owned(), 0)]
    );
}

/// A wildcard pattern past every NFA bitset width falls back to filtering
/// whole candidate keys, which is an advance path of its own. The pattern
/// below has 131 atoms (129 literals, `*`, `b`), so its 132 positions clear
/// the widest bitset.
#[test]
fn lending_wildcard_skips_rejected_candidates_on_the_filter_backend() {
    // Both keys sit under the pattern's literal prefix, so the candidate
    // walk visits them and only the pattern itself can tell them apart.
    let prefix = "a".repeat(129);
    let hit = format!("{prefix}xb");
    let miss = format!("{prefix}xc");
    let mut trie = StrTrieMap::new();
    trie.insert(&hit, 1);
    trie.insert(&miss, 2);
    let pattern = format!("{prefix}*b");

    let hits = lent(trie.wildcard_iter(&pattern));

    assert_eq!(hits, vec![(hit, 1)]);
}

/// The C entry points erase these iterators behind one trait object, so
/// [`LendingStrIter`] has to stay dyn-compatible.
#[test]
fn a_boxed_iterator_still_lends() {
    let trie = seeded();
    let mut erased: Box<dyn LendingStrIter<'_, Data = i32>> = Box::new(trie.prefixed_iter("apple"));

    let mut hits = Vec::new();
    while let Some((key, value)) = erased.next_borrowed() {
        hits.push((key.to_owned(), *value));
    }

    assert_eq!(hits, owned(trie.prefixed_iter("apple")));
}
