/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Drive [`TrieMap::automaton_iter`] with an automaton defined outside the
//! crate, pinning the public extension point: the [`Automaton`] trait is
//! implementable by consumers, and the [`Automaton::literal_prefix`] subtree
//! jump yields the same entries as a plain root descent.

use trie_rs::TrieMap;
use trie_rs::iter::{Automaton, StateClass};

/// Accepts exactly the keys equal to `needle`, byte for byte. The state is
/// the number of needle bytes matched so far. `advertise_prefix` toggles the
/// [`Automaton::literal_prefix`] hint (the full needle) so tests can compare
/// the subtree-jump path against the root descent.
struct ExactBytes {
    needle: Vec<u8>,
    advertise_prefix: bool,
}

impl Automaton for ExactBytes {
    type State = usize;

    fn start(&self) -> usize {
        0
    }

    fn step(&mut self, state: &usize, byte: u8) -> Option<usize> {
        (self.needle.get(*state) == Some(&byte)).then_some(state + 1)
    }

    fn classify(&self, state: &usize) -> StateClass {
        if *state == self.needle.len() {
            StateClass::Terminal
        } else {
            StateClass::Live
        }
    }

    fn literal_prefix(&self) -> Option<&[u8]> {
        self.advertise_prefix.then_some(&self.needle)
    }
}

fn build_trie() -> TrieMap<u32> {
    let mut trie = TrieMap::new();
    for (i, word) in [
        b"".as_ref(),
        b"ban",
        b"banana",
        b"band",
        b"bandana",
        b"apple",
    ]
    .into_iter()
    .enumerate()
    {
        trie.insert(word, i as u32);
    }
    trie
}

fn matches(trie: &TrieMap<u32>, needle: &[u8], advertise_prefix: bool) -> Vec<Vec<u8>> {
    let automaton = ExactBytes {
        needle: needle.to_vec(),
        advertise_prefix,
    };
    trie.automaton_iter(automaton).map(|(k, _)| k).collect()
}

#[test]
fn custom_automaton_yields_full_keys() {
    let trie = build_trie();

    // "band" sits below the "ban" node: the prefix jump must still yield the
    // full key, not the suffix relative to the subtree root.
    assert_eq!(matches(&trie, b"band", true), vec![b"band".to_vec()]);
}

#[test]
fn prefix_jump_agrees_with_root_descent() {
    let trie = build_trie();

    for needle in [
        b"".as_ref(),
        b"ban",
        b"banana",
        b"band",
        b"bandana",
        b"bandanas",
        b"apple",
        b"app",
        b"zebra",
    ] {
        assert_eq!(
            matches(&trie, needle, true),
            matches(&trie, needle, false),
            "prefix jump and root descent disagree for {needle:?}",
        );
    }
}

#[test]
fn absent_prefix_yields_nothing() {
    let trie = build_trie();

    assert!(matches(&trie, b"zebra", true).is_empty());
}

#[test]
fn empty_trie_yields_nothing() {
    let trie = TrieMap::<u32>::new();

    assert!(matches(&trie, b"anything", true).is_empty());
    assert!(matches(&trie, b"anything", false).is_empty());
}
