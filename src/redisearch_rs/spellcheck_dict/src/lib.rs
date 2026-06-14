/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

#[derive(Debug, Default)]
pub struct SpellCheckDictionary {
    trie: StrTrieMap<()>,
}

impl SpellCheckDictionary {
    pub const fn new() -> Self {
        Self {
            trie: StrTrieMap::new(),
        }
    }

    pub fn add(&mut self, term: &str) -> bool {
        self.trie.insert(term, ()).is_none()
    }

    pub fn remove(&mut self, term: &str) -> bool {
        self.trie.remove(term).is_some()
    }

    pub const fn len(&self) -> usize {
        self.trie.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    pub fn dump(&self) -> impl Iterator<Item = String> {
        self.trie.iter().map(|(term, ())| term)
    }

    pub fn contains(&self, term: &str) -> bool {
        let needle = unicode_tolower(term);
        self.trie
            .iter()
            .any(|(key, ())| unicode_tolower(&key) == needle)
    }

    pub fn fuzzy_matches(&self, _term: &str, _max_dist: u32) -> impl Iterator<Item = String> + '_ {
        todo!("needs fold-aware edit-distance iteration on StrTrieMap (shared with sp->terms)");
        #[expect(unreachable_code, reason = "establishes the return type for the stub")]
        std::iter::empty()
    }
}

fn unicode_tolower(s: &str) -> String {
    s.chars().flat_map(char::to_lowercase).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;
    use std::collections::BTreeSet;

    #[rstest]
    #[case(&["Hello"], "Hello", true)]
    #[case(&["Hello"], "hello", true)]
    #[case(&["Hello"], "HELLO", true)]
    #[case(&["Hello"], "world", false)]
    #[case(&["Fußball"], "fußball", true)]
    #[case(&["И"], "и", true)]
    fn contains_folds_case(#[case] stored: &[&str], #[case] query: &str, #[case] expected: bool) {
        let mut sut = SpellCheckDictionary::new();
        for term in stored {
            sut.add(term);
        }

        assert_eq!(sut.contains(query), expected);
    }

    #[test]
    fn remove_is_case_sensitive_but_contains_is_not() {
        let mut sut = SpellCheckDictionary::new();
        sut.add("Foo");

        assert!(!sut.remove("foo"));
        assert!(sut.contains("foo"));

        assert!(sut.remove("Foo"));
        assert!(!sut.contains("foo"));
    }

    proptest! {
        #[test]
        fn add_then_contains_roundtrip(term in "\\PC{1,8}") {
            let mut sut = SpellCheckDictionary::new();
            sut.add(&term);

            prop_assert!(sut.contains(&term));
        }

        #[test]
        fn tracks_set_model(ops in prop::collection::vec((any::<bool>(), "[a-zA-Z]{1,5}"), 0..50)) {
            let mut sut = SpellCheckDictionary::new();
            let mut model = BTreeSet::new();

            for (is_add, term) in ops {
                if is_add {
                    prop_assert_eq!(sut.add(&term), model.insert(term.clone()));
                } else {
                    prop_assert_eq!(sut.remove(&term), model.remove(&term));
                }
            }

            prop_assert_eq!(sut.len(), model.len());
            prop_assert_eq!(sut.dump().collect::<BTreeSet<_>>(), model);
        }
    }
}
