/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::borrow::Cow;

use crate::{TrieMap, iter, str::iter::iter_::key_to_string};

/// Contains-iterator over a [`StrTrieMap`](crate::str::StrTrieMap).
///
/// See [`WildcardIter`](super::WildcardIter) for the two-variant
/// borrowed-vs-drained dispatch contract; this iterator uses the same
/// pattern. Empty targets yield zero matches (mirrors the C
/// `Trie_IterateContains` short-circuit).
pub enum ContainsIter<'tm, 'p, Data: 'tm> {
    Borrowed(Option<iter::ContainsIter<'tm, 'p, Data>>),
    Drained(std::vec::IntoIter<(String, &'tm Data)>),
}

impl<'tm, 'p, Data: 'tm> ContainsIter<'tm, 'p, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, target: &'p str) -> Self {
        if target.is_empty() {
            return Self::Borrowed(None);
        }
        Self::Borrowed(Some(trie.contains_iter(target.as_bytes())))
    }

    pub(crate) fn new_cow(trie: &'tm TrieMap<Data>, target: Cow<'p, str>) -> Self {
        match target {
            Cow::Borrowed(s) => Self::new(trie, s),
            Cow::Owned(s) => {
                if s.is_empty() {
                    return Self::Borrowed(None);
                }
                let drained: Vec<(String, &'tm Data)> = trie
                    .contains_iter(s.as_bytes())
                    .map(|(k, v)| (key_to_string(k), v))
                    .collect();
                Self::Drained(drained.into_iter())
            }
        }
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for ContainsIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Borrowed(Some(inner)) => inner.next().map(|(k, v)| (key_to_string(k), v)),
            Self::Borrowed(None) => None,
            Self::Drained(iter) => iter.next(),
        }
    }
}
