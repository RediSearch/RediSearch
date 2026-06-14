/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::borrow::Cow;

use rqe_wildcard::WildcardPattern;

use crate::{TrieMap, iter, str_trie_map::iter::unfiltered::key_to_string};

/// Wildcard iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap).
///
/// Two-variant dispatch:
///
/// - [`WildcardIter::Borrowed`] wraps a lazy inner iterator that borrows
///   the caller's pattern directly. Used for the common case (e.g. a
///   pre-lowercased pattern handed to
///   [`StrTrieMap::wildcard_iter_owned`](super::super::StrTrieMap::wildcard_iter_owned)).
/// - [`WildcardIter::Drained`] returns an iterator over a pre-collected
///   `Vec`. Used when the caller passes a [`Cow::Owned`] pattern: the
///   inner iterator is built and drained inside [`Self::new_cow`] under
///   the owned buffer's stack frame, so the pattern bytes never need to
///   outlive the call. Items yield `String` keys and `&'tm Data`
///   references — both survive the drain because they borrow the trie's
///   value storage, not the (now-dropped) pattern bytes.
pub enum WildcardIter<'tm, 'p, Data: 'tm> {
    Borrowed(iter::Utf8WildcardIter<'tm, 'p, Data>),
    Drained(std::vec::IntoIter<(String, &'tm Data)>),
}

impl<'tm, 'p, Data: 'tm> WildcardIter<'tm, 'p, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, pattern: &'p str) -> Self {
        Self::Borrowed(trie.wildcard_iter_utf8(WildcardPattern::parse(pattern.as_bytes())))
    }

    pub(crate) fn new_cow(trie: &'tm TrieMap<Data>, pattern: Cow<'p, str>) -> Self {
        match pattern {
            Cow::Borrowed(s) => Self::new(trie, s),
            Cow::Owned(s) => {
                let drained: Vec<(String, &'tm Data)> = trie
                    .wildcard_iter_utf8(WildcardPattern::parse(s.as_bytes()))
                    .map(|(k, v)| (key_to_string(k), v))
                    .collect();
                Self::Drained(drained.into_iter())
            }
        }
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for WildcardIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Borrowed(inner) => inner.next().map(|(k, v)| (key_to_string(k), v)),
            Self::Drained(iter) => iter.next(),
        }
    }
}
