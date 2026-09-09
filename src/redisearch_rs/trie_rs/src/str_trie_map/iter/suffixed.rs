/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    TrieMap,
    iter::{self, filter},
    str_trie_map::iter::{LendingStrIter, key_to_str},
};

/// Suffix-filtered iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// Wrapper-only — [`crate::iter`] has no suffix iterator. Byte `ends_with`
/// on UTF-8 keys agrees with [`str::ends_with`] because UTF-8 is
/// self-synchronizing: a multibyte sequence cannot be a suffix of another
/// codepoint. Empty `suffix` yields every entry — the empty string is a
/// suffix of every key.
///
/// See [`crate::iter::Iter`] for the underlying traversal.
pub struct SuffixedIter<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    iter: iter::Iter<'tm, Data, filter::VisitAll>,
}

impl<'tm, Data: 'tm> SuffixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, suffix: &str) -> Self {
        Self {
            target_bytes: suffix.as_bytes().to_vec().into_boxed_slice(),
            iter: trie.iter(),
        }
    }

    /// An iterator that yields no entries, for callers whose suffix
    /// semantics differ from this iterator's on some input — see
    /// [`StrTrieMap::suffixed_iter`](crate::str_trie_map::StrTrieMap::suffixed_iter)
    /// for what it does with an empty suffix.
    pub fn empty() -> Self {
        Self {
            target_bytes: Box::new([]),
            iter: iter::Iter::empty(),
        }
    }
}

impl<'tm, Data: 'tm> SuffixedIter<'tm, Data> {
    /// Advance the underlying traversal to the next key ending in the target
    /// suffix, skipping the keys that do not.
    fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let data = self.iter.advance()?;
            if self.iter.key().ends_with(&self.target_bytes) {
                return Some(data);
            }
        }
    }
}

impl<'tm, Data: 'tm> LendingStrIter<'tm> for SuffixedIter<'tm, Data> {
    type Data = Data;

    fn next_borrowed(&mut self) -> Option<(&str, &'tm Data)> {
        let data = self.advance()?;
        Some((key_to_str(self.iter.key()), data))
    }
}

impl<'tm, Data: 'tm> Iterator for SuffixedIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.next_borrowed()?;
        Some((key.to_owned(), data))
    }
}
