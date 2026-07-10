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
    str_trie_map::iter::unfiltered::key_to_str,
};
use lending_iterator::prelude::*;

/// Suffix-filtered iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// Wrapper-only — [`crate::iter`] has no suffix iterator. Byte `ends_with`
/// on UTF-8 keys agrees with `&str::ends_with` because UTF-8 is
/// self-synchronizing: a multibyte sequence cannot be a suffix of another
/// codepoint. Empty `suffix` yields zero matches by delegating to an empty
/// inner iterator.
///
/// An ends-with predicate can never prune a subtree — any key may still gain
/// a matching descendant — so the scan visits every entry either way. The
/// inner [`LendingIter`](crate::iter::LendingIter) borrows each candidate
/// key in place; only yielded keys are allocated into `String`s.
pub struct SuffixedIter<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    iter: iter::LendingIter<'tm, Data, filter::VisitAll>,
}

impl<'tm, Data: 'tm> SuffixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, suffix: &str) -> Self {
        if suffix.is_empty() {
            return Self {
                target_bytes: Box::new([]),
                iter: iter::Iter::empty().into(),
            };
        }
        Self {
            target_bytes: suffix.as_bytes().to_vec().into_boxed_slice(),
            iter: trie.lending_iter(),
        }
    }
}

impl<'tm, Data: 'tm> Iterator for SuffixedIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (k, v) = LendingIterator::next(&mut self.iter)?;
            if k.ends_with(&self.target_bytes) {
                return Some((key_to_str(k).to_owned(), v));
            }
        }
    }
}
