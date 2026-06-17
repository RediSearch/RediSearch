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
    str_trie_map::iter::unfiltered::key_to_string,
};

/// Suffix-filtered iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// Wrapper-only — [`crate::iter`] has no suffix iterator. Byte `ends_with`
/// on UTF-8 keys agrees with `&str::ends_with` because UTF-8 is
/// self-synchronizing: a multibyte sequence cannot be a suffix of another
/// codepoint. Empty `suffix` yields zero matches by delegating to an empty
/// inner iterator.
///
/// See [`crate::iter::Iter`] for the underlying traversal.
pub struct SuffixedIter<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    iter: iter::Iter<'tm, Data, filter::VisitAll>,
}

impl<'tm, Data: 'tm> SuffixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, suffix: &str) -> Self {
        if suffix.is_empty() {
            return Self {
                target_bytes: Box::new([]),
                iter: iter::Iter::empty(),
            };
        }
        Self {
            target_bytes: suffix.as_bytes().to_vec().into_boxed_slice(),
            iter: trie.iter(),
        }
    }
}

impl<'tm, Data: 'tm> Iterator for SuffixedIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (k, v) = self.iter.next()?;
            if k.ends_with(&self.target_bytes) {
                return Some((key_to_string(k), v));
            }
        }
    }
}
