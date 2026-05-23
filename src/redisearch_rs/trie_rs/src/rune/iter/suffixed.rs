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
    rune::{Rune, bytes_to_rune, rune_to_bytes},
};

pub struct RuneTrieMapSuffixedIter<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    iter: Option<iter::Iter<'tm, Data, filter::VisitAll>>,
}

impl<'tm, Data: 'tm> RuneTrieMapSuffixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, suffix: &[Rune]) -> Self {
        if suffix.is_empty() {
            return Self {
                target_bytes: Box::new([]),
                iter: None,
            };
        }
        Self {
            target_bytes: rune_to_bytes(suffix).into_boxed_slice(),
            iter: Some(trie.iter()),
        }
    }
}

impl<'tm, Data: 'tm> Iterator for RuneTrieMapSuffixedIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.as_mut()?;
        loop {
            let (k, v) = iter.next()?;
            if k.ends_with(&self.target_bytes) {
                return Some((bytes_to_rune(&k), v));
            }
        }
    }
}
