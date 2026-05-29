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
    str::iter::iter_::key_to_string,
};

pub struct SuffixedIter<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    iter: Option<iter::Iter<'tm, Data, filter::VisitAll>>,
}

impl<'tm, Data: 'tm> SuffixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, suffix: &str) -> Self {
        if suffix.is_empty() {
            return Self {
                target_bytes: Box::new([]),
                iter: None,
            };
        }
        Self {
            target_bytes: suffix.as_bytes().to_vec().into_boxed_slice(),
            iter: Some(trie.iter()),
        }
    }
}

impl<'tm, Data: 'tm> Iterator for SuffixedIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.as_mut()?;
        loop {
            let (k, v) = iter.next()?;
            if k.ends_with(&self.target_bytes) {
                return Some((key_to_string(k), v));
            }
        }
    }
}
