/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use lending_iterator::prelude::*;
use std::{ffi::c_void, time::Instant};
use trie_rs::iter::{WildcardLendingIter, filter::VisitAll};

pub type BoxedPredicate = Box<dyn Fn(&(&[u8], &*mut c_void)) -> bool>;

pub enum TrieMapIteratorImpl<'tm> {
    Plain(trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>),
    Filtered(
        trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>,
        BoxedPredicate,
    ),
    // Boxing to reduce the size of overall enum, since the contains variant
    // is much larger than the others due to how much space `memchr::memmem::Finder`
    // takes on the stack.
    //
    // Both lifetime parameters collapse to `'tm` at the FFI boundary: the
    // trie reference and the target byte slice originate from the same
    // C-side scope.
    Contains(Box<trie_rs::iter::ContainsLendingIter<'tm, 'tm, *mut c_void>>),
    Wildcard(WildcardLendingIter<'tm, 'tm, *mut c_void>),
}

impl TrieMapIteratorImpl<'_> {
    pub fn set_timeout(&mut self, timeout: Option<Instant>) {
        match self {
            Self::Plain(i) => i.set_timeout(timeout),
            Self::Filtered(i, _) => i.set_timeout(timeout),
            Self::Contains(i) => i.set_timeout(timeout),
            Self::Wildcard(i) => i.set_timeout(timeout),
        }
    }
}

#[gat]
impl<'tm> LendingIterator for TrieMapIteratorImpl<'tm> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm *mut c_void);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            TrieMapIteratorImpl::Plain(iter) => LendingIterator::next(iter),
            TrieMapIteratorImpl::Filtered(iter, should_yield) => iter.find(&mut *should_yield),
            TrieMapIteratorImpl::Contains(iter) => {
                let iter: &mut trie_rs::iter::ContainsLendingIter<'_, '_, *mut c_void> = &mut *iter;
                LendingIterator::next(iter)
            }
            TrieMapIteratorImpl::Wildcard(iter) => LendingIterator::next(iter),
        }
    }
}
