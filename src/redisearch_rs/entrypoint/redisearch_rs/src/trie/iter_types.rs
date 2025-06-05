/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use lending_iterator::{lending_iterator::adapters::Filter, prelude::*};
use std::ffi::c_void;
use trie_rs::iter::{WildcardFilter, filter::VisitAll};

pub type BoxedPredicate = Box<dyn Fn(&(&[u8], &*mut c_void)) -> bool>;

pub enum TrieMapIteratorImpl<'tm> {
    Plain(trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>),
    Filtered(Filter<trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>, BoxedPredicate>),
    // Boxing to reduce the size of overall enum, since the contains variant
    // is much larger than the others due to how much space `memchr::memmem::Finder`
    // takes on the stack.
    Contains(Box<trie_rs::iter::ContainsLendingIter<'tm, *mut c_void>>),
    Wildcard(trie_rs::iter::LendingIter<'tm, *mut c_void, WildcardFilter<'tm>>),
}

#[gat]
#[allow(clippy::needless_lifetimes)]
impl<'tm> LendingIterator for TrieMapIteratorImpl<'tm> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm *mut c_void);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            TrieMapIteratorImpl::Plain(iter) => LendingIterator::next(iter),
            TrieMapIteratorImpl::Filtered(iter) => LendingIterator::next(iter),
            TrieMapIteratorImpl::Contains(iter) => LendingIterator::next(iter),
            TrieMapIteratorImpl::Wildcard(iter) => LendingIterator::next(iter),
        }
    }
}
