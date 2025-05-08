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
use trie_rs::iter::filter::{VisitAll, WildcardFilter};

pub type BoxedPredicate = Box<dyn Fn(&(&[u8], &*mut c_void)) -> bool>;

pub enum TrieMapIteratorImpl<'tm> {
    Plain(trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>),
    Filtered(Filter<trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>, BoxedPredicate>),
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
            TrieMapIteratorImpl::Wildcard(iter) => LendingIterator::next(iter),
        }
    }
}
