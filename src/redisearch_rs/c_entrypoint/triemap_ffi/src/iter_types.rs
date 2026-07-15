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
use trie_rs::iter::{WildcardLendingIter, filter::VisitAll};

pub type BoxedPredicate = Box<dyn Fn(&(&[u8], &*mut c_void)) -> bool>;

/// A lending iterator over `(key, value address)` pairs where the value type
/// has been erased.
///
/// Lets [`TrieMapIterator`](super::TrieMapIterator) iterate Rust tries whose
/// values are stored inline (rather than as `*mut c_void`), handing C a
/// pointer to each inline value.
pub trait ErasedValuesIter {
    /// Advance to the next entry, returning its key and the address of its
    /// value. The key slice is invalidated by the next call.
    fn next(&mut self) -> Option<(&[u8], *mut c_void)>;
}

/// Adapter erasing the value type of a [`trie_rs::iter::LendingIter`], yielding
/// the address of each inline value instead of the value itself.
pub struct InlineValuesIter<'tm, V>(pub trie_rs::iter::LendingIter<'tm, V, VisitAll>);

impl<'tm, V> ErasedValuesIter for InlineValuesIter<'tm, V> {
    fn next(&mut self) -> Option<(&[u8], *mut c_void)> {
        LendingIterator::next(&mut self.0)
            .map(|(k, v)| (k, std::ptr::from_ref(v).cast_mut().cast()))
    }
}

/// Like [`InlineValuesIter`], for a [`trie_rs::iter::ContainsLendingIter`]
/// (entries whose key contains a target fragment).
pub struct InlineContainsIter<'tm, V>(pub trie_rs::iter::ContainsLendingIter<'tm, 'tm, V>);

impl<'tm, V> ErasedValuesIter for InlineContainsIter<'tm, V> {
    fn next(&mut self) -> Option<(&[u8], *mut c_void)> {
        LendingIterator::next(&mut self.0)
            .map(|(k, v)| (k, std::ptr::from_ref(v).cast_mut().cast()))
    }
}

/// Like [`InlineValuesIter`], for a [`WildcardLendingIter`]
/// (entries whose key matches a wildcard pattern).
pub struct InlineWildcardIter<'tm, V>(pub WildcardLendingIter<'tm, 'tm, V>);

impl<'tm, V> ErasedValuesIter for InlineWildcardIter<'tm, V> {
    fn next(&mut self) -> Option<(&[u8], *mut c_void)> {
        LendingIterator::next(&mut self.0)
            .map(|(k, v)| (k, std::ptr::from_ref(v).cast_mut().cast()))
    }
}

/// Key predicate for [`InlineFilteredIter`]. Owns whatever state it needs
/// (e.g. the suffix pattern), unlike [`BoxedPredicate`] whose captures collapse
/// to the trie lifetime at the FFI boundary.
pub type BoxedInlinePredicate<V> = Box<dyn Fn(&(&[u8], &V)) -> bool>;

/// Like [`InlineValuesIter`], filtered by a predicate on the entries (used for
/// suffix mode, where the trie offers no dedicated traversal).
pub struct InlineFilteredIter<'tm, V>(
    pub Filter<trie_rs::iter::LendingIter<'tm, V, VisitAll>, BoxedInlinePredicate<V>>,
);

impl<'tm, V> ErasedValuesIter for InlineFilteredIter<'tm, V> {
    fn next(&mut self) -> Option<(&[u8], *mut c_void)> {
        LendingIterator::next(&mut self.0)
            .map(|(k, v)| (k, std::ptr::from_ref(v).cast_mut().cast()))
    }
}

pub enum TrieMapIteratorImpl<'tm> {
    Plain(trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>),
    Filtered(Filter<trie_rs::iter::LendingIter<'tm, *mut c_void, VisitAll>, BoxedPredicate>),
    // Boxing to reduce the size of overall enum, since the contains variant
    // is much larger than the others due to how much space `memchr::memmem::Finder`
    // takes on the stack.
    //
    // Both lifetime parameters collapse to `'tm` at the FFI boundary: the
    // trie reference and the target byte slice originate from the same
    // C-side scope.
    Contains(Box<trie_rs::iter::ContainsLendingIter<'tm, 'tm, *mut c_void>>),
    Wildcard(WildcardLendingIter<'tm, 'tm, *mut c_void>),
    // A trie whose values are stored inline, with the value type erased:
    // yields the address of each value instead of a stored pointer.
    Erased(Box<dyn ErasedValuesIter + 'tm>),
}

#[gat]
impl<'tm> LendingIterator for TrieMapIteratorImpl<'tm> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], *mut c_void);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            TrieMapIteratorImpl::Plain(iter) => LendingIterator::next(iter).map(|(k, v)| (k, *v)),
            TrieMapIteratorImpl::Filtered(iter) => {
                LendingIterator::next(iter).map(|(k, v)| (k, *v))
            }
            TrieMapIteratorImpl::Contains(iter) => {
                LendingIterator::next(iter).map(|(k, v)| (k, *v))
            }
            TrieMapIteratorImpl::Wildcard(iter) => {
                LendingIterator::next(iter).map(|(k, v)| (k, *v))
            }
            TrieMapIteratorImpl::Erased(iter) => iter.next(),
        }
    }
}
