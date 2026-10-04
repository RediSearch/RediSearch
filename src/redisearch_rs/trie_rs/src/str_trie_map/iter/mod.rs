/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Different iterators to traverse a [`StrTrieMap`](crate::str_trie_map::StrTrieMap).

mod case_insensitive;
mod contains;
mod fuzzy;
mod prefixed;
mod prefixed_values;
mod range;
mod suffixed;
mod unfiltered;
mod wildcard;

pub use case_insensitive::CaseInsensitiveIter;
pub use contains::ContainsIter;
pub use fuzzy::FuzzyIter;
pub use prefixed::PrefixedIter;
pub use prefixed_values::PrefixedValues;
pub use range::{RangeBoundary, RangeFilter, RangeIter};
pub use suffixed::SuffixedIter;
pub use unfiltered::Iter;
pub use wildcard::WildcardIter;

/// Traversal that lends the key it stopped on, rather than handing out an
/// owned [`String`].
///
/// Every key-yielding iterator in this module implements both this trait and
/// [`Iterator`], sharing one traversal: the [`Iterator`] impl is this trait
/// plus a copy of the lent key into a [`String`]. A caller that only reads
/// each key — comparing it, writing it to a buffer, handing it to C — takes
/// this trait instead and leaves that allocation unmade.
///
/// It is deliberately not [`LendingIterator`](lending_iterator::LendingIterator)
/// from the [`lending_iterator`] crate, whose generic associated type buys
/// adapters at the cost of being usable as a `dyn` type. Here the borrow is
/// tied to `&mut self` by ordinary elision, so `dyn LendingStrIter` erases the
/// concrete iterator away.
pub trait LendingStrIter<'tm> {
    /// The payload stored alongside each key of the trie being traversed.
    type Data: 'tm;

    /// Advance to the next entry, yielding its key and payload.
    ///
    /// The key is borrowed from the iterator's own traversal buffer, so it
    /// is only valid until the next call to this method (or until the
    /// iterator is dropped).
    fn next_borrowed(&mut self) -> Option<(&str, &'tm Self::Data)>;
}

/// Read a trie byte key back as a [`str`]. Keys enter the
/// [`StrTrieMap`](crate::str_trie_map::StrTrieMap) exclusively via `&str` so
/// they are UTF-8 by construction; the validating [`str::from_utf8`] call
/// here is cheap and protects against any future raw-byte insertion at the
/// lower layer.
pub(super) fn key_to_str(bytes: &[u8]) -> &str {
    str::from_utf8(bytes).expect("StrTrieMap keys are UTF-8 by construction")
}
