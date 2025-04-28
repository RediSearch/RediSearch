use super::{Iter, filter::TraversalFilter};
use lending_iterator::prelude::*;
use std::ffi::c_char;

/// Iterates over the entries of a [`TrieMap`](crate::TrieMap) in lexicographical order, with minimal cloning.
///
/// Unlike [`Iter`], this iterator lets you borrow the current key, rather than having to clone it.
///
/// Invoke [`TrieMap::lending_iter`](crate::TrieMap::lending_iter) or
/// [`TrieMap::prefixed_lending_iter`](crate::TrieMap::prefixed_lending_iter)
/// to create an instance of this iterator.
pub struct LendingIter<'tm, Data, F>(Iter<'tm, Data, F>);

impl<'tm, Data, F> From<Iter<'tm, Data, F>> for LendingIter<'tm, Data, F> {
    fn from(iter: Iter<'tm, Data, F>) -> Self {
        LendingIter(iter)
    }
}

impl<'a, Data, F> LendingIter<'a, Data, F>
where
    F: TraversalFilter,
{
    /// Change the traversal filter used by this iterator.
    pub fn traversal_filter<F1>(self, f: F1) -> LendingIter<'a, Data, F1> {
        LendingIter(self.0.traversal_filter(f))
    }
}

// The [`LendingIterator`] trait allows us to obtain a reference to
// the key corresponding to the value, which is stored in `Iter::prefixes`.
// The [`Iterator`] trait does not allow for its `Item` to be a reference
// to the Iterator itself.
//
// Why do we need a crate? Well: <https://sabrinajewson.org/blog/the-better-alternative-to-lifetime-gats>
#[gat]
// The 'tm lifetime parameter is not actually needless.
#[allow(clippy::needless_lifetimes)]
impl<'tm, Data, F> LendingIterator for LendingIter<'tm, Data, F>
where
    F: TraversalFilter,
{
    type Item<'next>
    where
        Self: 'next,
    = (&'next [c_char], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let item = self.0.advance()?;
        Some((self.0.key(), item))
    }
}
