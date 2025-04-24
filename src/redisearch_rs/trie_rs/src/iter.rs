use std::ffi::c_char;

use crate::trie::Node;
use lending_iterator::prelude::*;
use wildcard::MatchOutcome;

/// Iterates over the entries of a [`crate::trie::TrieMap`] in lexicographical order
/// of the keys.
/// Can be instantiated by calling [`crate::trie::TrieMap::iter`]
/// or [`crate::trie::TrieMap::iter_prefix`]
pub struct Iter<'tm, Data, F> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<(&'tm Node<Data>, bool)>,
    /// Determine if the current node should be yielded and
    /// if its children should be visited.
    filter: F,
    /// Concatention of the labels of current node and its ancestors,
    /// i.e. the key of the current node.
    key: Vec<c_char>,
}

impl<'a, Data, F> Iter<'a, Data, F>
where
    F: TraversalFilter,
{
    pub fn with_traversal_filter<F1>(self, f: F1) -> Iter<'a, Data, F1> {
        let Self { stack, key, .. } = self;
        Iter {
            stack,
            filter: f,
            key,
        }
    }
}

#[derive(Clone, Copy)]
pub struct FilterOutcome {
    yield_current: bool,
    visit_descendants: bool,
}

impl From<MatchOutcome> for FilterOutcome {
    fn from(value: MatchOutcome) -> Self {
        match value {
            wildcard::MatchOutcome::Match => FilterOutcome {
                yield_current: true,
                visit_descendants: true,
            },
            wildcard::MatchOutcome::PartialMatch => FilterOutcome {
                yield_current: false,
                visit_descendants: true,
            },
            wildcard::MatchOutcome::NoMatch => FilterOutcome {
                yield_current: false,
                visit_descendants: false,
            },
        }
    }
}

pub trait TraversalFilter {
    fn filter(&self, key: &[c_char]) -> FilterOutcome;
}

pub struct VisitAll;

impl TraversalFilter for VisitAll {
    fn filter(&self, _key: &[c_char]) -> FilterOutcome {
        FilterOutcome {
            yield_current: true,
            visit_descendants: true,
        }
    }
}

pub struct WildcardFilter<'a>(pub wildcard::WildcardPattern<'a, c_char>);

impl TraversalFilter for WildcardFilter<'_> {
    fn filter(&self, key: &[c_char]) -> FilterOutcome {
        self.0.matches(key).into()
    }
}

pub struct WildcardFixedLenFilter<'a>(pub wildcard::WildcardPattern<'a, c_char>);

impl TraversalFilter for WildcardFixedLenFilter<'_> {
    fn filter(&self, key: &[c_char]) -> FilterOutcome {
        self.0.matches(key).into()
    }
}

impl<'tm, Data, F> Iter<'tm, Data, F>
where
    F: TraversalFilter,
{
    /// Creates a new iterator over the entries of a [`crate::trie::TrieMap`].
    pub(crate) fn new(
        root: Option<&'tm Node<Data>>,
        prefix: impl Into<Vec<c_char>>,
        visit_descendants: F,
    ) -> Self {
        Self {
            stack: root.into_iter().map(|node| (node, false)).collect(),
            key: prefix.into(),
            filter: visit_descendants,
        }
    }

    /// Advance this iterator to the next node, and set the
    /// key to the one matching that node's entry
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let (node, was_visited) = self.stack.pop()?;

            if !was_visited {
                let data = node.data.as_ref();
                self.stack.push((node, true));
                self.key.extend(&node.label);

                let filter_outcome = self.filter.filter(&self.key);

                if filter_outcome.visit_descendants {
                    for child in node.children.0.iter().rev() {
                        self.stack.push((&child.node, false));
                    }
                }

                if filter_outcome.yield_current {
                    if let Some(data) = data {
                        return Some(data);
                    }
                }
            } else {
                self.key.truncate(self.key.len() - node.label.len());
            }
        }
    }

    /// Convert into a [`LendingIter`].
    pub(crate) fn into_lending_iter(self) -> LendingIter<'tm, Data, F> {
        LendingIter(self)
    }
}

impl<'tm, Data, F> Iterator for Iter<'tm, Data, F>
where
    F: TraversalFilter,
{
    type Item = (Vec<c_char>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}

/// A lending iterator over [`Node`]s. Allows for
/// obtaining the entries, yielding the reconstructed
/// key by reference.
/// Can be instantiated by calling [`crate::trie::TrieMap::lending_iter`] or
/// [`crate::trie::TrieMap::lending_iter_prefix`].
pub struct LendingIter<'tm, Data, F>(Iter<'tm, Data, F>);

impl<'a, Data, F> LendingIter<'a, Data, F>
where
    F: TraversalFilter,
{
    pub fn with_traversal_filter<F1>(self, f: F1) -> LendingIter<'a, Data, F1> {
        LendingIter(self.0.with_traversal_filter(f))
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
        Some((&self.0.key, item))
    }
}

/// An iterator over the values of [`Node`]s in
/// lexicographical order of the corresponding keys.
/// Makes no attempt to reconstruct the keys.
/// Can be instantiated by calling [`crate::trie::TrieMap::values`].
pub struct Values<'tm, Data> {
    stack: Vec<&'tm Node<Data>>,
}

impl<'tm, Data> Values<'tm, Data> {
    /// Create a new [`Values`] iterator.
    pub(crate) fn new(root: Option<&'tm Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
        }
    }
}

impl<'tm, Data> Iterator for Values<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;

        for child in node.children.0.iter().rev() {
            self.stack.push(&child.node);
        }

        if let Some(data) = node.data.as_ref() {
            return Some(data);
        }

        self.next()
    }
}

/// Consuming iterator over the values of a trie in lexicographical
/// order of the corresponding keys. Removes an entry each iteration.
/// Can be instantiated by calling [`crate::trie::TrieMap::into_values`].
pub struct IntoValues<Data> {
    stack: Vec<Node<Data>>,
}

impl<Data> IntoValues<Data> {
    pub(crate) fn new(root: Option<Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
        }
    }
}

impl<Data> Iterator for IntoValues<Data> {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        let mut node = self.stack.pop()?;

        for child in node.children.0.into_iter().rev() {
            self.stack.push(child.node);
        }

        if let Some(data) = node.data.take() {
            return Some(data);
        }

        self.next()
    }
}

#[cfg(test)]
mod test {
    #[cfg(not(miri))]
    proptest::proptest! {
        #[test]
        /// Test whether the [`super::Iter`] iterator yields the same results as the BTreeMap entries iterator.
        fn test_iter(entries: std::collections::BTreeMap<Vec<std::ffi::c_char>,i32> ) {
            let mut trie = crate::trie::TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<std::ffi::c_char>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();
            let btree_entries: Vec<(Vec<std::ffi::c_char>, i32)> = entries.iter().map(|(k, v)| (k.clone(), *v)).collect();

            assert_eq!(trie_entries, btree_entries);
        }

        #[test]
        /// Test whether the [`super::IntoValues`] iterator yields the same results as the BTreeMap values iterator.
        fn test_values(entries: std::collections::BTreeMap<Vec<std::ffi::c_char>,i32> ) {
            let mut trie = crate::trie::TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }

            let trie_values: Vec<i32> = trie.into_values().collect();
            let btree_values: Vec<i32> = entries.into_values().collect();

            assert_eq!(trie_values, btree_values);
        }
    }
}
