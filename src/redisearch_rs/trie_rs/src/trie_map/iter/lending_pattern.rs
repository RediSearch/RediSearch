/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Instant;

use super::{ContainsLendingIter, LendingIter, WildcardLendingIter, filter::VisitAll};
use crate::TrieMap;
use lending_iterator::prelude::*;
use rqe_wildcard::WildcardPattern;

/// How [`TrieMap::pattern_lending_iter`] matches each key against its pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternMode {
    /// Keys starting with the pattern.
    Prefix,
    /// Keys containing the pattern.
    Contains,
    /// Keys ending with the pattern.
    ///
    /// A trie is ordered by prefix, so this mode walks and tests every key
    /// however selective the pattern is. Callers that keep a suffix index
    /// should query it instead.
    Suffix,
    /// Keys matching the pattern as a [`WildcardPattern`].
    Wildcard,
}

/// Iterates over the entries of a [`TrieMap`] whose key matches
/// a pattern under a [`PatternMode`], in lexicographical order, borrowing the
/// current key from the iterator.
///
/// Each mode is served by a different underlying iterator, held by one variant
/// per [`PatternMode`] that needs a distinct traversal; this type lets a caller
/// pick the mode at runtime and still drive a single iterator type.
///
/// Invoke [`TrieMap::pattern_lending_iter`] to create a filtered instance, or
/// convert a [`LendingIter`] into one to walk every entry it yields.
///
/// The non-prefix modes keep matching against the pattern while they walk,
/// hence the separate `'pattern` borrow.
pub enum PatternLendingIter<'trie, 'pattern, Data> {
    /// Serves [`PatternMode::Prefix`] and the unfiltered walk obtained by
    /// converting a [`LendingIter`]: the prefix bounds the walk itself, so
    /// every entry it reaches is yielded.
    All(LendingIter<'trie, Data, VisitAll>),
    /// Serves [`PatternMode::Suffix`]: the trie offers no way to descend by
    /// suffix, so every entry is walked and only those whose key ends with
    /// the held pattern are yielded.
    Suffix(LendingIter<'trie, Data, VisitAll>, &'pattern [u8]),
    /// Serves [`PatternMode::Contains`].
    // Boxed because the substring searcher it embeds makes this variant far
    // larger than the others.
    Contains(Box<ContainsLendingIter<'trie, 'pattern, Data>>),
    /// Serves [`PatternMode::Wildcard`], matching keys against the pattern
    /// parsed as a [`WildcardPattern`].
    Wildcard(WildcardLendingIter<'trie, 'pattern, Data>),
}

impl<'trie, 'pattern, Data> PatternLendingIter<'trie, 'pattern, Data> {
    pub(crate) fn new(
        trie: &'trie TrieMap<Data>,
        pattern: &'pattern [u8],
        mode: PatternMode,
    ) -> Self {
        match mode {
            PatternMode::Prefix => Self::All(trie.prefixed_lending_iter(pattern)),
            PatternMode::Contains => Self::Contains(Box::new(trie.contains_iter(pattern).into())),
            PatternMode::Suffix => Self::Suffix(trie.lending_iter(), pattern),
            PatternMode::Wildcard => {
                Self::Wildcard(trie.wildcard_iter(WildcardPattern::parse(pattern)).into())
            }
        }
    }

    /// Set the deadline after which iteration stops, or clear it with `None`.
    ///
    /// The deadline is probed periodically during the traversal rather than on
    /// every step. Once it has passed, [`next`](LendingIterator::next) returns
    /// `None`, exactly as if the matching keys had run out.
    pub fn set_timeout(&mut self, timeout: Option<Instant>) {
        match self {
            Self::All(it) | Self::Suffix(it, _) => it.set_timeout(timeout),
            Self::Contains(it) => it.set_timeout(timeout),
            Self::Wildcard(it) => it.set_timeout(timeout),
        }
    }
}

impl<'trie, 'pattern, Data> From<LendingIter<'trie, Data, VisitAll>>
    for PatternLendingIter<'trie, 'pattern, Data>
{
    fn from(iter: LendingIter<'trie, Data, VisitAll>) -> Self {
        Self::All(iter)
    }
}

// See `LendingIter` for why this is a `LendingIterator` rather than an `Iterator`.
#[gat]
impl<'trie, 'pattern, Data> LendingIterator for PatternLendingIter<'trie, 'pattern, Data> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'trie Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self {
            Self::All(it) => it.next(),
            Self::Suffix(it, suffix) => {
                let suffix = *suffix;
                it.find(move |(k, _)| k.ends_with(suffix))
            }
            Self::Contains(it) => {
                let it: &mut ContainsLendingIter<'_, '_, Data> = it;
                it.next()
            }
            Self::Wildcard(it) => it.next(),
        }
    }
}
