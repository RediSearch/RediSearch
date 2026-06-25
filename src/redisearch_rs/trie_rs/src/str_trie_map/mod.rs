/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod iter;

use crate::TrieMap;
use std::fmt;

/// UTF-8 keyed view over [`TrieMap`].
///
/// Keys are stored as their UTF-8 byte representation inside the underlying
/// [`TrieMap`]; this wrapper enforces the UTF-8 invariant at the API boundary
/// so callers can work with `&str` and own `String` keys back from iterators.
///
/// Method semantics defer to the corresponding [`TrieMap`] method unless
/// noted otherwise on the wrapper method itself.
pub struct StrTrieMap<Data> {
    inner: TrieMap<Data>,
}

impl<Data> StrTrieMap<Data> {
    /// Create a new (empty) [`StrTrieMap`]. See [`TrieMap::new`].
    pub const fn new() -> Self {
        Self {
            inner: TrieMap::new(),
        }
    }

    /// Insert a `&str`-keyed entry. See [`TrieMap::insert`].
    pub fn insert(&mut self, key: &str, data: Data) -> Option<Data> {
        self.inner.insert(key.as_bytes(), data)
    }

    /// Insert or update a `&str`-keyed entry via a callback.
    /// See [`TrieMap::insert_with`].
    pub fn insert_with<F>(&mut self, key: &str, f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        self.inner.insert_with(key.as_bytes(), f);
    }

    /// Remove a `&str`-keyed entry. See [`TrieMap::remove`].
    pub fn remove(&mut self, key: &str) -> Option<Data> {
        self.inner.remove(key.as_bytes())
    }

    /// Get a reference to the value associated with `key`.
    /// See [`TrieMap::find`].
    pub fn get(&self, key: &str) -> Option<&Data> {
        self.inner.find(key.as_bytes())
    }

    /// Get a mutable reference to the value associated with `key`.
    /// See [`TrieMap::find_mut`].
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Data> {
        self.inner.find_mut(key.as_bytes())
    }

    /// The number of unique keys stored in this map.
    /// See [`TrieMap::n_unique_keys`].
    pub const fn len(&self) -> usize {
        self.inner.n_unique_keys()
    }

    /// `true` when no keys are stored.
    pub const fn is_empty(&self) -> bool {
        self.inner.n_unique_keys() == 0
    }

    /// Estimated heap memory currently held by this map. Mirrors the cached
    /// counter on the underlying [`TrieMap`] — O(1). See [`TrieMap::mem_usage`].
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Iterate over all entries in lexicographical key order. See [`TrieMap::iter`].
    ///
    /// Yields `(String, &Data)` — keys are decoded back into owned `String`s
    /// at the wrapper boundary (the inner trie yields `Vec<u8>`).
    pub fn iter(&self) -> iter::Iter<'_, Data> {
        iter::Iter::new(&self.inner)
    }

    /// Yield every entry whose key starts with `prefix`, in lexicographical
    /// order. See [`TrieMap::prefixed_iter`].
    ///
    /// Byte-prefix matching is codepoint-safe because UTF-8 codepoint
    /// boundaries align with byte boundaries. Empty `prefix` yields zero
    /// matches (this differs from the inner method, which would yield all
    /// entries).
    pub fn prefixed_iter(&self, prefix: &str) -> iter::PrefixedIter<'_, Data> {
        iter::PrefixedIter::new(&self.inner, prefix)
    }

    /// Yield every entry whose key ends with `suffix`. Filters by byte
    /// `ends_with` — correct because UTF-8 is self-synchronizing (a
    /// multibyte sequence cannot be a suffix of another codepoint). Empty
    /// `suffix` yields zero matches.
    pub fn suffixed_iter(&self, suffix: &str) -> iter::SuffixedIter<'_, Data> {
        iter::SuffixedIter::new(&self.inner, suffix)
    }

    /// Yield every entry whose key contains `target` as a substring.
    /// Empty `target` yields zero matches — without this short-circuit
    /// memchr semantics would match every term.
    pub fn contains_iter<'tm, 'p>(&'tm self, target: &'p str) -> iter::ContainsIter<'tm, 'p, Data> {
        iter::ContainsIter::new(&self.inner, target)
    }

    /// Iterate over entries with keys inside `filter`, in lexicographical
    /// order. See [`TrieMap::range_iter`].
    pub fn range_iter<'tm, 'p>(
        &'tm self,
        filter: iter::RangeFilter<'p>,
    ) -> iter::RangeIter<'tm, 'p, Data> {
        iter::RangeIter::build_from(&self.inner, filter)
    }

    /// Wildcard iteration over UTF-8 keys with codepoint-aware semantics.
    ///
    /// Differs from [`TrieMap::wildcard_iter`], which matches pattern tokens
    /// against raw bytes — here `?` matches one codepoint, not one byte.
    pub fn wildcard_iter<'tm>(
        &'tm self,
        _pattern: &str,
    ) -> impl Iterator<Item = (String, &'tm Data)> + 'tm {
        todo!("UTF-8 wildcard iteration over StrTrieMap not yet implemented");
        #[expect(unreachable_code)]
        std::iter::empty()
    }
}

impl<Data> Default for StrTrieMap<Data> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Data: fmt::Debug> fmt::Debug for StrTrieMap<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}
